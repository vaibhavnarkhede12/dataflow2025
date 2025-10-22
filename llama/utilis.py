import json
import logging
# import pg8000 # Appears commented out
import threading
import time
import psycopg2
from psycopg2 import pool # Inferred to support SimpleConnectionPool
import re
import datetime
from datetime import datetime
import hashlib
import pytz # Inferred from process_utc_convert
from contextlib import closing
from rule_engine import rule_engine
# Assuming these are custom imports
from core.io.cloud_sql_io import EventStoreMetadata
from .event_metadata import EventMetadata

# Constants
TRANSFORM = "transform"
COLUMN = "column"
# Assuming PROCESSING_TIME_METADATA is defined elsewhere (e.g., global or passed in)
# PROCESSING_TIME_METADATA = {'EventStoreMetadata.INTERNAL_EVENTID': 'internalEventId'} 


class RuleUtils:
    """
    A collection of static methods for data transformation, filtering,
    and database interaction within a rule-based processing engine.
    """

    # ------------------------------------------------------------------------
    ## Filtering Methods
    # ------------------------------------------------------------------------

    @staticmethod
    def rule_filter(record, rules):
        """Filters rules based on the 'subscription' in the record."""
        sub_name = record.get('subscription', None)
        if sub_name:
            return rules['subscriptions'].get(sub_name, [])
        return []

    # modify for multi event_name
    @staticmethod
    def rule_filter_record(record, rules):
        """Filters rules based on the subscription defined in EventMetadata."""
        sub_name = record.get(EventMetadata.SUBSCRIPTION, None)
        if sub_name:
            rule_config = rules['subscriptions'].get(sub_name, {})
            return rule_config.get('rules', [])
        # # end
        return []

    @staticmethod
    def filtered(element, filter_rule_texts, filter_rules):
        """Checks if an element is filtered out by any rule in filter_rule_texts."""
        if filter_rule_texts:
            for filter_rule in filter_rule_texts:
                if not filter_rules[filter_rule].matches(element):
                    logging.info(f"filter out the element ({element}) because of rule ({filter_rule})")
                    return True
        return False

    # ------------------------------------------------------------------------
    ## Connection Management
    # ------------------------------------------------------------------------

    @staticmethod
    def reset_connection_pool(conn_pool, ds_config, datasources):
        """Closes all connections in the pool and creates a new SimpleConnectionPool."""
        try:
            logging.info("Closing all connection....")
            conn_pool.closeall()
        except Exception as e:
            logging.warning(f"Error while closing all connection before resetting the connection pool: {e}")

        ds_confs = ds_config.get(datasources)
        logging.info(f"Connecting to connection pool for {datasources}")
        new_pool = pool.SimpleConnectionPool(
            minconn = ds_confs.get('min_conns', 1),
            maxconn = ds_confs.get('max_conns', 1),
            user = ds_confs.get('user'),
            password = ds_confs.get('password'),
            host = ds_confs.get('host'),
            port = ds_confs.get('port'),
            database = ds_confs.get('database')
        )
        return new_pool

    @staticmethod
    def get_params(where_columns, element):
        """Populates parameters for the database query."""
        params = {}
        for where_col in where_columns:
            # Assumes mapping column is either the key or defined in 'mapping_column'
            mapping_column = where_col.get("mapping_column", where_col[COLUMN])
            params[where_col[COLUMN]] = element.get(mapping_column)
        return params

    @staticmethod
    def get_conn(pool, datasources, ds_confs, debug):
        """Retrieves a connection from the pool, resetting the pool if necessary."""
        connection = None
        wait_time = 0
        while connection is None:
            if wait_time >= 60:
                raise Exception("Max connection waiting time exceeded")
            
            time.sleep(1)
            wait_time += 1
            
            try:
                # pool[datasources].getconn() is implicit
                connection = pool[datasources].getconn()
            except Exception as e:
                # Below code is added to check the connection status and reset the pool
                logging.info(f"[Cloud SQL] Connection pool is closed or inactive: {e}")
                pool[datasources] = RuleUtils.reset_connection_pool(pool[datasources], ds_confs, datasources)

                # Try getting connection again
                try:
                    connection = pool[datasources].getconn()
                except Exception as e:
                    logging.warning(f"Error occurred while resetting connection pool: {e}")
                    connection = None # Ensure connection is None if reset failed
        return connection


    @staticmethod
    def process_add_cloudsql_data(element: dict, enrichCloudSQL, conns_ds_config, pool, debug=False):
        """Selects one row from the database to enrich the element using a connection pool."""
        enrichments = enrichCloudSQL.get("enrichments", {})
        ds_name = enrichCloudSQL.get('datasource', None)
        where_columns = enrichCloudSQL.get('where_columns', None)
        projections = enrichCloudSQL.get('projections', ['*']) # Inferred default of ['*']

        if ds_name is not None and where_columns and projections:
            
            # 1. Generate SQL
            where_strings = [f"{col[COLUMN]} = %({col[COLUMN]})s" for col in where_columns]
            where_clause = " AND ".join(where_strings)
            sql = f"SELECT {', '.join(projections)} FROM {ds_name} WHERE {where_clause} limit 1"
            
            # 2. Get parameters
            params = RuleUtils.get_params(where_columns, element)

            # 3. Get connection and execute query
            connection = None
            result = None
            try:
                # Assumes get_conn and related logic handles connection pooling
                connection = RuleUtils.get_conn(pool, ds_name, conns_ds_config, debug)
                
                with closing(connection.cursor()) as cur:
                    # execute query
                    cur.execute(sql, params)
                    result = cur.fetchone()
                    
                pool[ds_name].putconn(connection) # Put connection back to pool

            except Exception as e:
                # try: logging.error(f"Database error: {e}")
                logging.error(f"Database error: {e}")
                if connection:
                    pool[ds_name].putconn(connection)
                # except Exception as ee:
                # logging.error(f"Putting connection error: {ee}")
                result = None

            # 4. Process result
            if debug and result:
                 logging.info(f"[RuleUtils.process_add_cloudsql_data] query result (result) from query '{sql}' with params {params}")

            if result and enrichments:
                for enrichment in enrichments:
                    output_col = enrichment.get('output_column', None)
                    value_col = enrichment.get('value_column', None)
                    default_value = enrichment.get('default_value')

                    try:
                        result_index = projections.index(value_col)
                    except ValueError:
                        continue # Value column not in projections

                    if result[result_index] is not None:
                        element[output_col] = result[result_index]
                    else:
                        # # if column is None or Null, assign default value
                        if default_value is not None:
                            element[output_col] = default_value
                        # # else, if value column resides in db and is null, assign default value, if no default_value specified, then assign None
                        else:
                            element[output_col] = None

        return element

    # ------------------------------------------------------------------------
    ## Data Transformation Methods
    # ------------------------------------------------------------------------

    @staticmethod
    def process_projection(element, projection, PROCESSING_TIME_METADATA):
        """Filters element keys based on 'include' or 'exclude' projection type."""
        projection_type = projection.get('projection_type')
        columns = projection['columns']

        if projection_type.lower() == 'exclude':
            for col in columns:
                # ########## check column exists and is not a required column
                if col in element:
                    # ########## remove the column
                    del element[col]
            return element
        else:
            # ########## new dict with just the columns we want to include (plus required)
            new_elem = {}
            # ########## below one liner equivalent
            # ########## for key in columns:
            # ##########     if key in element:
            # ##########         print(f"key: {element[key]}")

            columns_to_include = columns + [PROCESSING_TIME_METADATA['EventStoreMetadata.INTERNAL_EVENTID']]

            # ########## Apply regex check
            reg_comp = re.compile('projection\\d+')

            new_elem = {}
            for key in columns_to_include:
                if key in element:
                    # ########## Skip regex check (Implied logic is complex/ambiguous, sticking to inclusion)
                    # if not reg_comp.match(str(element.get(key))):
                    #     new_elem[key] = element[key]
                    new_elem[key] = element[key]

            return new_elem

    @staticmethod
    def process_copy_attribute(element, transform):
        """Copies value from input_column to output_column."""
        # # get transform config
        input_column = transform.get('input_column', None)
        output_column = transform.get('output_column', None)
        delete_input = transform.get('delete_input', False)

        # # get input value if available
        value = element.get(input_column)

        if value is not None:
            # # add attribute
            element[output_column] = value

        if delete_input is True and input_column in element:
            # # if delete is true then remove input attribute
            del element[input_column]

        return element

    @staticmethod
    def process_add_hash_guid(element, transform):
        """Generates an SHA256 hash from input_columns and sets it to output_column."""
        output_column = transform.get('output_column', None)
        input_values = []
        for col in transform.get('input_columns'):
            # # for col in element: (Partial line comment)
            value = element.get(col, None)
            if value is not None:
                input_values.append(str(value)) # # if value, append str(value)

        if input_values:
            input_formatted = '|'.join(input_values)
            hashguid = hashlib.sha256(input_formatted.encode()).hexdigest()
            element[output_column] = hashguid

        return element

    @staticmethod
    def process_null_replace(element, transform):
        """Replaces None values in a column with a specified replacement."""
        column = transform.get(COLUMN, None)
        replacement = transform.get('replacement', None)

        if column is not None and replacement is not None:
            if element.get(column) is None:
                # # if element[column] is None:
                element[column] = replacement

        return element

    @staticmethod
    def process_concat(element, transform):
        """Concatenates values from a list of 'fields' into 'output_column'."""
        output_column = transform.get('output_column', None)
        fields_list = transform.get('fields', [])

        out_str = ""
        delete_input = transform.get('delete_input', False)

        for field in fields_list:
            if element.get(field, None) is not None:
                out_str += str(element[field])
            # else: # (Ambiguous line in snippet 1000318645, omitted for clarity)
            #     out_str = out_str + str(element[field])

        # # delete input
        if delete_input:
            for field in fields_list:
                if element.get(field) is not None:
                    del element[field]

        element[output_column] = out_str
        return element

    @staticmethod
    def process_data_format(element: dict, transform):
        """Formats a date/time string in a column to a new format."""
        column = transform.get(COLUMN)
        current_format = transform.get('current_format', None)
        new_format = transform.get('new_format', None)

        if column and current_format and new_format:
            value = element.get(column, None)

            if value:
                current_date = datetime.strptime(value, current_format)
                element[column] = current_date.strftime(new_format)

        return element

    @staticmethod
    def process_regex_sub(element, transform):
        """Performs a regular expression substitution on a column value."""
        column = transform.get(COLUMN, None)
        pattern = transform.get('pattern', None)
        replacement = transform.get('replacement', None)

        if column and pattern and replacement:
            if element.get(column) is not None:

                # # if type of column value is integer, need transform value type to string, after regex transform back to integer (elin 2022.06.29)
                if isinstance(element.get(column), int):
                    value_str = str(element.get(column))
                    element[column] = int(re.sub(pattern, replacement, value_str))
                else:
                    element[column] = re.sub(pattern, replacement, element.get(column))

        return element

    @staticmethod
    def process_regex_replace(element, transform):
        """Performs multiple regex replacements based on 'replacements' config."""
        column = transform.get(COLUMN, None)
        replacements = transform.get('replacements', {})

        if column and element.get(column) is not None:
            value = str(element[column])
            for exist_char, replacement in replacements.items():
                element[column] = value.replace(exist_char, replacement)

        return element

    @staticmethod
    def process_padding(element, transform):
        """Pads a string value to a minimum length."""
        column = transform.get(COLUMN, None)
        number = transform.get('number', None)
        replacement_char = transform.get('replacement_char', '0')
        attribute = transform.get('attribute', 'left') # 'left' or 'right'

        if column and number and isinstance(number, int) and element.get(column) is not None:
            value = str(element.get(column))
            num_to_pad = number

            if attribute == "right":
                element[column] = value.ljust(num_to_pad, replacement_char)
            else: # Defaults to "left" padding
                # # R just and L just in python need to be revert to be close to users expectation
                element[column] = value.rjust(num_to_pad, replacement_char)

        return element

    @staticmethod
    def process_currency_format(element, transform):
        """Formats a number field, specifically handling VND currency."""
        currency_name = transform.get("currency_name_field")
        number_field = transform.get("number_field")
        output_column = transform.get("output_column")

        if element.get(currency_name) and element.get(currency_name).upper() == "VND":
            if element.get(number_field) is not None:
                number_val = str(element[number_field])
                # Remove VND/unit and convert to float
                number = float(number_val.strip().replace(" VND", "").replace(",", "")) # Added replace(",", "") for safety
                out = "%.0f" % number # Format as integer string
                element[output_column] = out
                return element
        else:
            # Original logic for non-VND or missing currency
            if element.get(number_field) is not None:
                out = element[number_field]
                element[output_column] = out
                return element

        return element

    @staticmethod
    def process_post_filter(element, transform, post_filtered=None):
        """Applies a single rule from the transform to filter the element."""
        if element is None:
            return None

        # rule_engine.Rule assumed to be available
        rule_filter = rule_engine.Rule(transform.get("rule"))

        if rule_filter.matches(element):
            return element
        else:
            if post_filtered:
                post_filtered.inc()
            return None # Filtered out

    @staticmethod
    def process_switch_case(element, transform):
        """Applies a switch/case logic based on input column value."""
        # # get input column name
        input_column = transform.get("input_column") # # input field name
        input_column_val = element.get(input_column) # # value of input
        output_column = transform.get("output_column")
        case_value = transform.get("case_value") # # all switch cases
        default_column = transform.get("default") # # output value of the switch case

        if input_column_val is None: # # If output is not empty
            case_val_output = case_value.get(input_column_val)
            
            if case_val_output is not None: # # first try get value from message
                out_value = case_val_output
            else:
                out_value = case_value # # second get value from switch cases (Ambiguous line)

            element[output_column] = out_value
            
        elif default_column: # #elif default_column:
            if element.get(default_column, None): # # First key get value from default column
                out_value = element.get(default_column)
                element[output_column] = out_value
            # else: # Missing or implied else

        return element

    @staticmethod
    def evaluate_condition(row, cond):
        """Evaluates a single condition using a match dictionary."""
        val = row.get(cond["column"])
        op = cond["op"]
        match_val = cond["reference_value"]

        op_map = {
            "==": lambda val, match_val: str(val) == str(match_val),
            "in": lambda val, match_val: str(val) in str(match_val),
            "notIn": lambda val, match_val: str(val) not in str(match_val),
            "startsWith": lambda val, match_val: str(val).startswith(str(match_val)),
            "notStartsWith": lambda val, match_val: not str(val).startswith(str(match_val)),
            "endsWith": lambda val, match_val: str(val).endswith(str(match_val)),
            "notEndsWith": lambda val, match_val: not str(val).endswith(str(match_val)),
            "gt": lambda val, match_val: float(val) > float(match_val),
            "ge": lambda val, match_val: float(val) >= float(match_val),
            "lt": lambda val, match_val: float(val) < float(match_val),
            "le": lambda val, match_val: float(val) <= float(match_val),
            "str": lambda val, match_val: str(val) == str(match_val), # Corrected interpretation of 'str'
            "float": lambda val, match_val: float(val) == float(match_val),
        }

        try:
            return op_map[op](val, match_val)
        except Exception as err:
            logging.error(f"Error eval