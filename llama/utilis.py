import json
import logging
#import pg8000 # This import is commented out, kept as a placeholder
import threading
import time
import psycopg2
import re
from datetime import datetime
#from datetime import datetime # Already imported above, this is likely a commented-out duplicate
from contextlib import closing
import pytz

from rule_engine import rule_engine
from core.io.cloud_sql_io import EventStoreMetadata
from .io.event_metadata import EventMetadata

TRANSFORM = "transform"
COLUMN = "column"

class RuleUtils:

    @staticmethod
    def rule_filter(record, rules):
        sub_name = record.get('subscription', None)
        if sub_name:
            return rules['subscriptions'].get(sub_name, [])
        return []

    # modify for multi event_name
    @staticmethod
    def rule_filter_record(record, rules):
        sub_name = record.get(EventMetadata.SUBSCRIPTION, None)
        if sub_name:
            rule_config = rules['subscriptions'].get(sub_name, {})
            return rule_config.get("rules", [])
        return []
    # end

    @staticmethod
    def filtered(element, filter_rule_texts, filter_rule_files):
        if filter_rule_texts:
            for filter_rule in filter_rule_texts:
                if not filter_rule_files[filter_rule].matches(element):
                    logging.info(f"Filter out the element ({element}) because of rule ({filter_rule})")
                    return True
        return False

    @staticmethod
    def process_projection(element, projection, PROCESSING_TIME_METADATA):
        projection_type = projection.get('projection_type')
        columns = projection.get('columns')

        if projection_type.lower() == "exclude":
            for col in columns:
                # check column exists and is not a required param
                if col in element and col not in (
                    columns + [EventStoreMetadata.INTERNAL_EVENTID] + [PROCESSING_TIME_METADATA.COLUMN]
                ): # The original line is broken, this is a guess at the full list
                    # Remove the column
                    del element[col]
            return element
        else:
            # new dict with just the columns we want to include (plus required)

            # below one liner equivalent
            # for key in columns:
            #     if key in element:
            #         print(f"{key}: {element[key]}")

            columns = projection.get('columns') + [PROCESSING_TIME_METADATA.COLUMN] + [EventStoreMetadata.INTERNAL_EVENTID]
            new_elem = {}

            # Apply regex check
            regex_check = re.compile(projection['regex_check']) # assuming 'regex_check' is a key in projection
            
            for key in element:
                if key in columns or regex_check.match(str(element[key])):
                    new_elem[key] = element[key]
            
            # Skip regex check
            else:
                new_elem = {key: element[key] for key in columns if key in element}
            
            return new_elem


    @staticmethod
    def transform_processes(element, transforms, thread_id, conns, ds_config, pool, PROCESSING_TIME_METADATA, debug=False):
        # def transform_processes(element, transforms, conns, ds_config, debug=False): # Alternative commented-out signature
        
        # element = copy.deepcopy(element) # Takes copy of element

        for transform in transforms:
            # for transform in transforms: # This is a confusing comment structure, likely referring to the inner loop
            # for each transformation, we apply to the element
            transform_type = transform.get(TRANSFORM)
            
            if transform_type == "addHashGuid":
                cloned_element = RuleUtils.process_add_hash_guid(element, transform)
            elif transform_type == "copy_attribute":
                cloned_element = RuleUtils.process_copy_attribute(element, transform)
            elif transform_type == "projection":
                cloned_element = RuleUtils.process_projection(element, transform, PROCESSING_TIME_METADATA)
            elif transform_type == "addStaticData":
                cloned_element = RuleUtils.process_add_static_data(cloned_element, transform)
            elif transform_type == "enrichCloudSQL":
                cloned_element = RuleUtils.process_add_cloudsql_data(element, conns, thread_id, transform, ds_config, pool, debug)
            elif transform_type == "padding":
                cloned_element = RuleUtils.process_padding(cloned_element, transform)
            elif transform_type == "regex_sub":
                cloned_element = RuleUtils.process_regex_sub(cloned_element, transform)
            elif transform_type == "regex_extract":
                # transform_type = "regex_extract" is not fully visible/handled in elif chain
                # The line: elif transform_type == "regex_extract": cloned_element = RuleUtils.process_regex_extract(cloned_element, transform) is likely missing.
                pass 
            elif transform_type == "date_format":
                cloned_element = RuleUtils.process_date_format(cloned_element, transform)
            elif transform_type == "replace":
                cloned_element = RuleUtils.process_replace(cloned_element, transform)
            elif transform_type == "arithmetic":
                cloned_element = RuleUtils.process_arithmetic(cloned_element, transform)
            elif transform_type == "concatenation":
                cloned_element = RuleUtils.process_concatenation(cloned_element, transform)
            elif transform_type == "data_type":
                cloned_element = RuleUtils.process_data_type(cloned_element, transform)
            elif transform_type == "null_replace":
                cloned_element = RuleUtils.process_null_replace(cloned_element, transform)
            elif transform_type == "concat":
                cloned_element = RuleUtils.process_concat(cloned_element, transform)
            elif transform_type == "currency_format":
                cloned_element = RuleUtils.process_currency_format(cloned_element, transform)
                # TODO dynamic logging keyword
            elif transform_type == "post_filter":
                cloned_element = RuleUtils.process_post_filter(cloned_element, transform)
            elif transform_type == "switch_case":
                cloned_element = RuleUtils.process_switch_case(cloned_element, transform)
            elif transform_type == "multiple_condition":
                cloned_element = RuleUtils.process_multiple_condition(cloned_element, transform)
            elif transform_type == "utc_convert":
                cloned_element = RuleUtils.process_utc_convert(cloned_element, transform)
            elif transform_type == "current_time":
                cloned_element = RuleUtils.process_current_time(cloned_element, transform)
            elif transform_type == "end_time":
                cloned_element = RuleUtils.process_end_time(cloned_element, transform)
            # Lines 129-138 show more transformation types, but the full method signature in image 1000318642.jpg
            # is cut off, so the elif chain continues but is not fully captured:
            # ...
            # elif transform_type == "openbank_extract_sapi_format":
            #     cloned_element = RuleUtils.process_openbank_extract_sapi_format(cloned_element, transform)
            # elif transform_type == "openbank_add_currency":
            #     cloned_element = RuleUtils.process_openbank_add_currency(cloned_element, transform)
            # elif transform_type == "openbank_add_currency_rate_enrichment":
            #     cloned_element = RuleUtils.process_openbank_add_currency_rate_enrichment(cloned_element, transform)
            # elif transform_type == "openbank_convert_currency":
            #     cloned_element = RuleUtils.process_openbank_convert_currency(cloned_element, transform)
            # elif transform_type == "openbank_add_derived_fields":
            #     cloned_element = RuleUtils.process_openbank_add_derived_fields(cloned_element, transform)
            # elif transform_type == "openbank_format_for_sapi":
            #     cloned_element = RuleUtils.process_openbank_format_for_sapi(cloned_element, transform)
            #
            # The structure for `current_time` and `end_time` also appear in a separate, possibly older/modified block:
            # elif transform_type == "current_time":
            #     cloned_element = RuleUtils.process_current_time(cloned_element, transform)
            # elif transform_type == "end_time":
            #     cloned_element = RuleUtils.process_end_time(cloned_element, transform)
            # elif transform_type == "multi_column_conditional_operation":
            #     cloned_element = RuleUtils.process_multi_column_conditional_operation(cloned_element, transform)

            # Error handling block from line 170
            try:
                # ... inside the main transform loop, the above elifs are executed ...
                pass
            except Exception as e:
                # An error occurred while doing transformation, error message: ({e})"
                logging.warning(f"[element[EventStoreMetadata.INTERNAL_EVENTID]] An error occurred while doing transformation, error message: ({e})")
                return element, "transform_error", e

        return element, "transform_success", None


    @staticmethod
    def process_copy_attribute(element, transform):
        # Get transform config
        input_column = transform.get("input_column", None)
        output_column = transform.get("output_column", None)
        delete_input = transform.get("delete_input", False)

        # Get element value if available
        column_value = element.get(input_column, None)

        if column_value is not None:
            # Set attribute
            element[output_column] = column_value

        if delete_input is True and input_column in element:
            # If delete is true then remove input attribute
            del element[input_column]

        return element

    @staticmethod
    def process_add_hash_guid(element, transform):
        input_columns = transform.get("input_columns", [])
        output_column = transform.get("output_column", None)
        input_values = []

        for col in input_columns:
            element_value = element.get(col, None)
            if element_value is not None:
                input_values.append(str(element_value))

        if input_values:
            input_formatted = "|".join(input_values)
            hash_guid = hashlib.sha256(input_formatted.encode()).hexdigest()
            element[output_column] = hash_guid

        return element

    @staticmethod
    def process_null_replace(element, transform):
        column = transform.get(COLUMN, None)
        replacement = transform.get("replacement", None)
        
        if column is not None and replacement is not None:
            if element.get(column) is None:
                element[column] = replacement
        
        return element

    @staticmethod
    def process_concat(element, transform):
        output_column = transform.get("output_column")
        fields_list = transform.get("fields")
        out_str = ""

        for field in fields_list:
            if field.get("field", None) is not None:
                # field is an element attribute
                out_str += str(element.get(field["field"]))
            else:
                # field is a static string
                out_str += str(field)

        delete_input = transform.get("delete_input", False)

        if delete_input:
            for field in fields_list:
                if element.get(field) is not None:
                    del element[field]

        element[output_column] = out_str
        return element

    @staticmethod
    def process_date_format(element, transform):
        column = transform.get(COLUMN, None)
        current_format = transform.get("format", None)
        new_format = transform.get("new_format", None)
        
        column_value = element.get(column, None)

        if column_value:
            current_date = datetime.strptime(column_value, current_format)
            value = current_date.strftime(new_format)
            element[column] = value
        
        return element

    @staticmethod
    def process_regex_sub(element, transform):
        column = transform.get(COLUMN, None)
        pattern = transform.get("pattern", None)
        replacement = transform.get("replacement", None)
        
        if column and pattern and replacement:
            element_value = element.get(column, None)
            
            if element_value is not None:
                if isinstance(element_value, int):
                    # For int type, convert to string, do regex, then convert back to int (if possible)
                    value_str = str(element_value)
                    value_str = re.sub(pattern, replacement, value_str)
                    
                    try:
                        element[column] = int(value_str)
                    except ValueError:
                        element[column] = value_str # Fallback to string if conversion fails
                else:
                    element[column] = re.sub(pattern, replacement, str(element_value))
        
        return element

    @staticmethod
    def process_replace(element, transform):
        column = transform.get(COLUMN, None)
        replacements = transform.get("replacements", {})
        
        if column in element:
            exist_char = str(element[column])
            if exist_char in replacements:
                element[column] = replacements[exist_char]
        
        return element

    @staticmethod
    def process_padding(element, transform):
        column = transform.get(COLUMN, None)
        number = transform.get("number", None)
        replacement_char = transform.get("replacement_char", "0")
        
        if column and number:
            attribute_value = element.get(column, None)
            
            if attribute_value is not None:
                # Convert to string
                str_value = str(attribute_value)

                if isinstance(number, int):
                    # rjust will do padding to the right
                    # LJUST will do padding to the left
                    # rjust and ljust in python need to be revert to be close to users expectation
                    element[column] = str_value.rjust(number, replacement_char) # Based on comments, this seems to be the intended behavior

        return element

    @staticmethod
    def reset_connection_pool(conn_pool, ds_config, conn_type='all'):
        try:
            logging.info(f"Closing all connections....")
            conn_pool.closeall()
        except Exception as e:
            logging.warning(f"Error while closing all connection before resetting the connection pool: {e}")

        # The connection pool is recreated here.
        ds_confs_pool = ds_config.get(ds_config.get("datasource"), None)
        
        # This section is cut off, but appears to set up a new connection pool
        # This is a reconstruction of what is likely happening:
        # if ds_confs_pool:
        #     logging.info(f"Creating new connection pool for {ds_config.get('datasource')}")
        #     # Example of a new pool creation:
        #     # conn_pool = psycopg2.pool.SimpleConnectionPool(1, 20, 
        #     #     user=ds_confs_pool.get('user'), 
        #     #     password=ds_confs_pool.get('password'), 
        #     #     host=ds_confs_pool.get('host'), 
        #     #     port=ds_confs_pool.get('port', 5432), 
        #     #     database=ds_confs_pool.get('database')
        #     # )
        #     return conn_pool
        # else:
        #     return None # Return new pool or None

        return "newpool" # placeholder, the actual code is cut off

    @staticmethod
    def access_add_cloudsql_data(element, enrichCloudSQL, conns, ds_confs, debug=False):
        # Select one row from database to enrich the element using cached connection
        
        # Parameters: dict for element, transform for rule files, enrichCloudSQL for enrichment config,
        # conns for connection dictionary, ds_confs for datasource config.

        enrichments = enrichCloudSQL.get("enrichments", {})
        rule_file = enrichCloudSQL.get("rule_file", None)
        cached_connection_dictionary = enrichCloudSQL.get("cached_connection_dictionary", {})
        
        projections = enrichCloudSQL.get('projections', ['*'])
        where_columns = enrichCloudSQL.get('where_columns', [])

        # The rest of the function definition is cut off, but the logic likely involves:
        # 1. Generating the SQL query based on projections and where_columns.
        # 2. Executing the query using connection pool from `conns`.
        # 3. Handling query results (e.g., getting the first row).
        # 4. Enriching the `element` with the result data.

        # The logic for SQL query generation is visible in another helper function:

        # def generate_sql(projections, table, where_columns, limit=1):
        #     select_clause = ", ".join(projections)
        #     where_clause = " and ".join([f"{col} = %s" for col in where_columns])
        #     return f"SELECT {select_clause} FROM {table} WHERE {where_clause} LIMIT {limit}"
        
        # ... logic continues, but is not fully visible ...
        
        # Visible lines:
        if debug:
            logging.info(f"RuleUtils process_add_cloudsql_data debug result: {result} from query \"{sql}\" with params {params}")
        
        # This part of the code shows how the element is enriched:
        # for projection in projections:
        #     default_value = enrichCloudSQL.get("default_value", None)
        #     index = projections.index(projection) # Assuming simple list projection
        #     value = result[index] # Assuming result is a list/tuple
        #     element[projection] = value if value is not None else default_value
        
        # Visible enrichment logic (lines 430-438):
        if result:
            result_index = result[0] # Assuming single row result
            for enrichment in enrichments:
                column = enrichment.get("column")
                default_value = enrichment.get("default_value")
                
                # Assuming result_index is a dictionary or has an indexable structure
                # The line element[column] = result_index[enrichment.get("value_column")] is likely what happens
                
                element[column] = result_index[0] # Placeholder for the actual column value retrieval

        # Final return logic is:
        # return element


    # Helper function to get SQL, used inside access_add_cloudsql_data
    @staticmethod
    def _get_sql_for_enrichment(projections, table, where_columns):
        select_strings = ", ".join([f'"{p}"' for p in projections])
        where_strings = " AND ".join([f'"{col}"=%s' for col in where_columns])

        # This part of the code is cut off but is likely a helper for SQL generation
        # sql = f"SELECT {select_strings} FROM {table} WHERE {where_strings} LIMIT 1"

        return "sql" # Placeholder

    @staticmethod
    def _get_sql_add_static_data(element, add_static_data, rule_file):
        enrichments = add_static_data.get("enrichments", None)
        
        if enrichments:
            for enrichment in enrichments:
                column = enrichment.get("column", None)
                value = enrichment.get("value") # This is a static value, not a SQL query

        # The function seems to be misnamed or doing something non-SQL related for static data
        # Based on its name and context (line 467-474) it is likely for *adding* static values, not getting SQL
        
        # The true intent is probably to iterate and add key/value pairs:
        # element[column] = value 
        
        return element # The return is element, suggesting it modifies the element directly

    # Function visible in 1000318648.jpg, which executes a query
    # The name is missing, but it handles a connection pool and query execution
    def _execute_query_with_pool_and_wait(params, sql_query): 
        # ... function body is complex, involves getting connection, executing query, and handling wait/reset ...
        
        # Connection handling with a wait loop (lines 379-397):
        connection = None
        # WAIT_TIME = XX # Not visible
        wait_time = 0 
        
        # Loop to get connection
        # while connection is None and wait_time < MAX_WAIT_TIME: # MAX_WAIT_TIME not visible
        try:
            connection = pool.getconn(datasource)
        except Exception as e:
            # Handle connection pool error
            pass
        
        # This block shows connection status check and reset if needed
        # if connection is None or connection.closed:
        #     RuleUtils.reset_connection_pool(...)
            
        
        # Query execution block (lines 370-385, 401-405):
        try:
            with connection.cursor() as cur:
                # cur.execute(sql, params) # Execute query
                result = cur.fetchall() # Fetch results
            
            # Commit the transaction for non-SELECT queries
            # connection.commit()
            
            return result
        except Exception as e:
            # Handle query execution error
            pass
        finally:
            # Always return connection to the pool
            # pool.putconn(connection)
            pass

        return None # Return result or None

# Assuming this is a continuation of the RuleUtils class
# from the previous set of images.

# ... (Previous RuleUtils methods: transform_processes, process_projection, etc.)

    @staticmethod
    def process_arithmetic(element, transform):
        # A full view of this function is not available in the snippets, 
        # but the purpose is to perform arithmetic operations.
        # This reconstruction is based on the visible lines 461-475 (Image 1000318650.jpg)
        
        output_column = transform.get("output_column")
        delete_input = transform.get("delete_input", False)
        
        # variables_dict for storing operands/variables
        variables_dict = {}

        # Loop through variables/operands (par)
        for par in transform.get("variables"): # Assuming 'variables' key exists in transform
            variables_dict[par] = float(element.get(par)) # Convert to float for calculation

        # Use eval for the expression, which is risky but common in dynamic configs
        # Assuming 'expression' key exists in transform
        out = eval(transform.get("expression"), {"__builtins__": None}, variables_dict)
        
        if delete_input:
            for col in transform.get("variables"):
                if col in element:
                    del element[col]
                    
        element[output_column] = str(out)
        return element

    @staticmethod
    def process_concatenation(element, transform):
        output_column = transform.get("output_column")
        fields_list = transform.get("fields")
        out_str = ""

        for field in fields_list:
            if element.get(field, None) is not None:
                # field is an element attribute
                out_str = out_str + str(element[field])
            else:
                # field is a static string
                out_str = out_str + str(field)

        element[output_column] = out_str
        return element

    @staticmethod
    def process_data_type(element, transform):
        input_column = transform.get("input_column")
        output_column = transform.get("output_column")
        target_data_type = transform.get("output_data_type").lower()

        out = None

        if target_data_type == "int":
            try:
                out = int(element[input_column])
            except (ValueError, TypeError):
                out = None # Handle conversion error
        elif target_data_type == "float":
            try:
                out = float(element[input_column])
            except (ValueError, TypeError):
                out = None
        elif target_data_type == "str":
            out = str(element[input_column])
        
        # ... other data types likely handled here ...
        
        # Simplified final block (lines 506-511):
        elif target_data_type == "str":
            out = str(element[input_column]) # Appears to be a duplicate/re-check
        
        element[output_column] = out
        return element

    @staticmethod
    def process_currency_format(element, transform):
        currency_name = transform.get("currency_name_field")
        number_field = transform.get("number_field")
        output_column = transform.get("output_column")

        # Check for currency name and ensure it's not None
        if element.get(currency_name) and element[currency_name].strip() is not None:
            if element[currency_name].upper().strip() == "VND":
                number = float(element[number_field])
                # Format to a specific decimal precision for VND
                out = f"%.0f" % number # Formatted to 0 decimal places
            else:
                out = element[number_field]
        else:
            out = element[number_field]
            
        element[output_column] = out
        return element


    @staticmethod
    def process_post_filter(element, transform, post_filtered):
        if element is not None:
            # Recompile the rule if not already done, or retrieve from cache
            rule_filter = rule_engine.Rule(transform.get("rule"))
            
            if rule_filter.matches(element):
                return element
            else:
                # Filtered out, add to a list of filtered elements
                # post_filtered is likely a list passed by reference
                post_filtered.append(element)
                return None # Return None if filtered out
        
        return None # Should return None if element is None

    @staticmethod
    def process_switch_case(element, transform):
        # get input column name
        input_column = transform.get("input_column") # input field name
        input_column_val = element.get(input_column) # value of input
        output_column = transform.get("output_column")
        
        # Get all switch cases
        case_value = transform.get("switch_case") # Should be a dictionary mapping input val to output val
        default_column = transform.get("default") # output value of the switch case

        out_value = None

        if input_column_val is not None:
            # 1. First try to get value from switch cases
            out_value = case_value.get(input_column_val)
            
            # 2. Second get value from switch cases using default key if not found
            if out_value is None:
                out_value = case_value.get("default")
        
        # 3. If no value found in switch cases, check for default column/value
        if out_value is None:
            if element.get(default_column, None) is not None:
                # First key get value from default column
                out_value = element.get(default_column)
            else:
                out_value = default_column # Use the static default value
        
        element[output_column] = out_value
        return element

    @staticmethod
    def process_multiple_condition(element, transform):
        input_column = transform.get("input_column")
        output_column = transform.get("output_column")
        target_column = transform.get("target_column")
        
        # 'rules' is the list of conditional operations
        rules = transform.get("rules")

        try:
            if input_column in element:
                for rule in rules:
                    key = rule.get("key")
                    condition = rule.get("condition")
                    val = rule.get("val")

                    # Check if key is present in the element
                    if key in element:
                        # Logic for substring extraction/array indexing
                        if type(val) is list:
                            # Assuming val is [start_idx, end_idx]
                            start_idx = val[0]
                            end_idx = val[1]
                            
                            # Check condition (not fully visible, but likely a comparison using 'condition')
                            # If condition is met, perform substring/array slice
                            if True: # Placeholder for actual condition evaluation
                                substring = element[input_column][start_idx:end_idx]
                                element[output_column] = substring
                                return element
                        
                        # Logic for concatenation/simple assignment (not fully visible)
                        elif True: # Placeholder for other types (e.g., concatenation)
                            for idx in val: # Assuming val is a list of keys to concatenate
                                # concated_str = ...
                                pass # Logic is cut off
        except Exception as e:
            # Handle exception
            logging.error(f"Error: {e}")
            pass
            
        return element

    @staticmethod
    def evaluate_condition(row, cond):
        # Function to evaluate a single condition within process_multi_column_conditional_operation
        col = cond.get("column")
        op = cond.get("op")
        match_val = cond.get("reference_value")
        
        val = row.get(col)

        op_map = {
            "==": lambda val, match_val: val == match_val,
            "!=": lambda val, match_val: val != match_val,
            "in": lambda val, match_val: val in match_val,
            "notin": lambda val, match_val: val not in match_val,
            "startsWith": lambda val, match_val: str(val).startswith(str(match_val)),
            "notStartsWith": lambda val, match_val: not str(val).startswith(str(match_val)),
            "endsWith": lambda val, match_val: str(val).endswith(str(match_val)),
            "notEndsWith": lambda val, match_val: not str(val).endswith(str(match_val)),
            ">": lambda val, match_val: float(val) > float(match_val),
            ">=": lambda val, match_val: float(val) >= float(match_val),
            "<": lambda val, match_val: float(val) < float(match_val),
            "<=": lambda val, match_val: float(val) <= float(match_val),
            # ... other operators likely included ...
        }

        try:
            return op_map[op](val, match_val)
        except Exception as e:
            # print(f"error for evaluating condition (cond): {e}")
            return False

    @staticmethod
    def process_multi_column_conditional_operation(element, transform):
        # Evaluate multiple conditions and then use an expression to determine the final result
        
        # 1. Evaluate conditions
        conditions = transform.get("input_conditions")
        expression = transform.get("expression")
        row = element
        results = {}

        for idx, cond in enumerate(conditions):
            results[idx] = RuleUtils.evaluate_condition(row, cond)

        # 2. Evaluate expression (e.g., "0 and 1 or 2")
        expr_eval = expression
        for k, v in results.items():
            # Replace condition indices in the expression with their boolean results (as strings 'True'/'False')
            expr_eval = expr_eval.replace(str(k), str(v))
        
        # Replace 'AND'/'OR' keywords (if any) with Python's 'and'/'or'
        expr_eval = expr_eval.replace("AND", "and").replace("OR", "or")

        # Evaluate the final boolean expression
        final_result = eval(expr_eval)

        # 3. Set output value based on final result
        if final_result:
            # Success value
            if transform.get("output_success_value_method") == "direct":
                element[transform.get("output_column")] = transform.get("output_success_value")
            elif transform.get("output_success_value_method") == "derived":
                element[transform.get("output_column")] = row.get(transform.get("output_success_value"))
        else:
            # Failed value
            if transform.get("output_failed_value_method") == "direct":
                element[transform.get("output_column")] = transform.get("output_failed_value")
            elif transform.get("output_failed_value_method") == "derived":
                element[transform.get("output_column")] = row.get(transform.get("output_failed_value"))
        
        return element

    @staticmethod
    def process_utc_convert(element, transform):
        # Convert a datetime string from one timezone to another
        from datetime import datetime
        import pytz # Dependency check

        time_field = transform.get("time_field")
        format = transform.get("time_format")
        local_time_zone = transform.get("local_time_zone")
        dst_time_zone = transform.get("dst_time_zone")
        
        local_time_str = element[time_field]

        # 1. Parse string to datetime object with local timezone info
        local_time = datetime.strptime(local_time_str, format)
        timezone = pytz.timezone(local_time_zone)
        local_time = timezone.localize(local_time)

        # 2. Convert to destination timezone
        dst_timezone = pytz.timezone(dst_time_zone)
        dst_time = local_time.astimezone(dst_timezone)

        # 3. Convert back to string and update element
        element[time_field] = str(dst_time)
        return element

    @staticmethod
    def process_current_time(element, transform):
        from datetime import datetime
        element["start_time"] = str(datetime.utcnow()) # 'start_time' is the hardcoded output column
        return element

    @staticmethod
    def process_end_time(element, transform):
        from datetime import datetime
        element["end_time"] = str(datetime.utcnow()) # 'end_time' is the hardcoded output column
        return element

    # --- Open Banking / SAPI Format Helpers (Visible in Images 1000318655-1000318659) ---
    
    def add_details_in_account_balance_details(res_bankCode_details, account):
        # Helper function to structure account details for Open Banking format
        # This function seems to handle both a list of bank/consent data and a single account object
        
        consentId = account.get('internalConsentId')
        productName = account.get('productName')
        accountNumber = account.get('data', {}).get('account', {}).get('accountNumber')
        accountStatus = account.get('data', {}).get('account', {}).get('accountStatus')
        isSuccess = account.get('isSuccess')
        
        details = res_bankCode_details.get("details", []) # Get the existing list or an empty one
        
        details.append({
            "accountNumber": accountNumber,
            "consentId": consentId,
            "productName": productName,
            "isSuccess": isSuccess,
            "accountStatus": accountStatus,
        })
        
        # The function signature suggests it returns a list, but the internal logic modifies the passed dict (res_bankCode_details)
        # return res_bankCode_details # Likely modifies the dict passed by reference

    def get_account_balance_details(data):
        res = {}
        for bank in data.get('banks') or []:
            bank_consent = bank.get('consent') or {}
            
            # This loop aggregates account info from bank consents
            for account in bank_consent.get('accounts') or []:
                bankCode = account['bankCode']
                
                if bankCode in res:
                    res_bankCode = res[bankCode]
                else:
                    res_bankCode = {
                        "bankId": bankCode,
                        "details": []
                    }
                    res[bankCode] = res_bankCode

                # Process balances (visible in Image 1000318656.jpg and 1000318659.jpg)
                account_data = account.get('data') or {}
                balances = account_data.get('balances') or []

                # Add dummy balance to details to keep the account info even if no balances
                if not balances:
                    RuleUtils.add_details_in_account_balance_details(res_bankCode, account) # Call the helper
                    
                for balance in balances:
                    credit_debit_indicator = balance.get('creditDebitIndicator')
                    balance_type = balance.get('type')
                    
                    # For DBS balance type handling, transform "AvailableBalance" to "Available Balance" (if typo)
                    if balance_type == "AvailableBalance":
                        balance_type = "Available Balance"

                    datetime = balance.get('dateTime')
                    currency = balance.get('currency')
                    amount = balance.get('amount')
                    
                    # Check if amount is string and convert to float
                    if isinstance(amount, str):
                        amount = float(amount)
                    
                    # Apply sign based on indicator
                    if credit_debit_indicator and credit_debit_indicator.lower() == 'credit':
                        amount = abs(amount) # Ensure positive
                    elif credit_debit_indicator and credit_debit_indicator.lower() == 'debit':
                        amount = abs(amount) * -1 # Ensure negative

                    # Convert amount to string for output structure
                    amount_str = str(amount)
                    
                    # Append the processed balance details
                    res_bankCode["details"].append({
                        "accountNumber": accountNumber, # From outer scope (account)
                        "consentId": consentId,
                        "productName": productName,
                        "balance": amount_str,
                        "currency": currency,
                        "balanceType": balance_type,
                        "isSuccess": isSuccess,
                        "accountStatus": accountStatus,
                        "datetime": datetime
                    })

        return res.values() # Return list of dictionaries

    def get_cloudsql_sapi_formatted_data(data):
        # Top-level formatting function for SAPI
        return {
            "req_not_id": data.get("reqNotId"),
            "task_create_date": data.get("taskCreateDate"),
            "task_id": data.get("taskId"),
            "account_balance_details": RuleUtils.get_account_balance_details(data)
        }
        
    @staticmethod
    def openbank_extract_sapi_format(element):
        # The function name is misleading, it appears to be the main processor for Open Banking data
        data = element
        
        sapi_data = RuleUtils.get_cloudsql_sapi_formatted_data(data)
        
        element["sapi"] = sapi_data
        return element

    @staticmethod
    def process_add_currency_rate_enrichment_key(element):
        # Add a currency rate enrichment key to the element (hardcoded to 'currency_rate')
        element["currency_rate_enrichment_key"] = "currency_rate"
        return element

    @staticmethod
    def openbank_convert_currency(element):
        # Inner helper to get a rate and convert an amount (visible in 1000318657.jpg)
        def convert_currency(source_amount, source_currency, target_currency, exchange_rate):
            rate_record = exchange_rate.get(f"({source_currency})-({target_currency})")
            
            if rate_record is None:
                raise ValueError(f"Currency pair ({source_currency})-({target_currency}) not found")
                
            rate = rate_record.get("CONV_RT")
            return round(source_amount * float(rate), 2), rate_record

        # ... The rest of the function is missing, but it would call convert_currency ...
        return element # Placeholder

    def add_converted_HKD_data(data, exchange_rate):
        # Function to convert all balances to HKD (visible in 1000318658.jpg)
        used_rates = {} # To track which exchange rates were used

        for bank in data.get('account_balance_details') or []:
            for balance in bank.get('details') or []:
                
                amount = balance.get('balance')
                currency = balance.get('currency')
                
                if currency is None:
                    continue # Skip if no currency
                    
                if currency.upper() != 'HKD':
                    try:
                        amount = float(amount)
                        # Convert to HKD
                        balance['balanceHKD'], rate_record = RuleUtils.convert_currency(amount, currency, "HKD", exchange_rate)
                        
                        if rate_record is not None:
                            used_rates[rate_record.get('RATE_RECORD')] = rate_record
                    except Exception as e:
                        # Handle unexpected currency or conversion error
                        logging.error(f"Unexpected currency or conversion error: {e}")
                
        # data["fx_rates"] = list(used_rates.values()) # Likely adds the list of used rates to the data
        return data

    @staticmethod
    def process_add_derived_fields(element):
        # Processes timestamp string from a full format to just milliseconds and adds derived fields
        def parse_timestamp(timestamp_str):
            # Full timestamp with milliseconds
            match = re.match(r"(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})\.(\d{1,6})([\+|\-]\d{2}:\d{2}|Z)", timestamp_str)
            if match:
                dt_str, ms_str, tz_str = match.groups()
                # pad milliseconds to 6 digits (microseconds) for strptime
                ms_str = ms_str.ljust(6, '0')
                return datetime.strptime(f"{dt_str}.{ms_str}{tz_str}", "%Y-%m-%dT%H:%M:%S.%f%z")

            # Timestamp missing milliseconds
            match = re.match(r"(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})([\+|\-]\d{2}:\d{2}|Z)", timestamp_str)
            if match:
                dt_str, tz_str = match.groups()
                return datetime.strptime(f"{dt_str}{tz_str}", "%Y-%m-%dT%H:%M:%S%z")

            # Unknown timestamp format
            raise ValueError("Unknown timestamp format")

        # ... (Rest of the function is cut off, but it continues with a logic block for 
        # generating `bank_warning_indicator` which is visible in 1000318659.jpg)
        
        # The logic visible in 1000318659.jpg (lines 862-904) is for creating a warning indicator:
        def get_bank_warning_indicator(bank_details):
            accounts_with_null_available_balance = []
            accounts_balance_types = {}
            accounts_in_scope = []
            accounts_is_success = []

            for bank in bank_details or []: # bank_details is the list of bank data
                for balance in bank.get('details') or []: # details are the balances
                    account_id = balance.get('accountNumber')
                    
                    # If no balance from EDX, there will be one record in bank_detail without balance info
                    balance_type = balance.get('balanceType')
                    balance_amount = balance.get('balance')
                    balance_status = balance.get('accountStatus')
                    account_is_success = balance.get('isSuccess')

                    if balance_status == "Active":
                        continue # Skip active accounts

                    # Check for Null Available Balance
                    if balance_type == "Available Balance" and balance_amount is None:
                        accounts_with_null_available_balance.append(account_id)
                    
                    # Record all balance types for an account
                    acct_type_id = f"{account_id}|{balance_type}"
                    accounts_balance_types[acct_type_id] = balance_type
                    
                    accounts_is_success.append(account_is_success)
                    
                    # accounts_in_scope to check for null balance
                    accounts_in_scope.append(account_id)
            
            # Check if "Available Balance" is missing for any account in scope
            accounts_without_available_balance = []
            for acct_id in accounts_in_scope:
                if f"{acct_id}|Available Balance" not in accounts_balance_types:
                    accounts_without_available_balance.append(acct_id)

            # Determine the warning indicator
            if len(accounts_without_available_balance) >= 1 or \
               len(accounts_with_null_available_balance) >= 1 or \
               not all(accounts_is_success) is False: # This condition is confusingly written but likely checks if all are successful
                warning_indicator = "Y"
            elif len(accounts_in_scope) == 0:
                warning_indicator = None
            else:
                warning_indicator = "N"

            return warning_indicator

        def add_derived_fields(data):
            bank_details = data.get('sapi', {}).get('account_balance_details')
            # Add the warning indicator to the data structure
            data['bank_level_warning_indicator'] = get_bank_warning_indicator(bank_details)
            
            return data

        # ... The main process_add_derived_fields would call add_derived_fields(element)
        return element # Placeholder


def add_derived_fields(data):
    # Bank level
    for bank in data.get('account_balance_details') or []:
        # Warning indicator
        bank['partialAmountFlag'] = get_bank_warning_indicator(bank.get('details') or [])
        if bank['partialAmountFlag'] in ("Y", None):
            bank['totalBankBalance'] = None
            bank['bankBalanceTimestamp'] = None
            continue

        # Bank level total balance & max timestamp
        bank_HKD_amount_sum = 0
        has_sum = False
        bank_max_timestamp = datetime.datetime.min
        bank_max_timestamp = datetime.datetime.min # This line seems redundant but is present in the image
        for balance in bank.get('details') or []:
            if balance.get('status') in ["Active"] or balance.get('balanceType') in ["Available Balance"]:
                continue
            bank_HKD_amount_sum += balance['balanceHKD']
            has_sum = True
            bank_max_timestamp = max(
                bank_max_timestamp,
                parse_timestamp(balance['datetime'])
            )

        bank['totalBankBalance'] = bank_HKD_amount_sum if has_sum else None
        bank['bankBalanceTimestamp'] = bank_max_timestamp.isoformat('milliseconds') + 'Z' \
            if bank_max_timestamp is not datetime.datetime.min else None

    # Customer level
    cus_HKD_amount_sum = 0
    has_sum = False
    cus_max_timestamp = datetime.datetime.min
    for bank in data.get('account_balance_details') or []:
        if bank.get('totalBankBalance') is None:
            continue
        cus_HKD_amount_sum += bank['totalBankBalance']
        has_sum = True
        cus_max_timestamp = max(
            cus_max_timestamp,
            parse_timestamp(bank['bankBalanceTimestamp'])
        )

    data['total_customer_balance'] = cus_HKD_amount_sum if has_sum else None
    data['customer_balance_timestamp'] = cus_max_timestamp.isoformat('milliseconds') + 'Z' \
        if cus_max_timestamp is not datetime.datetime.min else None

    return data

def getdictasMethod(element):
    return data

def get_pbm_format_for_sapi(element):
    def get_pbm_format_for_sapi(element): # Function definition appears repeated in image
        if data.get('total_customer_balance', None) is not None:
            data['total_customer_balance'] = '{:,.2f}'.format(data['total_customer_balance'])
        
        for bank in data.get('account_balance_details') or []:
            if bank.get('totalBankBalance', None) is not None:
                bank['totalBankBalance'] = '{:,.2f}'.format(bank['totalBankBalance'])
            
            for balance in bank.get('details') or []:
                if balance.get('balanceHKD', None) is not None:
                    balance['balanceHKD'] = '{:,.2f}'.format(balance['balanceHKD'])
        
        data['resource_balance_details'] = json.dumps(data['account_balance_details'])
        data['fx_rate'] = json.dumps(data['fx_rate'])
        return data

def pop_data_points(data):
    for bank in data.get('account_balance_details') or []:
        for balance in bank.get('details') or []:
            balance.pop('datetime', None)
            balance.pop('pbp_datetime', None)
    
    return data

element['sapi'] = pop_data_points(element['sapi'])
element['sapi'] = stringify(element['sapi']) # This line seems to be part of the outer flow, not the function above

return element
