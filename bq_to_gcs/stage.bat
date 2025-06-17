set GCP_ENV=dev
set REGION=europe-west2 
set NUM_WORKERS=1 
set PROJECT=myproject-dev 
set TEMPLATE_BUCKET=gs://%PROJECT%-esp-template 
set BUCKET=gs://%PROJECT%-esp-tmp 
set KMS_PROJECT=myproject-63207744-kms-dev 
set JOB_NAME=bq_to_gcs_test

REM Debugging environment variables 
echo GCP_ENV=dev 
echo REGION=%REGION% 
echo NUM_WORKERS=%NUM_WORKERS% 
echo PROJECT=%PROJECT% 
echo TEMPLATE_BUCKET=%TEMPLATE_BUCKET% 
echo BUCKET=%BUCKET% 
echo KMS_PROJECT=%KMS_PROJECT% 
echo JOB_NAME=%JOB_NAME%

REM Running the Python script
python bq_to_gcs.py ^ 
--job_name %JOB_NAME% ^ 
--region %REGION% ^ 
--project %PROJECT% ^ 
--runner DataflowRunner ^ 
--subnetwork https://www.googleapis.com/compute/v1/projects/myproject-63207744/vpcHost-eu-dev/regions/%REGION%/subnetworks/cinternal-vpc1-%REGION% ^ 
--service_account_email=event-refinery@%PROJECT%.iam.gserviceaccount.com ^ 
--temp_location %BUCKET%/tmp/ ^ 
--template_location %TEMPLATE_BUCKET%/templates/%JOB_NAME%.json ^ 
--no_use_public_ips ^ 
--setup_file=./setup.py ^ 
--autoscaling_algorithm THROUGHPUT_BASED ^ 
--max_num_workers=2 ^ 
--num_workers=%NUM_WORKERS% ^ 
--machine_type=n1-standard-2 ^ 
--experiments=use_network_tags;tt-1234-ihubecud1-gcp-a-e-dataflow;tt-1234-ihubecud1-dataflow-a-e-aws;tt-1234-ihubecud1-dataflow-a-i-dataflow ^ 
--experiments=use_runner_v2 ^ 
--experiments=enable_kms_on_streaming_engine
