set GCP_ENV=dev 
set REGION=europe-west2 
set NUM_WORKERS=1 
set MAX_WORKERS=3 
set PROJECT=myproject-dev 
set TEMPLATE_BUCKET=gs://%PROJECT%-temp-template 
set BUCKET=gs://%PROJECT%-esp-tmp 
set KMS_PROJECT=myproject-63207744-kms-dev 
set JOB_NAME=bq_to_gcs_bat

gcloud --project=%PROJECT% dataflow jobs run %JOB_NAME%-%TX% ^ 
--gcs-location=gs://%TEMPLATE_BUCKET%/templates/%JOB_NAME%.json ^ 
--region=%REGION% ^ 
--service-account=event_refinery@%PROJECT%.iam.gserviceaccount.com ^ 
--staging-location=%BUCKET%/ ^ 
--subnetwork=https://www.googleapis.com/compute/v1/projects/%KMS_PROJECT%/regions/%REGION%/subnetworks/cinternal-vpc1-%REGION% ^ 
--num-workers=%NUM_WORKERS%
