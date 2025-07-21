set GCP_ENV=dev
set REGION=europe-west2
set MARKET=uk
set NUM_WORKERS=2

set PROJECT=hsbc-113932821-ihubeucl1-%GCP_ENV%
set TEMPLATE_BUCKET=%PROJECT%-esp-template
set BUCKET=gs://%PROJECT%-esp-tmp
set KMS_PROJECT=hsbc-6320774-kms-%GCP_ENV%
set JOB_NAME=bq_to_gcs_test
set DT=11062025

gcloud --project=%PROJECT% dataflow jobs run %JOB_NAME%-%DT% ^
--gcs-location %TEMPLATE_BUCKET%/templates/%JOB_NAME% ^
--region %REGION% ^
--service-account-email event-refinery@%PROJECT%.iam.gserviceaccount.com ^
--staging-location %BUCKET%/tmp ^
--dataflow-kms-key projects/%KMS_PROJECT%/locations/%REGION%/keyRings/dataFlow/cryptoKeys/dataFlow ^
--subnetwork https://www.googleapis.com/compute/v1/projects/hsbc-4327957-vpehost-eu-%GCP_ENV%/regions/%REGION%/subnetworks/cinternal-vpc1-%REGION% ^
--disable-public-ips ^
--num-workers %NUM_WORKERS%