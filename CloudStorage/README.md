##
Important: gsutil is not the recommended CLI for Cloud Storage. Use gcloud storage commands in the Google Cloud CLI instead.
## Create a Bucket
gcloud storage buckets create gs://my-test-cli-bucket/ --uniform-bucket-level-access

## Upload a file from local location to bucket.
gcloud storage cp /Users/prakashtimilsina/Documents/Learning/GCP/GCP-DATA-ENGINEERING-Demo-Codes/README.md gs://my-test-cli-bucket-pt

