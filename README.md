# GCP-DATA-ENGINEERING-Demo-Codes
# Setup and Requirements
gcloud auth list
gcloud config list project
gcloud config set compute/region us-west1
gcloud config get-value compute/region

gcloud config set compute/zone us-west1-c
gcloud config get-value compute/zone

gcloud config get-value project 
gcloud compute project-info describe --project $(gcloud config get-value project)

## Setting environment variables
export PROJECT_ID=$(gcloud config get-value project)
export ZONE=$(gcloud config get-value compute/zone)

echoe -e "PROJECT ID: $PROJECT_ID\nZone : $ZONE"


## Creating virtual machine with Gcloud
gcloud compute instances create gcelab2 --machine-type e2-medium --zone $ZONE
    Output:
        Created [https://www.googleapis.com/compute/v1/projects/qwiklabs-gcp-00-7a54a50e6f55/zones/us-west1-c/instances/gcelab2].
        NAME: gcelab2
        ZONE: us-west1-c
        MACHINE_TYPE: e2-medium
        PREEMPTIBLE: 
        INTERNAL_IP: 10.138.0.2
        EXTERNAL_IP: 34.82.104.211
        STATUS: RUNNING

## Exploring glcoud Commands:
gcloud -h or 
gcloud --help

gcloud config list
gcloud config list --all

gcloud components list

gcloud compute instances list --filter="name=('gcelab2')"
gcloud compute firewall-rules list
gcloud compute firewall-rules list --filter="network='default'"

## Connecting to VM
gcloud compute ssh gcelab2 --zone $ZONE

# install nginx on vm
sudo apt install -y nginx

gcloud compute instances add-tags gcelab2 --tags http-server,https-server




# Cloud Composer 

# DAG
Use of Python
# BigQuery Operator
