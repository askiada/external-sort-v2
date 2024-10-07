#!/usr/bin/env bash
set -e

msg_color="\e[31;46m"     #at least it's visible...
success_color="\e[33;42m" #at least it's visible...
err_color="\e[36;41m"     #at least it's visible...
nc="\e[0m"                #clean it

# msg prints a "nice" and visible information to the screen.
msg() {
  echo -e "${msg_color}[$(date "+%Y-%m-%d %H:%M:%S")] $*${nc}"
}

# err prints an "error" message and exits the program
err() {
  echo -e "${err_color}[$(date "+%Y-%m-%d %H:%M:%S")] $*; aborting...${nc}"
  exit 1
}

success() {
  echo -e "${success_color}[$(date "+%Y-%m-%d %H:%M:%S")] $*${nc}"
}

export LOG_LEVEL="info"



cleanup() {
  msg "> Cleaning up..."
  msg "< Clean up complete"
}

# Make sure we always clean after ourselves.
trap cleanup SIGHUP SIGINT SIGQUIT SIGABRT SIGTERM EXIT

export AWS_REGION="eu-west-1"
export AWS_RETRIES=1

echo "Starting external services"

export INPUT_S3_URL="s3://blokur-data/mlc-recordings-cluster/recording-identifiers/output/MRI/POC/initial_pos_unique_sound_recordings_intermediary_other_clusters-all.csv"
export OUTPUT_S3_URL="s3://blokur-data/mlc-recordings-cluster/recording-identifiers/output/MRI/POC/initial_pos_unique_sound_recordings_intermediary_other_clusters-all_unique.csv"

go build -o bin/clustering scripts/prod/clustering/main.go

./bin/clustering
