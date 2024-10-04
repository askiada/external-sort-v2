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

# Linux / MacOS compatibility
load_os_settings() {
  msg "> Loading OS-specific values..."
  case "$(uname -s)" in
  "Linux")
    localhost="localhost"
    network_param="net=host"
    arch=""
    ;;
  "Darwin")
    localhost="host.docker.internal"
    network_param="add-host=host.docker.internal:host-gateway"
    arch="_arm"
    ;;
  esac

  msg "< OS-specific values set"
}


load_os_settings


export COMPOSE_PROJECT_NAME="external_sort"

export LOG_LEVEL="info"
export EXTERNAL_LOCALSTACK_PORT=4569

export AWS_ENDPOINT="http://localhost:${EXTERNAL_LOCALSTACK_PORT}"
export AWS_REGION="eu-west-1"
export AWS_RETRIES=1



cleanup() {
  msg "> Cleaning up..."
  msg "< Clean up complete"
}

# Make sure we always clean after ourselves.
trap cleanup SIGHUP SIGINT SIGQUIT SIGABRT SIGTERM EXIT


echo "Starting external services"

BASEDIR=$(dirname "$0")
echo $BASEDIR

docker-compose -f ${BASEDIR}/docker-compose.yaml --profile external down -v
docker-compose -f ${BASEDIR}/docker-compose.yaml --profile external up -d


${BASEDIR}/wait_for_health.sh external_sort_localstack_local

aws --endpoint-url=$AWS_ENDPOINT s3 mb s3://test-bucket
aws --endpoint-url=$AWS_ENDPOINT s3api put-bucket-acl --bucket test-bucket --acl public-read-write


aws --endpoint-url=$AWS_ENDPOINT s3 sync "${BASEDIR}/testdata/input" s3://test-bucket/input


export INPUT_S3_URL="s3://test-bucket/input/mood_media_admin_no_rollup.csv"
export OUTPUT_S3_URL="s3://test-bucket/output/sample_sorted.tsv"

go build -o bin/csvs3 examples/csvs3/main.go

./bin/csvs3

aws --endpoint-url=$AWS_ENDPOINT s3 cp $OUTPUT_S3_URL "${BASEDIR}/testdata/output/sample_sorted.tsv" 
