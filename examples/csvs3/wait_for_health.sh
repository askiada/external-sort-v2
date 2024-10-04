#!/bin/bash
service=$1

until [ "$(docker inspect --format='{{json .State.Health.Status}}' $service)" == "\"healthy\"" ]; do
  echo "Waiting for $service to become healthy"
  sleep 1
done
