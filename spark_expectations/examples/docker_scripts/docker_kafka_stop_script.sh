#!/bin/bash
#shell scripts to remove docker container

docker_container_name="spark_expectations_kafka_docker"
docker_image_name="spark_expectations_kafka_topic"

if [[ $(docker ps -a | grep "$docker_container_name") ]]; then
  docker rm -f "$docker_container_name"
  echo sucessfully removed the docker conatiner from registry
fi