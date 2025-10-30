#!/bin/bash
#shell scripts to create, start and run docker container, which contains kafka cluster
script_dir=$(dirname "$0")
project_root="$script_dir/../../.."

docker_container_name="spark_expectations_kafka_docker"
docker_image_name="spark_expectations_kafka_topic"

if [[ $(docker ps -a | grep "$docker_container_name") ]]; then
  if [[ $(docker ps | grep "$docker_container_name") ]]; then
    echo container is running
  else
    echo starting the docker container

    # remove the existing docker container
    docker rm -f "$docker_container_name"

    # rebuild the container with existing image
    docker build -f "$project_root/containers/kafka/Dockerfile.kafka" -t $docker_image_name "$project_root" && docker run --name $docker_container_name -d -h localhost -p 9092:9092 -i $docker_image_name

  fi
else

   # rebuild the container with existing image
  docker build -f "$project_root/containers/kafka/Dockerfile.kafka" -t $docker_image_name "$project_root" && docker run --name $docker_container_name -d -h localhost -p 9092:9092 -i $docker_image_name

fi