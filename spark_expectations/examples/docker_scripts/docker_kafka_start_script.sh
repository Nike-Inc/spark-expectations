#!/bin/bash
#shell scripts to create, start and run docker container, which contains kafka cluster
file_dir=$(dirname "$0")

docker_container_name="spark_expectations_kafka_docker"
docker_image_name="spark_expectations_kafka_topic"
docker_network_name="se-net"

# check if docker network exists, if not create it
if [[ $(docker network ls | grep "$docker_network_name") ]]; then
  echo "Docker network $docker_network_name already exists."
else
  echo "Creating Docker network $docker_network_name."
  docker network create "$docker_network_name"
fi

if [[ $(docker ps -a | grep "$docker_container_name") ]]; then
  if [[ $(docker ps | grep "$docker_container_name") ]]; then
    echo container is running
  else
    echo starting the docker container

    # remove the existing docker container
    docker rm -f "$docker_container_name"

    # rebuild the container with exuisting image
    docker build -t $docker_image_name $file_dir && docker run --name $docker_container_name -d -h localhost -p 9092:9092 -i $docker_image_name

  fi
else

   # rebuild the container with exuisting image
  docker build -t $docker_image_name $file_dir && docker run --name $docker_container_name --network se-net -d -h localhost -p 9092:9092 -i $docker_image_name

fi