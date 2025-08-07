#!/bin/bash
#script to stop a mail server

docker_container_name="se-mailpit"

# check if docker is running
if ! docker info > /dev/null 2>&1; then
  echo "**Docker is not running. Please start Docker and try again."
  exit 1
fi
# check if docker compose is installed 
if ! command -v docker-compose &> /dev/null; then
  echo "**Docker Compose is not installed. Please install Docker Compose and try again."
  exit 1
fi
# check if the container is running
if [[ $(docker ps | grep "$docker_container_name") ]]; then
  echo "**Stopping the docker container"
  docker stop "$docker_container_name"
  docker rm "$docker_container_name"
else
  echo "**Container is not running"
fi