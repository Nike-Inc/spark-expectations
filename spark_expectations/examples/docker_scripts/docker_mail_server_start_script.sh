#!/bin/bash
# shell scripts to create, start and run docker container, which contains mail server

file_dir=$(dirname "$0")
docker_container_name="se-mailpit"
docker_network_name="se-net"

# check if docker network exists, if not create it
if [[ $(docker network ls | grep "$docker_network_name") ]]; then
  echo "** Docker network '$docker_network_name' already exists."
else
  echo "** Creating Docker network $docker_network_name."
  docker network create "$docker_network_name"
fi

# check if docker is running
if ! docker info > /dev/null 2>&1; then
  echo "** Docker is not running. Please start Docker and try again."
  exit 1
fi

# Generate self-signed SSL certificate for mailpit
if [[ ! -d "${file_dir}/certs" ]]; then
  echo "** Creating 'certs' directory for SSL certificates."
  mkdir "${file_dir}/certs"
else
  echo "** 'certs' directory already exists."
  # Check if certs/mailpit.key and certs/mailpit.crt exist
  if [[ ! -f "${file_dir}/certs/mailpit.key" || ! -f "${file_dir}/certs/mailpit.crt" ]]; then
    echo "** Generating self-signed SSL certificate for mailpit."
    openssl req -x509 -newkey rsa:4096 -keyout "${file_dir}/certs/mailpit.key" -out "${file_dir}/certs/mailpit.crt" -days 365 -nodes -subj "/CN=localhost"
  else
    echo "** SSL certificate files already exist."
  fi
fi

# run docker compose to start the mail server
docker_compose_file="$file_dir/docker-compose.yml"
if [[ $(docker ps -a | grep "$docker_container_name") ]]; then
    echo "** Container '$docker_container_name' is already running"
else
  echo "** Starting the docker container"
  # rebuild the container with existing image
  docker compose -f $docker_compose_file up -d --build
fi