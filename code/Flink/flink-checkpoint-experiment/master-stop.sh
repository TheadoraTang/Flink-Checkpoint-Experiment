#!/bin/bash

sudo chown "$(id -un):$(id -gn)" -R ./docker/assets/

cd ./docker/minio
sudo docker compose down

cd ../..

cd ./docker/kafka
sudo docker compose down

cd ../..

cd ./docker/master
sudo docker compose down
