#!/bin/bash

sudo chown 9999:9999 -R ./docker/assets/

cd ./docker/kafka
sudo docker-compose up -d

cd ../..

cd ./docker/master
sudo docker-compose up -d
