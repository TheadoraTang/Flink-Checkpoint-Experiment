#!/bin/bash

for d in docker/slave*/; do
  echo "Starting $d"
  (cd "$d" && sudo docker-compose up -d)
done
