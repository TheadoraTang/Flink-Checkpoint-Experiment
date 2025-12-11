#!/bin/bash

sudo chown 9999:9999 -R ./docker/assets/

for d in docker/slave*/; do
  echo "Starting $d"
  (cd "$d" && docker compose restart)
done
