#!/bin/bash

sudo chown "$(id -un):$(id -gn)" -R ./docker/assets/

for d in docker/slave*/; do
  echo "Starting $d"
  (cd "$d" && docker compose down)
done
