#!/usr/bin/env bash
set -ex

make docker-otelcontribcol
docker-compose up --build -d