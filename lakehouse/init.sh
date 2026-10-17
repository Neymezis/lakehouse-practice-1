#!/usr/bin/env bash
set -uo pipefail
echo "Ждём Lakekeeper..."
until curl -sf http://localhost:8181/health >/dev/null; do sleep 2; done

curl -s -X POST http://localhost:8181/management/v1/bootstrap \
  -H 'Content-Type: application/json' \
  -d '{"accept-terms-of-use": true}'
echo

curl -s -X POST http://localhost:8181/management/v1/warehouse \
  -H 'Content-Type: application/json' \
  -d '{
    "warehouse-name": "lakehouse",
    "storage-profile": {
      "type": "s3",
      "bucket": "lakehouse",
      "endpoint": "http://minio:9000",
      "region": "us-east-1",
      "path-style-access": true,
      "flavor": "s3-compat",
      "sts-enabled": false
    },
    "storage-credential": {
      "type": "s3",
      "credential-type": "access-key",
      "aws-access-key-id": "minioadmin",
      "aws-secret-access-key": "minioadmin"
    }
  }'
echo
echo "Готово."
