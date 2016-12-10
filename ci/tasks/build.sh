#!/bin/bash

set -e -u

cat << EOF > credentials.json
{
  "username": "${AUTH_USERNAME}",
  "password": "${AUTH_PASSWORD}",
  "s3_config": {
    "region": "${AWS_REGION}",
    "bucket_prefix": "${BUCKET_PREFIX}"
  }
}
EOF

cp -r broker-src/. broker-src-built

jq -s '.[0] * .[1]' broker-src/config-sample.json credentials.json > \
  broker-src-built/config.json
