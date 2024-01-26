#!/bin/bash

# Wait for LocalStack to be ready
until aws --endpoint-url=http://localhost:4566 s3 ls; do
    echo "LocalStack is not ready yet, waiting..."
    sleep 2
done

# Create S3 bucket
aws --endpoint-url=http://localhost:4566 s3 mb s3://cachedominiosbucket

echo "S3 bucket 'cachedominiosbucket' created successfully."
