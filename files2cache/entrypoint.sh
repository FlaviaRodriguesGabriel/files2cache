#!/bin/bash

# Wait for LocalStack to be ready
until aws --endpoint-url=http://localhost:4566 s3 ls; do
    echo "LocalStack is not ready yet, waiting..."
    sleep 2
done

# Create S3 bucket
if aws --endpoint-url=http://localhost:4566 s3 mb s3://cachedominiosbucket; then
    echo "S3 bucket 'cachedominiosbucket' created successfully."
else
    echo "Failed to create S3 bucket."
fi

# Create S3 mock objects - QA
if aws --endpoint-url=http://localhost:4566 s3 cp ./app/mock/qa/ \
            s3://cachedominiosbucket/qa \
            --recursive; then 
                echo "S3 objects for QA created successfully."
else
    echo "FAILED to create S3 objects for QA."
fi

# Create S3 mock objects - CW3
if aws --endpoint-url=http://localhost:4566 s3 cp ./app/mock/cw3/ \
            s3://cachedominiosbucket/cw3 \
            --recursive; then 
                echo "S3 objects for CW3 created successfully."
else
    echo "FAILED to create S3 objects for CW3."
fi

# Create S3 mock objects - EQ3
if aws --endpoint-url=http://localhost:4566 s3 cp ./app/mock/eq3/ \
            s3://cachedominiosbucket/eq3 \
            --recursive; then 
                echo "S3 objects for EQ3 created successfully."
else
    echo "FAILED to create S3 objects for EQ3."
fi