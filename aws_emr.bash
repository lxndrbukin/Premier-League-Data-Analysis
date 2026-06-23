#!/bin/bash
set -a
source .env
set +a
DT=$(date +%Y-%m-%d)

aws emr-serverless start-job-run \
    --application-id $APPLICATION_ID \
    --execution-role-arn $EXECUTION_ROLE_ARN \
    --region $REGION \
    --job-driver '{
        "sparkSubmit": {
            "entryPoint": "s3://'$BUCKET_NAME'/scripts/spark_pipeline.py",
            "entryPointArguments": [
                "s3://'$BUCKET_NAME'/landing/",
                "s3://'$BUCKET_NAME'/processed/"
            ],
            "sparkSubmitParameters": "--conf spark.executor.cores=2 --conf spark.executor.memory=4g --conf spark.driver.memory=2g"
        }
    }' \
    --configuration-overrides '{
        "monitoringConfiguration": {
            "s3MonitoringConfiguration": {
                "logUri": "s3://'$BUCKET_NAME'/logs/"
            }
        }
    }'