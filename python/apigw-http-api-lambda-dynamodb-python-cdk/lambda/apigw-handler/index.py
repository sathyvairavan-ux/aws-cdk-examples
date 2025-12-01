# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

import boto3
import os
import json
import logging
import uuid
from aws_xray_sdk.core import xray_recorder
from aws_xray_sdk.core import patch_all

# Patch all supported libraries for X-Ray tracing
patch_all()

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb_client = boto3.client("dynamodb")


def handler(event, context):
    table = os.environ.get("TABLE_NAME")
    
    # Log request context for security investigations
    logger.info(f"Request ID: {context.request_id}")
    logger.info(f"Function ARN: {context.invoked_function_arn}")
    
    # Extract and log API Gateway context if available
    request_context = event.get("requestContext", {})
    logger.info(f"Source IP: {request_context.get('identity', {}).get('sourceIp', 'unknown')}")
    logger.info(f"User Agent: {request_context.get('identity', {}).get('userAgent', 'unknown')}")
    
    try:
        if event["body"]:
            item = json.loads(event["body"])
            logger.info(f"Processing item with id: {item.get('id', 'unknown')}")
            year = str(item["year"])
            title = str(item["title"])
            id = str(item["id"])
            dynamodb_client.put_item(
                TableName=table,
                Item={"year": {"N": year}, "title": {"S": title}, "id": {"S": id}},
            )
            logger.info(f"Successfully inserted item with id: {id}")
            message = "Successfully inserted data!"
            return {
                "statusCode": 200,
                "headers": {"Content-Type": "application/json"},
                "body": json.dumps({"message": message}),
            }
        else:
            logger.info("Received request without payload, using default data")
            default_id = str(uuid.uuid4())
            dynamodb_client.put_item(
                TableName=table,
                Item={
                    "year": {"N": "2012"},
                    "title": {"S": "The Amazing Spider-Man 2"},
                    "id": {"S": default_id},
                },
            )
            logger.info(f"Successfully inserted default item with id: {default_id}")
            message = "Successfully inserted data!"
            return {
                "statusCode": 200,
                "headers": {"Content-Type": "application/json"},
                "body": json.dumps({"message": message}),
            }
    except Exception as e:
        logger.error(f"Error processing request: {str(e)}", exc_info=True)
        raise
