#!/bin/bash
awslocal s3 mb s3://${S3_BUCKET_BRONZE:-bronze}
awslocal s3 mb s3://${S3_BUCKET_SILVER:-silver}
awslocal s3 mb s3://${S3_BUCKET_GOLD:-gold}
