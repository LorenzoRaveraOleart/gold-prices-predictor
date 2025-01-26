# Gold Prices Predictor - README

## Overview
This project predicts gold prices using a machine learning (Tensorflow) model deployed on a transient AWS EMR instance. The architecture includes:

1. **ECR Repositories:**
   - **gold-backfill-feature-store-app:** Runs an EMR job to create initial features in the bucket under `feature-store/`.
   - **gold-daily-update-app:** Updates the feature store with data from the Yahoo Finance API.
   - **gold-predictor-app:** Downloads the model and scaler, makes predictions, and saves them in `prediction/`.

2. **Lambda Functions:**
   - **gold-cyclical-training-app:** Runs an EMR job every 30 days to retrain the model and save artifacts in `model-artifacts/`.
   - **gold-prediction-image-fetcher:** Fetches prediction plots from the bucket and serves them via an API Gateway.

3. **S3 Bucket:**
   - **gold-prices-predictor:** Stores scripts, data, model artifacts, and predictions.

4. **Scripts (saved in `scripts/` in the bucket):**
   - EMR job definition for `gold-backfill-feature-store-app`.
   - Installation script for boto3.
   - EMR script for cyclical training.
   - Dependency installation script for model execution on EMR.

5. **IAM Roles:**
   - **EMR_EC2_DefaultRole**
   - **EMR_DefaultRole**

---

## Setup Instructions

### 1. **Pre-requisites**
Ensure you have the following installed:
- AWS CLI
- Docker
- Python 3.x (for local testing)

### 2. **Create and Configure the S3 Bucket**
1. Create an S3 bucket named `gold-prices-predictor` (or update references if using a different name).
   ```bash
   aws s3 mb s3://gold-prices-predictor
   ```
2. Upload the necessary scripts to the bucket:
   ```bash
   aws s3 cp ./scripts/emr_job_definition.py s3://gold-prices-predictor/scripts/
   aws s3 cp ./scripts/install_boto3.sh s3://gold-prices-predictor/scripts/
   aws s3 cp ./scripts/emr_cyclical_training.py s3://gold-prices-predictor/scripts/
   aws s3 cp ./scripts/install_dependencies.sh s3://gold-prices-predictor/scripts/
   ```

### 3. **Set Up IAM Roles and Policies**
1. **EMR_EC2_DefaultRole:**
   Attach the `AmazonS3FullAccess` and `AmazonEMRRoleforEC2` policies.
   ```bash
   aws iam create-role --role-name EMR_EC2_DefaultRole --assume-role-policy-document file://emr_ec2_trust_policy.json
   aws iam attach-role-policy --role-name EMR_EC2_DefaultRole --policy-arn arn:aws:iam::aws:policy/AmazonS3FullAccess
   aws iam attach-role-policy --role-name EMR_EC2_DefaultRole --policy-arn arn:aws:iam::aws:policy/service-role/AmazonEMRRoleforEC2
   ```

2. **EMR_DefaultRole:**
   Attach the `AmazonS3FullAccess` and `AmazonEMRFullAccessPolicy_v2` policies.
   ```bash
   aws iam create-role --role-name EMR_DefaultRole --assume-role-policy-document file://emr_service_role_policy.json
   aws iam attach-role-policy --role-name EMR_DefaultRole --policy-arn arn:aws:iam::aws:policy/AmazonS3FullAccess
   aws iam attach-role-policy --role-name EMR_DefaultRole --policy-arn arn:aws:iam::aws:policy/service-role/AmazonEMRFullAccessPolicy_v2
   ```

### 4. **Create Docker Images and Push to ECR**
1. **Authenticate Docker to ECR:**
   ```bash
   aws ecr get-login-password --region <region> | docker login --username AWS --password-stdin <aws_account_id>.dkr.ecr.<region>.amazonaws.com
   ```

2. **Create ECR Repositories:**
   ```bash
   aws ecr create-repository --repository-name gold-backfill-feature-store-app
   aws ecr create-repository --repository-name gold-daily-update-app
   aws ecr create-repository --repository-name gold-predictor-app
   ```
3. **Build and Push Docker Images:**
   - For `gold-backfill-feature-store-app`:
     ```bash
     docker build -t gold-backfill-feature-store-app .
     docker tag gold-backfill-feature-store-app:latest <aws_account_id>.dkr.ecr.<region>.amazonaws.com/gold-backfill-feature-store-app:latest
     docker push <aws_account_id>.dkr.ecr.<region>.amazonaws.com/gold-backfill-feature-store-app:latest
     ```
   - Repeat for `gold-daily-update-app` and `gold-predictor-app`.



### 5. **Create and Deploy Lambda Functions**
1. **Gold-Backfill-Feature-Store-App:**
   - Create a Lambda function for the Docker image:
     ```bash
     aws lambda create-function --function-name gold-backfill-feature-store-app \
       --package-type Image \
       --code ImageUri=<aws_account_id>.dkr.ecr.<region>.amazonaws.com/gold-backfill-feature-store-app:latest \
       --role <lambda_execution_role_arn>
     ```

2. **Gold-Daily-Update-App:**
   - Create a Lambda function for the Docker image:
     ```bash
     aws lambda create-function --function-name gold-daily-update-app \
       --package-type Image \
       --code ImageUri=<aws_account_id>.dkr.ecr.<region>.amazonaws.com/gold-daily-update-app:latest \
       --role <lambda_execution_role_arn>
     ```

3. **Gold-Predictor-App:**
   - Create a Lambda function for the Docker image:
     ```bash
     aws lambda create-function --function-name gold-predictor-app \
       --package-type Image \
       --code ImageUri=<aws_account_id>.dkr.ecr.<region>.amazonaws.com/gold-predictor-app:latest \
       --role <lambda_execution_role_arn>
     ```

4. **Gold-Cyclical-Training-App:**
   - Zip the function and upload it to Lambda:
     ```bash
     zip gold-cyclical-training-app.zip cyclical_training.py
     aws lambda create-function --function-name gold-cyclical-training-app \
       --runtime python3.x \
       --role <lambda_execution_role_arn> \
       --handler cyclical_training.lambda_handler \
       --zip-file fileb://gold-cyclical-training-app.zip
     ```

5. **Gold-Prediction-Image-Fetcher:**
   - Zip the function and upload it to Lambda:
     ```bash
     zip gold-prediction-image-fetcher.zip prediction_image_fetcher.py
     aws lambda create-function --function-name gold-prediction-image-fetcher \
       --runtime python3.x \
       --role <lambda_execution_role_arn> \
       --handler prediction_image_fetcher.lambda_handler \
       --zip-file fileb://gold-prediction-image-fetcher.zip
     ```

6. **Add Trigger for Gold-Predictor-App:**
   - Add an S3 event trigger for the `gold-predictor-app` Lambda function to execute when new parquet data is added to the `feature-store/` directory:
     ```bash
     aws s3api put-bucket-notification-configuration --bucket gold-prices-predictor --notification-configuration '{
       "LambdaFunctionConfigurations": [
         {
           "LambdaFunctionArn": "<gold_predictor_lambda_arn>",
           "Events": ["s3:ObjectCreated:Put"],
           "Filter": {
             "Key": {
               "FilterRules": [
                 {
                   "Name": ".parquet",
                   "Value": "feature-store/"
                 }
               ]
             }
           }
         }
       ]
     }'
     ```

### 6. **Deploy and Test the System**
1. Add the necessary environment variables to the lambda functions.
2. Add cron expression triggers through AWS EventBridge in order to run the daily updates and the monthly retraining.
3. Trigger the `gold-backfill-feature-store-app` to generate initial features in the S3 bucket.
4. Run `gold-daily-update-app` to update features with new data from Yahoo Finance.
5. Use `gold-predictor-app` to generate predictions and save results in `prediction/`.
6. Trigger `gold-cyclical-training-app` for model retraining and storing artifacts in `model-artifacts/`.
7. Test the API Gateway connected to `gold-prediction-image-fetcher` for fetching plots.

---

## Notes
- Ensure all IAM roles have the correct trust relationships to allow Lambda and EMR services to assume them.
- Replace placeholders (e.g., `<region>`, `<aws_account_id>`, `<lambda_execution_role_arn>`) with appropriate values.
- Monitor CloudWatch logs for debugging and performance insights.

