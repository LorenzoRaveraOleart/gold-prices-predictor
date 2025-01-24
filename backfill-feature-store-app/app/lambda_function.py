import json
import boto3
from yfinance import download
import os
# AWS clients
S3_CLIENT = boto3.client("s3")
EMR_CLIENT = boto3.client("emr")

# Constants
S3_BUCKET = os.environ['BUCKET_NAME']
SCRIPT_PATH = os.environ['SCRIPT_PATH']
EMR_RELEASE = "emr-6.10.0"


def fetch_data_from_yahoo_finance(ticker, start_date, end_date):
    """
    Fetch data from Yahoo Finance and save it as a CSV file in S3.
    """
    print(f"Fetching data from yahoo finance... from {start_date}")
    data = download(ticker, start=start_date, end=end_date)
    if data.empty:
        raise ValueError(
            f"No data returned for ticker {ticker} between {start_date} and {end_date}")

    # Process data
    data.reset_index(inplace=True)

    data['close'] = data['Close']['GC=F']
    data.rename(columns={'Date': 'date'}, inplace=True)
    data['date'] = data['date'].dt.strftime('%Y-%m-%d')
    data.columns = [col[0] for col in data.columns]
    # 'year' column for partitioning
    data['year'] = data['date'].dt.year

    # Save CSV to S3
    csv_file_path = f"yahoo_finance/{ticker}_data_{start_date}_to_{end_date}.csv"
    csv_buffer = data[['date', 'close', 'year']].to_csv(
        index=False, header=True)
    S3_CLIENT.put_object(Bucket=S3_BUCKET, Key=csv_file_path, Body=csv_buffer)

    return f"s3://{S3_BUCKET}/{csv_file_path}"


def create_emr_cluster(csv_s3_path):
    """
    Create a transient EMR cluster and submit a Spark job, passing the CSV S3 path as an argument.
    """
    response = EMR_CLIENT.run_job_flow(
        Name="YahooFinanceDataProcessing",
        ReleaseLabel=EMR_RELEASE,
        Instances={
            "InstanceGroups": [
                {
                    "InstanceRole": "MASTER",
                    "InstanceType": "m5.xlarge",
                    "InstanceCount": 1,
                },
                {
                    "InstanceRole": "CORE",
                    "InstanceType": "m5.xlarge",
                    "InstanceCount": 2,
                },
            ],
            "Ec2KeyName": "key-for-emr-gold",
            "KeepJobFlowAliveWhenNoSteps": False,
            "TerminationProtected": False,
        },
        Applications=[{"Name": "Spark"}],
        JobFlowRole="EMR_EC2_DefaultRole",
        ServiceRole="EMR_DefaultRole",
        LogUri=f"s3://{S3_BUCKET}/logs/",
        BootstrapActions=[
            {
                "Name": "Install boto3",
                "ScriptBootstrapAction": {
                    "Path": f"s3://{S3_BUCKET}/scripts/install_boto3.sh",
                },
            }
        ],
        Steps=[
            {
                "Name": "Process Yahoo Finance Data",
                "ActionOnFailure": "TERMINATE_CLUSTER",
                "HadoopJarStep": {
                    "Jar": "command-runner.jar",
                    "Args": [
                        "spark-submit",
                        "--deploy-mode",
                        "cluster",
                        "--executor-memory", "6G",
                        "--driver-memory", "4G",
                        SCRIPT_PATH,
                        csv_s3_path,
                        f"s3://{S3_BUCKET}/output/",
                    ],
                },
            }
        ],
    )

    return response["JobFlowId"]


def lambda_handler(event, context):
    """
    AWS Lambda function entry point.
    """
    try:
        ticker = event.get("ticker", "GC=F")
        start_date = event.get("start_date", "2024-01-01")
        end_date = event.get("end_date", "2024-12-12")

        # Fetch raw data and save it as CSV
        csv_s3_path = fetch_data_from_yahoo_finance(
            ticker, start_date, end_date)

        # Create EMR cluster and process data
        job_flow_id = create_emr_cluster(csv_s3_path)

        return {
            "statusCode": 200,
            "body": json.dumps(
                {
                    "message": "EMR cluster created and data processing started.",
                    "jobFlowId": job_flow_id,
                }
            ),
        }

    except Exception as e:
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)}),
        }
