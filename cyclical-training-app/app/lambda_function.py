import boto3
import json
import os
import uuid


def lambda_handler(event, context):
    S3_BUCKET = os.environ.get("S3_BUCKET")
    SCRIPT_PATH = os.environ.get("SCRIPT_PATH")
    INPUT_PATH = os.environ.get("INPUT_PATH")
    OUTPUT_PATH = os.environ.get("OUTPUT_PATH")
    EMR_RELEASE = "emr-6.10.0"

    # Generate unique cluster and step names
    cluster_name = f"timeseries-forecasting-{uuid.uuid4()}"
    step_name = "Train TimeSeries Model"

    # Initialize Boto3 clients
    emr_client = boto3.client("emr")

    # Create the transient EMR cluster with a single Spark step
    response = emr_client.run_job_flow(
        Name=cluster_name,
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
                "Name": "Install Python Dependencies",
                "ScriptBootstrapAction": {
                    "Path": f"s3://{S3_BUCKET}/scripts/install_dependencies.sh",
                },
            }
        ],
        Steps=[
            {
                "Name": step_name,
                "ActionOnFailure": "TERMINATE_CLUSTER",
                "HadoopJarStep": {
                    "Jar": "command-runner.jar",
                    "Args": [
                        "spark-submit",
                        "--deploy-mode",
                        "cluster",
                        "--executor-memory", "6G",
                        "--driver-memory", "4G",
                        SCRIPT_PATH,  # 0
                        OUTPUT_PATH,  # 1
                        "--epochs",  # 2
                        "20",  # 3
                        "--batch_size",  # 4
                        "32",  # 5
                        INPUT_PATH,  # 6
                        S3_BUCKET,  # 7
                    ],
                },
            }
        ],
    )

    # Return the cluster ID and status
    return {
        "statusCode": 200,
        "body": json.dumps({
            "ClusterId": response["JobFlowId"],
            "Message": f"EMR cluster {response['JobFlowId']} created successfully to train the time series model."
        })
    }
