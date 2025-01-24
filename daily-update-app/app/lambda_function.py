import boto3
import json
import os
import pandas as pd
import pyarrow.parquet as pq
from datetime import datetime, timedelta
import yfinance as yf
import uuid
import pyarrow as pa


def get_most_recent_date(s3_bucket, output_path):
    """
    Finds the most recent date in the time series dataset stored in S3 structured by year.
    """
    s3_client = boto3.client('s3')

    # List all year directories
    paginator = s3_client.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=s3_bucket, Prefix=output_path)

    latest_date = None
    # latest_file = None

    for page in pages:
        for obj in page.get('Contents', []):
            if obj['Key'].endswith('.parquet'):
                year_key = obj['Key']
                # Download the Parquet file locally
                local_file = f'/tmp/{os.path.basename(year_key)}'
                s3_client.download_file(s3_bucket, year_key, local_file)

                # Read the file and find the max date
                table = pq.read_table(local_file)
                df = table.to_pandas()

                max_date_in_file = pd.to_datetime(df['date']).max()
                if latest_date is None or max_date_in_file > latest_date:
                    latest_date = max_date_in_file

    return latest_date


def update_dataset_with_yfinance(s3_bucket, output_path, latest_date):
    """
    Downloads new data from Yahoo Finance starting from the most recent date in the dataset.
    """
    new_end_date = datetime.today()
    new_start_date = latest_date + timedelta(days=1)

    if new_start_date >= new_end_date:
        print("Dataset is already up-to-date.")
        return

    print(f"Fetching data from {new_start_date} to {new_end_date}...")

    # Fetch new data using yfinance
    ticker = "GC=F"
    new_data = yf.download(ticker, start=new_start_date, end=new_end_date)
    new_data.reset_index(inplace=True)

    if new_data.empty:
        print("No new data available.")
        return

    # Preprocess the data
    new_data['close'] = new_data['Close']['GC=F']
    new_data.rename(columns={'Date': 'date'}, inplace=True)

    new_data.columns = [col[0] for col in new_data.columns]

    # 'year' column for partitioning
    new_data['year'] = new_data['date'].dt.year

    print(new_data.head())

    # Group data by year and save each year separately
    for year, group in new_data.groupby('year'):
        local_new_data_file = f"/tmp/{uuid.uuid4()}.parquet"

        # Write group to Parquet
        table = pa.Table.from_pandas(group[['date', 'close']], schema=pa.schema([
            pa.field("date", pa.date32()),
            pa.field("close", pa.float64())
        ]))
        pq.write_table(table, local_new_data_file)

        # Define partition path for the current year
        new_file_key = f"{output_path}/year={year}/{os.path.basename(local_new_data_file)}"

        # Upload the Parquet file to S3
        s3_client = boto3.client("s3")
        s3_client.upload_file(local_new_data_file, s3_bucket, new_file_key)

        print(
            f"Year {year} data successfully uploaded to s3://{s3_bucket}/{new_file_key}.")


def lambda_handler(event, context):

    S3_BUCKET = os.environ.get("S3_BUCKET")
    # Path to the time series dataset in S3
    OUTPUT_PATH = os.environ.get("OUTPUT_PATH")

    # Step 1: Find the most recent date in the dataset
    most_recent_date = get_most_recent_date(S3_BUCKET, OUTPUT_PATH)

    if most_recent_date is None:
        return {
            "statusCode": 500,
            "body": json.dumps({"Message": "No data found in the dataset."})
        }

    # Step 2: Update the dataset with new data from yfinance
    update_dataset_with_yfinance(S3_BUCKET, OUTPUT_PATH, most_recent_date)

    return {
        "statusCode": 200,
        "body": json.dumps({"Message": "Dataset updated successfully."})
    }
