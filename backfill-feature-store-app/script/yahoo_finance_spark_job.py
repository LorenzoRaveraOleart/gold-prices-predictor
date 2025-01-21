from pyspark.sql import SparkSession
from pyspark.sql.functions import year, to_date
import sys
import boto3
import os

if __name__ == "__main__":
    # Initialize Spark session
    spark = SparkSession.builder.appName(
        "YahooFinanceDataProcessing").getOrCreate()

    # print(f"### csv_path: {sys.argv[5]} ###")
    # print(f"### output_path: {sys.argv[6]} ###")
    print("arguments:-----------------------------------")
    for arg in sys.argv:
        print(arg)
    print("arguments:-----------------------------------")

    try:
        # Read arguments
        csv_path = sys.argv[1]  # S3 path of the input CSV
        output_path = sys.argv[2]  # Output S3 path

        # Read CSV file into Spark DataFrame
        df = spark.read.csv(csv_path, header=True, inferSchema=True)

        # Check if required columns exist
        if "date" not in df.columns or "close" not in df.columns:
            raise ValueError(
                "Input CSV does not contain the required fields: 'date' and 'close'")

        # Add a year column and process data
        df = df.withColumn("date", to_date("date", "yyyy-MM-dd"))
        df = df.withColumn("year", year("date"))

        # Save data partitioned by year
        df.write.partitionBy("year").mode("overwrite").parquet(output_path)
        print("Data successfully written to:", output_path)

        # Delete the CSV file from S3 after processing
        s3 = boto3.client("s3")
        bucket_name, key = csv_path.replace("s3://", "").split("/", 1)
        s3.delete_object(Bucket=bucket_name, Key=key)
        print(f"Deleted input CSV from S3: {csv_path}")

    except Exception as e:
        print(f"Error processing data: {e}")
        sys.exit(1)

    finally:
        spark.stop()
