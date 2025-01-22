from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import numpy as np
import tensorflow as tf
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import LSTM, Dense
from sklearn.preprocessing import MinMaxScaler
import sys
import os
import boto3
import joblib


def train_and_save_model(data, n_lookback, n_forecast, epochs, batch_size, model_save_path, scaler_save_path, bucket_name):
    # Prepare the data for LSTM
    y = data['close'].values.reshape(-1, 1)
    scaler = MinMaxScaler(feature_range=(0, 1))
    y_scaled = scaler.fit_transform(y)

    X, Y = [], []
    for i in range(n_lookback, len(y_scaled) - n_forecast + 1):
        X.append(y_scaled[i - n_lookback: i])
        Y.append(y_scaled[i: i + n_forecast])

    X, Y = np.array(X), np.array(Y)
    print(f"Training data shapes: X={X.shape}, Y={Y.shape}")

    # Define the LSTM model
    model = Sequential([
        LSTM(units=128, return_sequences=True, input_shape=(n_lookback, 1)),
        LSTM(units=64, return_sequences=True),
        LSTM(units=64, return_sequences=False),
        Dense(n_forecast)
    ])
    model.compile(loss='mean_squared_error', optimizer='adam')

    # Train the model
    model.fit(X, Y, epochs=epochs, batch_size=batch_size, verbose=1)

    # ----------------------------------------------------------
    model_path = "/tmp/model.h5"
    scaler_path = "/tmp/scaler.pkl"

    model.save(model_path)
    joblib.dump(scaler, scaler_path)

    s3 = boto3.client('s3')
    model_s3_key = os.path.join(model_save_path, "model.h5")
    scaler_s3_key = os.path.join(model_save_path, "scaler.pkl")

    s3.upload_file(model_path, bucket_name, model_s3_key)
    s3.upload_file(scaler_path, bucket_name, scaler_s3_key)
    # ----------------------------------------------------------

    print(f"Model saved to: {model_save_path}")
    np.save(scaler_save_path, scaler)
    print(f"Scaler saved to: {scaler_save_path}")


if __name__ == "__main__":
    # Initialize Spark session
    spark = SparkSession.builder.appName("TimeSeriesForecasting").getOrCreate()
    print("arguments:-----------------------------------")
    for arg in sys.argv:
        print(arg)
    print("arguments:-----------------------------------")

    try:
        # Parse arguments
        input_data_path = sys.argv[6]
        model_output_path = sys.argv[1]
        epochs = int(sys.argv[3])
        batch_size = int(sys.argv[5])
        bucket_name = sys.argv[7]
        n_forecast = 90
        n_lookback = 3 * n_forecast  # Lookback period is 3 times the forecast period

        # Read the partitioned parquet data
        df = spark.read.parquet(input_data_path)
        df = df.select("date", "close").orderBy("date")

        # Convert the data to Pandas for model training
        data = df.toPandas()

        # Validate data
        if "close" not in data.columns:
            raise ValueError("Input data does not contain the 'close' column.")

        # Train the model and save artifacts
        train_and_save_model(
            data=data,
            n_lookback=n_lookback,
            n_forecast=n_forecast,
            epochs=epochs,
            batch_size=batch_size,
            model_save_path=model_output_path,
            scaler_save_path=model_output_path,
            bucket_name=bucket_name
        )

    except Exception as e:
        print(f"Error during training: {e}")
        sys.exit(1)

    finally:
        spark.stop()
