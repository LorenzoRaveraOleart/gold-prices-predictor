import datetime
import boto3
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import os
import tensorflow as tf
import pickle
import tempfile
import uuid
import sklearn

print("STARTING PREDICTION...")
print(f"tensorflow verison: {tf.__version__}")
print(f"sklearn verison: {sklearn.__version__}")


os.environ["PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION"] = "python"
os.environ["MPLCONFIGDIR"] = tempfile.mkdtemp()

bucket_name = os.environ.get("S3_BUCKET")


pred_len = 90
look_back = 3 * pred_len
s3_client = boto3.client('s3')


def lambda_handler(event, context):
    try:
        gold_model, scaler_gold = get_gold_model_and_scaler_from_s3()
    except Exception as e:
        return {
            "statusCode": 500,
            "body": f"Error getting model or scaler from S3: {str(e)}"
        }

    try:
        predict(gold_model, scaler_gold)
    except Exception as e:
        return {
            "statusCode": 500,
            "body": f"Error generating prediction: {str(e)}"
        }

    try:
        s3_client.upload_file("/tmp/gold_prediction.png",
                              bucket_name, "prediction/gold_prediction.png")
        return {
            "statusCode": 200,
            "body": f"File uploaded to bucket '{bucket_name}'"
        }
    except Exception as e:
        return {
            "statusCode": 500,
            "body": f"Error uploading file: {str(e)}"
        }


# Load model and scaler from S3 (model is 'model.h5' and scaler is 'scaler.pkl')
def get_gold_model_and_scaler_from_s3():
    # The model is stored in the 'model' directory with the name 'model.h5'
    model_key = "model_artifacts/model.h5"
    # The scaler is stored in the same directory with the name 'scaler.pkl'
    scaler_key = "model_artifacts/scaler.pkl"

    local_model_path = "/tmp/gold_model.h5"
    local_scaler_path = "/tmp/scaler.pkl"

    # Download model (.h5) and scaler (.pkl) from S3
    s3_client.download_file(bucket_name, model_key, local_model_path)
    s3_client.download_file(bucket_name, scaler_key, local_scaler_path)
    print("Model and scaler downloaded from S3")

    # Load model and scaler
    gold_model = tf.keras.models.load_model(local_model_path, compile=True)

    with open(local_scaler_path, 'rb') as f:
        scaler_gold = pickle.load(f)

    print("Model and scaler loaded successfully.")
    return gold_model, scaler_gold


def predict(gold_model, scaler_gold):
    X_gold_ = gold_data_processing(scaler_gold, look_back)
    print("Predicting gold prices.....")
    print(f"predicting with data: {X_gold_[:5]}")
    gold_pred = gold_model.predict(X_gold_).reshape(-1, 1)
    print("Prediction finished!")
    print(f"prediction size: {len(gold_pred)}")
    print(f"prediction: {gold_pred}")
    gold_pred = scaler_gold.inverse_transform(gold_pred)

    # Save the plot after prediction
    save_prediction_image("/tmp/gold_prediction.png",
                          X_gold_, gold_pred, scaler_gold)


# Process gold data from parquet files in S3 directory (divided by year)
def gold_data_processing(scaler, look_back):
    parquet_dir = "feature-store/"
    local_parquet_dir = "/tmp/gold_data"

    if not os.path.exists(local_parquet_dir):
        os.makedirs(local_parquet_dir)

    print(f"Downloading parquet files to {local_parquet_dir}...")
    response = s3_client.list_objects_v2(
        Bucket=bucket_name, Prefix=parquet_dir)

    for obj in response.get('Contents', []):
        key = obj['Key']
        # Generate a unique file name
        unique_file_name = f"{uuid.uuid4().hex}.parquet"
        local_file_path = os.path.join(local_parquet_dir, unique_file_name)
        if (key != 'feature-store_SUCCESS'):
            s3_client.download_file(bucket_name, key, local_file_path)
            print(f"Downloaded {key} to {local_file_path}")

    # Load all parquet files into a single DataFrame

    df_gold = pd.read_parquet(local_parquet_dir)

    # Sort and reset index
    df_gold['date'] = pd.to_datetime(df_gold['date']).dt.date

    df_gold = df_gold.sort_values(by="date")
    df_gold.reset_index(drop=True, inplace=True)
    print(df_gold.tail())
    # Extract the 'close' column
    y_gold = df_gold['close'].values.reshape(-1, 1)

    # Scale the data using the provided scaler
    y_scaled = scaler.transform(y_gold)

    print(f'scaled data has non values : {np.isnan(y_scaled).any()}')

    # Extract the last `look_back` values and reshape them for prediction
    X_gold = y_scaled[-look_back:].reshape(1, look_back, 1)
    print(f"Prepared input shape: {X_gold.shape}")

    return X_gold


# Plot prediction results
def plot_gold(x_gold, gold_pred, gold_scaler):
    print("Creating gold prediction plot...")

    fig = plt.figure()
    ax = fig.add_subplot(111)
    X_gold_1 = x_gold.reshape(look_back, 1)
    X_gold_2 = gold_scaler.inverse_transform(X_gold_1)
    total = np.append(X_gold_2, gold_pred)

    ax.plot(total)
    total_length = len(total)

    # Overlay the last 90 elements in red
    ax.plot(range(total_length-91, total_length),
            total[total_length-91:], color='red', label='Prediction for the next 90 days')

    # Set titles and labels
    ax.set_title('Gold Value Over Time')
    ax.set_xlabel('Days')
    ax.set_ylabel('Price')

    label = f'Today {datetime.date.today()}'
    ax.axvline(x=total_length - 92, color='gray', linestyle='--', label=label)
    ax.legend(loc='best')

    print("Saving gold prediction plot...")
    return fig


def save_prediction_image(image_path, x_gold, gold_pred, scaler_gold):
    fig = plot_gold(x_gold, gold_pred, scaler_gold)
    fig.savefig(image_path)
    print(f"Prediction plot saved to {image_path}")
