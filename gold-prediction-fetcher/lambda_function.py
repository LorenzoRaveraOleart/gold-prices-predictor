import boto3
import base64

def lambda_handler(event, context):
    """
    Fetch a PNG file from S3 and return it as a response.
    """
    # S3 bucket and file key
    bucket_name = "gold-prices-predictor"
    file_key = "prediction/gold_prediction.png"
    
    # Initialize S3 client
    s3 = boto3.client('s3')
    
    try:
        # Get the file from S3
        response = s3.get_object(Bucket=bucket_name, Key=file_key)
        file_content = response['Body'].read()

        # Encode the file content as base64
        encoded_image = base64.b64encode(file_content).decode('utf-8')
        
        # Return the image in the HTTP response
        return {
            "statusCode": 200,
            "headers": {
                "Content-Type": "image/png",
                "Content-Disposition": "inline; filename=image.png"
            },
            "body": encoded_image,
            "isBase64Encoded": True
        }
    except Exception as e:
        return {
            "statusCode": 500,
            "body": f"Error: {str(e)}"
        }
