# Use the official AWS Lambda Python 3.9 runtime base image
FROM public.ecr.aws/lambda/python:3.9

# Copy your application code to the container
COPY app/ ${LAMBDA_TASK_ROOT}

# Set the working directory
WORKDIR ${LAMBDA_TASK_ROOT}

# Install Python dependencies
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt -t ${LAMBDA_TASK_ROOT}

# Set the Lambda function handler
# Replace 'lambda_function.handler' with your actual handler module and function
CMD ["lambda_function.lambda_handler"]

