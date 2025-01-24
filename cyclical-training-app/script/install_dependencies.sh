#!/bin/bash

# Install pip and required Python libraries
sudo python3 -m ensurepip --upgrade
sudo python3 -m pip install --upgrade pip
sudo python3 -m pip install tensorflow==2.11.0 scikit-learn==1.0.1 pandas==1.3.5 urllib3==1.26.20 boto3

