#!/bin/bash

# Install pip and required Python libraries
sudo python3 -m ensurepip --upgrade
sudo python3 -m pip install --upgrade pip
sudo python3 -m pip install tensorflow scikit-learn pandas protobuf==3.20.3 urllib3==1.26.20 boto3

