#!/bin/bash

curl https://bootstrap.pypa.io/get-pip.py -o get-pip.py
python get-pip.py
pip install --upgrade google-api-python-client google-auth-httplib2 google-auth-oauthlib
python -m pip install google-api-python-client google-auth-httplib2 google-auth-oauthlib --user