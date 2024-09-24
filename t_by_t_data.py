import yfinance as yf
import pandas as pd
import time
from datetime import datetime

import csv
import os
from google.cloud import storage

def upload_csv_to_bucket(data):
    # Set your bucket name
    bucket_name = 'tickbytick1'
    
    # Create a CSV file in memory
    csv_file_name = 'data.csv'
    csv_file_path = f'/tmp/{csv_file_name}'  # Use /tmp for temporary storage in Cloud Functions

    with open(csv_file_path, mode='w', newline='') as csv_file:
        writer = csv.writer(csv_file)
        writer.writerows(data)
    
    # Initialize the Cloud Storage client
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    
    # Create a blob and upload the CSV file
    blob = bucket.blob(csv_file_name)
    blob.upload_from_filename(csv_file_path)

    return f'Successfully uploaded {csv_file_name} to {bucket_name}.'


def fetch_and_append_data(scrip, csv_file):
    """
    Fetch data from Yahoo Finance and append it to a CSV file.

    Parameters:
    scrip (str): The ticker symbol of the stock.
    csv_file (str): The path to the CSV file.
    """
    # Fetch data
    info = yf.Ticker(scrip).info
    
    # Add a timestamp to the data
    info['timestamp'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print (f"Info: {info}")
    info['ltp'] = info['currentPrice']

    
    # Convert the info dictionary to a DataFrame
    df = pd.DataFrame([info])
        # Keep only the desired columns
    df = df[['timestamp', 'ltp']]
    # Append the data to the CSV file
   # df.to_csv(csv_file, mode='a', header=not pd.io.common.file_exists(csv_file), index=False)
    upload_csv_to_bucket(info)

# List of scrips
scrips = ['NYKAA.NS', 'CDSL.NS', 'HINDUNILVR.NS', 'TATAMOTORS.NS', 'RELIANCE.NS']

# Create the CSV files with headers if they don't exist
for scrip in scrips:
    csv_file = f'{scrip}_per_second_data.csv'
    if not pd.io.common.file_exists(csv_file):
        info = yf.Ticker(scrip).info
        info['timestamp'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        info['close'] = 0
        df = pd.DataFrame([info])
        df.to_csv(csv_file, index=False)

# Run the loop to fetch and append data every second
try:
    while True:
        for scrip in scrips:
            csv_file = f'{scrip}_per_second_data.csv'
            fetch_and_append_data(scrip, csv_file)
        time.sleep(1)
except KeyboardInterrupt:
    print("Data fetching stopped.")
    print("hi")
