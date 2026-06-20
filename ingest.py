from dotenv import load_dotenv
from datetime import datetime
import requests
import os
import pandas as pd
import boto3

load_dotenv()

API_KEY = os.getenv('API_KEY')

s3 = boto3.client('s3')

def fetch_data():
    req = requests.get(
        'https://api.football-data.org/v4/competitions/2021/standings',
        headers={'X-Auth-Token': API_KEY}
    )
    response = req.json()
    table = response['standings'][0]['table']

    df_normalized = pd.json_normalize(table)
    df_normalized.columns = [col.replace('team.', '') for col in df_normalized.columns]
    return df_normalized

def save_local(df, dt):
    path = f'data/date={dt}/standings.csv'
    os.makedirs(f'data/date={dt}', exist_ok=True)
    df.to_csv(path, index=False)
    return path

def upload_to_s3(local_path, bucket, key):
    s3.upload_file(local_path, bucket,key)

def main():
    df = fetch_data()
    dt = datetime.today().strftime('%Y-%m-%d')
    path = save_local(df, dt)
    upload_to_s3(
        path,
        'premier-league-analysis-bucket',
        f'landing/date={dt}/standings.csv'
    )

if __name__ == '__main__':
    main()