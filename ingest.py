import requests
import os
from dotenv import load_dotenv
import pandas as pd
from datetime import datetime

load_dotenv()

API_KEY = os.getenv('API_KEY')

def main():
    req = requests.get(
        'https://api.football-data.org/v4/competitions/2021/standings',
        headers={'X-Auth-Token': API_KEY}
    )
    response = req.json()
    table = response['standings'][0]['table']

    df_normalized = pd.json_normalize(table)

    df_normalized.columns = [col.replace('team.', '') for col in df_normalized.columns]

    dt = datetime.today().strftime('%Y-%m-%d')

    os.makedirs(f'data/date={dt}', exist_ok=True)

    df_normalized.to_csv(f'data/date={dt}/standings.csv', index=False)

if __name__ == '__main__':
    main()