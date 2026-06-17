import requests
import json
from dotenv import load_dotenv
import os

load_dotenv()

API_KEY = os.getenv('API_KEY')

req = requests.get(
    'https://api.football-data.org/v4/competitions/2021/standings',
    headers={'X-Auth-Token': API_KEY}
    )
response = req.json()
table = response['standings'][0]['table']

print(json.dumps(table, indent=2))