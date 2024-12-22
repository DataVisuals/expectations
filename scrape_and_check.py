import requests 
from bs4 import BeautifulSoup
import json
import pprint
from rules import DBT_RULES

headers = {'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/85.0.4183.121 Safari/537.36'}

url = "https://github.com/calogica/dbt-expectations?tab=readme-ov-file#expect_column_values_to_be_in_type_list"
r = requests.get(url, headers=headers)
r_html = r.text
soup = BeautifulSoup(r_html, "html.parser")

spans = soup.find_all("span", class_="pl-ent")
expectations = [span.text for span in spans if span.text.startswith("dbt_")]
for e in expectations:
    if e not in DBT_RULES:
        print(f"{e}")

