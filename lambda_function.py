from bs4 import BeautifulSoup
from botocore.vendored import requests
import itertools
import pandas as pd
import snowflake.connector
from snowflake.connector import connect


# Connect to Snowflake
try:
    conn = snowflake.connector.connect(
        user='vaivishal',
        password='Deloitte@123',
        account='EIB51757.us-east-1',
        warehouse='COMPUTE_WH',
        database='HANGMAN',
        schema='HANGMAN_SCHEMA'
    )
    
    cursor = conn.cursor()
    cursor
    print("Connected")
except:
    print("Invaid username or password")

source = requests.get('https://www.rottentomatoes.com/').text
soup = BeautifulSoup(source, 'lxml')

POPULAR_IN_THEATERS = []
for movies in soup.find_all('span', attrs={'class' : 'p--small'}):
  POPULAR_IN_THEATERS.append(movies.text)

# print(popular_In_Theaters)


POPULAR_STREAMING_MOVIES = []
for movies in soup.find_all('div', attrs={'data-curation' : 'rt-hp-text-list-popular-streaming-movies'}):
  new_span = movies.find_all('span', attrs={'class' : 'dynamic-text-list__item-title clamp clamp-1'})
  for span in new_span:
    POPULAR_STREAMING_MOVIES.append(span.text)


MOST_POPULA_TV_ON_RT = []
for movies in soup.find_all('div', attrs={'data-curation' : 'rt-hp-text-list-most-popular-tv-on-rt'}):
  new_span = movies.find_all('span', attrs={'class' : 'dynamic-text-list__item-title clamp clamp-1'})
  for span in new_span:
    MOST_POPULA_TV_ON_RT.append(span.text)


# Flatten the three lists into a single list
flat_list = list(itertools.chain(POPULAR_IN_THEATERS, POPULAR_STREAMING_MOVIES, MOST_POPULA_TV_ON_RT))


# Create a list of category names by repeating the original list names
category_list = (["POPULAR IN THEATERS THIS WEEK"] * len(POPULAR_IN_THEATERS)) + (["POPULAR STREAMING MOVIES THIS WEEK ON OTT"] * len(POPULAR_STREAMING_MOVIES)) + (["MOST POPULAR TV SHOWS THIS WEEK"] * len(MOST_POPULA_TV_ON_RT))

df = pd.DataFrame({"WORD": flat_list, "CATEGORY": category_list})

# Write the DataFrame to a sheet named "Sheet1" in the Excel file
df.to_excel("data.xlsx", sheet_name="Sheet1", index=False)

try:
  for i in range(len(df)):
    WORD = df.loc[i,"WORD"]
    CATEGORY = df.loc[i,"CATEGORY"]
    if "'" in WORD:
      WORD = WORD.replace("'", "''")
    insert_sql = f"INSERT INTO HANGMAN.HANGMAN_SCHEMA.WORDS_FOR_HANGMAN (WORD, CATEGORY) VALUES ('{WORD}', '{CATEGORY}')"
    print(insert_sql)
    conn.cursor().execute(insert_sql)

except Exception as e:
  print("Error occured")
  print(insert_sql)

conn.close()