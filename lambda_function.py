from bs4 import BeautifulSoup
import requests
import itertools
import pandas as pd
import boto3
from io import BytesIO


def lambda_handler(event, context):
    
    source = requests.get('https://www.rottentomatoes.com/').text
    soup = BeautifulSoup(source, 'lxml')

    POPULAR_IN_THEATERS = []
    for movies in soup.find_all('span', attrs={'class' : 'p--small'}):
        POPULAR_IN_THEATERS.append(movies.text)

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

    # Write the DataFrame to an Excel file
    excel_file = BytesIO()
    df.to_excel(excel_file, sheet_name="Sheet1", index=False)
    excel_file.seek(0)
    
    # Upload the file to S3 bucket
    s3 = boto3.resource('s3')
    bucket_name = 'vaivishal-s3-bucket'
    file_name = 'data.xlsx'
    s3.Bucket(bucket_name).put_object(Key=file_name, Body=excel_file.read(), ContentType='application/vnd.ms-excel')

    return {
        'statusCode': 200,
        'body': 'Data saved successfully!'
    }
