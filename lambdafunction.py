import boto3
from datetime import datetime, timezone
import json
import urllib3
import requests
import csv
import pandas as pd
import io
from io import StringIO


def lambda_handler(event, context):
    #Write Name of S3 Bucket Here
    S3_BUCKET = "dataswan"
    
    #name of final csv
    filename = 'Raw_reviews/review_data.csv'
    #empty list to store dataframes
    df_list = []
    
    #api url
    url = "https://app.datashake.com/api/v2/profiles/reviews"
    #token
    headers = {
    "spiderman-token": "4226c4cae1d3f632ce5b9b156c9c5bd98e2a07f4"}
    
    #loop to read in each job request
    for job in range(539031734,539032295):
        params = {
        "job_id": str(job) }

        response = requests.get(
        url=url,
        params=params,
        headers=headers)
        
        #get response from API
        try:
            read = response.json()
        
            #Get reviews info from response and turn it into a df
            if len(read['reviews']) == 0: #if no reviews, skip
                continue
        except:
            continue
            
        temp_df = pd.DataFrame.from_dict(read['reviews'])
        
        #create a new column containing doctors name for each loop
        str_meta = read['meta_data']
        
        temp_df['sourceurl']=read['source_url']
        
        #if no metadata, name is blank
        if str_meta is None:          
            temp_df["doctor"] = None
        else:
            temp_df["doctor"] = json.loads(str_meta)['name']
            
        #add dataframe to empty list
        df_list.append(temp_df)
    
    #combine lists to make final df
    df = pd.concat(df_list,axis = 0,ignore_index = True)
    
    #write to s3
    csv_buffer = StringIO()
    df.to_csv(csv_buffer)
    client = boto3.client("s3")
    
    
    client.put_object(Bucket = S3_BUCKET, Key = filename, Body = csv_buffer.getvalue())
    
    print('Complete!')
