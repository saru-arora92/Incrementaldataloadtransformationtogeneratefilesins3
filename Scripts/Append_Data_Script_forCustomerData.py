import pandas as pd
import boto3
import numpy as np
import json
from io import StringIO
from io import BytesIO
import io
from botocore.exceptions import ClientError
from datetime import date, datetime

client = boto3.client('s3')
bucket_name = 'dsp-data-lake-dev'

bucket = boto3.resource('s3').Bucket(bucket_name)
objects = bucket.objects.all()

for object in objects:
    if object.key.startswith('demo/CLIENT_SW_PP_DEMO') and object.key.endswith('.txt'):
        demog = client.get_object(Bucket=bucket_name,
                                  Key=object.key)
        df2 = pd.read_csv(demog['Body'], sep="|")

##check Cust_mstr file
for object in objects:
    if object.key.startswith('demo/DM_CUST_MSTR') and object.key.endswith('.csv'):
        custmstr = client.get_object(Bucket=bucket_name,
                                     Key=object.key)
        df1 = pd.read_csv(custmstr['Body'], sep=",")
        df = pd.concat([df1, df2[~df2.Rel_ID.isin(df1.Rel_ID)]])
        df.update(df2)
        FileName = 'cust_mstr.csv'
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False)
        response = client.put_object(
            ACL='private',
            Body=csv_buffer.getvalue(),
            Bucket=bucket_name,
            Key=FileName
        )
    else:
        df2.index = np.arange(1, len(df2) + 1)
        df2.index.name = 'Cust_Id'
        FileName = 'cust_mstr.csv'
        csv_buffer = StringIO()
        df2.to_csv(csv_buffer)
        response = client.put_object(
            ACL='private',
            Body=csv_buffer.getvalue(),
            Bucket=bucket_name,
            Key=FileName
        )
