client = boto3.client('s3')
bucket_name = 'dsp-data-lake-dev'

bucket = boto3.resource('s3').Bucket(bucket_name)
objects = bucket.objects.all()
for object in objects:
    if object.key.startswith('raw/external/SHA/batch/weekly/07272020/CLIENT_SW_PP_DEMO') and object.key.endswith('.txt'):
        demog = client.get_object(Bucket=bucket_name,
                    Key=object.key)
        demog_df = pd.read_csv(demog['Body'], sep="|")
    if object.key.startswith('raw/external/SHA/batch/weekly/07272020/CLIENT_SW_PP_DATA') and object.key.endswith('.txt'):
        ppdata= client.get_object(Bucket=bucket_name,
                      Key=object.key)
        ppdata_df = pd.read_csv(ppdata['Body'], sep="|")
print(demog_df)
print(ppdata_df)