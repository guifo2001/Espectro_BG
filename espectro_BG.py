import boto3
from io import StringIO
import logging
import pandas as pd
import pygeohash as pgh
akid=str(input('Agrege el Akid'))
asak=str(input('Agrege el asak'))

#s3://ookla-complete/intelligence/android/cell/android_cell_only_2020-11-01.zip
def uploadDataframe(dataFrame, keyName):
    csv_buffer = StringIO()
    dataFrame.to_csv(csv_buffer)
    s3_resource = boto3.resource(
        service_name='s3',
        region_name='us-east-2',
        aws_access_key_id=akid,
        aws_secret_access_key=asak
    )
    s3_resource.Object('ookla-siec-data', keyName).put(Body=csv_buffer.getvalue())


s3 = boto3.resource(
    service_name='s3',
    region_name='us-east-2',
        aws_access_key_id=akid,
        aws_secret_access_key=asak
)

logging.basicConfig(level=logging.INFO)
bucket = s3.Bucket('ookla-complete').objects.all()

s3_client = boto3.client(
    service_name='s3',
    region_name='us-east-2',
        aws_access_key_id=akid,
        aws_secret_access_key=asak
)
array = []

for obj in bucket:
    if f'bgv2/' in obj.key:
        array.append(obj.key)
lali=[]
for obj in array:
    if f'bgv2/' in obj and ('2021-04' in obj or '2021-05' in obj or '2021-06' in obj):
        #print(obj)
        logging.info(obj)
        objB = s3_client.get_object(Bucket='ookla-complete', Key=obj)
        cols=['connection_type','network_operator_mnc_code','client_latitude','client_longitude','earfcn','rsrp','cell_bandwidth','is_using_carrier_aggregation','cell_bandwidths']
        with io.BytesIO(objB["Body"].read()) as tf:
          df=pd.read_csv(tf, compression='zip',usecols=cols)
          df=df[df['connection_type']==15]
          df=df[df['network_operator_mnc_code']==140]
          df=df[df['client_latitude'].notnull()]
          df=df[df['client_latitude'].notnull()]
          df=df[(df['earfcn']>=9000) & (df['earfcn']<=10000)]
          df=df[(df['rsrp']<0) & (df['rsrp']>=-121)]
          df=df[df['is_using_carrier_aggregation']!='true']
          #df['band_def']=np.where(((df['cell_bandwidth'].notnull() & df['cell_bandwidths'].notnull()) & (df['cell_bandwidth']==df['cell_bandwidths']) | (df['cell_bandwidths'].notnull())),df['cell_bandwidths'],df['cell_bandwidth'])
          print(df.shape)
          print(df.columns)
          print(df.head())
          uploadDataframe(
            df,
            f'Espectro/Background_{obj[25:35]}_Espectro.csv')      
          