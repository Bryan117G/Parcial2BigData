import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import json
import boto3
import re
from io import StringIO
from datetime import datetime, timedelta
import requests
import pandas as pd
import urllib.request
import time
from bs4 import BeautifulSoup
import ast

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node S3 bucket
S3bucket_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={"paths": ["s3://parcialbryangaravito"], "recurse": True},
    transformation_ctx="S3bucket_node1",
)

# Script generated for node ApplyMapping
ApplyMapping_node2 = ApplyMapping.apply(
    frame=S3bucket_node1, mappings=[], transformation_ctx="ApplyMapping_node2"
)

# Script generated for node S3 bucket
S3bucket_node3 = glueContext.write_dynamic_frame.from_options(
    frame=ApplyMapping_node2,
    connection_type="s3",
    format="json",
    connection_options={"path": "s3://parcialbryangaravito", "partitionKeys": []},
    transformation_ctx="S3bucket_node3",
)

s3 = boto3.client('s3')
ahora = datetime.now()
papers = ['eltiempo','elespectador']

for i in papers:
    name = '{}_{}_{}_{}'.format(i,ahora.year,ahora.month,ahora.day)
    r = requests.get('https://www.{}.com/'.format(i))
    doc = open("/tmp/doc.txt","w")
    doc.write(r.text)
    doc.close()
    meses = ['enero','febrero','marzo','abril','mayo','junio']
    ruta = 'news/raw/periodico={}/year={}/month={}/day={}/{}.txt'.format(i,ahora.year,meses[ahora.month-1],ahora.day,name)
    s3.upload_file("/tmp/doc.txt","parcialbryangaravito",ruta)
    s3.upload_file("/tmp/doc.txt","parcialbryangaravito","headlines/raw/{}.txt".format(i))

job.commit()
