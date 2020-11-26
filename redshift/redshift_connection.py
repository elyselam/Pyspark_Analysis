import pandas as pd
import boto3
import json

import configparser

config = configparser.ConfigParser()
config.read_file(open('dwh.cfg'))

KEY = config.get('AWS', 'KEY')
SECRET = config.get('AWS', 'SECRET')

DWH_CLUSTER_TYPE = config.get("DWH", "DWH_CLUSTER_TYPE")
DWH_NUM_NODES = config.get("DWH", "DWH_NUM_NODES")
DWH_NODE_TYPE = config.get("DWH", "DWH_NODE_TYPE")

DWH_CLUSTER_IDENTIFIER = config.get("DWH", "DWH_CLUSTER_IDENTIFIER")
DWH_DB = config.get("DWH", "DWH_DB")
DWH_DB_USER = config.get("DWH", "DWH_DB_USER")
DWH_DB_PASSWORD = config.get("DWH", "DWH_DB_PASSWORD")
DWH_PORT = config.get("DWH", "DWH_PORT")

DWH_IAM_ROLE_NAME = config.get("DWH", "DWH_IAM_ROLE_NAME")

(DWH_DB_USER, DWH_DB_PASSWORD, DWH_DB)

pd.DataFrame({"Param":
                  ["DWH_CLUSTER_TYPE", "DWH_NUM_NODES", "DWH_NODE_TYPE", "DWH_CLUSTER_IDENTIFIER", "DWH_DB",
                   "DWH_DB_USER", "DWH_DB_PASSWORD", "DWH_PORT", "DWH_IAM_ROLE_NAME"],
              "Value":
                  [DWH_CLUSTER_TYPE, DWH_NUM_NODES, DWH_NODE_TYPE, DWH_CLUSTER_IDENTIFIER, DWH_DB, DWH_DB_USER,
                   DWH_DB_PASSWORD, DWH_PORT, DWH_IAM_ROLE_NAME]
              })



ec2 = boto3.resource('ec2',
                     region_name="us-west-2",
                     aws_access_key_id=KEY,
                     aws_secret_access_key=SECRET
                     )

s3 = boto3.resource('s3',
                    region_name="us-west-2",
                    aws_access_key_id=KEY,
                    aws_secret_access_key=SECRET
                    )

iam = boto3.client('iam', aws_access_key_id=KEY,
                   aws_secret_access_key=SECRET,
                   region_name='us-west-2'
                   )

redshift = boto3.client('redshift',
                        region_name="us-west-2",
                        aws_access_key_id=KEY,
                        aws_secret_access_key=SECRET
                        )

sampleDbBucket = s3.Bucket("awssampledbuswest2")
for obj in sampleDbBucket.objects.filter(Prefix="ssbgz"):
    print(obj)
