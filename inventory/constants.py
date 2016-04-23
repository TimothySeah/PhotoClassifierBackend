"""
Store constants and utility functions relevant throughout the application
"""
import boto3
import botocore
import pickle
import json
import shutil
import os
import re
import zipfile
from pyspark.mllib.tree import RandomForest, RandomForestModel

PASSWORD = "mFnfWD6tZfrsAAt" # overfishing simple security mechanism
BUCKET_NAME = "pcirisdata"
CLUSTER_NAME = "my-spark-cluster"

MISC_INFO = "MiscInfo"
Z_FILE = "Z.pkl"

# warning: do not include numbers in the title
# numbers are used for regular expression matching on the version
RFCM = "RFCM"

NUM_CLASSES = {"irises": 3}
LABELTOINT = {"irises": {"Iris setosa": 0, "Iris virginica": 1, "Iris versicolor": 2}}
INTTOLABEL = ["Iris setosa", "Iris virginica", "Iris versicolor"]

aws_access_key_id = "AKIAIAUNBLRII6Q3BY4Q"
aws_secret_access_key = "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"

EC2_HOME = "spark-1.5.2-bin-hadoop2.6/ec2"
BIN_HOME = "spark-1.5.2-bin-hadoop2.6/bin"
SCRIPT = "inventory/updateclassifier.py"

# download FILENAME from s3
# return true on success, false on failure
def download_file(filename, bucket):
    local_url = '/tmp/' + filename
    try:
        bucket.download_file(filename, local_url)
    except botocore.exceptions.ClientError as e:
        print(e.response)
        print('need to create ' + filename)
        return False
    else:
        return True

# download filename from s3 and return unpickled data
def get_pickle(filename, bucket):
    local_url = '/tmp/' + filename
    if download_file(filename, bucket):
        with open(local_url, 'rb') as file:
            return pickle.load(file)
    else:
        return {}

# download filename from s3 and return unjsoned data
def get_json(filename, bucket):
    local_url = '/tmp/' + filename
    if download_file(filename, bucket):
        with open(local_url, 'rb') as file:
            return json.load(file)
    else:
        return {}

# download latest rfc model from s3 and return rfc model
def get_rfc_model(filename, bucket, client, sc):

    # get download_url
    objects_dict = client.list_objects(Bucket=BUCKET_NAME)
    filenames = map(lambda x: x["Key"], objects_dict["Contents"])
    filenames = filter(lambda x: True if re.match(filename + '-v' + "[0-9]+", x) else False, filenames)
    if len(filenames) == 0:
        print "NO RFC MODELS FOUND"
        return False
    else:
        versions = map(lambda x: int(re.search("[0-9]+", x).group()), filenames)
        download_url = filename + "-v" + str(max(versions)) + '.zip'

    # download zip and unzip it
    local_url = '/tmp/' + download_url
    os.system("rm -r " + local_url) # remove zip just in case
    os.system("rm -r " + local_url[:-4]) # remove unzipped folder just in case
    if download_file(download_url, bucket):
        with zipfile.ZipFile(local_url, "r") as z:
            z.extractall(local_url[:-4])
        return RandomForestModel.load(sc, local_url[:-4]) #-4 for zip
    else:
        print "ZIP DOWNLOAD FAILED"
        return False


# upload FILENAME with DATA to s3
def put_pickle(filename, data, bucket):
    local_url = '/tmp/' + filename
    with open(local_url, 'wb') as f:
        pickle.dump(data, f)
    bucket.upload_file(local_url, filename)

# upload FILENAME with DATA to s3
def put_json(filename, data, bucket):
    local_url = '/tmp/' + filename
    with open(local_url, 'wb') as f:
        json.dump(data, f)
    bucket.upload_file(local_url, filename)

# upload DIRNAME with rfcm to s3
# tested successfully
def put_rfc_model(dirname, rfcm, bucket, client, sc):

    # get version number
    objects_dict = client.list_objects(Bucket=BUCKET_NAME)
    filenames = map(lambda x: x["Key"], objects_dict["Contents"])
    filenames = filter(lambda x: True if re.match(dirname + '-v' + "[0-9]+", x) else False, filenames)
    if len(filenames) == 0:
        new_vno = 1
    else:
        versions = map(lambda x: int(re.search("[0-9]+", x).group()), filenames)
        new_vno = max(versions) + 1

    # write and zip locally, then upload
    local_url = '/tmp/' + dirname + "-v" + str(new_vno)
    local_url_zip = local_url + '.zip'
    upload_url_zip = dirname + "-v" + str(new_vno) + '.zip'
    os.system("rm -r " + local_url) # remove folder just in case
    os.system("rm -r " + local_url_zip) # remove zip file just in case
    rfcm.save(sc, local_url)
    shutil.make_archive(local_url, 'zip', local_url)
    bucket.upload_file(local_url_zip, upload_url_zip)