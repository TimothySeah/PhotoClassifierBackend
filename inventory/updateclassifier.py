__author__ = 'timothyseah'

from pickleML import *
from constants import *

from pyspark import SparkContext

# set up
sc = SparkContext("local", "Pickle")
s3 = boto3.resource('s3')
client = boto3.client('s3')
bucket = s3.Bucket(BUCKET_NAME)

# create data frame
Xmodus = {}
yus = {}
Tus = {}
json = get_json(MISC_INFO, bucket)
for user in json['users']:
    data = get_pickle(user + ".pkl", bucket)
    if data == {} or 'features' not in data or 'labels' not in data or 'T' not in data:
        continue
    Xmodus[user] = np.asarray(data['features'])
    yus[user] = data['labels']
    Tus[user] = np.asarray(data['T'])
rdd = createRDDandDict(Xmodus, Tus, yus, LABELTOINT["irises"], sc)

# create and update classifier, dict
# overfishing irises for now
rfcm = createRFC(rdd, NUM_CLASSES["irises"], {}) # overfishing generalize
put_rfc_model(RFCM, rfcm, bucket, client, sc)