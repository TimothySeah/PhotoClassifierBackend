import numpy as np
from multiprocessing import Process

from constants import *
import pickleML

from django.apps import apps
from django.http import HttpResponse
from django.views.decorators.csrf import csrf_exempt

import boto3

@csrf_exempt
def testing(request):

    return HttpResponse("testing")

@csrf_exempt
def sendfeatures(request):

    # json contains "label", "category", "features", "userid", and "
    # overfishing: this is a minimal version to go through one workflow execution
    # overfishing: later on i will add rds integration and other good stuff
    # note: files have names like userid:X, userid:cov, userid:mean, etc
    if request.method == "POST":

        # load relevant resources
        mydict = json.loads(request.body)
        if not ('password' in mydict and mydict['password'] == PASSWORD):
            return HttpResponse("Wrong Password")
        s3 = boto3.resource('s3')
        bucket = s3.Bucket(BUCKET_NAME)

        ## step 1: save extra features in s3

        # update feature data
        data = get_pickle(mydict['userid'] + ".pkl", bucket)
        if data == {}:
            pass # do stuff if NOT THERE AT ALL (initialization failed)
        elif 'features' not in data or 'labels' not in data:
            data['features'] = mydict['features']
            data['labels'] = [mydict['label']]
        else:
            data['features'] = np.append(data['features'], mydict['features'], axis=1)
            data['labels'].append(mydict['label'])
        put_pickle(mydict['userid'] + ".pkl", data, bucket)

        # update miscinfo
        misc_info = get_json(MISC_INFO, bucket)
        misc_info['numpoints'] += 1
        put_json(MISC_INFO, misc_info, bucket)

        #--------

        ## step 2: check if data size has reached next benchmark.
        ## if data size has reached next benchmark, make new classifier
        classifier_lock = apps.get_app_config('inventory').classifier_lock
        if misc_info['numpoints'] >= misc_info['rerunat'] and classifier_lock.acquire(block=False):

            # update rerunat now so wont get called again
            misc_info['rerunat'] *= 2 # overfishing: not sure when to rerun again
            put_json(MISC_INFO, misc_info, bucket)

            # update classifier asynchronously, release lock when done
            p = Process(target=updateClassifier, args=(classifier_lock,))
            p.start()
        else:
            if misc_info['numpoints'] < misc_info['rerunat']:
                print "RERUNAT LATER"
            else:
                print "failed to get lock"

        return HttpResponse("POST RESPONSE")
    return HttpResponse("SHOULD NOT REACH HERE")

# json request contains "userid"
@csrf_exempt
def init(request):
    if request.method == "POST":
        mydict = json.loads(request.body)
        if not ('password' in mydict and mydict['password'] == PASSWORD):
            return HttpResponse("Wrong Password")
        bucket = boto3.resource('s3').Bucket(BUCKET_NAME)

        # send Z
        Z = get_pickle(Z_FILE, bucket)
        return HttpResponse(json.dumps(Z.tolist()), content_type="application/json")

    return HttpResponse("GET does not do anything for init")

# json request contains "userid" and "Zu"
@csrf_exempt
def init2(request):
    if request.method == "POST":
        bucket = boto3.resource('s3').Bucket(BUCKET_NAME)
        mydict = json.loads(request.body)
        if not ('password' in mydict and mydict['password'] == PASSWORD):
            return HttpResponse("Wrong Password")

        # upload Tu
        Z = get_pickle(Z_FILE, bucket)
        data = get_pickle(mydict['userid'] + ".pkl", bucket)
        data['T'] = pickleML.findTu(Z, mydict['Zu'])
        put_pickle(mydict['userid'] + ".pkl", data, bucket)

        # update list of users
        misc_info = get_json(MISC_INFO, bucket)
        misc_info['users'].append(mydict['userid'])
        put_json(MISC_INFO, misc_info, bucket)

        return HttpResponse("Uploaded T and updated list of users")

    return HttpResponse("GET does not do anything for init2")

@csrf_exempt
def meancov(request):

    # json contains 'means' and 'covs' 2d arrays, as well as 'userid'
    if request.method == "POST":
        mydict = json.loads(request.body)
        if not ('password' in mydict and mydict['password'] == PASSWORD):
            return HttpResponse("Wrong Password")
        bucket = boto3.resource('s3').Bucket(BUCKET_NAME)
        put_pickle(mydict['userid'] + ":means.pkl", mydict['means'], bucket)
        put_pickle(mydict['userid'] + ":covs.pkl", mydict['covs'], bucket)
        return HttpResponse("response from meancov")

    return HttpResponse("did not do post request on meancov")

## --------------------------------


# update classifier
# to be called asynchronously
# launch cluster, submit classifier construction/modification to cluster, terminate cluster
def updateClassifier(classifier_lock):

    # launch spark cluster
    # overfishing WORKS
    """
    os.system("export AWS_SECRET_ACCESS_KEY={0} export AWS_ACCESS_KEY_ID={1}".format(
        aws_secret_access_key, aws_access_key_id))
    os.system("{0}/spark-ec2 --key-pair=learn --identity-file={0}/learn.pem "
              "--region=us-west-2 --slaves=1 --spark-version=1.5.2 "
              "launch {1}".format(EC2_HOME, CLUSTER_NAME))
    """

    # submit script locally
    os.system("{0}/spark-submit "
              "--class org.apache.spark.examples.SparkPi "
              "--master local[2] "
              "{1}".format(BIN_HOME, SCRIPT))

    # terminate cluster
    # overfishing UNTESTED
    # os.system("{0}/spark-ec2 destroy {1}".format(EC2_HOME, CLUSTER_NAME))

    classifier_lock.release()

