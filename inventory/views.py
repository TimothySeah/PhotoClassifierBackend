import numpy as np
import os

from constants import *
import pickleML

from django.shortcuts import render
from django.http import HttpResponse
from django.http import Http404
from django.views.decorators.csrf import csrf_exempt

import boto3
import botocore

# overfishing: in conjunction with decorator use auth mech like from drf
@csrf_exempt
def index(request):

    # overfishing: test get method that writes spark stuff to s3
    if request.method == "GET":
        print("accessed by get")

        return HttpResponse("GET RESPONSE")

    # json contains "label", "category", "features", and "userid"
    # overfishing: this is a minimal version to go through one workflow execution
    # overfishing: later on i will add rds integration and other good stuff
    # note: files have names like userid:X, userid:cov, userid:mean, etc
    if request.method == "POST":

        # load relevant resources
        mydict = json.loads(request.body)
        s3 = boto3.resource('s3')
        bucket = s3.Bucket(BUCKET_NAME)

        ## step 1: save extra features in s3

        # update Xu
        data = get_pickle(mydict['userid'] + ":X.pkl", bucket)
        if data == []: # first time / new user
            data = mydict['features']
        else:
            data = np.append(data, mydict['features'], axis=1)
        put_pickle(mydict['userid'] + ":X.pkl", data, bucket)
        print(data)

        # update yu
        data = get_pickle(mydict['userid'] + ":y.pkl", bucket)
        data.append(mydict['label'])
        put_pickle(mydict['userid'] + ":y.pkl", data, bucket)
        print(data)

        # update miscinfo
        misc_info = get_json(MISC_INFO, bucket)
        misc_info['numpoints'] += 1
        put_json(MISC_INFO, misc_info, bucket)
        print(misc_info)

        #--------

        ## step 2: check if data size has reached next benchmark.
        ## if data size has reached next benchmark, make new classifier
        if misc_info['numpoints'] >= misc_info['rerunat']:

            # make new classifier
            print("IGNORE")





            # update rerunat
            # misc_info['rerunat'] *= 2 # overfishing: not sure when to rerun again
            # put_json(MISC_INFO, misc_info, bucket)


        """
		print(request.session)
		print(request.user)
		print(request.get_host())
		"""

        return HttpResponse("POST RESPONSE")

    return HttpResponse("SHOULD NOT REACH HERE")

# overfishing: in conjunction with decorator use auth mech
# json request contains "userid"
@csrf_exempt
def init(request):

    if request.method == "POST":
        mydict = json.loads(request.body)
        bucket = boto3.resource('s3').Bucket(BUCKET_NAME)

        # update list of users
        misc_info = get_json(MISC_INFO, bucket)
        misc_info['users'].append(mydict['userid'])
        put_json(MISC_INFO, misc_info, bucket)

        # send Z
        Z = get_pickle(Z_FILE, bucket)
        return HttpResponse(json.dumps(Z.tolist()), content_type="application/json")

    return HttpResponse("GET does not do anything for init")

# overfishing: use auth mech not just decorator
# json request contains "userid" and "Zu"
@csrf_exempt
def init2(request):

    # upload Tu
    if request.method == "POST":
        bucket = boto3.resource('s3').Bucket(BUCKET_NAME)
        mydict = json.loads(request.body)
        Z = get_pickle(Z_FILE, bucket)
        Tu = pickleML.findTu(Z, mydict['Zu'])
        put_pickle(mydict['userid'] + ":T.pkl", Tu, bucket)
        return HttpResponse("Uploaded T")



    return HttpResponse("GET does not do anything for init2")

# overfishing: in conjunction with decorator use auth mech
@csrf_exempt
def meancov(request):

    # json contains 'means' and 'covs' 2d arrays, as well as 'userid'
    if request.method == "POST":
        mydict = json.loads(request.body)
        bucket = boto3.resource('s3').Bucket(BUCKET_NAME)
        put_pickle(mydict['userid'] + ":means.pkl", mydict['means'], bucket)
        put_pickle(mydict['userid'] + ":covs.pkl", mydict['covs'], bucket)
        return HttpResponse("response from meancov")

    return HttpResponse("post request unsuccessful")

# overfishing launch cluster, submit script, then terminate
@csrf_exempt
def testing(request):

    updateClassifier()

    return HttpResponse("testing")

# update classifier
# to be called asynchronously
# launch cluster, submit classifier construction/modification to cluster, terminate cluster
def updateClassifier():

    # acquire lock

    # launch spark cluster
    # overfishing WORKS
    """
    os.system("export AWS_SECRET_ACCESS_KEY={0} export AWS_ACCESS_KEY_ID={1}".format(
        aws_secret_access_key, aws_access_key_id))
    os.system("{0}/spark-ec2 --key-pair=learn --identity-file={0}/learn.pem "
              "--region=us-west-2 --slaves=1 --spark-version=1.5.2 "
              "launch {1}".format(EC2_HOME, CLUSTER_NAME))
    """

    # submit script
    os.system("{0}/spark-submit "
              "--class org.apache.spark.examples.SparkPi "
              "--master local[2] "
              "{1}".format(BIN_HOME, SCRIPT))

    # terminate cluster
    # overfishing UNTESTED
    #os.system("{0}/spark-ec2 destroy {1}".format(EC2_HOME, CLUSTER_NAME))


    # release lock

"""
def item_detail(request, id):
	try:
		item = Item.objects.get(id=id)
	except Item.DoesNotExist:
		raise Http404('This item does not exist')
	return render(request, 'inventory/item_detail.html', {
		'item': item,
	})
"""