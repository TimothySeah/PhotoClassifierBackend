import json
import constants
import pickle
import numpy as np

from django.shortcuts import render
from django.http import HttpResponse
from django.http import Http404
from django.views.decorators.csrf import csrf_exempt

from inventory.models import Item

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
    if request.method == "POST":

        # note: files have names like userid:X, userid:cov, userid:mean, etc

        # load relevant resources
        mydict = json.loads(request.body)
        s3 = boto3.resource('s3')
        bucket = s3.Bucket(constants.BUCKET_NAME)

        ## step 1: save extra features in s3 FOR SURE
        # download file, modify it, put file back

        # download X and modify its data (or create new data)
        s3_url = mydict['userid'] + ":X.pkl"
        local_url = '/tmp/' + mydict['userid'] + ":X.pkl"
        try:
            bucket.download_file(s3_url, local_url)
        except botocore.exceptions.ClientError as e:
            print(e.response)
            data = mydict['features']
        else:
            pkl_file = open(local_url, 'rb')
            data = pickle.load(pkl_file)
            data = np.append(data, mydict['features'], axis=1)
            pkl_file.close()

        # put X back
        new_local_url = '/tmp/' + mydict['userid'] + ':X_new.pkl'
        with open(new_local_url, 'wb') as f:
            pickle.dump(data, f)
        bucket.upload_file(new_local_url, s3_url)

        # overfishing print x
        print(data)

        # download y and modify its data (or create new data)
        s3_url = mydict['userid'] + ":y.pkl"
        local_url = '/tmp/' + mydict['userid'] + ":y.pkl"
        try:
            bucket.download_file(s3_url, local_url)
        except botocore.exceptions.ClientError as e:
            print(e.response)
            data = [mydict['label']]
        else:
            pkl_file = open(local_url, 'rb')
            data = pickle.load(pkl_file)
            data.append(mydict['label'])
            pkl_file.close()

        # put y back
        new_local_url = '/tmp/' + mydict['userid'] + ':y_new.pkl'
        with open(new_local_url, 'wb') as f:
            pickle.dump(data, f)
        bucket.upload_file(new_local_url, s3_url)

        # overfishing print y
        print(data)

        #----------

        ## update MiscInfo

        # download MiscInfo
        s3_url = constants.MISC_INFO
        local_url = '/tmp/' + constants.MISC_INFO
        try:
            bucket.download_file(s3_url, local_url)
        except botocore.exceptions.ClientError as e:
            print(e.response)
            return # should never happen
        else:
            with open(local_url, 'rb') as file:
                misc_info = json.load(file)

        # add new user to MiscInfo
        """
        users_set = set(misc_info['users'])
        users_set.add(mydict['userid'])
        misc_info['users'] = list(users_set)
        """

        # increment number of data points
        misc_info['numpoints'] += 1

        # upload misc_info
        with open('/tmp/misc_info', 'wb') as f:
            json.dump(misc_info, f)
        bucket.upload_file(local_url, s3_url)

        # overfishing print miscinfo
        print(misc_info)

        #--------

        ## step 2: check if data size has reached next benchmark.
        # if data size has reached next benchmark, make new classifier
        if misc_info['numpoints'] >= misc_info['rerunat']:

            # make new classifier
            print("adsf")

            # update rerunat, upload misc_info
            misc_info['rerunat'] *= 2 # overfishing: not sure when to rerun again
            with open('/tmp/misc_info', 'wb') as f:
                json.dump(misc_info)
            bucket.upload_file('/tmp/misc_info', constants.MISC_INFO)


        """
		print(request.session)
		print(request.user)
		print(request.get_host())
		"""

        return HttpResponse("POST RESPONSE")

    return HttpResponse("SHOULD NOT REACH HERE")


# overfishing: in conjunction with decorator use auth mech
@csrf_exempt
def meancov(request):

    # json contains 'means' and 'covs' 2d arrays, as well as 'userid'
    if request.method == "POST":

        # set up resources
        mydict = json.loads(request.body)
        bucket = boto3.resource('s3').Bucket(constants.BUCKET_NAME)
        s3_mean_url = mydict['userid'] + ":means.pkl"
        s3_covs_url = mydict['userid'] + ":covs.pkl"
        local_mean_url = '/tmp/' + mydict['userid'] + ":means.pkl"
        local_covs_url = '/tmp/' + mydict['userid'] + ":covs.pkl"

        # upload files to s3
        with open(local_mean_url, 'wb') as f:
            pickle.dump(mydict['means'], f)
        with open(local_covs_url, 'wb') as f:
            pickle.dump(mydict['covs'], f)
        bucket.upload_file(local_mean_url, s3_mean_url)
        bucket.upload_file(local_covs_url, s3_covs_url)

        return HttpResponse("response from meancov")

    return HttpResponse("post request unsuccessful")


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