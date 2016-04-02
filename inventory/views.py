from django.shortcuts import render
import json

from django.http import HttpResponse
from django.http import Http404
from django.views.decorators.csrf import csrf_exempt

from inventory.models import Item

# overfishing: in conjunction with decorator use auth mech like from drf
@csrf_exempt
def index(request):
    # overfishing: test get method that writes spark stuff to s3
    if request.method == "GET":
        print("accessed by get")
        return HttpResponse("GET RESPONSE")

    # note: successfully able to receive json
    # json contains "label", "category", and "features"
    if request.method == "POST":
        mydict = json.loads(request.body)

        print('label: ' + mydict['label'])
        print("category: " + mydict['category'])
        print("features: ")
        print(mydict['features'])

        """
		print(request.session)
		print(request.user)
		print(request.get_host())
		"""
        return HttpResponse("POST RESPONSE")

    return HttpResponse("REG RESPONSE")


# overfishing: in conjunction with decorator use auth mech
@csrf_exempt
def meancov(request):
    # overfishing test: print mean and cov
    if request.method == "POST":
        mydict = json.loads(request.body)

        print('means')
        print(mydict['means'])
        print('covs')
        print(mydict['covs'])

    return HttpResponse("response from meancov")


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