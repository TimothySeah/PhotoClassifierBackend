"""
This file contains Spark functions relevant to classifier construction.
"""

# test push

from pyspark import SparkContext
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.tree import RandomForest, RandomForestModel
import numpy as np

# find Tu for a specific user u
def findTu(Z, Zu):
    return Z.dot(np.linalg.pinv(Zu))

# overfishing: modify to scale better: dont load from memory
# create an RDD for machine learning from the user supplied vectors
# Xmodus: dict where key = user and value = ndarray
# yus: dict where key = user and value = list of strings
# Tus: dict where key = user and value = ndarray
# dict: dict where key = label and value = int
# sc and sqlContext are spark and sql contexts respectively
# assume that this is only called with at least 1 user
# returns (rdd of LabeledPoint, dict of label: int)
def createRDDandDict(Xmodus, Tus, yus, dict, sc):
    # Note: ndarray -> rdd -> rdd of tuples -> dataframe

    # obtain array with appropriate features and labels (label first column)
    Xus = {}
    numusers = 0 # to help in 1 user case
    lastuser = "" # to help in 1 user case
    for user in Xmodus:
        Xus[user] = Tus[user].dot(Xmodus[user])
        numusers += 1
        lastuser = user
    if numusers == 1:
        flatXus = Xus[lastuser]
        flatyus = yus[lastuser]
    else:
        flatXus = reduce(lambda x, y: np.append(Xus[x], Xus[y], axis=1), Xus)
        flatyus = reduce(lambda x, y: np.append(yus[x], yus[y]), yus)
    total = np.append([flatyus], flatXus, axis=0)
    total = np.transpose(total)

    # create rdd from that array
    rdd = sc.parallelize(total.tolist())
    return rdd.map(lambda x: LabeledPoint(dict[x[0]], x[1:]))


# create random forest classifier from rdd of labeledpoint(num, [num,num])
def createRFC(rdd, numclasses, catfeatinfo):

    # overfishing tune later
    return RandomForest.trainClassifier(rdd, numClasses=numclasses,
                                         categoricalFeaturesInfo=catfeatinfo,
                                         numTrees=3)

# testing
# note: predict works
"""
sc = SparkContext("local", "Pickle")
F = 5
Q = 3
V = 10
Tus = {}
Tus['u1'] = np.random.rand(F, Q)
Tus['u2'] = np.random.rand(F, Q)
Xmodus = {}
Xmodus['u1'] = np.random.rand(Q, V)
Xmodus['u2'] = np.random.rand(Q, V)
yus = {}
yus['u1'] = ['a','bb','d','bb','bb','d','ac','a','bb','a']
yus['u2'] = ['d','bb','a','ac','bb','bb','a','bd','a','ac'] #np.random.rand(1, V)
(rdd, dict) = createRDDandDict(Xmodus, Tus, yus, sc)
print rdd.collect()
print dict
rfc = createRFC(rdd, len(dict), {})
print rfc.toDebugString()
"""