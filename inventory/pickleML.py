"""
This file contains Spark functions relevant to classifier construction.
"""

from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.mllib.linalg import Vectors
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import StringIndexer

import numpy as np
from random import *

import boto3

# CHECKED
# generate public matrix Z to send to each user
# constructed using means and covariance matrices from U users
# F is #features, means is U*F vector of means, covs is U*F*F matrix of covariances
def genPublicFeatureVectors(F, means, covs):
    U = len(means)  # number of users
    Z = np.zeros((F, 3 * F))  # overfishing: 3f is set arbitrarily
    for i in range(0, 3 * F):
        u = randint(0, U - 1)
        randvec = np.random.multivariate_normal(means[u], covs[u])
        Z[:, i] = randvec
    return Z

# overfishing: this is more complicated method that uses inner products, might not need
# step B3: find Tuv for one pair of users u, v
# Z's are the public vectors
# F is #features, Q is #reducedfeatures, M is #publicvectors
"""
def findTuv(F, Q, M, Z, Zu, Zv):

	# initialize Zc
	Zc = np.zeros((2 * F, M * M))
	for i in range(0, M):
		for j in range(0, M):
			Zc[0:F,i*M+j] = Z[:,i]
			Zc[F:2*F,i*M+j] = Z[:,j]

	# initialize Quv
	Quv = np.zeros((2 * Q, M * M))
	for i in range(0, M):
		for j in range(0, M):
			Quv[0:Q,i*M+j] = Zu[:,i]
			Quv[Q:2*Q,i*M+j] = Zv[:,j]

	# compute Tuv
	Tuv = Zc.dot(np.linalg.pinv(Quv))
	return Tuv
"""

# find Tu for a specific user u
def findTu(Z, Zu):
    return Z.dot(np.linalg.pinv(Zu))


# find Xu for a specific user u
def findXu(Xmodu, Tu):
    return Tu.dot(Xmodu)


# CHECKED
# create a dataframe for machine learning from the user supplied vectors
# Xmodus: dict where key = user and value = ndarray
# yus: dict where key = user and value = vectors
# sc and sqlContext are spark and sql contexts respectively
def createDF(Xmodus, Tus, yus, sc, sqlContext):
    # Note: ndarray -> rdd -> rdd of tuples -> dataframe

    # obtain array with appropriate features and labels (label first column)
    Xus = {}
    for user in Xmodus:
        Xus[user] = Tus[user].dot(Xmodus[user])
    flatXus = reduce(lambda x, y: np.append(Xus[x], Xus[y], axis=1), Xus)
    flatYus = reduce(lambda x, y: np.append(yus[x], yus[y], axis=1), yus)
    total = np.append(flatYus, flatXus, axis=0)
    total = np.transpose(total)

    # create dataframe from that array
    rdd = sc.parallelize(total)
    rddlab = rdd.map(lambda x: (float(x[0]), Vectors.dense(x[1:len(x)])))
    return sqlContext.createDataFrame(rddlab, ['label', 'features'])


# create random forest classifier from dataframe
# CHECKED
def createRFC(df):
    stringIndexer = StringIndexer(inputCol="label", outputCol="indexed")
    si_model = stringIndexer.fit(df)
    td = si_model.transform(df)

    # overfishing tune later
    rf = RandomForestClassifier(numTrees=3, maxDepth=2, labelCol="indexed", seed=42)
    return rf.fit(td)

# testing
"""
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
yus['u1'] = np.random.rand(1, V)
yus['u2'] = np.random.rand(1, V)
df = createDF(Xmodus, Tus, yus)
rfc = createRFC(df)
print rfc
"""