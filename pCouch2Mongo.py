#! /usr/bin/env python

#
# This script is a quick and dirty one for cloning dbs from CouchDB to MongoDB
# (This was used to do stress test for MongoDB.)
#

import couchdb
import pymongo, math, urllib2, json
from multiprocessing import Process, Queue
import sys, time

couch_host = "localhost"
couch_port = 1234
mongo_host = "localhost"
mongo_port = 27017
mongo_dbname = ""
max_process = 8
pagesize = 100

def syncWorker(jobq):
	"""Unit worker"""

	mong  = pymongo.Connection(host=mongo_host, port=mongo_port)
	pdb = mong[mongo_dbname]

	while True:
		dbname, i = jobq.get()
		if dbname = "END":
			return

		pcol = pdb[dbname]
	
		print >>sys.stderr, dbname, i
		data = json.loads(bulkReadCouchDocs(dbname, i, docset=True, pagesize=pagesize))
		datas = []
		for d in data["rows"]:
			datas.append(d)
			if len(datas) > 500:
				pcol.insert(datas)
				datas = []
		if datas != []:
			pcol.insert(datas)

def bulkReadCouchDocs(db, page, docset=True, pagesize=100):
	"""Get documents in bulk"""

	pageskip = page*pagesize
	url = "http://%s:%d/%s/_all_docs?include_docs=true&limit=%d&skip=%d" % 
			(couch_host, couch_port, db, pagesize, pageskip)
	ret = urllib2.urlopen(url).read()
	return ret 

def makeProcess(n):
	"""forking paralle processes"""

	jobq = Queue()
	procs = [ Process(target=syncWorker, args=(jobq,)) for i in xrange(n) ]
	[ p.start() for p in procs ]

	return q, procs

if __name__ == "__main__":
	# prepare db handles

	store = couchdb.Server("http://%s:%d" % (couch_host, couch_port))
	mong  = pymongo.Connection(host=mongo_host, port=mongo_port)
	pdb = mong[mongo_dbname]

	# make prallel processes
	jobq, procs = makeProcess(max_process)

	# push to processes
	for dbname in store:
		if dbname[0] == "_":
			continue

		db = store[dbname]
		# calculate the number of total pages with a fixed pagesize
		totalpage = int(math.ceil(len(db) / float(pagesize)))

		# clear the collection
		pdb[dbname].remove()

		print dbname, totalpage

		# put dbname and page
		for i in xrange(totalpage):
			jobq.put( [dbname, i] )

	# signal all processes
	[ jobq.put([ "END", None ]) for i in xrange(max_process) ]

	# monitoring
	while True:
		liveness = sum([ p.is_alive() for p in procs ])
		if liveness == 0:
			break
		time.sleep(1)
