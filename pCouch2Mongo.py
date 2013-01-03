import couchdb
import pymongo, math, urllib2, json
from multiprocessing import Process, Queue
import sys, time

couch_host = ""
couch_port = 1234
mongo_host = ""
mongo_port = 27017
mongo_dbname = ""
max_process = 8
pagesize = 100

def syncWorker(jobq):
	mong  = pymongo.Connection(host=mongo_host, port=mongo_port)
	pdb = mong[mongo_dbname]

	while True:
		dbname, i = jobq.get()
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
	pageskip = page*pagesize
	url = "http://%s:%d/%s/_all_docs?include_docs=true&limit=%d&skip=%d" % (couch_host, couch_port, db, pagesize, pageskip)
	ret = urllib2.urlopen(url).read()
	return ret 

def makeProcess():
	# forking paralle processes
	jobq = Queue()
	procs = [ Process(target=syncWorker, args=(jobq,)) for i in xrange(max_process) ]
	[ p.start() for p in procs ]

	return q, procs

if __name__ == "__main__":
	# prepare db handles

	store = couchdb.Server("http://%s:%d" % (couch_host, couch_port))
	mong  = pymongo.Connection(host=mongo_host, port=mongo_port)
	pdb = mong[mongo_dbname]
	jobq, procs = makeProcess()

	# push to processes
	for dbname in store:
		if dbname[0] == "_":
			continue

		db = store[dbname]
		totalpage = int(math.ceil(len(db) / float(pagesize)))
		pdb[dbname].remove()

		print dbname, totalpage
		for i in xrange(totalpage):
			jobq.put( [dbname, i] )

	# monitoring
	while True:
		print >>sys.stderr, [ p.is_alive() for p in procs ]
		time.sleep(1)
