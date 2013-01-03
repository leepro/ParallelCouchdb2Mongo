import couchdb
import pymongo, math, urllib2, json
from multiprocessing import Process, Queue
import sys, time

host = ""
port = 1234

def worker(q):

	p = pymongo.Connection()
	pdb = p["ats"]

	while True:
		dbname, i = q.get()
		pcol = pdb[dbname]
	
		print >>sys.stderr, dbname, i
		data = json.loads(bulkDocs(dbname, i))
		datas = []
		for d in data["rows"]:
			datas.append(d)
			if len(datas) > 500:
				pcol.insert(datas)
				datas = []
		if datas != []:
			pcol.insert(datas)

def bulkDocs(db, page, docset=True, pagesize=100):
	pageskip = page*pagesize
	url = "http://%s:%d/%s/_all_docs?include_docs=true&limit=%d&skip=%d" % (host, port, db, pagesize, pageskip)
	ret = urllib2.urlopen(url).read()
	return ret 

#-------------------------

store = couchdb.Server("http://%s:%d" % (host, port))
pagesize = 100

# paralles
q = Queue()
procs = [ Process(target=worker, args=(q,)) for i in xrange(8) ]
[ p.start() for p in procs ]

p = pymongo.Connection()
pdb = p["ats"]

# go
for dbname in store:
	if dbname[0] == "_":
		continue

	db = store[dbname]
	totalpage = int(math.ceil(len(db) / float(pagesize)))
	pdb[dbname].remove()

	print dbname, totalpage
	for i in xrange(totalpage):
		q.put( [dbname, i] )

# wait

while True:
	print >>sys.stderr, [ p.is_alive() for p in procs ]
	time.sleep(1)

	