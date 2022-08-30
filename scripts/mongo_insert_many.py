import pymongo
import glob
from multiprocessing import Pool, cpu_count
import time
import json
import gzip
import argparse


parser = argparse.ArgumentParser(description='Insert many docs in mongodb')
parser.add_argument('folder', help='directory')
parser.add_argument('-p', '--nproc', type=int, nargs='?', default=cpu_count(), help='number of process')
parser.add_argument('-b', '--batchSize', type=int, nargs='?', default=1000, help='size of batch')
parser.add_argument('-c', '--config', nargs='?', default="config.json", help='size of batch')
parser.add_argument('-i', '--initiate', action="store_true", help='create indexes')
args = parser.parse_args()

nproc = args.nproc
batchSize = args.batchSize
confile = args.config
#iterate over your files here and generate file object you can also get files list using os module
input_csv_path = args.folder #'/volume2/amoya/'
input_directory = input_csv_path + '/*.json'

infile = open(confile, 'r')
config = json.load(infile)
infile.close()

start = time.time()
names = glob.glob(input_directory)

if args.initiate:
    myclient = pymongo.MongoClient(
        host=config['host'],  # <-- IP and port go here
        serverSelectionTimeoutMS=3000,  # 3 second timeout
        username=config['user'],
        password=config['password'],
        port=config['port'],
    )
#    myclient = pymongo.MongoClient("mongodb://localhost:27017/")
    mydb = myclient[config['database']]
    mycolObj = mydb["objects"]
    resp = mycolObj.create_index([("epochs", 1)])
    print("index response:", resp)
    resp = mycolObj.create_index([("fid", "hashed")])
    print("index response:", resp)
    resp = mycolObj.create_index([("field", 1)])
    print("index response:", resp)

    # http://strakul.blogspot.com/2019/07/data-science-mongodb-sky-searches-with.html
    resp = mycolObj.create_index([("loc", "2dsphere")])
    print("index response:", resp)

    mycolDet = mydb["detections"]
    resp = mycolDet.create_index([("oid", "hashed")])
    print("index response:", resp)
    resp = mycolDet.create_index([("hmjd", 1)])
    print("index response:", resp)
    resp = mycolDet.create_index([("mag", 1)])
    print("index response:", resp)
    resp = mycolDet.create_index([("magerr", 1)])
    print("index response:", resp)
    resp = mycolDet.create_index([("clrcoeff", 1)])
    print("index response:", resp)

#define a function
def execute_copy(file):
    myclient = pymongo.MongoClient(
            host=config['host'],  # <-- IP and port go here
            serverSelectionTimeoutMS=3000,  # 3 second timeout
            username=config['user'],
            password=config['password'],
            port=config['port'],
        )
#    myclient = pymongo.MongoClient("mongodb://localhost:27017/")
    mydb = myclient[config['database']]
    mycolObj = mydb["objects"]
    objects = []
    count = batchSize
    total_inserted=0
    total = 0
    line_num = 0
    with open(file, 'rt') as f:
        for line in f:
            line_num += 1
            print("Line: " + str(line_num))
            doc = json.loads(line)
            result = mycolObj.find({"source_id": doc['source_id']})
            if len(list(result)) == 0:
                objects.append(doc)
            count -= 1
            if count <= 0 or line == '':
                try:
                    x = mycolObj.insert_many(objects)
                    total_inserted += len(x.inserted_ids)
                    objects.clear()
                    total += batchSize
                    count = batchSize
                except:
                    continue
    print("#objects: " + str(len(line_num)))
    print("total_inserted: " + str(total_inserted))
    return total

with Pool(nproc) as p:
    print(p.map(execute_copy, names))

total = time.time() - start
print("TOTAL_TIME=%s" % (str(total)))
