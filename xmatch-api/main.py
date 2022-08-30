import json
from datetime import datetime, timedelta

from fastapi import FastAPI, File, UploadFile
from pymongo import MongoClient

from utils import cone_search, parse_file

app = FastAPI()

config = json.load(open("config.json"))

client = MongoClient("mongodb://localhost:27017/experiments", username=config['user'], password=config['password'])
database = client[config['database']]

@app.get("/")
def home():
    return {"message": "Welcome to SconeAPI"}

@app.get("/catalog")
def get_catalog_list():
    return database.list_collection_names()

@app.get("/catalog/{catalog_name}")
def get_catalog_info(catalog_name: str):
    catalog = database[catalog_name]
    return list(catalog.aggregate([{'$collStats': {'count': {}}}, {'$project': {'host': 0, 'localTime': 0}}]))

@app.get("/objects")
def find_catalog_objects(ra_in: float, dec_in: float, catalog_name: str = "catwise2020"):
    catalog = database[catalog_name]
    return list(catalog.find({'ra': ra_in, 'dec': dec_in}))

@app.post("/crossmatch/")
async def get_conesearch(radius: float, input: UploadFile, limit: int = None, catalog: str = "catwise2020"):
    collection = database[catalog]
    input_arr = parse_file(input.file)
    search_time = timedelta(0)
    generate_file_time = timedelta(0)
    cursors_list = []
    for i in input_arr:
        start_search = datetime.now()
        cursor = cone_search(collection, i[0], i[1], radius, limit)
        end_search = datetime.now()
        search_time += (end_search-start_search)
        start_gen = datetime.now()
        for doc in cursor:
            cursors_list.append(doc)
        end_gen = datetime.now()
        generate_file_time += (end_gen - start_gen)
    return {"total_search_time": str(search_time), "generate_file_time": str(generate_file_time),"num_objs": len(cursors_list), "results": cursors_list}
