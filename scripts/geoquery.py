import pymongo
import numpy as np

from datetime import datetime, timedelta
import json
import sys

# config settings
config = json.load(open('config.json'))

sample_file = sys.argv[1]
output_file = sys.argv[2]
r = int(sys.argv[3])
myclient = pymongo.MongoClient("mongodb://localhost:27017/experiments", username=config['user'], password=config['password'])  # host=config['host'],port=config['port'])
mydb = myclient[config['database']]  # config['database']
mycolObj = mydb['catwise2020']

def wgs_scale(lat):
    # Get scaling to convert degrees to meters at a given geodetic latitude (declination)

    # Values from WGS 84
    a = 6378137.000000000000  # Semi-major axis of Earth
    b = 6356752.314140000000  # Semi-minor axis of Earth
    e = 0.081819190842600  # eccentricity
    angle = np.radians(1.0)

    # Compute radius of curvature along meridian (see https://en.wikipedia.org/wiki/Meridian_arc)
    rm = a * (1 - np.power(e, 2)) / np.power(((1 - np.power(e, 2) * np.power(np.sin(np.radians(lat)), 2))), 1.5)

    # Compute length of arc at this latitude (meters/degree)
    arc = rm * angle
    return arc


def cone_search(ra, dec, radius=10.):
    scaling = wgs_scale(dec)
    meter_radius = radius * scaling
    lon, lat = ra, dec

    cursor = mycolObj.find({'loc': {
        '$nearSphere': {
            '$geometry': {'type': 'Point',
                          'coordinates': [lon, lat]},
            '$maxDistance': meter_radius
            }}}).limit(10)
    return cursor


def cone_search_2(ra, dec, radius=10., query={}, distance_field='distance'):
    scaling = wgs_scale(dec)
    meter_radius = radius * scaling
    lon, lat = ra, dec
    cursor = mycolObj.aggregate([
        {
            '$geoNear': {
                'near': {'type': "Point", 'coordinates': [lon, lat]},
                'spherical': True,
                'maxDistance': meter_radius,
                'query': query,
                'distanceField': distance_field,
                'distanceMultiplier': 1/(scaling*0.0002778)
            }},
            {'$project': {'loc': 1, distance_field: 1}},
        {'$limit': 10}
        ])
    return cursor

obj_list = []

with open(sample_file, "r") as f:
    for l in f.readlines():
        #obj = json.loads(l)
        #obj_list.append(obj["loc"]["coordinates"])
        print(l)
        if "ra" in l:
            continue
        else:
            ra, dec = float(l.split(",")[0]), float(l.split(",")[1])
            obj_list.append([ra, dec])

with open(output_file, "w") as f:
    total_time_1 = timedelta()
    total_time_2 = timedelta()
    for o in obj_list[0:20000]:
        start1 = datetime.now()
        cursor2 = cone_search(o[0], o[1], 0.0002778*r)
        end1 = datetime.now()

        start2 = datetime.now()
        for doc in cursor2:
            f.write(str(doc) + '\n')
        end2 = datetime.now()
            
        total_time_1 += (end1 - start1)
        total_time_2 += (end2 - start2)

#print("Total time conesearch 1: ", str(total_time_1))
print("Fixed radius: 8 arcsec")
print("Processing positions time: ", total_time_1)
print("Generating file time: ", total_time_2)
