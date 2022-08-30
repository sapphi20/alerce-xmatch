import pymongo
import numpy as np

from datetime import datetime
import json

config = json.load(open('config.json'))
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
    lon, lat = ra - 180., dec

    cursor = mycolObj.find({'loc': {
        '$nearSphere': {
            '$geometry': {'type': 'Point',
                          'coordinates': [lon, lat]},
            '$maxDistance': meter_radius
            }}}, {'loc': 1})
    return cursor


def cone_search_advanced(ra, dec, radius=10., query={}, distance_field='distance'):
    scaling = wgs_scale(dec)
    print("Scaling:", scaling)
    meter_radius = radius * scaling
    print("Meter radius:", meter_radius)
    lon, lat = ra - 180., dec

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
        {'$sort': {distance_field: 1}},
        {'$limit': 3}
        ])

    return cursor

ra_lmc, dec_lmc = 80.893860, -69.756126
ra_smc, dec_smc = 13.186588, -72.828599
#ra_sgrA, dec_sgrA = 266.41683708333335, -29.007810555555555
radius = [2**x for x in range(0, 5)]
# Perform the search

print("Center ra, dec: ", ra_lmc, dec_lmc)
for r in radius:
    print("Radius: {0} arcsec".format(r))
    start = datetime.now()

    lmc = cone_search_advanced(ra_lmc, dec_lmc, 0.0002778*r)
    smc = cone_search_advanced(ra_smc, dec_smc, 0.0002778*r)
    end = datetime.now()

    print("Large Magellanic Cloud")
    for doc in lmc:
        print(doc)

    print("Small Magellanic Cloud")
    for doc in smc:
        print(doc)

    end2 = datetime.now()
    print("Elapsed time (w/o prints): " + str(end-start))
    print("Elapsed time (with prints): " + str(end2-start))
