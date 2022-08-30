import numpy as np

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

def cone_search(coll, ra, dec, radius=10., k = None):
    scaling = wgs_scale(dec)
    meter_radius = radius * scaling
    lon, lat = ra, dec

    cursor = coll.find({'loc': {
        '$nearSphere': {
            '$geometry': {'type': 'Point',
                          'coordinates': [lon, lat]},
            '$maxDistance': meter_radius*0.0002778
            }}})
    if k is not None:
        cursor = cursor.limit(k)
    return cursor

def parse_file(input_file):
    file_arr = []
    with input_file as f:
        for line in f.readlines():
            line = line.decode()
            if "ra" in line or "dec" in line:
                continue
            aux_arr = line.split(",")
            ra, dec = float(aux_arr[0]), float(aux_arr[1])
            file_arr.append((ra, dec))
    return file_arr
