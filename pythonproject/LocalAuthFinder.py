import numpy as np
import json
import sys
import csv
from shapely.geometry import Point
from shapely.geometry.polygon import Polygon


class Counties:
    def __init__(self):
        with open('UK.geojson', 'r') as f:
            self.counties = json.load(f)
        self.geolist = []
        for c in self.counties['features']:
            self.geolist.append(c['properties']['geo_label'])
        self.z = {}
        for c in self.counties['features']:
            self.z[c['properties']['geo_label']] = []
            if len(c['geometry']['coordinates']) > 1:
                for z1 in c['geometry']['coordinates']:
                    for z in z1:
                        if len(z) > 2:
                            self.z[c['properties']['geo_label']].append(self.find_p(z))
            else:
                z1 = c['geometry']['coordinates']
                for z in z1:
                    self.z[c['properties']['geo_label']].append(self.find_p(z))

    def labels(self):
        return self.geolist

    def find_p(self, z):
        lats = []
        lons = []
        for pt in z:
            lats.append(pt[0])
            lons.append(pt[1])
        lons_lats_vect = np.column_stack((lats, lons))
        return Polygon(lons_lats_vect)

    def find_region(self, lat, long):
        try:
            lat = float(lat)
            long = float(long)
        except:
            lat = 0
            long = 0

        if long == 0 and lat == 0:
            return None
        point = Point(long, lat)
        for c in self.counties['features']:
            for z in self.z[c['properties']['geo_label']]:
                if z.contains(point) or point.within(z):
                    return c['properties']['geo_label']
        return None


# loading the counties
c = Counties()


def find_local_auth(ll, counties):
    la = counties.find_region(ll[0], ll[1])

    if la is not None:
        return la
    else:
        # print("ERROR FIND LOCAL AUTHORITY")
        return 'NULL'

