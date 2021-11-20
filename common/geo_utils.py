from math import radians, cos, sin, asin, sqrt
from geopy.distance import geodesic


class GeoUtils:
    @staticmethod
    def get_distance_by_lng_lat_cal(lng1, lat1, lng2, lat2):
        ''' return: km '''
        # lng1,lat1,lng2,lat2 = (120.12802999999997,30.28708,115.86572000000001,28.7427)
        lng1, lat1, lng2, lat2 = map(radians, [float(lng1), float(lat1), float(lng2), float(lat2)])  # 经纬度转换成弧度
        dlon = lng2 - lng1
        dlat = lat2 - lat1
        a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
        distance = 2 * asin(sqrt(a)) * 6371 * 1000  # 地球平均半径，6371km
        distance = round(distance / 1000, 3)
        return distance

    @staticmethod
    def get_distance_by_lng_lat(lng1, lat1, lng2, lat2, unit='miles'):
        # lng1,lat1,lng2,lat2 = (120.12802999999997,30.28708, 115.86572000000001,28.7427)
        lng1, lat1, lng2, lat2 = map(radians, [float(lng1), float(lat1), float(lng2), float(lat2)])  # 经纬度转换成弧度
        return geodesic((lat1, lng1), (lat2, lng2)).miles if unit == 'miles' else geodesic((lat1, lng1), (lat2, lng2)).m
