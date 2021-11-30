from math import radians, cos, sin, asin, sqrt
from geopy.distance import geodesic
from shapely.geometry import Point, asShape
from Logger import logger


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

    @staticmethod
    def is_exist_in_multi_poly(point_x, point_y, poly_shape):
        """
        Check if point in multiple polygon
        :param point_x: coordinates x
        :param point_y: coordinates y
        :param poly_shape: multipolygon
        :return: boolean
        """
        is_exist = False
        point = Point(point_x, point_y)
        # import shapely.geometry
        # poly_shape = asShape(poly_context)
        is_exist = poly_shape.intersects(point)

        # poly_context = {'type': 'MULTIPOLYGON', 'coordinates': [[[[0, 0], [0, 2], [2, 2], [2, 0]]]]}
        return is_exist

    @staticmethod
    def get_poly_shape(poly_context):
        """
        Get poly shape
        :param poly_context: multipolygon, json format
        :return: return poly_shape
        """
        # from shapely.geometry import MultiPolygon
        # poly_shape = MultiPolygon(poly_context)
        import shapely.geometry
        poly_shape = asShape(poly_context)
        return poly_shape


if __name__ == '__main__':
    poly_context = {"type": "MultiPolygon",
                    "coordinates":
                    [
                        [
                            [
                                [
                                    -118.703392,
                                    34.168591
                                ],
                                [
                                    -118.703367,
                                    34.16859
                                ],
                                [
                                    -118.703254,
                                    34.16859
                                ],
                                [
                                    -118.702619,
                                    34.168588
                                ]
                            ]]]}
    # poly_context = {'type': 'MULTIPOLYGON', 'coordinates': [[[[0, 0], [0, 2], [2, 2], [2, 0]]]]}
    res = GeoUtils.is_exist_in_multi_poly(34.035679, -118.270813, poly_context)
    print(res)
