import sys
from bus_controller.trip_controller import TripController
from Logger import logger


class MasterController:
    def __init__(self, spark):
        logger.info("Initiating master ...")
        self.__spark = spark

    def trip_handler(self):
        TripController(self.__spark)
