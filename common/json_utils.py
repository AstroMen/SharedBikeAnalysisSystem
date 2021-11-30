# -*- coding: UTF-8 -*- 
"""
Description: MultiTool 
@author: Men Luyao 
@date: 2019/9/12 
"""
import json
from jsonschema import validate, exceptions
from common.Logger import logger


class JsonUtils:
    @staticmethod
    def validate_schema(json_txt, schema):
        try:
            validate(instance=json_txt, schema=schema)
        except exceptions.ValidationError as e:
            logger.warn(e)
            return e.message
        return 'Success'

    @staticmethod
    def json_loads_byteified(json_text):
        return JsonUtils.__byteify(
            json.loads(json_text, object_hook=JsonUtils.__byteify),
            ignore_dicts=True
        )

    @staticmethod
    def __byteify(data, ignore_dicts=False):
        # if isinstance(data, unicode): # python3 cannot be used
        #     return data.encode('utf-8')
        if isinstance(data, list):
            return [JsonUtils.__byteify(item, ignore_dicts=True) for item in data]
        if isinstance(data, dict) and not ignore_dicts:
            return {
                JsonUtils.__byteify(key, ignore_dicts=True): JsonUtils.__byteify(value, ignore_dicts=True)
                for key, value in data.iteritems()
            }
        return data

    @staticmethod
    def nrm_encode_raise_by_dumps(ori_str):
        return ori_str.decode('unicode_escape').encode("utf-8")
