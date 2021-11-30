# -*- coding: UTF-8 -*- 
"""
Description: MultiTool 
@author: Men Luyao 
@date: 2019/9/12 
"""
import os, stat, sys
import csv, zipfile, json
import codecs
from common.time_utils import TimeUtils
from common.json_utils import JsonUtils
from common.Logger import logger


class FileUtils:
    __working_dir = "./"

    @staticmethod
    def re_name(src, dst):
        os.rename(src, dst)

    @staticmethod
    def get_working_dir():
        return FileUtils.__working_dir

    @staticmethod
    def path_exists(path):
        return os.path.exists(path)

    @staticmethod
    def create_dir(path):
        return os.makedirs(path) and os.chmod(path, stat.S_IWUSR)

    @staticmethod
    def normalize_path(path):
        path = FileUtils.__working_dir if path is None or len(path) == 0 else '{}{}/'.format(FileUtils.__working_dir, path)
        if not FileUtils.path_exists(path):
            FileUtils.create_dir(path)
        return path

    @staticmethod
    def imp_csv(path, filename):
        data = list()
        path = FileUtils.normalize_path(path)
        full_path = "{}{}.csv".format(path, filename)
        if not FileUtils.path_exists(full_path):
            return None

        csv_reader = csv.reader(open(full_path))
        for row in csv_reader:
            if '\xef\xbb\xbf' in row[0]:
                row[0] = row[0].lstrip('\xef\xbb\xbf')
            data.append(row)
        return data

    @staticmethod
    def exp_csv(path, filename, cols, data, save_append=False, code_without_bom=False):
        path = '' if path is None or len(path) == 0 else '{}/'.format(path)
        is_exist = FileUtils.path_exists("{}{}.csv".format(path, filename))
        mode = "a+" if save_append and is_exist else "w"
        # Todo: check col name is the same or not
        with open("{}{}.csv".format(path, filename), mode) as csv_file:
            if code_without_bom:
                csv_file.write(codecs.BOM_UTF8)
            writer = csv.writer(csv_file)  # dialect='excel'
            # write columns name into file
            if cols and len(cols) > 0 and (not save_append or (not is_exist and save_append)):
                writer.writerow(cols)
            # write data with batch
            writer.writerows(data)

    @staticmethod
    def zip_folder(src_dir, output_filename, output_ext='.zip'):
        """
        zip folder
        :param src_dir: directory of folder
        :param output_filename: output file name
        :param output_ext: extension of output file, eg. '.zip'
        :return:
        """
        output_full_filename = '{}{}'.format(output_filename, output_ext)
        z = zipfile.ZipFile(output_full_filename, 'w', zipfile.ZIP_DEFLATED)  # first param: folder name
        for dir_path, dir_names, file_names in os.walk(src_dir):
            f_path = dir_path.replace(src_dir, '')  # replace to avoid copy from root
            f_path = f_path and f_path + os.sep or ''  # zip current folder and all files included
            for filename in file_names:
                z.write(os.path.join(dir_path, filename), f_path + filename)
        z.close()
        logger.info('Finish zipping folder.')

    @staticmethod
    def backup_dir(src_dir, bk_dir='data_bk'):
        path = FileUtils.normalize_path(bk_dir)
        if not FileUtils.path_exists(path):
            FileUtils.create_dir(path)
        FileUtils.zip_folder(src_dir, '{}/{}_{}'.format(path, src_dir, TimeUtils.get_now_as_str(format_str='%Y%m%d%H%M%S')))
        logger.info('Finish backup folder {}.'.format(src_dir))

    @staticmethod
    def get_file_list_under_dir(path, is_abs=False):
        file_list = list()
        src_dir = FileUtils.normalize_path(path) if not is_abs else path
        for dir_path, dir_names, file_names in os.walk(src_dir):
            for filename_with_ext in file_names:
                ori_f_name, ori_f_ext = os.path.splitext(filename_with_ext)
                file_list.append(ori_f_name)
        return file_list

    @staticmethod
    def remove_folder(path, is_include_self=True):
        path = FileUtils.normalize_path(path)
        ls = os.listdir(path)
        for i in ls:
            c_path = os.path.join(path, i)
            if os.path.isdir(c_path):
                FileUtils.remove_folder(c_path)
            else:
                os.remove(c_path)
        if is_include_self:
            os.rmdir(path)

    @staticmethod
    def imp_json_file(path, filename, is_abs=False, ext='json', btf=False):
        content, json_data = None, None
        path = FileUtils.normalize_path(path) if not is_abs else '{}/'.format(path)
        full_path = "{}{}.{}".format(path, filename, ext)

        try:
            f = open(full_path, "rb")
            content = f.read().decode('utf-8')
            f.close()
            if btf:
                return JsonUtils.json_loads_byteified(content)
        except ValueError as e:
            logger.error('Read json file fail. {}'.format(e))
        except BaseException as e:
            logger.error('Read json file fail. {}'.format(e))
        return content

    @staticmethod
    def exp_json_file(path, filename, data, indent=4, ensure_ascii=False):
        path = FileUtils.normalize_path(path)
        full_path = "{}{}.json".format(path, filename)

        try:
            # if sys.getdefaultencoding() != 'utf-8':
            #     reload(sys)
            #     sys.setdefaultencoding('utf-8')
            json_str = json.dumps(data, indent=indent, ensure_ascii=ensure_ascii)
            with open(full_path, 'w') as json_file:
                json_file.write(json_str)
            logger.info('Finish export json file [{}].'.format(full_path))
        except BaseException as e:
            logger.error('Write to json file fail. {}'.format(e))
