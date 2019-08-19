import zipfile
import re
import os
import configparser
import logging


# 解压缩文件
def file_unzip():
    # 加载现有配置文件
    conf = configparser.ConfigParser()
    conf.read("conf.ini", encoding="utf-8")
    download_dir = conf.get('local_dir', 'download_dir')
    z = zipfile
    for dir_path, dir_names, file_names in os.walk(download_dir):
        # 过滤csv文件
        for filename in file_names:
            res = re.match('(.*?).zip', filename)
            if res:
                z = zipfile.ZipFile(download_dir + filename, 'r')
                z.extractall(path=download_dir)
    print('解压缩文件成功')
    z.close()
    return True


def del_download_file():
    # 加载现有配置文件
    conf = configparser.ConfigParser()
    conf.read("conf.ini", encoding="utf-8")
    download_dir = conf.get('local_dir', 'download_dir')
    ls = os.listdir(download_dir)
    for i in ls:
        c_path = os.path.join(download_dir, i)
        os.remove(c_path)
    print('删除本地文件成功')
    return True


def mkdir():
    # 加载现有配置文件
    conf = configparser.ConfigParser()
    conf.read("conf.ini", encoding="utf-8")
    download_dir = conf.get('local_dir', 'download_dir')
    # 判断路径是否存在
    # 存在     True
    # 不存在   False
    is_exists = os.path.exists(download_dir)
    # 判断结果
    if not is_exists:
        # 如果不存在则创建目录
        # 创建目录操作函数
        os.makedirs(download_dir)
        print(download_dir + '创建成功')
        return True
    else:
        # 如果目录存在则不创建，并提示目录已存在
        print(download_dir + '目录已存在')
        return False


def log_set():
    logging.basicConfig(level=logging.INFO,  # 控制台打印的日志级别
                        filename='result.log',
                        # 模式，有w和a，w就是写模式，每次都会重新写日志，覆盖之前的日志
                        # a是追加模式，默认如果不写的话，就是追加模式
                        filemode='a',
                        # format=
                        # '%(asctime)s - %(pathname)s[line:%(lineno)d] - %(levelname)s: %(message)s'
                        # 日志格式
                        format=
                        '%(asctime)s - %(levelname)s: %(message)s'
                        )
