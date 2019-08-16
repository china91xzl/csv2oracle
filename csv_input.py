import cx_Oracle
import csv
import datetime
import time
import re
import os
import ftplib
import zipfile
import traceback
import logging
import configparser


class FtpDownload:
    # 定义一个ftp对象
    ftp = ftplib.FTP()

    def __init__(self, host, port):
        try:
            self.ftp.connect(host, port)
        except ftplib.error_perm as e:
            s = traceback.format_exc()
            print('FTP服务器连接失败，请检查网络')
            logging.error(s)
            print(e)
            print(s)

    # 登录
    def login(self, username, password):
        try:
            self.ftp.login(username, password)
            # 打印出欢迎信息
            print(self.ftp.welcome)
        except ftplib.Error as e:
            s = traceback.format_exc()
            print('FTP服务器登录失败，请检查口令')
            logging.error(s)
            print(e)
            print(s)

    # 下载单个文件
    # local_file为本地文件路径（带文件名）,remote_file为ftp文件路径(不带文件名)
    def download_file(self, local_file, remote_file):
        if (os.path.exists(local_file)):
            os.remove(local_file)
        file_handler = open(local_file, 'wb')
        print(file_handler)
        # 下载ftp文件
        self.ftp.retrbinary('RETR ' + remote_file, file_handler.write)
        file_handler.close()
        return True

    # 下载整个目录下的文件
    # local_dir为本地目录（不带文件名）,remote_dir为远程目录(不带文件名)
    def download_file_tree(self, local_dir, remote_dir, username, password):
        try:
            self.ftp.login(username, password)
            # 打印出欢迎信息
            print(self.ftp.welcome)
            print("打开远程目录:", remote_dir)
            if not os.path.exists(local_dir):
                os.makedirs(local_dir)
            # 打开该远程目录
            self.ftp.cwd(remote_dir)
            # 获取该目录下所有文件名，列表形式
            remote_names = self.ftp.nlst()
            for file in remote_names:
                local = os.path.join(local_dir, file)  # 下载到当地的全路径
                if file.find(".") == -1:  # 是否子目录 如test.txt就非子目录
                    if not os.path.exists(local):
                        os.makedirs(local)
                    self.download_file_tree(local, file, username, password)  # 下载子目录路径
                else:
                    self.download_file(local, file)
                print(self.ftp.nlst(file))
                print('下载成功')
                # 删除FTP上的文件，防止产生大量的历史文件占用硬盘内存
                self.ftp.delete(file)
                print('删除FTP文件成功')
                # 解压缩下载的文件
                file_unzip(download_dir)
            self.ftp.cwd("..")  # 返回路径最外侧
        except ftplib.Error as e:
            s = traceback.format_exc()
            print('FTP服务器登录失败，请检查口令')
            logging.error('FTP服务器登录失败，请检查口令'+s)
            print(e)
            print(s)
        return True

    # 删除FTP文件
    def del_ftp_file(self):
        remote_names = self.ftp.nlst()
        for file in remote_names:
            self.ftp.delete(file)
        print('删除FTP文件成功')
        return True

    # 关闭ftp连接
    def close(self):
        self.ftp.close()
        print("FTP连接已关闭")
        return True


class ImportOracle:
    # 定义一个oracle对象
    db = cx_Oracle

    def __init__(self, username, password, host):
        try:
            # 连接数据库
            global conn
            global cur
            conn = self.db.connect(username, password, host)
            # 游标操作
            cur = conn.cursor()
        except cx_Oracle.Error as e:
            s = traceback.format_exc()
            print('数据库连接失败')
            logging.error('数据库连接失败' + s)
            print(e)
            print(s)

    def import_csv(self, down_dir):
        ins = 0
        # 获取根目录下所有文件
        for dir_path, dir_names, file_names in os.walk(down_dir):
            # print('Directory:', dir_path)
            for filename in file_names:
                # 过滤csv文件
                res = re.match('(.*?).csv', filename)
                try:
                    if res:
                        # 完整路径名
                        dir_comp = dir_path + filename
                        # 表名，即csv文件名
                        tab_name = res.group(1)
                        # 通过 reader读取单个文件内容
                        with open(dir_comp, "rt", encoding='gbk') as csv_file:
                            reader = csv.reader(csv_file, dialect='excel')
                            # 获取数据的第一列，作为后续要转为字典的键名生成器，next方法获取
                            fieldnames = next(reader)
                            csv_reader = csv.DictReader(csv_file, dialect='excel')
                            sql_col = ''
                            sql_values = ''
                            # 获取INSERT语句中的列名和值
                            for col_name in fieldnames:
                                sql_col += col_name + ', '
                                sql_values += ':' + col_name + ', '
                            # 使用切片删掉最后一个多余的逗号
                            sql_col = sql_col[:-2]
                            sql_values = sql_values[:-2]
                            sql_insert = 'INSERT INTO' + ' ' + tab_name + ' ' + '(' + sql_col + ')' \
                                         + ' ' + 'VALUES' + '(' + sql_values + ')'
                            # print(sql_insert)
                            param_list = []
                            j = 0
                            # 遍历文件中的所有行和列，数据清洗，获取字典列表param_list
                            # for row in csv_reader:
                            #     j = j + 1
                            #     print('第%s次' % str(j))
                            for row in csv_reader:
                                param = {}
                                for k, v in row.items():
                                    # 通过正则表达式匹配日期字符串，转换成日期格式
                                    res = re.match('(.*?).000000000', v)
                                    # kettle导出中文字符数据会在末尾产生多个空格，使用正则去掉
                                    res_r = re.match('(.*?)    ', v)
                                    if res:
                                        v = datetime.datetime.strptime(res.group(1), '%Y/%m/%d %H:%M:%S')
                                        param[k] = v
                                        # print(res.group(0))
                                    elif res_r:
                                        v = res_r.group(1)
                                        param[k] = v
                                    else:
                                        param[k] = v
                                param_list.append(param)
                                j = j + 1
                                if j == 127:
                                    print('第%s次' % str(j))
                                print('第%s次' % str(j))
                            # print(param_list)
                            # 批量插入数据库
                            cur.executemany(sql_insert, param_list)
                            # 提交事务
                            conn.commit()
                        print('csv文件导入成功:' + filename)
                        # 关闭csv文档
                        csv_file.close()
                except:
                    s = traceback.format_exc()
                    # 释放数据库连接
                    cur.close()
                    conn.close()
                    print('数据库连接已释放')
                    print('文件导入失败')
                    logging.error('文件导入失败' + s)
                    print(s)
        cur.close()
        conn.close()
        print('数据库连接已释放')
        # 删除下载目录的文件
        del_download_file(down_dir)
        return True


# 解压缩文件
def file_unzip(down_dir):
    z = zipfile
    for dir_path, dir_names, file_names in os.walk(down_dir):
        # 过滤csv文件
        for filename in file_names:
            res = re.match('(.*?).zip', filename)
            if res:
                z = zipfile.ZipFile(down_dir + filename, 'r')
                z.extractall(path=down_dir)
    print('解压缩文件成功')
    z.close()
    return True


def del_download_file(down_dir):
    ls = os.listdir(down_dir)
    for i in ls:
        c_path = os.path.join(down_dir, i)
        os.remove(c_path)
    print('删除本地文件成功')
    return True


def mkdir(path):
    # 判断路径是否存在
    # 存在     True
    # 不存在   False
    is_exists = os.path.exists(path)
    # 判断结果
    if not is_exists:
        # 如果不存在则创建目录
        # 创建目录操作函数
        os.makedirs(path)
        print(path + '创建成功')
        return True
    else:
        # 如果目录存在则不创建，并提示目录已存在
        print(path + '目录已存在')
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


if __name__ == "__main__":
    # 加载现有配置文件
    conf = configparser.ConfigParser()
    conf.read("conf.ini", encoding="utf-8")
    # 读取配置信息
    db_host = conf.get('db_config', 'db_host')
    db_user = conf.get('db_config', 'db_user')
    db_password = conf.get('db_config', 'db_password')
    ftp_host = conf.get('ftp_config', 'ftp_host')
    ftp_port = conf.getint('ftp_config', 'ftp_port')
    ftp_user = conf.get('ftp_config', 'ftp_user')
    ftp_password = conf.get('ftp_config', 'ftp_password')
    ftp_dir = conf.get('ftp_config', 'ftp_dir')
    download_dir = conf.get('local_dir', 'download_dir')
    interval = conf.getint('local_dir', 'interval')
    # 创建下载目录
    mkdir(download_dir)
    # 设置日志文件
    log_set()
    i = 0
    # 定时同步FTP
    while True:
        # 建立一个ftp连接
        ftp = FtpDownload(ftp_host, ftp_port)
        # 下载整个目录下的文件
        ftp.download_file_tree(download_dir, ftp_dir, ftp_user, ftp_password)
        # 关闭ftp连接
        ftp.close()
        # csv文件导入数据库
        db = ImportOracle(db_user, db_password, db_host)
        db.import_csv(download_dir)
        i = i + 1
        print('同步次数：' + str(i))
        time.sleep(interval)











