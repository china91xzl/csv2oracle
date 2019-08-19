import cx_Oracle
import csv
import datetime
import time
import re
import os
import traceback
import configparser
import fileutil


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
            fileutil.logging.error('数据库连接失败' + s)
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
                    fileutil.logging.error('文件导入失败' + s)
                    print(s)
        cur.close()
        conn.close()
        print('数据库连接已释放')
        # 删除下载目录的文件
        fileutil.del_download_file()
        return True


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
    fileutil.mkdir()
    # 设置日志文件
    fileutil.log_set()
    i = 0
    # 定时同步FTP
    while True:
        # 建立一个ftp连接
        # ftp = ftpdownload.FtpDownload(ftp_host, ftp_port)
        # 下载整个目录下的文件
        # ftp.download_file_tree(download_dir, ftp_dir, ftp_user, ftp_password)
        # 关闭ftp连接
        # ftp.close()
        # csv文件导入数据库

        db = ImportOracle(db_user, db_password, db_host)
        db.import_csv(download_dir)
        i = i + 1
        print('同步次数：' + str(i))
        time.sleep(interval)











