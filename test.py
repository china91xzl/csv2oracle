# import xml.etree.ElementTree as ET
#
#
# def display_book(book):
#
#     root = ET.parse(source="test.xml")
#     info = root.getroot()
#
#     for elem in info:
#         name = elem.attrib['id']
#         print(name)
#         # root = ET.parse(source="test.xml")
#         # info = root.iter("catalog")
#         #
#         # for elem in info:
#         #     books = elem.findall("book")
#         #     for book in books:
#         #         print(book.attrib['id'])
#         #
#         # return "Book Not Found"
#     return "Book Not Found"
#
#
#
# display_book("bk101")

import csv
import os
import traceback


def import_csv(down_dir):
    # 获取根目录下所有文件
    for dir_path, dir_names, file_names in os.walk(down_dir):
        for filename in file_names:
            try:
                # 完整路径名
                dir_comp = dir_path + filename
                # 通过 reader读取单个文件内容
                with open(dir_comp, 'rb') as csv_file:
                    reader = csv.reader(csv_file, dialect='excel')
                    csv_reader = csv.DictReader(csv_file)
                    j = 0
                    # 遍历文件中的所有行和列，数据清洗，获取字典列表param_list
                    for row in csv_reader:
                        j = j + 1
                        print('第%s次' % str(j) +str(row))
                # 关闭csv文档
                csv_file.close()
            except:
                s = traceback.format_exc()
                print('文件导入失败')
                print(s)


import_csv('E:/download/')
