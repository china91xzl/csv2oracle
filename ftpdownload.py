import ftplib
import traceback
import logging
import os
import fileutil


class FtpDownload:
    # 定义一个ftp对象
    ftp = ftplib.FTP()

    def __init__(self, host, port):
        self.ftp.connect(host, port)

    # 登录
    def login(self, username, password):
        self.ftp.login(username, password)
        # 打印出欢迎信息
        print(self.ftp.welcome)

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
                fileutil.file_unzip()
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



