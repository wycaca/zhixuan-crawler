# coding:utf-8
import configparser
import logging
import os
import random
import re
import threading
import urllib
from urllib import error, request
from bs4 import BeautifulSoup
import rarfile

# 简单日志
# logging.basicConfig(filename='知轩爬虫.log',
#                     level=logging.DEBUG,
#                     format='%(asctime)s %(levelname)s Line:%(lineno)s==> %(message)s',
#                     datefmt='%Y-%m-%d %H:%M:%S',
#                     # 模式，有w和a，w就是写模式，每次都会重新写日志，覆盖之前的日志
#                     # a是追加模式，默认如果不写的话，就是追加模式
#                     filemode='a')

# 获取上级目录的绝对路径
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
log_dir = BASE_DIR + '/zhixuan.log'
# 获得一个logger对象，默认是root
logger = logging.getLogger()
# 日志输出到屏幕控制台
ch = logging.StreamHandler()
# 设置日志等级
ch.setLevel(logging.DEBUG)
# 创建一个文件流并设置编码utf8
fh = logging.FileHandler(log_dir, encoding='utf-8')
# 设置最低等级debug
logger.setLevel(logging.INFO)
# 设置日志格式
fm = logging.Formatter(
    "%(asctime)s %(levelname)s %(threadName)s Line:%(lineno)s==> %(message)s")
# 把文件流添加写入格式
fh.setFormatter(fm)
ch.setFormatter(fm)
# 把文件流添加进来，流向写入到文件
logger.addHandler(fh)
logger.addHandler(ch)

# 配置文件
conf = configparser.ConfigParser()
conf.read(BASE_DIR + os.path.sep + "config.ini", encoding='utf-8')
try:
    start_id = conf.getint('config', 'start_id')
    num = conf.getint('config', 'num')
    min_good = conf.getint('config', 'min_good')
    max_good = conf.getint('config', 'max_good')
    max_bad = conf.getint('config', 'max_bad')
    good_book_path = conf.get('config', 'good_book_path')
    down_now = conf.getint('config', 'down_now')
    local_dir = conf.get('config', 'local_dir')
    unrar_path = conf.get('config', 'unrar_path')
except configparser.NoOptionError as e:
    logging.error("读取 配置文件 错误")

# 常量
url = "http://www.zxcs.me/post/"
thread_list = []
book_list = BASE_DIR + os.path.sep + "book_list.txt"
uncompress_path = good_book_path + "unrar" + os.path.sep

def crawl(_id):
    # 创建 本地电子书列表
    get_local_book_list(local_dir)
    try:
        for id in range(_id, _id - num, -1):
            vote_thread = VoteJudgeThread(id)
            vote_thread.start()
            vote_thread.join(500)
            thread_list.append(vote_thread)
        # print("正在运行的线程": " + str(threading.activeCount()))
    finally:
        set_start_id(id)
        # 遍历线程列表, 确保所有线程结束
        for thread in thread_list:
            thread.join()
        # print("最后一个主线程: " + str(threading.activeCount()))
        delete_url_file()

    
class VoteJudgeThread(threading.Thread):
    def __init__(self, _id):
        threading.Thread.__init__(self)
        self._id = str(_id)

    def run(self):
        get_vote(self._id)


# 判断
def judge(_good, _bad):
    # print(_vote_good)
    # print(_vote_bad)
    # 好评为0
    if _bad + _good == 0 or _bad == 0:
        return False
    elif _bad > _good:
        return False
    elif _good > max_good:
        return True
    elif _bad > max_bad:
        return False
    # 差评为0
    elif _bad == 0 and _good >= min_good:
        return True
    elif _good / _bad > 3:
        return True
    elif _good < min_good:
        return False


# 获取投票情况
def get_vote(_id):
    # print("开始评论判断线程")
    _base_url = "http://www.zxcs.me/content/plugins/cgz_xinqing/cgz_xinqing_action.php"
    # 拼接投票链接
    _vote_url = _base_url + "?action=show&id=" + \
        _id + "&m=" + str(random.random())
    try:
        # 先获取投票信息
        _respone = request.urlopen(_vote_url)
        _vote_info = _respone.read().decode('utf-8')
        # print(_vote_info)
        _vote_good = _vote_info.split(",")[0]
        _vote_bad = _vote_info.split(",")[len(_vote_info.split(",")) - 1]
        # 进入判断逻辑
        flag = judge(int(_vote_good), int(_vote_bad))
        # 如果通过, 开始获取书籍信息
        if flag:
            context_thread = ContextThread(_id, _vote_good, _vote_bad)
            context_thread.start()
            thread_list.append(context_thread)
            # context_thread.join()

        else:
            print(str(_id) + " 未通过, 好评: " + str(_vote_good) + ",差评: " + str(_vote_bad))
        return _id
    except error.HTTPError as e:
        logging.error("请求错误: " + e.code)
    # print("退出评论判断线程")


# 更新起始书籍id
def set_start_id(_id):
    # conf = configparser.ConfigParser()
    # conf.add_section('config')
    conf.set('config', 'start_id', str(_id))
    conf.set('config', 'num', str(num))
    with open('config.ini', 'w+', encoding='utf-8') as fw:
        conf.write(fw)


# 创建 本地电子书列表
def get_local_book_list(_local_dir):
    if os.path.exists(book_list):
        logger.info("本地电子书列表已存在, 即将删除并重建")
        os.remove(book_list)
    if os.path.exists(_local_dir):
        with open(book_list, 'a', encoding='utf-8') as fw:
            names = [filename for filename in os.listdir(_local_dir)
                     if filename.endswith('.txt')]
            for filename in names:
                fw.write(os.path.splitext(filename)[0] + '\n')
    else:
        logger.error("本地电子书位置" + _local_dir + "不存在")
        return False


# 本地电子书是否存在
def search_local_book(_book_name):
    if os.path.exists(book_list):
        with open(book_list, 'r', encoding='utf-8') as fr:
            booklist = fr.readlines()
            if booklist.count(_book_name + "\n") > 0:
                return True
            else:
                return False
    else:
        logger.error("本地电子书列表: " + book_list + " 不存在")


# 内容提取
def content_handle(_id, content, _good_info):
    # 标题标签为  h1 直接获取标题内容
    _name = content.h1.string
    _book_name = _name.split("《")[1].split("》")[0]
    # 查找本地是否存在
    if search_local_book(_book_name):
        logger.info(str(_id) + " " + _book_name + " 本地已存在, 跳过爬取")
    else:
        logging.info(_name + " " + _good_info)
        # 简介 过滤大小
        _strs = content.find(text=re.compile("内容简介")).parent.stripped_strings
        _abstract = ""
        for _str in _strs:
            if _str.find('MB') == -1:
                _abstract += _str + "\n"
        # _abstract = content.find(text=re.compile("内容简介")).parent.get_text().split("介】：")[1]
        # print(_abstract)
        # print("********************")
        # 下载页面
        _down_load_page = content.find(attrs={"class": "down_2"}).a.get("href")
        # print(_down_load_url)
        # print("********************")
        _info_list = [_down_load_page + "\n", _name + "\n", _abstract, _good_info]
        if not os.path.exists(good_book_path):
            os.mkdir(good_book_path)
        _file_name = good_book_path + _name.split("（")[0] + "-介绍.txt"
        if os.path.exists(_file_name) and (os.path.getsize(_file_name) > 0):
            logger.info(_file_name + ", 介绍文件已存在, 将不再写入")
        else:
            with open(_file_name, 'w', encoding="utf-8") as fp:
                fp.writelines(_info_list)
            # 立即下载 0,其他 不立即下载
            if down_now == 0:
                down_thread = DownThread(_down_load_page)
                down_thread.start()
                # down_thread.join()
                thread_list.append(down_thread)


class ContextThread(threading.Thread):
    def __init__(self, _id, _vote_good, _vote_bad):
        threading.Thread.__init__(self)
        self._id = _id
        self._vote_good = _vote_good
        self._vote_bad = _vote_bad

    def run(self):
        get_content(self._id, self._vote_good, self._vote_bad)


# 获取简介
def get_content(_id, _vote_good, _vote_bad):
    # print("开始内容提取线程")
    _good_info = str(_id) + " 通过, 好评: " + str(_vote_good) + " ,差评: " + str(_vote_bad)
    #logging.info(_good_info)
    # 发送书籍页面请求
    _respone = request.urlopen(url + str(_id))
    # print("开始爬取页面: " + url + str(_id))
    # 获取源码, 开始解析
    _html = _respone.read().decode('utf-8')
    _bs = BeautifulSoup(_html, "html.parser")
    # print(_bs.prettify())
    # 主要内容
    content = _bs.find(id='content')
    # 输出测试
    # print(content.prettify())
    content_handle(_id, content, _good_info)
    # print("退出内容提取线程")


# 事后下载
def get_down_page():
    for root, dirs, filenames in os.walk(good_book_path):
        for filename in filenames:
            if filename.endswith('txt') != -1:
                # print('文件名：%s' % filename)
                file_path = os.path.join(root, filename)
                out = open(file_path, encoding='utf-8')
                line = out.readline()
                if line.find('http') != -1:
                    down_thread = DownThread(line)
                    down_thread.start()
                    thread_list.append(down_thread)
                else:
                    logger.error(file_path + ", 介绍文件第一行, 下载链接错误, 请删除重新获取")


class DownThread(threading.Thread):
    def __init__(self, down_page):
        threading.Thread.__init__(self)
        self.down_page = down_page

    def run(self):
        down_load_txt(self.down_page)


# 下载
def down_load_txt(_down_page):
    # print("开始下载线程")
    try:
        _respone = request.urlopen(_down_page)
    except error.HTTPError as e:
        logging.error("请求错误: " + e.code)
    _html = _respone.read().decode('utf-8')
    _bs = BeautifulSoup(_html, "html.parser")
    # 下载
    _down_url = _bs.find(text=re.compile('线路一')).parent.get("href")
    # 名称
    _name = _bs.h2.string
    _down_info = [_name, _down_url]
    # cqwww,qyxfT.rar
    _full_name = _down_url.split('/')[len(_down_url.split('/')) - 1]
    # cqwww.rar
    # _file_name = _full_name.split(',')[0] + '.' + _full_name.split('.')[len(_full_name.split('.')) - 1]
    # 春秋我为王.rar
    _file_name = _name.split(
        "（")[0] + '.' + _full_name.split('.')[len(_full_name.split('.')) - 1]
    _down_path = good_book_path + _file_name
    # 返回字节数 / 1024 = MB
    if os.path.exists(_down_path) and (os.path.getsize(_down_path) / 1024 > 500):
        logger.info(_down_path + ", 压缩包已存在, 将不再写入")
    else:
        # print("开始下载: " + _name + ",下载链接为: " + _down_url)
        urllib.request.urlretrieve(_down_url, _down_path)
        logging.info(_name + " 下载完成, 保存为: " + _down_path)
        rar_thread = RarThread(_down_path, uncompress_path)
        rar_thread.start()
        thread_list.append(rar_thread)
    # print("退出下载线程")


class RarThread(threading.Thread):
    def __init__(self, src_file, dest_dir):
        threading.Thread.__init__(self)
        self.src_file = src_file
        self.dest_dir = dest_dir

    def run(self):
        uncompress(self.src_file, self.dest_dir)

def uncompress(src_file, dest_dir):
    # print("开始解压线程")
    if os.path.isfile and os.path.splitext(src_file)[1] == '.rar':
        if not os.path.exists(dest_dir):
            os.mkdir(dest_dir)
        # 设置 unrar.exe路径
        rarfile.UNRAR_TOOL = unrar_path + "unrar.exe"
        with rarfile.RarFile(src_file) as rf:
            rf.extractall(dest_dir)
    else:
        logging.error(src_file + ", 这不是一个有效的rar文件")
    # print("退出解压线程")


# 去掉书名号, 删除链接文件
def delete_url_file():
    for root, dirs, files in os.walk(uncompress_path, topdown=True):
        for filename in files:
            if filename.endswith('txt') and filename.find("《") != -1:
                # print('文件名：%s' % filename)
                file_path = os.path.join(root, filename)
                newname = filename.split("《")[1].split("》")[0]
                os.rename(file_path, os.path.join(root, newname + ".txt"))
    # 删链接文件
    url_file_path = uncompress_path + "知轩藏书.url"
    if os.path.exists(url_file_path):
        os.remove(url_file_path)

if __name__ == '__main__':
    # 爬虫
    crawl(start_id)
    # 根据简介文件 另外下载
    #get_down_page()
