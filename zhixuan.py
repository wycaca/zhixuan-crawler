#coding:utf-8
from urllib import request
from urllib import error
from bs4 import BeautifulSoup
import re
import random
import logging
import configparser
import os
import threading
import urllib

#简单日志
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
fh = logging.FileHandler(log_dir,encoding='utf-8')
# 设置最低等级debug
logger.setLevel(logging.INFO)
# 设置日志格式
fm = logging.Formatter("%(asctime)s %(levelname)s %(threadName)s Line:%(lineno)s==> %(message)s")
# 把文件流添加写入格式
fh.setFormatter(fm)
ch.setFormatter(fm)
# 把文件流添加进来，流向写入到文件
logger.addHandler(fh)
logger.addHandler(ch)

url = 'http://www.zxcs.me/post/'
conf = configparser.ConfigParser()
conf.read(BASE_DIR + "/config.ini", encoding='utf-8')
try:
    start_id = conf.getint('config', 'start_id')
    num = conf.getint('config', 'num')
    min_good = conf.getint('config', 'min_good')
    max_good = conf.getint('config', 'max_good')
    max_bad = conf.getint('config', 'max_bad')
    good_book_path = conf.get('config', 'good_book_path')
    down_now = conf.getint('config', 'down_now')
except configparser.NoOptionError as e:
    logging.error("读取 配置文件 错误")


def crawl(_id):
    _url = url
    try:
        # 先获取投票信息
        _vote_info = get_vote(_id)
        # print(_vote_info)
        _vote_good = _vote_info.split(",")[0]
        _vote_bad = _vote_info.split(",")[len(_vote_info.split(",")) - 1]
        # 进入判断逻辑
        flag = judge(int(_vote_good), int(_vote_bad))
        if flag == 1:
            logging.info(str(_id) + " 未通过, 好评: " + str(_vote_good) + ",差评: " + str(_vote_bad))
        # 如果通过, 开始获取书籍信息
        elif flag == 0:
            context_thread = ContextThread(_id, _vote_good, _vote_bad)
            context_thread.start()
            context_thread.join()
        return _id
    except error.HTTPError as e:
        logging.error("请求错误: " + e.code)


def judge(_good, _bad):
    # print(_vote_good)
    # print(_vote_bad)
    if _bad + _good == 0:
        return -1
    elif _bad > _good:
        return 1
    elif _good > max_good:
        return 0
    elif _good < min_good:
        return 1
    elif _bad > max_bad:
        return 1
    elif _good / _bad > 3:
        return 0


def get_vote(_id):
    _base_url = "http://www.zxcs.me/content/plugins/cgz_xinqing/cgz_xinqing_action.php"
    # 拼接投票链接
    _vote_url = _base_url + "?action=show&id=" + _id + "&m=" + str(random.random())
    try:
        _respone = request.urlopen(_vote_url)
    except error.HTTPError as e:
        logging.error("请求错误: " + e.code)
    return _respone.read().decode('utf-8')


def set_start_id(_id):
    # conf = configparser.ConfigParser()
    # conf.add_section('config')
    conf.set('config', 'start_id', str(_id))
    conf.set('config', 'num', str(num))
    with open('config.ini', 'w+', encoding='utf-8') as fw:
        conf.write(fw)


def content_handle(_id, content, _good_info):
    # 标题标签为  h1 直接获取标题内容
    _name = content.h1.string
    logging.info(_id + " : " + _name)
    # 简介 过滤大小
    _strs = content.find(text=re.compile("内容简介")).parent.stripped_strings
    _abstract = ""
    for _str in _strs:
        if _str.find('MB') == -1:
            _abstract += _str + "\n"
    # _abstract = content.find(text=re.compile("内容简介")).parent.get_text().split("介】：")[1]
    # logging.info(_abstract)
    # logging.info("********************")
    # 下载页面
    _down_load_page = content.find(attrs={"class": "down_2"}).a.get("href")
    # logging.info(_down_load_url)
    # logging.info("********************")
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


class ContextThread(threading.Thread):
    def __init__(self, _id, _vote_good,_vote_bad):
        threading.Thread.__init__(self)
        self._id = _id
        self._vote_good = _vote_good
        self._vote_bad = _vote_bad

    def run(self):
        # print("开始内容提取线程")
        get_content(self._id, self._vote_good, self._vote_bad)
        # print("退出内容提取线程")


def get_content(_id, _vote_good, _vote_bad):
    _good_info = str(_id) + " 通过, 好评: " + str(_vote_good) + " ,差评: " + str(_vote_bad)
    logging.info(_good_info)
    # 发送书籍页面请求
    _respone = request.urlopen(url + str(_id))
    logging.info("开始爬取页面: " + url + str(_id))
    # 获取源码, 开始解析
    _html = _respone.read().decode('utf-8')
    _bs = BeautifulSoup(_html, "html.parser")
    # print(_bs.prettify())
    # 主要内容
    content = _bs.find(id='content')
    # 输出测试
    # print(content.prettify())
    content_handle(_id, content, _good_info)


def get_down_page():
    for parent, filenames in os.walk(good_book_path,  followlinks=True):
        for filename in filenames:
            if filename.find('txt') != -1:
                # print('文件名：%s' % filename)
                file_path = os.path.join(parent, filename)
                out = open(file_path, encoding='utf-8')
                line = out.readline()
                if line.find('http') != -1:
                    down_thread = DownThread(line)
                    down_thread.start()
                else:
                    logger.error(file_path + ", 介绍文件第一行, 下载链接错误, 请删除重新获取")


class DownThread(threading.Thread):
    def __init__(self, down_page):
        threading.Thread.__init__(self)
        self.down_page = down_page

    def run(self):
        # print("开始下载线程")
        down_load_txt(self.down_page)
        # print("退出下载线程")


def down_load_txt(_down_page):
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
    _file_name = _name.split("（")[0] + '.' + _full_name.split('.')[len(_full_name.split('.')) - 1]
    _down_path = good_book_path + _file_name
    # 返回字节数 / 1024 = MB
    if os.path.exists(_down_path) and (os.path.getsize(_down_path) / 1024 > 500):
        logger.info(_down_path + ", 压缩包已存在, 将不再写入")
    else:
        logging.info("开始下载: " + _name + ",下载链接为: " + _down_url)
        urllib.request.urlretrieve(_down_url, _down_path)
        logging.info(_name + "下载完成, 存放在: " + _down_path)


if __name__ == '__main__':
    # 爬虫
    for id in range(start_id, start_id - num, -1):
        crawl(str(id))
    set_start_id(id)

    # 下载
    # get_down_page()