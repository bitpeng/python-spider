#! /usr/bin/env python
# -*- coding: utf-8 -*-

import sys
reload(sys)
sys.setdefaultencoding('utf-8')

import urllib2
import re
from BeautifulSoup import BeautifulSoup as bs
import Queue
from optparse import OptionParser
import threading
import md5
import gzip
import chardet
from StringIO import StringIO
import time
import logging
import sqlite3

LOG_LEVELS = {
        1: logging.CRITICAL,
        2: logging.ERROR,
        3: logging.WARNING,
        4: logging.INFO,
        5: logging.DEBUG
}

def ProcessInfo(info):
    pass

def Spider(url, deep, key):
    '''
      抓取页面, 并且根据deep返回页面链接，根据key确定是否保存该页面,其中:
      deep == 0时，是抓取的最后一层深度，即只抓取并保存页面，不分析链接
      deep > 0时，返回页面html和该页面链接
    '''
    html = GetPageHtml(url)
    SaveToDB(url, html, key)
    links = []
    if deep > 0:
        links = GetLinks(html)
    return links

def SaveToDB(url, html, key):
    def save(url, html):
        try:
            db_lock.acquire()
            #cursor.execute("insert into spider(url, html) values(%s, %s)"%(url, html[:300]))
            #cmd = 'insert into spider_table(url, html) values("%s", "%s")'%(url[:10], html[:50])
            #print "%s"%cmd
            cursor.execute("insert into spider_table(url, html) values(?, ?)",(url[:10], html[:50]))
            conn.commit()
            print "<<<<< commit success in: %s >>>>>"%(
                    threading.current_thread().getName())
            #db_lock.release()
        except Exception, e:
            print '========%s========'%str(e)
        finally:
            db_lock.release()

    if key:
        # chardet 监测html编码不一定完全正确，只有一定的可信度
        # 比如检测jd.com页面时，使用的gbk编码, 但是检测结果是gb2312, 
        # 虽然gb2312兼容gbk, 但是据此进一步处理时会出错
        charset = chardet.detect(html)
        encoding = charset['encoding']
        match = re.search(re.compile(key), html.decode(encoding))
        if match:
            save(url, html)
    else:
        save(url, html)

def GetPageHtml(url):
    html = ''
    try:
        response = urllib2.urlopen(url, timeout=20)
        if response.info().get('Content-Encoding') == 'gzip':
            buf = StringIO(response.read())
            f = gzip.GzipFile(fileobj=buf)
            html = f.read()
        else:
            html = response.read()
    except urllib2.URLError as e:
        logging.warning("{0} URL error: {1}".format(url.encode("utf8"), e.reason))
    except urllib2.HTTPError as e:
        logging.warning("{0} HTTP error: {1}".format(url.encode("utf8"), e.code))
    except Exception as e:
        logging.warning("{0} Unexpected: {1}".format(url.encode("utf8"), str(e)))
    else:
        logging.info("{0} downloaded {1}".format(threading.current_thread().getName(), url.encode("utf8")))

    return html

def GetLinks(html):
    links = []
    soup = bs(html)

    # 一个问题是这种方法不一定能提取所有的链接, 或者提取的链接不是合法的URL
    for link in soup.findAll('a', attrs={'href': re.compile("^https?://")}):
    #for link in soup.findAll('a'):#, attrs={'href': re.compile("^https?://")}):
        href = link.get('href')
        links.append(href)
    return links

class ThreadPool(object):
    '''
        线程池，也可以认为是管理线程
    '''
    def __init__(self, opts):
        self.thread_num = opts.thread
        self.options = opts
        self.urls_queue = Queue.Queue()

        # 这里是否需要，待定
        # self.save_to_db_queue = Queue.Queue()
        self.threads = []
        self.urls_hash = {}
        self.urls_hash_lock = threading.Lock()

        # 为了避免处理重复的URL, 因此用列表或者字典处理保存过的.但是由于
        # list或dict是非线程安全的，因此这里需要加锁对他们进行保护
        self.urls_hash[md5.new(opts.url.encode('utf-8')).hexdigest()] = opts.url
        self.urls_queue.put((opts.url, opts.deep))

        self.__init_work_threads(opts.thread)
        #self.urls_queue.join()

    def __init_work_threads(self, thread_num):
        for i in range(thread_num):
            self.threads.append(WorkThread(self))

    # 判断线程池是否繁忙(是否有线程在工作，这是其他暂时没有得到任务
    # 的线程退出的重要判断条件)
    def busy(self):
        for i in self.threads:
            if i.running == True:
                return True
        return False

    def threads_start(self):
        for i in self.threads:
            i.start()

    def wait_all_complete(self):
        for i in self.threads:
            if i.isAlive():
                i.join()
        #self.urls_queue.join()

class WorkThread(threading.Thread):
    def __init__(self, threadpool):
        # 特别注意，这一行代码不能遗忘，否则程序会发生运行时错误！
        threading.Thread.__init__(self)
        self.threadpool = threadpool
        self.running = False
        self.key = threadpool.options.key

    def run(self):
        while True:
            try:
                url, deep = self.threadpool.urls_queue.get(block = True, timeout = 20)
            except Queue.Empty:
                print "%s, qsize: %s"%(threading.current_thread().getName(),
                                  self.threadpool.urls_queue.qsize())
                if not self.threadpool.busy():
                    break
            else:
                self.running = True
                print "%s, url=%s, deep=%s, qsize=%s"%(
                      threading.current_thread().getName(), url, deep, self.threadpool.urls_queue.qsize())
                links = Spider(url, deep, self.key)
                if links:
                    print "++++++++++ In WorkThread: %s, links_lenth: %s ++++++++++"%(
                            threading.current_thread().getName(), len(links))
                    for i in links:
                        hash_code = md5.new(i.encode('utf-8')).hexdigest()

                        self.threadpool.urls_hash_lock.acquire()
                        if not hash_code in self.threadpool.urls_hash:
                            self.threadpool.urls_hash[hash_code] = i
                            self.threadpool.urls_queue.put((i, deep - 1))
                        #千万不能忘记解锁啊!!!
                        self.threadpool.urls_hash_lock.release()
                self.running = False
                self.threadpool.urls_queue.task_done()
                #print
            #except Exception:
            #    print "Unknown Exception in:%s"%(threading.current_thread().getName())
            #    break
        print "%s, ended*******\n"%(threading.current_thread().getName())

def ParseArgs():
    usage = "usage: %prog [options] arg..."
    parser = OptionParser(usage)
    parser.add_option("-u", dest="url", default="http://www.baidu.com",
                      help="spider start url")
    parser.add_option("-d", type=int, dest="deep", default=1,
                      help="scrawl the page only if depth == 0")
    parser.add_option("--thread", type=int, dest="thread", default=10,
                      help="thread num of the spider")
    parser.add_option('--dbfile', dest="dbfile", default="spider.db",
                      help="db filename to store result")
    parser.add_option('-f', dest="logfile", default="spider.log", help="logfile name")
    parser.add_option('-l', dest="loglevel", default="5", type=int,
                      help="log level of log file, the bigger, the more detailed(1-5)")
    parser.add_option('--key', dest="key", default=None,
                      help="save the page if it matches the keyword, save all if keyword is None")
    parser.add_option('--testself', action="store_true", dest="testself", default=False,
                      help="test the program itself")
    (options, args) = parser.parse_args()
    if len(args) != 0:
        parser.print_help()
        return

    return options


if __name__ == '__main__':
    start_time = time.time()
    opts = ParseArgs()
    print opts

    def test():
        html = GetPageHtml(opts.url)
        links = GetLinks(html, 1)
        print len(links)
        print time.time() - start_time

    if opts.testself:
        import doctest
        doctest.testmod(verbose = True)
    else:
        db_lock = threading.Lock()
        conn = sqlite3.connect(opts.dbfile, check_same_thread = False)
        conn.text_factory = str
        cursor = conn.cursor()
        cursor.execute("""
            create table if not exists spider_table(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                url text,
                html text
            )
        """)
        cursor.execute("delete from spider_table")
        conn.commit()

        level = LOG_LEVELS[opts.loglevel]
        logging.basicConfig(filename = opts.logfile, level = opts.loglevel)
        #logging.critical('Invalid URL:%s'%opts.url)

        pool = ThreadPool(opts)
        pool.threads_start()
        pool.wait_all_complete()
        print 'successful completion.....'
