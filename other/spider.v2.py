#! /usr/bin/env python
# -*- coding: utf-8 -*-

import sys
reload(sys)
sys.setdefaultencoding('utf-8')

import urllib2
import re
from BeautifulSoup import BeautifulSoup as bs
#from Queue import Queue
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

def processInfo(info):
    pass

def GetPageHtml_WithExceptionHandler(url, threadpool):
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

        # 这里需要保存的页面是不是不用保存到队列里？保存到队列里另外起一个线程保存到数据库
        # 这样操作反而使得问题复杂化，可不可以简单处理呢？即每次匹配关键字的页面，直接在工作
        # 线程里，将他保存起来.先把这种方式弄出来，然后测试不另起线程的方式！
        if threadpool.options.key:
            # chardet 监测html编码不一定完全正确，只有一定的可信度
            charset = chardet.detect(html)
            encoding = charset['encoding']

            match = re.search(re.compile(threadpool.options.key), html.decode(encoding))
            if match:
                threadpool.save_to_db_queue.put(url, html)
        else:
            threadpool.save_to_db_queue.put(url, html)

    finally:
        return html

def GetPageHtml(url):
    page_html = urllib2.urlopen(url).read()
    # 页面不一定是utf-8编码，还需要转换编码
    # 编码问题先略过
    #page_html = page_html.decode('gbk')
    return page_html

def GetLinks(page_html,deep = 1):
    '''
    奇怪，为什么这个测试用例通不过?
    >>> GetLinks('page', 0)
    None
    '''
    if deep < 1:
        return None
    links = []
    soup = bs(page_html)

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
        self.save_to_db_queue = Queue.Queue()
        self.threads = []
        self.urls_hash = {}
        self.urls_hash_lock = threading.Lock()

        # 非线程安全
        self.urls_hash[md5.new(opts.url.encode('utf-8')).hexdigest()] = opts.url
        self.urls_queue.put((opts.url, opts.deep))

        self.__init_work_threads(opts.thread)
        #self.urls_queue.join()

    def __init_work_threads(self, thread_num):
        for i in range(thread_num):
            self.threads.append(WorkThread(self))

    # 判断线程池是否繁忙(是否有线程在工作，这是其他暂时没有得到任务
    # 的线程退出的重要判断条件)
    def busy(slef):
        for i in self.threads:
            if i.running == True:
                return True
        return False

    def threads_start(self):
        for i in self.threads:
            i.start()

    def wait_all_complete(self):
        print "%s,%s"%(threading.current_thread().getName(), '22222222222222222222222222')
        for i in self.threads:
            if i.isAlive():
                i.join()
        print "%s,%s"%(threading.current_thread().getName(), '11111111111111111111111111')
        print "%s,%s"%('in wait:', self.urls_queue.qsize())
        #self.urls_queue.join()
        print "%s,%s"%(threading.current_thread().getName(), '33333333333333333333333333')

class WorkThread(threading.Thread):
    def __init__(self, threadpool):
        # 特别注意，这一行代码不能遗忘，否则程序会发生运行时错误！
        threading.Thread.__init__(self)
        self.threadpool = threadpool
        self.running = False

    def run(self):
        while True:
            try:
                url, deep = self.threadpool.urls_queue.get(block = True, timeout = 20)
                self.running = True
                print "%s, url=%s, deep=%s, qsize=%s"%(
                      threading.current_thread().getName(), url, deep, self.threadpool.urls_queue.qsize())
                page_html = GetPageHtml_WithExceptionHandler(url, self.threadpool)
                links = GetLinks(page_html, deep)
                if links:
                    print "=========== In WorkThread: %s, links_lenth: %s ==========="%(
                            threading.current_thread().getName(), len(links))
                    for i in links:
                        hash_code = md5.new(i.encode('utf-8')).hexdigest()

                        self.threadpool.urls_hash_lock.acquire()
                        if not hash_code in self.threadpool.urls_hash:
                            self.threadpool.urls_hash[hash_code] = i
                            self.threadpool.urls_queue.put((i, deep - 1))
                        #千万不能忘记解锁啊！！！
                        self.threadpool.urls_hash_lock.release()
                self.running = False
                self.threadpool.urls_queue.task_done()
                #print
            except Queue.Empty:
                print "%s,%s,%s"%(threading.current_thread().getName(), 
                                  'qsize:', self.threadpool.urls_queue.qsize())
                if not self.threadpool.busy():
                    break
            except Exception:
                print "Unknown Exception in:%s"%(threading.current_thread().getName())
                break
        print "%s,%s"%(threading.current_thread().getName(), 'ended*******')
        print

def ParseArgs():
    usage = "usage: %prog [options] arg..."
    parser = OptionParser(usage)
    parser.add_option('-u', dest="url", default="http://www.baidu.com",
                      help="爬虫起始地址")
    parser.add_option("-d", type=int, dest="deep", default=1,
                      help="爬取深度，默认深度是1；而深度为0即只爬取url指定的页面")
    parser.add_option("--thread", type=int, dest="thread", default=10,
                      help="爬虫使用的线程数")
    parser.add_option('--dbfile', dest="dbfile", default="spider.db",
                      help="存放数据库（sqlite）文件名")
    parser.add_option('-f', dest="logfile", default="spider.log", help="日志文件名")
    parser.add_option('-l', dest="loglevel", default="5", type=int,
                      help="日志记录文件记录详细程度，数字越大记录越详细(1-5)")
    parser.add_option('--key', dest="key", default=None,
                      help="页面内的关键词，获取满足该关键词的网页，不指定关键字时获取所有页面")
    parser.add_option('--testself', action="store_true", dest="testself", default=False,
                      help="程序自测自测")
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
        level = LOG_LEVELS[opts.loglevel]
        logging.basicConfig(filename = opts.logfile, level = opts.loglevel)
        logging.critical('Invalid URL:%s'%opts.url)

        pool = ThreadPool(opts)
        pool.threads_start()
        print "%s,%s"%('started in:', threading.current_thread().getName())
        pool.wait_all_complete()
        print "%s,%s"%('after call Join:', threading.current_thread().getName())
