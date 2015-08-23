#! /usr/bin/env python
# -*- coding: utf-8 -*-

import urllib2
import re
from BeautifulSoup import BeautifulSoup as bs
#from Queue import Queue
import Queue
from optparse import OptionParser
import threading
import md5
import time
import logging

import sys
reload(sys)
sys.setdefaultencoding('utf-8')

LOG_LEVELS = {
        1: logging.CRITICAL,
        2: logging.ERROR,
        3: logging.WARNING,
        4: logging.INFO,
        5: logging.DEBUG
}

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
    def __init__(self, opts):
        self.thread_num = opts.thread
        self.urls_queue = Queue.Queue()
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

    def threads_start(self):
        for i in self.threads:
            i.start()

    def wait_all_complete_2(self):
        print '11111111111111111111111111111', 'in', threading.current_thread().getName()
        logging.debug('111111111111111111111111: %s'%threading.current_thread().getName()
)
        self.urls_queue.join()
        print '22222222222222222222222222', 'in', threading.current_thread().getName()
        logging.debug('22222222222222222222222222: %s'%threading.current_thread().getName())
        for i in self.threads:
            if i.isAlive():
                i.join()
        print '333333333333333333333333333', 'in', threading.current_thread().getName()

    def wait_all_complete(self):
        print '22222222222222222222222222', 'in', threading.current_thread().getName()
        logging.debug('111111111111111111111111: %s'%threading.current_thread().getName())
        for i in self.threads:
            if i.isAlive():
                i.join()
        print '11111111111111111111111111111', 'in', threading.current_thread().getName()
        logging.debug('22222222222222222222222222: %s'%threading.current_thread().getName())
        print 'in wait:', self.urls_queue.qsize()
        self.urls_queue.join()
        print '333333333333333333333333333', 'in', threading.current_thread().getName()

class WorkThread(threading.Thread):
    def __init__(self, threadpool):
        # 特别注意，这一行代码不能遗忘，否则程序会发生运行时错误！
        threading.Thread.__init__(self)
        self.threadpool = threadpool

    def run(self):
        while True:
            #url, deep = self.urls_queue.get()
            print 'In run',threading.current_thread().getName()
            #time.sleep(1)
            try:
                url, deep = self.threadpool.urls_queue.get(block = True, timeout = 10)
                print threading.current_thread().getName(), 'url, deep: ', url, deep
            except Queue.Empty:
                #print 'Exception Exception......',
                print threading.current_thread().getName(), 'Queue Empty',
                print threading.current_thread().getName(), ', qsize****:', self.threadpool.urls_queue.qsize()
                break
            except Exception as e:
                print e
            page_html = GetPageHtml(url)
            links = GetLinks(page_html, deep)
            #print 'In WorkThread:', len(links)
            print threading.current_thread().getName(), ', qsize:', self.threadpool.urls_queue.qsize()
            #print 'In WrokThread ', threading.current_thread().getName(), ': ', \
            #        self.threadpool.urls_queue.qsize()
            #print 'In WrokThread ', threading.current_thread().getName(), ': ', links
            #if not links:
            if links:
                print 'In WorkThread:', threading.current_thread().getName(), len(links)
                for i in links:
                    #print self.threadpool.urls_queue.qsize()
                    #hash_code = md5.new(opts.url.encode('utf-8')).hexdigest()
                    #print i
                    hash_code = md5.new(i.encode('utf-8')).hexdigest()

                    self.threadpool.urls_hash_lock.acquire()
                    if not hash_code in self.threadpool.urls_hash:
                        #self.threadpool.urls_queue[hash_code] = i
                        self.threadpool.urls_hash[hash_code] = i
                        #self.threadpool.urls_queue.put(i, deep - 1)
                        self.threadpool.urls_queue.put((i, deep - 1))
                    #千万不能忘记解锁啊！！！
                    self.threadpool.urls_hash_lock.release()

                    #if self.threadpool.urls_hash_lock.acquire():
                    #    if not hash_code in self.threadpool.urls_hash:
                    #        #self.threadpool.urls_queue[hash_code] = i
                    #        self.threadpool.urls_hash[hash_code] = i
                    #        #self.threadpool.urls_queue.put(i, deep - 1)
                    #        self.threadpool.urls_queue.put((i, deep - 1))
                    #    #千万不能忘记解锁啊！！！
                    #    #self.threadpool.urls_hash.release()
                    #    self.threadpool.urls_hash_lock.release()

            #else:
            #    pass
            #    print 'In WrokThread ', threading.current_thread().getName(), ': ', links

            self.threadpool.urls_queue.task_done()
            print
        #self.threadpool.urls_queue.task_done()
        print threading.current_thread().getName(), 'ended*******'
        print

def ParseArgs():
    usage = "usage: %prog [options] arg..."
    parser = OptionParser(usage)
    #parser.add_option('-u', dest="url", default="http://www.sohu.com",
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
    parser.add_option('--key', dest="key", default="",
                      help="页面内的关键词，获取满足该关键词的网页，不指定关键字时获取所有页面")
    #parser.add_option('--encoding', dest="encoding", default=None,
    #                  help="指定页面编码，如果不指定将自动测试页面编码")
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
    html = GetPageHtml(opts.url)
    links = GetLinks(html, 1)
    #print links
    print len(links)
    #print time.time() - start_time

    if opts.testself:
        import doctest
        doctest.testmod(verbose = True)
    else:
        level = LOG_LEVELS[opts.loglevel]
        logging.basicConfig(filename = opts.logfile, level = opts.loglevel)
        logging.critical('Invalid URL:%s'%opts.url)

        pool = ThreadPool(opts)
        pool.threads_start()
        print '=========started in:', threading.current_thread().getName()
        pool.wait_all_complete()
        print 'after call Join:', threading.current_thread().getName()
