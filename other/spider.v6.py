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

start_time = time.time()
#INFO = '''\
#**************************************************
#*           当前总任务数:    {total_number:>8}            *
#*           待处理任务数:    {wait_to_process:>8}            *
#*           成功下载并保存:  {success:>8}            *
#*           处理失败数:      {failure:>8}            *
#**************************************************\n
#'''
INFO = '''\
**************************************************
*           当前总任务数:    {0:>8}            *
*           待处理任务数:    {1:>8}            *
*           成功下载并保存:  {2:>8}            *
*           处理失败数:      {3:>8}            *
**************************************************\n
'''

# 打印进度信息
class PrintProgress(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)

    def run(self):
        while True:
            time.sleep(10)

            pool_busy = pool.busy()
            p_info = pool.progress_info()

            if not pool_busy:
                break

            print INFO.format(p_info['total_number'], p_info['wait_to_process'],
                              p_info['success'], p_info['failure'])
            #print INFO.format(total_number = p_info['total_number'],
            #                  wait_to_process = p_info['wait_to_process'],
            #                  success = p_info['success'],
            #                  failure = p_info['failure'])
            logging.debug("Print progress")


class Spider(object):
    def __init__(self, url, deep, key):
        self.url = url
        self.deep = deep
        self.key = key

        self.html = self.get_html()

    def run(self):
        '''
          抓取页面, 并且根据deep返回页面链接，根据key确定是否保存该页面,其中:
          deep == 0时，是抓取的最后一层深度，即只抓取并保存页面，不分析链接
          deep > 0时，返回该页面链接
        '''
        self.save_to_db()
        links = []
        if self.deep > 0:
            links = self.get_links()
        return links

    def save_to_db(self):
        def save():
            try:
                db_lock.acquire()
                #cursor.execute("insert into spider(url, html) values(%s, %s)"%(url, html[:300]))
                #cmd = 'insert into spider_table(url, html) values("%s", "%s")'%(url[:10], html[:50])
                #print "%s"%cmd
                cursor.execute("insert into spider_table(url, html) values(?, ?)",
                        (self.url[:10], self.html[:50]))
                conn.commit()
                pool.success += 1
                print "<<<<< commit success in: %s >>>>>"%(threading.current_thread().getName())
                #db_lock.release()
            except Exception, e:
                pool.failure += 1
                print '========%s========'%str(e)
            finally:
                db_lock.release()

        if self.key:
            # chardet 监测html编码不一定完全正确，只有一定的可信度
            # 比如检测jd.com页面时，使用的gbk编码, 但是检测结果是gb2312, 
            # 虽然gb2312兼容gbk, 但是据此进一步处理时会出错
            charset = chardet.detect(self.html)
            encoding = charset['encoding']
            match = re.search(re.compile(self.key), self.html.decode(encoding))
            if match:
                save()
        else:
            save()

    def get_html(self):
        html = ''
        try:
            response = urllib2.urlopen(self.url, timeout=20)
            if response.info().get('Content-Encoding') == 'gzip':
                buf = StringIO(response.read())
                f = gzip.GzipFile(fileobj=buf)
                html = f.read()
            else:
                html = response.read()
        except urllib2.URLError as e:
            logging.warning("{0} URL error: {1}".format(self.url.encode("utf8"), e.reason))
        except urllib2.HTTPError as e:
            logging.warning("{0} HTTP error: {1}".format(self.url.encode("utf8"), e.code))
        except Exception as e:
            logging.warning("{0} Unexpected: {1}".format(self.url.encode("utf8"), str(e)))
        else:
            logging.info("{0} downloaded {1}".format(
                    threading.current_thread().getName(), self.url.encode("utf8")))

        return html

    def get_links(self):
        links = []
        soup = bs(self.html)

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
        #self.options = opts
        self.key = opts.key

        # 待下载保存的urls
        self.urls_queue = Queue.Queue()
        self.threads = []
        # 已经处理完毕的url, 避免重复
        self.saved_urls = {}
        # 为了避免处理重复的URL, 因此用列表或者字典处理保存过的.但是由于
        # list或dict是非线程安全的，因此这里需要加锁对他们进行保护
        self.saved_urls_lock = threading.Lock()

        self.saved_urls[md5.new(opts.url.encode('utf-8')).hexdigest()] = opts.url
        self.urls_queue.put((opts.url, opts.deep))

        self.__init_work_threads(opts.thread)
        #self.urls_queue.join()

        # 成功下载并保存到数据库条目计数
        self.success = 0
        # 处理失败计数
        self.failure = 0

    def progress_info(self):
        p_info = {}
        p_info['total_number'] = self.urls_queue.qsize() + len(self.saved_urls)
        p_info['wait_to_process'] = self.urls_queue.qsize()
        p_info['success'] = self.success
        p_info['failure'] = self.failure
        return p_info

    def __init_work_threads(self, thread_num):
        for i in range(thread_num):
            self.threads.append(WorkThread(self))

    # 判断线程池是否繁忙(是否有线程在工作，这是其他暂时没有得到任务
    # 的线程退出的重要判断条件)
    def busy(self):
        for i in self.threads:
            #if i.running == True:
            if i.isAlive() and i.running == True:
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
    def __init__(self, pool):
        # 特别注意，这一行代码不能遗忘，否则程序会发生运行时错误！
        threading.Thread.__init__(self)
        self.pool = pool
        self.running = False
        self.key = pool.key

    def run(self):
        while True:
            try:
                url, deep = self.pool.urls_queue.get(block = True, timeout = 5)
                #url, deep = self.pool.urls_queue.get(block = False)
            except Queue.Empty:
                #time.sleep(5) # 非阻塞时使用
                print "%s, qsize: %s"%(threading.current_thread().getName(),
                                  self.pool.urls_queue.qsize())
                if not self.pool.busy():
                    break
            else:
                self.running = True
                print "%s, url=%s, deep=%s, qsize=%s"%(
                      threading.current_thread().getName(), url, deep, self.pool.urls_queue.qsize())
                _spider = Spider(url, deep, self.key)
                links = _spider.run()
                if links:
                    print "++++++++++ In WorkThread: %s, links_lenth: %s ++++++++++"%(
                            threading.current_thread().getName(), len(links))
                    for i in links:
                        hash_code = md5.new(i.encode('utf-8')).hexdigest()

                        self.pool.saved_urls_lock.acquire()
                        if not hash_code in self.pool.saved_urls:
                            self.pool.saved_urls[hash_code] = i
                            self.pool.urls_queue.put((i, deep - 1))
                        #千万不能忘记解锁啊!!!
                        self.pool.saved_urls_lock.release()
                self.running = False
                self.pool.urls_queue.task_done()
                #print
            #except Exception:
            #    print "Unknown Exception in:%s"%(threading.current_thread().getName())
            #    break
        print "%s, ended*******\n"%(threading.current_thread().getName())

def parse_args():
    usage = "usage: %prog [Options] arg..."
    parser = OptionParser(usage)
    parser.add_option("-u", dest="url", default="http://www.baidu.com",
                      help="spider start url")
    parser.add_option("-d", type=int, dest="deep", default=1,
                      help="scrawl the page only if depth == 0")
    parser.add_option("-t", "--thread", type=int, dest="thread", default=10,
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
    opts = parse_args()
    print opts

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
        #cursor.execute("select * from spider_table")
        #for i in cursor.fetchall():
        #    print i

        level = LOG_LEVELS[opts.loglevel]
        #logging.basicConfig(filename = opts.logfile, level = opts.loglevel)
        logging.basicConfig(filename = opts.logfile, level = level)

        pool = ThreadPool(opts)
        pool.threads_start()

        gress = PrintProgress()
        gress.start()
        gress.join()

        pool.wait_all_complete()
        print 'successful completion.....\ntotal time used:%s'%str(time.time() - start_time)
