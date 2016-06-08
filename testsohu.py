from urllib.request import urlopen
from urllib.error import URLError
from urllib.parse import quote
from bs4 import BeautifulSoup
import threading
import socket
import queue
import re
import time

test_url = "http://m.sohu.com"
re_url = re.compile(r"^(/.)|(http://m\.sohu\.com/.)")
re_test_url = r"^http://m\.sohu\.com"

url_set = set()
threads = 32
timeout = 10
socket.setdefaulttimeout(timeout)
lock = threading.Lock()
q = queue.Queue()


class Worker(threading.Thread):
    def __init__(self, q):
        super(Worker, self).__init__()
        self._queue = q

    def run(self):
        while True:
            try:
                current_url = self._queue.get()
                new_url = ''
                # 对url中的中文等超过ascii处理范围的字符进行处理
                for i in current_url:
                    if ord(i) > 127:
                        i = quote(i)
                    new_url += i
                try:
                    html = urlopen(test_url + new_url)
                except URLError as e:
                    with open('URLErrorLog.txt', 'a') as Error_Log:
                        Error_Log.write('链接 ' + test_url + current_url + ' 发生URL错误: ' +
                                        str(e) + '  时间:' + time.ctime() + '\n')
                    continue
                except Exception as e:
                    with open('URLErrorLog.txt', 'a') as Error_Log:
                        Error_Log.write('链接 ' + test_url + current_url + ' 发生其他错误: ' + str(e) +
                                        '  时间:' + time.ctime() + '\n')
                    continue
                if html is None:
                    with open('URLErrorLog.txt', 'a') as Error_Log:
                        Error_Log.write('链接 ' + test_url + current_url + ' 不存在: ' +
                                        '  时间:' + time.ctime() + '\n')
                    continue
                # 判断提取的网页是否出现跳转，若已转到m.sohu.com域外，则舍去
                tag = re.match(re_test_url, html.geturl())
                if not tag:
                    with open('RedirectLog.txt', 'a') as Redirect_Log:
                        Redirect_Log.write('链接 ' + test_url + current_url + ' 重定向为 ' + html.geturl() +
                                           " 不在'm.sohu.com'域内，暂不考虑. 时间：" + time.ctime() + '\n')
                    continue
                try:
                    bsObj = BeautifulSoup(html, 'lxml')
                except Exception as e:
                    with open('URLErrorLog.txt', 'a') as Error_Log:
                        Error_Log.write('链接 ' + test_url + current_url + ' 解析时发生错误: ' + str(e) +
                                        '  时间:' + time.ctime() + '\n')
                    continue
                # 提取此网页中符合条件的URL
                namelist = bsObj.findAll("a", {"href": re_url})
                with open('LinksFile.txt', 'a') as LinksFile:
                    for name in namelist:
                        if 'href' in name.attrs:
                            link = name.attrs.get("href")
                            # 统一URL格式
                            if re.match(re_test_url, link):
                                link = re.split(re_test_url, link)[1]
                            # 如果此url未存在于url_set中，则将其加入到url_set中，并加入队列q等待处理，如已存在则跳过
                            if link not in url_set:
                                url_set.add(link)
                                LinksFile.write(link + ' ' + time.ctime() + '\n')
                                lock.acquire()
                                print(link, self.name, time.ctime())
                                lock.release()
                                # 将符合条件的URL加入队列
                                self._queue.put(link)
            finally:
                self._queue.task_done()


# 主函数
def manager():
    start_time = time.time()
    # 提取主页面符合条件的URL，加入到队列中，作为初始队列
    html = urlopen(test_url)
    bsObj = BeautifulSoup(html, 'lxml')
    namelist = bsObj.findAll("a", {"href": re_url})
    with open('LinksFile.txt', 'a') as LinksFile:
        for name in namelist:
            if 'href' in name.attrs:
                link = name.attrs.get("href")
                # 统一URL格式
                if re.match(re_test_url, link):
                    link = re.split(re_test_url, link)[1]
                # 如果此url未存在于url_set中，则将其加入到url_set中，并加入队列q等待处理，如已存在则跳过
                if link not in url_set:
                    url_set.add(link)
                    LinksFile.write(link + ' ' + time.ctime() + '\n')
                    print(link)
                    q.put(link)
    for i in range(threads):
        worker = Worker(q)
        worker.setDaemon(True)
        worker.start()
    q.join()
    print('Job done! Time taken: {}'.format(time.time()-start_time))


if __name__ == '__main__':
    manager()
