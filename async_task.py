# coding:utf-8
#
import os
import signal
import time
import re
import concurrent.futures
import functools
import copy
import logging
from urllib.parse import urlparse

import asyncio
import aiohttp
from colorama import init

init()

from . import variables

from .utils import write_data, logger, get_file_name_by_pattern, get_file_name, write_to_file


class AsyncTask(object):
    def __init__(self, loop, task_id, url, base_dir, thread_number=1, headers=None, overwrite=False):
        """

        :param loop:
        :param task_id:
        :param url:
        :param headers:
        :param file_path:
        :param segment_infos:  [start_position, end_position]
        """
        self.task_id = task_id
        self.url = url
        self.headers = headers
        self.base_dir = base_dir
        self.file_path = None
        self.file_size = 0
        self.segment_info = []
        self.thread_number = thread_number
        self.download_task = None
        self.session = None
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=5)
        self.loop = loop
        self.main_fd = -1
        self.update_segments = []  # 用于更新segment，或者重新下载时用
        self.download_record = {}  # 下载记录
        self.download_progress_size = 0
        self.download_data = {}
        self.lock = asyncio.Lock()
        self.overwrite = overwrite
        self.task_status = variables.TASK_STATUS[1]
        self.last_update_time = None
        self.last_file_size = None

    def set_segment_info(self, segments):
        """
        :param segments: [start, end, origin_url]
        :return:
        """
        if not isinstance(segments, list):
            return
        self.update_segments = segments

    async def submit_data(self, current_position, end_position, data):
        if current_position in self.download_data:
            logger.warn("repeat start_position: %d", current_position)
        self.download_data[current_position] = data
        await self.update_download_info(current_position, end_position, data)

    def create_segment_info(self, main_fd):
        end = -1
        step = self.file_size // self.thread_number
        origin_url = urlparse(self.url).hostname
        while end < self.file_size - 1:
            start = end + 1
            end = start + step - 1
            # print("start:end:", start,end)
            if end > self.file_size:
                end = self.file_size
            fd = os.dup(main_fd)
            self.segment_info.append([start, end, fd, origin_url])

    async def update_task_info(self, last_update_time, last_file_size):
        self.last_update_time = last_update_time
        self.last_file_size = last_file_size

    async def update_download_info(self, current_position, end_position, chunk_size):
        self.download_record[end_position] = [current_position, end_position]
        self.download_progress_size += chunk_size
        if self.file_size and self.download_progress_size >= self.file_size:
            self.task_status = variables.TASK_STATUS[3]

    async def test(self):
        while True:
            # print("test")
            await self.loop.run_in_executor(self.executor, functools.partial(time.sleep, 0.1))
            await asyncio.sleep(0.2)

    def update_segment_info(self, main_fd):
        self.segment_info = []
        for item in self.update_segments:
            fd = os.dup(main_fd)
            self.segment_info.append([item[0], item[1], fd, item[3]])

    async def get_file_fd(self):
        try:
            f = await self.loop.run_in_executor(None, functools.partial(open, self.file_path, "rb+"))
            self.main_fd = f.fileno()
            return self.main_fd, f
        except Exception as e:
            logger.exception("open file error, %s", e)

    def create_file(self):
        if not self.overwrite:
            while os.path.exists(self.file_path):
                root, ext = os.path.splitext(self.file_path)
                self.file_path = "{}_1{}".format(root, ext)
        with open(self.file_path, "w")as f:
            pass

    async def fetch(self, startpos, endpos, fd, origin_url, request_headers=None):
        # print("fetching,", startpos)
        headers = copy.deepcopy(variables.COMMON_HEADS)
        headers["origin"] = origin_url
        headers["Range"] = "bytes=%s-%s" % (startpos, endpos)
        headers["connection"] = "keep-alive"
        headers["host"] = origin_url

        if isinstance(request_headers, dict):
            headers.update(request_headers)

        f = await self.loop.run_in_executor(self.executor, functools.partial(os.fdopen, fd, "rb+"))

        try:
            async with self.session.get(self.url, headers=headers) as response:
                while True:
                    chunk = await response.content.read(1024)
                    # print("chunk length:", len(chunk), not chunk, startpos)
                    if not chunk:
                        break
                    async with self.lock:
                        await self.loop.run_in_executor(None, functools.partial(write_to_file, f, startpos, chunk))
                        await self.update_download_info(startpos, endpos, len(chunk))
                    startpos += len(chunk)
                    # await loop.run_in_executor(executor, functools.partial(f.seek, startpos, 0))
                # print("---- done", startpos)
        except Exception as e:
            logging.exception("get error when downloading:%s", e)
            return variables.PROGRESS_ERROR.format("get error when downloading")
        finally:
            # pass
            await self.loop.run_in_executor(self.executor, functools.partial(os.close, fd))

    async def get_file_size_and_file_name(self, session, url):
        headers = {
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
            "accept-encoding": "gzip, deflate, br",
            "accept-language": "zh-CN,zh;q=0.9",
            "upgrade-insecure-requests": "1",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.81 Safari/537.36",
            # "cookie":"wyctoken=259539283; FTN5K=668d71d8",
        }
        urlobj = urlparse(url)
        filename = get_file_name_by_pattern(urlobj.path)
        accept_ranges = False
        filesize = 0
        # e =  Exception("url:{} get error status:{}".format(url, 407))
        # #loop.call_exception_handler({"message":"get error status", "exception": e})
        # raise e

        response = await session.head(url, headers=headers)
        if response.status_code // 100 == 4:
            raise Exception("url:{} get error status_code:{}".format(url, response.status_code))
        if response.status_code // 100 == 3:
            headers_content = response.headers
            for key in headers_content:
                match = re.search("^location$", key, re.IGNORECASE)
                if not match:
                    continue
                location_key = match.group()
                print("location_key", location_key, key)
                return await get_file_size_and_file_name(session, headers_content[location_key])
            else:
                raise Exception(
                    "get file name error, status_code is wrong, status_code:{}".format(response.status_code))
        headers_content = response.headers
        print("headers_content", headers_content, response.status_code)
        if "Content-Length" not in headers_content:
            raise Exception("can't get file size")
        if "Accept-Ranges" in headers_content:
            accept_ranges = True
        if 'Content-Disposition' in headers_content:
            tmp_file_name = await curio.run_in_thread(get_file_name, headers_content["Content-Disposition"])
            if tmp_file_name is not None and tmp_file_name:
                filename = tmp_file_name
        if "Content-Length" not in headers_content:
            raise Exception("can't get file size!")
        filesize = int(headers_content["Content-Length"])

        return filename, filesize, accept_ranges

    async def get_file_size_and_file_name(self, redirect_url=None):
        print("redirect_url", redirect_url)
        headers = {
            "Range": "bytes=0-0",
        }
        headers.update(variables.COMMON_HEADS)
        if self.headers is not None:
            headers.update(self.headers)
        urlobj = urlparse(self.url)
        filename = get_file_name_by_pattern(urlobj.path)
        accept_ranges = False
        try:
            async with self.session.get(self.url, headers=headers) as response:

                if response.status // 100 == 4:
                    logger.error("get file info error, for error status, url:%s, status:%d ",
                                 self.url, response.status)
                    return variables.PROGRESS_ERROR.format(variables.STATUS[10005])

                if response.status // 100 == 3:
                    headers_content = response.headers
                    for key in headers_content:
                        match = re.search(r"^location$", key, re.IGNORECASE)
                        if not match:
                            continue
                        location_key = match.group()
                        # print("location_key", location_key, key, headers_content[location_key])
                        return await self.get_file_size_and_file_name(redirect_url=headers_content[location_key])
                    else:
                        logger.error("redirect but can't get next location")
                        return variables.PROGRESS_ERROR.format("redirect but can't get next location")
                headers_content = response.headers
                # print("headers_content", headers_content, response.status)
                if "Content-Range" not in headers_content:
                    logger.error("Content-Range not in header")
                    return variables.PROGRESS_ERROR.format("Content-Range not in header")
                filesize = int(headers_content["Content-Range"].split("/")[1])
                if "Accept-Ranges" in headers_content:
                    accept_ranges = True
                if 'Content-Disposition' in headers_content:
                    loop = asyncio.get_event_loop()
                    tmp_file_name = await loop.run_in_executor(None, functools.partial(get_file_name,
                                                                                       headers_content[
                                                                                           "Content-Disposition"]))
                    if tmp_file_name is not None and tmp_file_name:
                        filename = tmp_file_name
                return filename, filesize, accept_ranges, redirect_url

        except Exception as e:

            logger.exception("get simple file error , not normal error,%s", e)
            return variables.PROGRESS_ERROR.format("not normal error")

    async def prepare_to_download(self, request_headers=None):
        tasks = []
        for i in self.segment_info:
            task = self.fetch(*i, request_headers=request_headers)
            tasks.append(task)
        # tasks.append(self.test())
        self.download_task = asyncio.gather(*tasks)
        # print("type", type(self.download_task),self.segment_info)

    def set_session(self, config=None):
        timeout = aiohttp.ClientTimeout(total=60 * 100)
        self.session = aiohttp.ClientSession(timeout=timeout)

    async def create_file_wrapper(self):
        await self.loop.run_in_executor(self.executor, functools.partial(self.create_file))

    async def start_download(self):
        try:
            self.set_session()
            print("get file info ...")
            file_info_result = await self.get_file_size_and_file_name()
            print("file info:", file_info_result)
            if isinstance(file_info_result, str) and file_info_result.startswith(variables.PROGRESS_ERROR.format("")):
                return variables.STATUS[10002]
            filename, filesize, accept_ranges, redirect_url = file_info_result
            self.file_path = os.path.join(self.base_dir, filename)
            await self.create_file_wrapper()
            fd, file_obj = await self.get_file_fd()
            if fd == -1:
                return variables.STATUS[10001]
            self.file_size = filesize
            if redirect_url is not None:
                self.url = redirect_url
            if not accept_ranges:
                self.thread_number = 1
            if self.update_segments:
                self.update_segment_info(fd)
            else:
                self.create_segment_info(fd)
            print("prepare to download...")
            await self.prepare_to_download(request_headers=self.headers)
            # asyncio.ensure_future(self.write_data_interval())
            self.task_status = variables.TASK_STATUS[2]
            async with self.session:
                await self.download_task
            await self.loop.run_in_executor(self.executor, functools.partial(file_obj.flush))
            await self.loop.run_in_executor(self.executor, functools.partial(file_obj.close))
        except Exception as e:
            self.task_status = variables.TASK_STATUS[5]
            logger.exception("get errow for not normal in start_download")
        finally:
            self.task_status = variables.TASK_STATUS[3]

    def cancel_download(self):
        self.download_task.cancel()
        self.task_status = variables.TASK_STATUS[4]

    async def get_task_progress(self):
        if self.file_size < 1:
            return 0
        return self.download_progress_size / self.file_size


class OnlyDownloadAsyncTask(AsyncTask):
    def __init__(self, loop, task_id, url, file_path, file_size, thread_number, headers=None):
        super(OnlyDownloadAsyncTask, self).__init__(loop, task_id, url, None, thread_number=thread_number, headers=headers)
        self.file_path = file_path
        self.file_size = file_size
        self.task_status = variables.TASK_STATUS[1]

    async def start_download(self):
        try:
            self.set_session()
            await self.create_file_wrapper()
            fd, file_obj = await self.get_file_fd()
            if fd == -1:
                return variables.STATUS[10001]
            if self.update_segments:
                self.update_segment_info(fd)
            else:
                self.create_segment_info(fd)
            await self.prepare_to_download(request_headers=self.headers)
            async with self.session:
                self.task_status = variables.TASK_STATUS[2]
                await self.download_task
            await self.loop.run_in_executor(self.executor, functools.partial(file_obj.flush))
            await self.loop.run_in_executor(self.executor, functools.partial(file_obj.close))

        except Exception as e:
            self.task_status = variables.TASK_STATUS[5]
            logger.exception("get errow for not normal in start_download")
        finally:
            # print("all done!!!!!!!!!!!!")
            # await self.session.close()
            pass


class TaskManager(object):
    def __init__(self):
        self._tasks = []

    @property
    def tasks(self):
        return self._tasks

    def add_task(self, task):
        self._tasks.append(task)


def kill_self():
    show_str = ('[%%-%ds]' % 50) % (int(50 * 1) * "#")  # 字符串拼接的嵌套使用
    print('\r\033[0;32;40m%s %d%%\033[0m' % (show_str, int(1 * 100)), end='')
    os.kill(os.getpid(), signal.SIGTERM)


async def print_task_progress(task):
    start_time = time.time()
    while True:
        await asyncio.sleep(0.1)
        if task.task_status in [variables.TASK_STATUS[3], variables.TASK_STATUS[4], variables.TASK_STATUS[5]]:
            kill_self()
            break
        percent = await task.get_task_progress()

        show_str = ('[%%-%ds]' % 50) % (int(50 * percent) * "#")  # 字符串拼接的嵌套使用
        print('\r\033[0;32;40m%s %d%%\033[0m' % (show_str, int(percent * 100)), end='')
        if percent >= 1:
            kill_self()
            break
    print("all over time", time.time() - start_time)


def receive_signal(signum, stack):
    print('Received:', signum)
    loop = asyncio.get_event_loop()
    loop.close()


if __name__ == "__main__":
    url = "https://d.pcs.baidu.com/file/b7e49763e5dafff66e693527ec2eb1e9?fid=1614778567-250528-612135224133168&dstime=1544277079&rt=sh&sign=FDtAERVY-DCb740ccc5511e5e8fedcff06b081203-HpAiGkb6fvLTPiks6J0FwHHd4RA%3D&expires=8h&chkv=1&chkbd=0&chkpc=et&dp-logid=7929873040921903453&dp-callid=0&shareid=2678582083&r=778943555"
    base_dir = r"D:\data\myPrograme\python\spider\file_download\output"
    file_path = os.path.join(base_dir, "test.pdf")
    loop = asyncio.ProactorEventLoop()
    asyncio.set_event_loop(loop)
    # Register signal handlers
    signal.signal(signal.SIGTERM, receive_signal)
    signal.signal(signal.SIGINT, receive_signal)
    header = {
        "Cookie":"BAIDUID=8172517B28320D467E5BFBBFE2A39908:FG=1; BIDUPSID=8172517B28320D467E5BFBBFE2A39908; PSTM=1543715245; pan_login_way=1; PANWEB=1; BDCLND=FKKZLJPob2tiS48uQe2nOaBdbNXbHta2xBThfOQa0jA%3D; recommendTime=guanjia2018-12-3%2010%3A40%3A00; BDUSS=M4bTZJZk1DbnBiUGpxYmdDVHI2eWpZbFNOcGxhZzA5YnlJT3NvLWZGdXNRREZjQVFBQUFBJCQAAAAAAAAAAAEAAABAvm0SztLKx87StcS80jUAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAKyzCVysswlca0; STOKEN=c9b9127a0372a11928236fcc71e76fccc32d0216a3d4fc99738211f3f381c427; SCRC=131fc099256f778cbf73005fd433beea; cflag=15%3A3; BDRCVFR[mkUqnUt8juD]=mk3SLVN4HKm; H_PS_PSSID=1430_21115_28019_26350_27244_27508; Hm_lvt_7a3960b6f067eb0085b7f96ff5e660b0=1544139655,1544228125,1544274798; PANPSC=6776371069947206978%3A5e7emfqPscKtoxLQqp8av%2Bn90oj%2BY%2FIsAtqdpC88XMbd3hduxgyMl452N20FG1l5C8H9%2FuNhuf8wwo4y0uRbvUih5SKauoInb1%2F7JABjVE%2FdMUNgReAAie6tOdLfgtiqfXOsWjuKC4E9CJq%2BpSvvwjNPZ7yhy1ADa3JDH%2FwMAjNjbYml1eowNq7IJzS%2Fi2HLfbZ%2FbYW506b%2ByIluCYs1Wp2Zb97G3%2BgS; Hm_lpvt_7a3960b6f067eb0085b7f96ff5e660b0=1544275090"
    }
    t = AsyncTask(loop, "123", url, base_dir, 256, headers=header)

    try:
        asyncio.ensure_future(print_task_progress(t,))
        asyncio.ensure_future(t.start_download())
        loop.run_forever()
    except KeyboardInterrupt:
        print(KeyboardInterrupt)
        loop.stop()
    finally:
        print("Closing Loop")
        loop.close()
