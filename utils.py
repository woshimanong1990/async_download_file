# coding:utf-8


import cgi
import re
import os

import logging.config

import requests

from async_download.logger_config import LOGGING
from async_download import variables
from urllib.parse import urlparse

logging.config.dictConfig(LOGGING)
logger = logging.getLogger("custom")

file_name_pattern = re.compile(r"((.+?)\.(\w+?))\W*$")



DOWNLOAD_COUNT = 0


def get_file_name(content_disposition):
    try:
        _, params = cgi.parse_header(content_disposition)
        if "filename" in params:
            return params["filename"]
        elif "FILENAME" in params:
            return params["FILENAME"]
        elif "filename*" in params:
            return re.split("utf-8'\s+'", params['filename*'], 1, flags=re.IGNORECASE)[1]
        elif "FILENAME*" in params:
            return re.split("utf-8'\s+'", params['FILENAME*'], 1, flags=re.IGNORECASE)[1]
        else:
            return None
    except Exception as e:
        logger.exception("get_file_name error: %s", e)

def write_data(file_path, download_data):
    try:
        with open(file_path, "rb+") as f:
            for start_position, value in download_data:
                f.seek(start_position)
                f.write(value)
    except Exception as e:
        logger.exception("write data fot file error: %s", e)


def write_to_file(f, startpos, data):
    # print("write_to_file fd:{}, pos:{}".format(f.fileno(), startpos))
    try:
        # print("---", f.fileno(),startpos)
        f.seek(startpos)
        f.write(data)
        f.flush()
        f.flush()
        f.flush()
        f.flush()
        # print("*******",f.fileno(),f.tell()-startpos, len(data) )
    except Exception as e:
        logger.exception("write_to_file error: %s", e)
        # print("---tell fd:{}, pos:{} ".format(f.fileno(), f.tell()))



def get_file_name_by_pattern(path):
    try:
        #print(path)
        path_params = path.split("/")
        match_names = []
        for p in path_params:
            match = file_name_pattern.search(p)
            if match:
                print(match.groups())
                match_names.append(match.groups()[0])
        if match_names:
            return match_names[-1]
        return ""
    except Exception as e:
        logger.exception("get_file_name_by_pattern error: %s", e)



def open_file(filename):
    try:
        with open(filename, 'w') as tempf:
            tempf.seek(0)
            tempf.write("hello")
    except Exception as e:
        logger.exception("open_file error:%s", e)


def open_file_to_fd(filename):
    try:
        with open(filename, 'w') as tempf:
            tempf.seek(0)
            tempf.write("hello")
    except Exception as e:
        logger.exception("open_file error:%s", e)

def get_file_size_and_file_name(url,custom_headers=None, redirect_url=None):
    headers = {
        "Range": "bytes=0-0",
    }
    headers.update(variables.COMMON_HEADS)
    if custom_headers is not None and isinstance(custom_headers, dict):
        headers.update(custom_headers)
    urlobj = urlparse(url)
    filename = get_file_name_by_pattern(urlobj.path)
    accept_ranges = True
    content_type = None
    try:
        with requests.Session() as session:
            response = session.get(url, headers=headers)

            if response.status_code // 100 == 4:
                logger.error("get file info error, for error status, url:%s, status:%d ",
                             url, response.status_code)
                return variables.PROGRESS_ERROR.format(variables.STATUS[10005])

            if response.status_code // 100 == 3:
                headers_content = response.headers
                for key in headers_content:
                    match = re.search("^location$", key, re.IGNORECASE)
                    if not match:
                        continue
                    location_key = match.group()
                    print("location_key", location_key, key, headers_content[location_key])
                    return get_file_size_and_file_name(url, redirect_url=headers_content[location_key])
                else:
                    logger.error("redirect but can't get next location")
                    return variables.PROGRESS_ERROR.format("redirect but can't get next location")
            headers_content = response.headers
            print("headers_content", headers_content, response.status_code)
            if "Content-Range" not in headers_content:
                logger.error("Content-Range not in header")
                return variables.PROGRESS_ERROR.format("Content-Range not in header")
            filesize = int(headers_content["Content-Range"].split("/")[1])
            if "Accept-Ranges" in headers_content and headers_content["Accept-Ranges"] == "none":
                accept_ranges = False
            if "Content-Type" in headers_content:
                content_type = headers_content["Content-Type"]
            if 'Content-Disposition' in headers_content:
                tmp_file_name = get_file_name(headers_content["Content-Disposition"])
                if tmp_file_name is not None and tmp_file_name:
                    filename = tmp_file_name
            return filename, filesize, accept_ranges, redirect_url, content_type

    except Exception as e:
        logger.exception("get simple file error , not normal error,%s", e)
        return variables.PROGRESS_ERROR.format("not normal error")

async def get_file_size_and_file_name(session, url):
    headers = {
        "accept":"text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
        "accept-encoding":"gzip, deflate, br",
        "accept-language":"zh-CN,zh;q=0.9",
        "upgrade-insecure-requests":"1",
        "user-agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.81 Safari/537.36",
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
            raise Exception("get file name error, status_code is wrong, status_code:{}".format(response.status_code))
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
if __name__ == "__main__":
    # url = "http://pic110.huitu.com/pic/20180925/1301968_20180925195933385080_0.jpg"
    url = "https://nodejs.org/dist/v10.13.0/node-v10.13.0-x64.msi"
    from urllib.parse import urlparse
    path = urlparse(url).path
    # print("====")
    print(get_file_name_by_pattern(path))
    #logger.error("gfsdfsdf")