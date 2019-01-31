# coding:utf-8

STATUS = {
    10001:"OPENFILE ERROR",
    10002:"GET FILE INFO ERROR",
    10003:"FIRST GET ERROR",
    10004:"TIME OUT",
    10005:"REMOTE FORBIDDEN"
}

COMMON_HEADS ={
    "accept":"text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
    "accept-encoding":"gzip, deflate, br",
    "accept-language":"zh-CN,zh;q=0.9",
    "upgrade-insecure-requests":"1",
    "user-agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.81 Safari/537.36",
}

PROGRESS_ERROR = "PROGRESS ERROR, REASON: {}"

TASK_STATUS = {
    1: "created",
    2: "working",
    3: "done",
    4: "cancel",
    5: "error"
}
