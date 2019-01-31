# coding:utf-8
from .async_task import TaskManager
import asyncio
main_loop = asyncio.ProactorEventLoop()
main_task_manager = TaskManager()

