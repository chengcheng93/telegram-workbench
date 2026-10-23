"""One priority mutation queue per account; never retries a Telegram write."""

import asyncio
from itertools import count

from telethon import errors


class OperationPaused(ValueError):
    pass


class AccountScheduler:
    def __init__(self, app):
        self.app = app
        self.queues = {}
        self.workers = {}
        self.sequence = count()
        self.closing = False

    def _check(self, account_id, automation):
        if self.closing:
            raise OperationPaused('后台正在退出，未提交新操作。')
        if not isinstance(account_id, str) or not account_id:
            raise ValueError('请明确选择操作账号。')
        if automation and self.app.global_paused:
            raise OperationPaused('全部自动化已暂停；人工聊天仍可使用。')
        wait = self.app.wait_state(account_id)
        if wait.get('blocked'):
            raise OperationPaused(wait.get('reason') or '本账号正在等待平台允许，请先检查账号状态。')

    async def execute(self, account_id, operation, *, priority=10, automation=True):
        self._check(account_id, automation)
        if not callable(operation):
            raise TypeError('操作必须是可调用函数。')
        queue = self.queues.setdefault(account_id, asyncio.PriorityQueue(maxsize=512))
        if queue.full():
            raise OperationPaused('本账号待处理队列已满，请等待后再操作。')
        future = asyncio.get_running_loop().create_future()
        queue.put_nowait((priority, next(self.sequence), operation, future, automation))
        worker = self.workers.get(account_id)
        if worker is None or worker.done():
            self.workers[account_id] = asyncio.create_task(self._worker(account_id, queue))
        try:
            return await asyncio.shield(future)
        except asyncio.CancelledError:
            # A queued action must not start after its caller cancelled it.
            # An already submitted callback completes its own durable journal.
            future.cancel()
            raise

    async def _worker(self, account_id, queue):
        while True:
            _, _, operation, future, automation = await queue.get()
            try:
                if future.cancelled():
                    continue
                self._check(account_id, automation)
                result = await operation()
                if not future.done():
                    future.set_result(result)
            except asyncio.CancelledError:
                if not future.done():
                    future.set_exception(OperationPaused('操作已中断，实际结果需要核实。'))
                raise
            except BaseException as error:
                if isinstance(error, (errors.FloodError, errors.PeerFloodError,
                                      errors.UnauthorizedError, ConnectionError, OSError)):
                    self.app.record_wait(account_id, error)
                if not future.done():
                    future.set_exception(error)
            finally:
                queue.task_done()

    async def drain(self, account_id):
        queue = self.queues.get(account_id)
        if queue:
            await queue.join()

    async def close(self):
        self.closing = True
        for queue in self.queues.values():
            while not queue.empty():
                _, _, _, future, _ = queue.get_nowait()
                if not future.done():
                    future.set_exception(OperationPaused('后台已停止，未执行排队操作。'))
                queue.task_done()
        # Let a submitted request reach its journal boundary, then cancel idle
        # workers. Timeouts are recorded by the owning workflow as unknown.
        joins = [asyncio.create_task(q.join()) for q in self.queues.values()]
        if joins:
            _, pending = await asyncio.wait(joins, timeout=10)
            for task in pending:
                task.cancel()
            await asyncio.gather(*joins, return_exceptions=True)
        for task in self.workers.values():
            task.cancel()
        if self.workers:
            await asyncio.gather(*self.workers.values(), return_exceptions=True)
