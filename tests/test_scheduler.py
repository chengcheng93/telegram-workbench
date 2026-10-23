import asyncio
import unittest

from telethon import errors

from scheduler import AccountScheduler, OperationPaused


class FakeApp:
    def __init__(self):
        self.global_paused = False
        self.waits = {}

    def wait_state(self, account_id):
        return self.waits.get(account_id, {})

    def record_wait(self, account_id, error):
        self.waits[account_id] = {'blocked': True, 'error': type(error).__name__}


class SchedulerTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.app = FakeApp()
        self.scheduler = AccountScheduler(self.app)

    async def asyncTearDown(self):
        await self.scheduler.close()

    async def test_manual_send_precedes_queued_automation_without_preempting_rpc(self):
        entered, release = asyncio.Event(), asyncio.Event()
        order = []

        async def first():
            entered.set()
            await release.wait()
            order.append('inflight')

        async def auto():
            order.append('automatic')

        async def manual():
            order.append('manual')

        one = asyncio.create_task(self.scheduler.execute('A', first))
        await entered.wait()
        two = asyncio.create_task(self.scheduler.execute('A', auto, priority=20))
        three = asyncio.create_task(self.scheduler.execute('A', manual, priority=0, automation=False))
        await asyncio.sleep(0)
        release.set()
        await asyncio.gather(one, two, three)
        self.assertEqual(order, ['inflight', 'manual', 'automatic'])

    async def test_account_b_does_not_wait_for_account_a(self):
        entered, release = asyncio.Event(), asyncio.Event()

        async def hold():
            entered.set()
            await release.wait()

        one = asyncio.create_task(self.scheduler.execute('A', hold))
        await entered.wait()

        async def other():
            return 'B completed'

        self.assertEqual(await asyncio.wait_for(self.scheduler.execute('B', other), .2), 'B completed')
        release.set()
        await one

    async def test_global_pause_blocks_automatic_but_allows_manual(self):
        self.app.global_paused = True
        called = []

        async def write():
            called.append(1)
            return 7

        with self.assertRaises(OperationPaused):
            await self.scheduler.execute('A', write)
        self.assertEqual(await self.scheduler.execute('A', write, automation=False), 7)
        self.assertEqual(called, [1])

    async def test_platform_wait_blocks_only_the_affected_account_and_is_not_retried(self):
        called = []

        async def flood():
            called.append(1)
            raise errors.FloodWaitError(request=None, capture=60)

        with self.assertRaises(errors.FloodWaitError):
            await self.scheduler.execute('A', flood)
        with self.assertRaises(OperationPaused):
            await self.scheduler.execute('A', flood)
        self.assertEqual(called, [1])
        self.assertFalse(self.app.wait_state('B'))

    async def test_cancel_queued_operation_does_not_issue_rpc(self):
        entered, release = asyncio.Event(), asyncio.Event()
        called = []

        async def hold():
            entered.set()
            await release.wait()

        async def later():
            called.append(1)

        one = asyncio.create_task(self.scheduler.execute('A', hold))
        await entered.wait()
        two = asyncio.create_task(self.scheduler.execute('A', later))
        await asyncio.sleep(0)
        two.cancel()
        await asyncio.gather(two, return_exceptions=True)
        release.set()
        await one
        await self.scheduler.drain('A')
        self.assertEqual(called, [])

    async def test_pause_is_checked_again_at_dispatch_not_only_enqueue(self):
        entered, release = asyncio.Event(), asyncio.Event()
        called = []

        async def hold():
            entered.set()
            await release.wait()

        async def later():
            called.append(1)

        one = asyncio.create_task(self.scheduler.execute('A', hold))
        await entered.wait()
        two = asyncio.create_task(self.scheduler.execute('A', later))
        await asyncio.sleep(0)
        self.app.global_paused = True
        release.set()
        await one
        with self.assertRaises(OperationPaused):
            await two
        self.assertEqual(called, [])


if __name__ == '__main__':
    unittest.main()
