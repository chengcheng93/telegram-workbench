"""Invitation RPC certainty regressions; transport is always an isolated fake."""
import asyncio
from tempfile import TemporaryDirectory
import unittest

from telethon import errors

from contacts import ContactService
from group_invites import invite_contact, resolve_target
from tasks import TaskService
from test_contacts import FakeApp, FakeClient


def uncertain_errors():
    return (
        errors.ServerError(None, 'INTERNAL', 500),
        errors.ServerError(None, 'INTERNAL', -500),
        errors.RPCError(None, 'UNKNOWN_SERVER_RESULT', 503),
        errors.RPCError(None, 'UNKNOWN_RESULT'),
    )


class InviteRpcTests(unittest.IsolatedAsyncioTestCase):
    async def test_type_or_value_error_is_unknown_only_after_actual_rpc_submission(self):
        for error_type in (TypeError, ValueError):
            for submitted in (False, True):
                with self.subTest(error=error_type.__name__, submitted=submitted):
                    client = FakeClient()
                    target = await resolve_target(client, -9, 100)
                    error = error_type('request or response data unavailable')
                    if submitted:
                        client.before_mutation = lambda request: client.members.add(1)
                        client.invite_errors[1] = error
                        before_submit = None
                    else:
                        def before_submit():
                            raise error
                    result = await invite_contact(client, target, client.users[1], before_submit=before_submit)
                    self.assertEqual(result['kind'], 'unverified' if submitted else 'failed')
                    self.assertEqual(len(client.mutations), 1 if submitted else 0)
                    self.assertEqual(client.members, {1} if submitted else set())

    async def test_submitted_5xx_or_missing_rpc_code_is_unverified_after_possible_effect(self):
        # A generic failed result would permit a retry of a possibly applied RPC.
        for error in uncertain_errors():
            with self.subTest(error=type(error).__name__, code=error.code):
                client = FakeClient()
                target = await resolve_target(client, -9, 100)
                client.invite_errors[1] = error
                client.before_mutation = lambda request: client.members.add(1)
                submissions = []
                result = await invite_contact(client, target, client.users[1],
                                              before_submit=lambda: submissions.append('submitted'))
                self.assertEqual(result['kind'], 'unverified')
                self.assertEqual(client.members, {1})
                self.assertEqual(len(client.mutations), 1)
                self.assertEqual(submissions, ['submitted'])

    async def test_read_only_precheck_5xx_or_unknown_code_raises_before_any_invitation(self):
        for error in uncertain_errors():
            with self.subTest(error=type(error).__name__, code=error.code):
                client = FakeClient()
                target = await resolve_target(client, -9, 100)
                async def unavailable(target, person):
                    raise error
                client.get_permissions = unavailable
                submissions = []
                with self.assertRaises(type(error)) as caught:
                    await invite_contact(client, target, client.users[1],
                                         before_submit=lambda: submissions.append('submitted'))
                self.assertIs(caught.exception, error)
                self.assertEqual(client.mutations, [])
                self.assertEqual(submissions, [])

    async def test_explicit_recipient_4xx_keeps_definite_failed_restricted_and_already(self):
        for error, expected in (
            (errors.UserIdInvalidError(None), 'failed'),
            (errors.RPCError(None, 'DEFINITE_BAD_REQUEST', 400), 'failed'),
            (errors.UserPrivacyRestrictedError(None), 'restricted'),
            (errors.UserAlreadyParticipantError(None), 'already'),
        ):
            with self.subTest(error=type(error).__name__):
                client = FakeClient()
                target = await resolve_target(client, -9, 100)
                client.invite_errors[1] = error
                result = await invite_contact(client, target, client.users[1])
                self.assertEqual(result['kind'], expected)
                self.assertEqual(len(client.mutations), 1)

    async def test_fatal_account_group_and_flood_errors_propagate_before_and_after_submit(self):
        for submitted in (False, True):
            for error in (errors.FloodWaitError(None, capture=11),
                          errors.RPCError(None, 'FLOOD_LIMIT', 420),
                          errors.ChatAdminRequiredError(None), errors.AuthKeyUnregisteredError(None)):
                with self.subTest(submitted=submitted, error=type(error).__name__):
                    client = FakeClient()
                    target = await resolve_target(client, -9, 100)
                    if submitted:
                        client.invite_errors[1] = error
                    else:
                        async def unavailable(target, person):
                            raise error
                        client.get_permissions = unavailable
                    with self.assertRaises(type(error)) as caught:
                        await invite_contact(client, target, client.users[1])
                    self.assertIs(caught.exception, error)
                    self.assertEqual(len(client.mutations), 1 if submitted else 0)

    async def _confirmed_uncertain_task(self, app, service, *, applied):
        await app.contacts.refresh('a')
        task = (await service.action('prepare_task', {'account_id': 'a', 'type': 'invite',
            'user_ids': [1], 'group_id': -9, 'consent_confirmed': True}))['task']
        client = app.sessions['a'].client
        client.invite_errors[1] = errors.ServerError(None, 'INTERNAL_AFTER_SUBMIT', 500)
        if applied:
            client.before_mutation = lambda request: client.members.add(1)
        await service.action('confirm_task', {'task_id': task['id'], 'confirmation_token': task['token']})
        await asyncio.wait_for(asyncio.shield(service._workers['a']), timeout=2)
        return task, client

    async def test_applied_then_500_is_only_read_verified_and_never_reinvited(self):
        with TemporaryDirectory() as root:
            app = FakeApp(root)
            app.contacts = ContactService(app)
            service = app.tasks = TaskService(app)
            try:
                task, client = await self._confirmed_uncertain_task(app, service, applied=True)
                detail = await service.action('task_detail', {'task_id': task['id']})
                self.assertEqual(detail['task']['counts']['unverified'], 1)
                self.assertEqual(detail['task']['counts']['failed'], 0)
                item = detail['items'][0]
                checked = await service.action('verify_task_item', {'task_id': task['id'], 'item_id': item['id']})
                self.assertEqual(checked['item']['status'], 'invited')
                with self.assertRaises(ValueError):
                    await service.action('retry_task', {'task_id': task['id'], 'confirmed': True})
                self.assertEqual(len(client.mutations), 1)
            finally:
                await service.close()

    async def test_unresolved_500_reserves_recipient_and_only_allows_read_only_checks(self):
        with TemporaryDirectory() as root:
            app = FakeApp(root)
            app.contacts = ContactService(app)
            service = app.tasks = TaskService(app)
            try:
                task, client = await self._confirmed_uncertain_task(app, service, applied=False)
                detail = await service.action('task_detail', {'task_id': task['id']})
                self.assertEqual(detail['task']['counts']['unverified'], 1)
                item = detail['items'][0]
                checked = await service.action('verify_task_item', {'task_id': task['id'], 'item_id': item['id']})
                self.assertEqual(checked['item']['status'], 'unverified')
                with self.assertRaises(ValueError):
                    await service.action('retry_task', {'task_id': task['id'], 'confirmed': True})
                next_task = (await service.action('prepare_task', {'account_id': 'a', 'type': 'invite',
                    'user_ids': [1], 'group_id': -9, 'consent_confirmed': True}))['task']
                self.assertEqual(next_task['counts']['unverified'], 1)
                self.assertEqual(next_task['counts']['remaining'], 0)
                await service.action('confirm_task', {'task_id': next_task['id'], 'confirmation_token': next_task['token']})
                await asyncio.wait_for(asyncio.shield(service._workers['a']), timeout=2)
                self.assertEqual(len(client.mutations), 1)
            finally:
                await service.close()


if __name__ == '__main__':
    unittest.main()
