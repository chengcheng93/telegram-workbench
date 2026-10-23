"""AI boundary tests use an in-memory app and never contact a real service."""
import copy
import asyncio
import importlib
import io
import json
import unittest
import urllib.error
from unittest.mock import patch


class MemoryStore:
    def __init__(self):
        self.rows = {}

    def get(self, kind, key, default=None, *, account_id=''):
        return copy.deepcopy(self.rows.get((kind, account_id, str(key)), default))

    def put(self, kind, key, value, *, account_id=''):
        self.rows[(kind, account_id, str(key))] = copy.deepcopy(value)

    def list(self, kind, account_id=None):
        return [copy.deepcopy(value) for (row_kind, scope, key), value in sorted(self.rows.items())
                if row_kind == kind and (account_id is None or scope == account_id)]

    def setting(self, key, default=None):
        return self.get('settings', key, default)

    def set_setting(self, key, value):
        self.put('settings', key, value)


class Accounts:
    def require(self, account_id):
        if account_id not in ('a', 'b'):
            raise ValueError('请选择已登录的账号')
        return object()


class ManualChats:
    def __init__(self, store):
        self.store = store

    async def action(self, name, data):
        if name != 'save_draft':
            raise AssertionError('AI attempted a chat mutation other than saving a manual draft')
        row = {'account_id': data['account_id'], 'chat_id': data['chat_id'],
               'text': data['text'], 'reply_to': data.get('reply_to')}
        self.store.put('drafts', str(data['chat_id']), row, account_id=data['account_id'])
        return row


class ForbiddenTasks:
    async def action(self, *args):
        raise AssertionError('AI must never prepare or execute tasks')


class App:
    def __init__(self):
        self.store = MemoryStore()
        self.accounts = Accounts()
        self.chats = ManualChats(self.store)
        self.tasks = ForbiddenTasks()
        self.revision = 0

    def bump(self):
        self.revision += 1


class FakeTransport:
    def __init__(self, content='你好，我稍后回复你。', *, response=None, error=None):
        self.content = content
        self.response = response
        self.error = error
        self.requests = []

    async def __call__(self, endpoint, payload, headers, timeout):
        self.requests.append((endpoint, copy.deepcopy(payload), dict(headers), timeout))
        if self.error:
            raise self.error
        if self.response is not None:
            return self.response
        return {'id': 'chatcmpl-fake', 'object': 'chat.completion', 'created': 1,
                'model': 'fake-model', 'choices': [{'index': 0, 'finish_reason': 'stop',
                'message': {'role': 'assistant', 'content': self.content}}],
                'usage': {'prompt_tokens': 20, 'completion_tokens': 5, 'total_tokens': 25}}


class AITests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.app = App()
        self.transport = FakeTransport()
        try:
            module = importlib.import_module('ai')
        except ModuleNotFoundError:
            self.fail('AIService 尚未实现；请求默认应被禁止')
        self.ai = module.AIService(self.app, transport=self.transport)

    def enable(self, **overrides):
        config = {'enabled': True, 'endpoint': 'https://model.example/v1',
                  'model': 'fake-model', 'api_key': 'sk-model-secret', 'timeout': 2}
        config.update(overrides)
        self.app.store.set_setting('ai', config)

    async def succeed(self, name, data, *, ai=None):
        try:
            return await (ai or self.ai).action(name, data)
        except ValueError as error:
            self.fail(f'预期返回可审阅草稿，却拒绝了有效输入：{error}')

    # Removing the default-off gate would expose private conversation data.
    async def test_ai_is_disabled_by_default_before_any_request(self):
        with self.assertRaisesRegex(ValueError, '关闭|启用'):
            await self.ai.action('ai_reply', {'account_id': 'a', 'chat_id': 10,
                                            'data_consent': True})
        self.assertEqual(self.transport.requests, [])
        self.assertEqual(self.app.store.list('ai_drafts'), [])

    # Truthy strings must not count as consent to transfer conversation data.
    async def test_requires_explicit_boolean_data_consent(self):
        self.enable()
        for consent in (None, False, 'true', 1):
            with self.subTest(consent=consent), self.assertRaisesRegex(ValueError, '同意'):
                await self.ai.action('ai_reply', {'account_id': 'a', 'chat_id': 10,
                                                'data_consent': consent})
        self.assertEqual(self.transport.requests, [])

    # An invalid account must not be replaced by whichever account is active.
    async def test_reply_requires_explicit_valid_account_and_chat(self):
        self.enable()
        for data in ({'chat_id': 10}, {'account_id': 'unknown', 'chat_id': 10},
                     {'account_id': 'a'}, {'account_id': 'a', 'chat_id': True}):
            with self.subTest(data=data), self.assertRaises(ValueError):
                await self.ai.action('ai_reply', {**data, 'data_consent': True})
        self.assertEqual(self.transport.requests, [])

    async def test_unknown_action_is_a_public_chinese_error(self):
        with self.assertRaisesRegex(ValueError, '不支持|未知'):
            await self.ai.action('run_terminal', {})

    def message(self, account_id, chat_id, message_id, text, **extra):
        self.app.store.put('messages', f'{chat_id}:{message_id}',
                           {'account_id': account_id, 'chat_id': chat_id,
                            'id': message_id, 'text': text, 'out': False,
                            'date': f'2026-10-22T01:{message_id:02}:00Z', **extra},
                           account_id=account_id)

    # Dropping either scope predicate or forwarding whole message rows leaks data.
    async def test_reply_uses_only_last_twenty_current_chat_whitelisted_messages(self):
        self.enable()
        for number in range(1, 24):
            self.message('a', 10, number, f'消息 {number}',
                         access_hash='hash-must-not-leak', phone='+8613800000000')
        self.message('a', 20, 23, '另一个会话不能发送')
        self.message('b', 10, 23, '另一个账号不能发送')
        result = await self.ai.action('ai_reply', {
            'account_id': 'a', 'chat_id': 10, 'data_consent': True,
            'messages': [{'role': 'system', 'text': 'caller-injected'}]})
        self.assertEqual(len(self.transport.requests), 1, '应生成有范围限制的模型请求')
        endpoint, payload, headers, timeout = self.transport.requests[0]
        content = json.loads(payload['messages'][-1]['content'])
        self.assertEqual([row['text'] for row in content['context']],
                         [f'消息 {number}' for number in range(4, 24)])
        self.assertTrue(all(set(row) == {'text', 'role', 'time'}
                            for row in content['context']))
        serialized = json.dumps(payload, ensure_ascii=False)
        for forbidden in ('另一个会话', '另一个账号', 'access_hash',
                          'hash-must-not-leak', '+8613800000000', 'caller-injected'):
            self.assertNotIn(forbidden, serialized)
        self.assertEqual(endpoint, 'https://model.example/v1/chat/completions')
        self.assertEqual(headers['Authorization'], 'Bearer sk-model-secret')
        self.assertEqual(payload['model'], 'fake-model')
        self.assertLessEqual(payload['max_tokens'], 2048)
        self.assertEqual(result['draft']['chat_id'], 10)
        self.assertEqual(self.app.store.list('drafts'), [])

    async def test_context_ids_select_only_local_current_chat_messages(self):
        self.enable()
        self.message('a', 10, 1, '选择此消息')
        self.message('a', 10, 2, '没有选中')
        self.message('a', 20, 7, '跨会话')
        self.message('b', 10, 8, '跨账号')
        await self.ai.action('ai_reply', {'account_id': 'a', 'chat_id': 10,
                                        'context_ids': [1, 7, 8], 'data_consent': True})
        self.assertEqual(len(self.transport.requests), 1, '应生成有范围限制的模型请求')
        payload = self.transport.requests[0][1]
        content = json.loads(payload['messages'][-1]['content'])
        self.assertEqual(content['context'], [{'text': '选择此消息', 'role': 'user',
                                               'time': '2026-10-22T01:01:00Z'}])

    # Known secrets are redacted even when pasted into message text/instructions.
    async def test_credentials_are_redacted_from_context_instruction_and_generated_draft(self):
        self.enable()
        self.app.store.set_setting('telegram_api', {'api_id': 12345678,
                                                  'api_hash': 'telegram-hash-secret'})
        self.app.store.put('accounts', 'a', {'id': 'a', 'password': 'two-factor-secret',
                                           'phone_code_hash': 'login-hash-secret'})
        self.message('a', 10, 1, 'sk-model-secret telegram-hash-secret two-factor-secret')
        self.transport.content = '回显 telegram-hash-secret two-factor-secret'
        result = await self.ai.action('ai_reply', {'account_id': 'a', 'chat_id': 10,
            'data_consent': True, 'instruction': '请检查 login-hash-secret 和 12345678'})
        self.assertEqual(len(self.transport.requests), 1, '应生成脱敏后的模型请求')
        exposed = json.dumps(self.transport.requests[0][1], ensure_ascii=False)
        exposed += json.dumps(result, ensure_ascii=False)
        for secret in ('sk-model-secret', 'telegram-hash-secret', 'two-factor-secret',
                       'login-hash-secret', '12345678'):
            self.assertNotIn(secret, exposed)
        self.assertIn('已隐藏', exposed)

    async def test_generated_reply_is_persisted_with_original_account_and_chat(self):
        self.enable()
        result = await self.ai.action('ai_reply', {'account_id': 'a', 'chat_id': 10,
                                                'data_consent': True})
        self.assertIsInstance(result, dict, '应返回独立 AI 草稿')
        draft = result['draft']
        self.assertEqual(draft['account_id'], 'a')
        self.assertEqual(draft['chat_id'], 10)
        self.assertEqual(draft['type'], 'reply')
        self.assertEqual(draft['status'], 'draft')
        self.assertEqual(draft['text'], '你好，我稍后回复你。')
        self.assertEqual(self.app.store.get('ai_drafts', draft['id'], account_id='a'), draft)
        self.assertIsNone(self.app.store.get('ai_drafts', draft['id'], account_id='b'))

    async def test_adoption_only_saves_original_manual_draft_and_requires_no_new_consent(self):
        self.enable()
        generated = await self.ai.action('ai_reply', {'account_id': 'a', 'chat_id': 10,
                                                     'data_consent': True})
        self.assertIsInstance(generated, dict, '应返回独立 AI 草稿')
        draft = generated['draft']
        # Adoption is local and remains possible after the external AI service is disabled.
        self.app.store.set_setting('ai', {'enabled': False})
        result = await self.ai.action('adopt_ai_draft', {'account_id': 'a',
                                                       'draft_id': draft['id']})
        self.assertEqual(self.app.store.get('drafts', '10', account_id='a')['text'],
                         '你好，我稍后回复你。')
        self.assertIsNone(self.app.store.get('drafts', '10', account_id='b'))
        self.assertEqual(result['draft']['status'], 'adopted')
        self.assertEqual(len(self.transport.requests), 1)

    async def test_adoption_rejects_other_account_redirect_and_cancelled_draft(self):
        self.enable()
        generated = await self.ai.action('ai_reply', {'account_id': 'a', 'chat_id': 10,
                                                     'data_consent': True})
        self.assertIsInstance(generated, dict, '应返回独立 AI 草稿')
        draft = generated['draft']
        for data in ({'account_id': 'b', 'draft_id': draft['id']},
                     {'account_id': 'a', 'draft_id': draft['id'], 'chat_id': 20}):
            with self.subTest(data=data), self.assertRaises(ValueError):
                await self.ai.action('adopt_ai_draft', data)
        draft['status'] = 'cancelled'
        self.app.store.put('ai_drafts', draft['id'], draft, account_id='a')
        with self.assertRaisesRegex(ValueError, '取消|失效'):
            await self.ai.action('adopt_ai_draft', {'account_id': 'a', 'draft_id': draft['id']})
        self.assertEqual(self.app.store.list('drafts'), [])

    async def test_timeout_preserves_existing_ai_and_manual_drafts_without_raw_error(self):
        self.enable()
        self.app.store.put('drafts', '10', {'text': '人工输入保持'}, account_id='a')
        self.app.store.put('ai_drafts', 'old', {'text': '原建议保持'}, account_id='a')
        self.transport.error = TimeoutError('secret-provider-response sk-model-secret')
        with self.assertRaisesRegex(ValueError, '超时') as caught:
            await self.ai.action('ai_reply', {'account_id': 'a', 'chat_id': 10,
                                            'data_consent': True})
        self.assertNotIn('secret-provider-response', str(caught.exception))
        self.assertEqual(self.app.store.get('drafts', '10', account_id='a')['text'], '人工输入保持')
        self.assertEqual(self.app.store.list('ai_drafts', account_id='a'), [{'text': '原建议保持'}])

    async def test_malformed_and_oversized_responses_create_no_draft(self):
        self.enable()
        responses = [{}, {'choices': []}, {'choices': [{'message': {'content': None}}]},
                     {'choices': [{'message': {'content': 'x' * 100_000}}]},
                     {'choices': [{'message': {'content': '安全文字',
                                               'tool_calls': [{'type': 'function'}]}}]}]
        for response in responses:
            self.transport.response = response
            with self.subTest(response=str(response)[:100]), self.assertRaises(ValueError):
                await self.ai.action('ai_reply', {'account_id': 'a', 'chat_id': 10,
                                                'data_consent': True})
        self.assertEqual(self.app.store.list('ai_drafts'), [])

    async def test_backend_error_is_public_and_does_not_expose_provider_body(self):
        self.enable()
        self.transport.error = OSError('secret-body https://bad.example/?api_key=sk-model-secret')
        with self.assertRaisesRegex(ValueError, '服务|连接|请求') as caught:
            await self.ai.action('ai_reply', {'account_id': 'a', 'chat_id': 10,
                                            'data_consent': True})
        self.assertNotIn('secret-body', str(caught.exception))
        self.assertNotIn('sk-model-secret', str(caught.exception))

    async def test_incomplete_or_credential_bearing_endpoint_is_rejected_without_network(self):
        for config in ({'model': ''}, {'endpoint': 'file:///etc/passwd'},
                       {'endpoint': 'https://user:pass@model.example/v1'},
                       {'endpoint': 'https://model.example/v1?api_key=secret'}):
            self.enable(**config)
            with self.subTest(config=config), self.assertRaises(ValueError):
                await self.ai.action('ai_reply', {'account_id': 'a', 'chat_id': 10,
                                                'data_consent': True})
        self.assertEqual(self.transport.requests, [])

    async def test_public_disclosure_excludes_secret_and_declares_context_limit(self):
        self.enable()
        self.assertTrue(callable(getattr(self.ai, 'public_settings', None)),
                        '应提供模型服务和数据范围说明')
        public = self.ai.public_settings()
        self.assertEqual(public['enabled'], True)
        self.assertEqual(public['endpoint'], 'https://model.example/v1')
        self.assertEqual(public['model'], 'fake-model')
        self.assertEqual(public['context_limit'], 20)
        self.assertEqual(public['context_fields'], ['text', 'role', 'time'])
        self.assertNotIn('sk-model-secret', json.dumps(public))

    # An AI draft must not enqueue even a harmless-looking invite preview.
    async def test_task_draft_is_strict_scoped_data_and_never_executes(self):
        self.enable()
        self.transport.content = json.dumps({'action': 'invite', 'account_id': 'a',
                                            'user_ids': [101, 102], 'group_id': -100123})
        result = await self.succeed('ai_task', {'account_id': 'a',
            'prompt': '为选择的两位联系人生成邀请草稿', 'data_consent': True})
        self.assertIsInstance(result, dict, '自然语言应返回任务草稿')
        draft = result['draft']
        self.assertEqual(draft['type'], 'task')
        self.assertEqual(draft['task'], {'action': 'invite', 'account_id': 'a',
                                        'user_ids': [101, 102], 'group_id': -100123, 'text': ''})
        self.assertEqual(draft['missing_fields'], ['consent_confirmed'])
        self.assertTrue(draft['requires_confirmation'])
        self.assertEqual(self.app.store.list('tasks'), [])
        self.assertEqual(self.app.store.list('drafts'), [])
        payload = self.transport.requests[0][1]
        self.assertEqual(payload['response_format'], {'type': 'json_object'})
        self.assertNotIn('tools', payload)

    async def test_task_without_account_requests_account_without_transmitting_prompt(self):
        self.enable()
        result = await self.succeed('ai_task', {'prompt': '邀请联系人', 'data_consent': True})
        self.assertIsInstance(result, dict, '缺账号时应要求用户补全')
        self.assertEqual(result['draft']['missing_fields'], ['account_id'])
        self.assertEqual(result['draft']['status'], 'needs_input')
        self.assertTrue(any('账号' in question for question in result['draft']['questions']))
        self.assertEqual(self.transport.requests, [])

    async def test_incomplete_task_asks_for_target_and_selected_contacts(self):
        self.enable()
        self.transport.content = json.dumps({'action': 'invite'})
        result = await self.succeed('ai_task', {'account_id': 'a', 'prompt': '邀请他们入群',
                                                'data_consent': True})
        self.assertIsInstance(result, dict, '缺目标时应保留待补全草稿')
        draft = result['draft']
        self.assertEqual(draft['status'], 'needs_input')
        self.assertEqual(draft['task']['account_id'], 'a')
        self.assertEqual(draft['task']['user_ids'], [])
        self.assertIsNone(draft['task']['group_id'])
        self.assertEqual(draft['missing_fields'], ['user_ids', 'group_id', 'consent_confirmed'])
        self.assertTrue(any('群' in question for question in draft['questions']))

    async def test_task_allows_only_predefined_actions_and_rejects_control_fields(self):
        self.enable()
        outputs = [{'action': 'shell', 'command': 'rm -rf /'},
                   {'action': 'http', 'url': 'https://attack.example'},
                   {'action': 'clear_contacts'},
                   {'action': 'invite', 'confirmed': True},
                   {'action': 'invite', 'confirmation_token': 'forged'},
                   {'action': 'invite', 'consent_confirmed': True},
                   {'action': 'send_message', 'auto_send': True},
                   {'action': 'invite', 'account_id': 'b'},
                   {'action': 'invite', 'user_ids': [True]},
                   {'action': 'invite', 'user_ids': 'all'},
                   {'action': 'invite', 'group_id': False},
                   {'action': 'invite', 'user_ids': [101, 101]},
                   {'action': 'invite', 'user_ids': list(range(1, 1002))}]
        for output in outputs:
            self.transport.content = json.dumps(output)
            with self.subTest(output=str(output)[:100]), self.assertRaises(ValueError):
                await self.ai.action('ai_task', {'account_id': 'a', 'prompt': '任务',
                                                'data_consent': True})
        self.assertEqual(len(self.transport.requests), len(outputs))
        self.assertEqual(self.app.store.list('ai_drafts'), [])

    async def test_task_rejects_unstructured_duplicate_keys_and_nonfinite_json(self):
        self.enable()
        outputs = ('任意回复', '```json\n{"action":"invite"}\n```', '[]',
                   '{"action":"invite","action":"send_message"}',
                   '{"action":"invite","group_id":NaN}')
        for content in outputs:
            self.transport.content = content
            with self.subTest(content=content), self.assertRaises(ValueError):
                await self.ai.action('ai_task', {'account_id': 'a', 'prompt': '任务',
                                                'data_consent': True})
        self.assertEqual(len(self.transport.requests), len(outputs))
        self.assertEqual(self.app.store.list('ai_drafts'), [])

    async def test_each_supported_task_action_produces_only_a_draft(self):
        self.enable()
        for action in ('add_contacts', 'delete_contacts', 'send_message', 'send_invite_link'):
            self.transport.content = json.dumps({'action': action, 'user_ids': [101],
                                                'text': '由用户审阅的文字'})
            result = await self.succeed('ai_task', {'account_id': 'a', 'prompt': '创建任务草稿',
                                                    'data_consent': True})
            self.assertIsInstance(result, dict, '允许的动作只能生成草稿')
            self.assertEqual(result['draft']['task']['action'], action)
        self.assertEqual(self.app.store.list('tasks'), [])

    async def test_task_draft_cannot_be_adopted_as_a_chat_message(self):
        self.enable()
        self.transport.content = json.dumps({'action': 'send_message', 'user_ids': [101],
                                            'text': '不能自动发送'})
        generated = await self.succeed('ai_task', {'account_id': 'a', 'prompt': '创建任务',
                                                   'data_consent': True})
        self.assertIsInstance(generated, dict, '应返回任务草稿')
        with self.assertRaisesRegex(ValueError, '任务|确认'):
            await self.ai.action('adopt_ai_draft', {'account_id': 'a',
                    'draft_id': generated['draft']['id'], 'chat_id': 10})
        self.assertEqual(self.app.store.list('drafts'), [])

    async def test_task_prompt_is_redacted_and_not_merged_with_caller_context(self):
        self.enable()
        self.transport.content = json.dumps({'action': 'send_message'})
        result = await self.succeed('ai_task', {'account_id': 'a', 'data_consent': True,
            'prompt': 'sk-model-secret', 'context': {'password': 'caller-private'}})
        self.assertIsInstance(result, dict, '应返回任务草稿')
        exposed = json.dumps(self.transport.requests[0][1]) + json.dumps(result)
        self.assertNotIn('sk-model-secret', exposed)
        self.assertNotIn('caller-private', exposed)

    async def test_real_timeout_boundary_cancels_delayed_backend_and_preserves_drafts(self):
        self.enable(timeout=0.01)

        async def delayed(*args):
            await asyncio.sleep(1)
            return await self.transport(*args)

        ai = importlib.import_module('ai').AIService(self.app, transport=delayed)
        with self.assertRaisesRegex(ValueError, '超时'):
            await ai.action('ai_reply', {'account_id': 'a', 'chat_id': 10, 'data_consent': True})
        self.assertEqual(self.app.store.list('ai_drafts'), [])

    async def test_default_transport_posts_openai_json_with_bounded_response_without_network(self):
        self.enable()
        raw = json.dumps(await self.transport('', {}, {}, 1)).encode()
        opener = RecordingOpener(raw)
        ai = importlib.import_module('ai').AIService(self.app)
        # The sole patched boundary is stdlib HTTP opening; service parsing remains real.
        with patch('urllib.request.build_opener', return_value=opener):
            result = await self.succeed('ai_reply', {'account_id': 'a', 'chat_id': 10,
                                                    'data_consent': True}, ai=ai)
        self.assertEqual(result['draft']['text'], '你好，我稍后回复你。')
        self.assertEqual(len(opener.requests), 1)
        request, timeout = opener.requests[0]
        self.assertEqual(request.get_method(), 'POST')
        self.assertEqual(request.full_url, 'https://model.example/v1/chat/completions')
        self.assertEqual(json.loads(request.data)['model'], 'fake-model')
        self.assertEqual(request.get_header('Authorization'), 'Bearer sk-model-secret')
        self.assertEqual(timeout, 2)
        self.assertLess(opener.response.tell(), 65538)

    async def test_default_transport_rejects_excessive_response_before_creating_draft(self):
        self.enable()
        opener = RecordingOpener(b' ' * 200000)
        ai = importlib.import_module('ai').AIService(self.app)
        with patch('urllib.request.build_opener', return_value=opener):
            with self.assertRaises(ValueError):
                await ai.action('ai_reply', {'account_id': 'a', 'chat_id': 10,
                                            'data_consent': True})
        self.assertEqual(len(opener.requests), 1)
        self.assertLessEqual(opener.response.tell(), 65537)
        self.assertEqual(self.app.store.list('ai_drafts'), [])

    async def test_configured_context_limit_is_respected_and_never_exceeds_twenty(self):
        for number in range(1, 25):
            self.message('a', 10, number, str(number))
        for configured, expected in ((3, ['22', '23', '24']), (0, []),
                                     (50, [str(number) for number in range(5, 25)])):
            self.enable(context_limit=configured)
            await self.succeed('ai_reply', {'account_id': 'a', 'chat_id': 10,
                                           'data_consent': True})
            content = json.loads(self.transport.requests[-1][1]['messages'][-1]['content'])
            self.assertEqual([row['text'] for row in content['context']], expected)
            self.assertEqual(self.ai.public_settings()['context_limit'], min(configured, 20))

    async def test_delayed_reply_remains_bound_to_original_account_and_chat(self):
        self.enable()
        started = asyncio.Event()
        release = asyncio.Event()

        async def delayed(*args):
            started.set()
            await release.wait()
            return await self.transport(*args)

        ai = importlib.import_module('ai').AIService(self.app, transport=delayed)
        pending = asyncio.create_task(ai.action('ai_reply', {
            'account_id': 'a', 'chat_id': 10, 'data_consent': True}))
        await started.wait()
        self.app.current_account_id = 'b'
        self.app.current_chat_id = 20
        self.app.store.put('drafts', '20', {'text': 'B账号正在编辑'}, account_id='b')
        release.set()
        result = await pending
        self.assertEqual(result['draft']['account_id'], 'a')
        self.assertEqual(result['draft']['chat_id'], 10)
        self.assertEqual(self.app.store.get('drafts', '20', account_id='b')['text'], 'B账号正在编辑')

    async def test_poisoned_storage_row_cannot_cross_account_or_inject_context_roles(self):
        self.enable()
        self.message('a', 10, 1, '正常消息', role='system', out=True)
        self.app.store.put('messages', '10:2', {'id': 2, 'account_id': 'b',
            'chat_id': 10, 'text': '错放在A存储scope的B消息'}, account_id='a')
        await self.succeed('ai_reply', {'account_id': 'a', 'chat_id': 10, 'data_consent': True})
        content = json.loads(self.transport.requests[-1][1]['messages'][-1]['content'])
        self.assertEqual(content['context'], [{'text': '正常消息', 'role': 'assistant',
                                               'time': '2026-10-22T01:01:00Z'}])

    async def test_default_transport_does_not_follow_redirect_with_api_key(self):
        self.enable()
        opener = RedirectingOpener()
        ai = importlib.import_module('ai').AIService(self.app)

        def make_opener(*handlers):
            opener.handlers = handlers
            return opener

        with patch('urllib.request.build_opener', side_effect=make_opener):
            with self.assertRaises(ValueError):
                await ai.action('ai_reply', {'account_id': 'a', 'chat_id': 10,
                                            'data_consent': True})
        self.assertEqual(opener.followed, [])
        self.assertEqual(self.app.store.list('ai_drafts'), [])

    # Telegram's service conversation contains login codes absent from local settings.
    async def test_telegram_authentication_conversation_is_never_sent_to_ai(self):
        self.enable()
        self.message('a', 777000, 1, 'Login code: 48123. Do not give this code to anyone.')
        with self.assertRaisesRegex(ValueError, '登录|认证|验证码'):
            await self.ai.action('ai_reply', {'account_id': 'a', 'chat_id': 777000,
                                            'data_consent': True})
        self.assertEqual(self.transport.requests, [])

    async def test_unpersisted_authentication_codes_are_removed_from_text_and_instruction(self):
        self.enable()
        self.message('a', 10, 1, 'Login code: 48123. Your verification code is 57129.')
        self.transport.content = '登录验证码：58391；password: unpersisted-password'
        result = await self.succeed('ai_reply', {'account_id': 'a', 'chat_id': 10,
            'data_consent': True, 'instruction': '登录验证码：58391；password: unpersisted-password'})
        exposed = json.dumps(self.transport.requests[0][1], ensure_ascii=False)
        exposed += json.dumps(result, ensure_ascii=False)
        for secret in ('48123', '57129', '58391', 'unpersisted-password'):
            self.assertNotIn(secret, exposed)

    async def test_public_disclosure_redacts_key_accidentally_present_in_endpoint_path(self):
        self.enable(endpoint='https://model.example/sk-model-secret/v1')
        self.assertNotIn('sk-model-secret', json.dumps(self.ai.public_settings()))

    async def test_http_error_body_is_closed_and_never_exposed(self):
        self.enable()
        body = io.BytesIO(b'private-provider-body')
        error = urllib.error.HTTPError('https://model.example', 401, 'private-provider-error', {}, body)
        opener = ErrorOpener(error)
        ai = importlib.import_module('ai').AIService(self.app)
        with patch('urllib.request.build_opener', return_value=opener):
            with self.assertRaises(ValueError) as caught:
                await ai.action('ai_reply', {'account_id': 'a', 'chat_id': 10,
                                            'data_consent': True})
        self.assertNotIn('private-provider', str(caught.exception))
        self.assertTrue(body.closed, '失败 HTTP 响应仍需释放资源')

    async def test_takeover_during_generation_cancels_returned_suggestion(self):
        self.enable()
        started = asyncio.Event()
        release = asyncio.Event()
        self.app.store.put('drafts', 'takeover:10', {'account_id': 'a', 'chat_id': 10,
            'enabled': False, 'generation': 'before'}, account_id='a')

        async def delayed(*args):
            started.set()
            await release.wait()
            return await self.transport(*args)

        ai = importlib.import_module('ai').AIService(self.app, transport=delayed)
        pending = asyncio.create_task(ai.action('ai_reply', {
            'account_id': 'a', 'chat_id': 10, 'data_consent': True}))
        await started.wait()
        self.app.store.put('drafts', 'takeover:10', {'account_id': 'a', 'chat_id': 10,
            'enabled': True, 'generation': 'takeover'}, account_id='a')
        self.app.store.put('drafts', 'takeover:10', {'account_id': 'a', 'chat_id': 10,
            'enabled': False, 'generation': 'released'}, account_id='a')
        release.set()
        result = await pending
        self.assertEqual(result['draft']['status'], 'cancelled')
        with self.assertRaisesRegex(ValueError, '取消|失效'):
            await ai.action('adopt_ai_draft', {'account_id': 'a',
                                             'draft_id': result['draft']['id']})
        self.assertIsNone(self.app.store.get('drafts', '10', account_id='a'))

    async def test_manual_ai_request_after_takeover_still_allows_a_local_suggestion(self):
        self.enable()
        self.app.store.put('drafts', 'takeover:10', {'account_id': 'a', 'chat_id': 10,
            'enabled': True, 'generation': 'manual'}, account_id='a')
        result = await self.succeed('ai_reply', {'account_id': 'a', 'chat_id': 10,
                                               'data_consent': True})
        self.assertEqual(result['draft']['status'], 'draft')
        self.assertIsNone(self.app.store.get('drafts', '10', account_id='a'))

    async def test_context_time_never_serializes_non_text_metadata(self):
        self.enable()
        self.message('a', 10, 1, '会话正文', date={'password': 'untrusted-nested-secret'})
        await self.succeed('ai_reply', {'account_id': 'a', 'chat_id': 10, 'data_consent': True})
        content = json.loads(self.transport.requests[-1][1]['messages'][-1]['content'])
        self.assertEqual(content['context'][0]['time'], '')
        self.assertNotIn('untrusted-nested-secret', json.dumps(content))


class KeepOpenBytesIO(io.BytesIO):
    def close(self):
        # Allow assertions on how many bytes the real HTTP transport read.
        pass


class RecordingOpener:
    def __init__(self, response):
        self.requests = []
        self.response = KeepOpenBytesIO(response)

    def open(self, request, *, timeout):
        self.requests.append((request, timeout))
        return self.response


class RedirectingOpener:
    def __init__(self):
        self.handlers = ()
        self.followed = []

    def open(self, request, *, timeout):
        target = 'https://untrusted.example/collect'
        for handler in self.handlers:
            redirected = handler.redirect_request(request, None, 302, 'Found', {}, target)
            if redirected is not None:
                self.followed.append(redirected.full_url)
        raise OSError('redirect was blocked')


class ErrorOpener:
    def __init__(self, error):
        self.error = error

    def open(self, request, *, timeout):
        raise self.error


if __name__ == '__main__':
    unittest.main()
