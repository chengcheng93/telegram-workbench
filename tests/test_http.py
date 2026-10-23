import json
from pathlib import Path
import tempfile
import threading
import unittest
import urllib.error
import urllib.request

from server import LocalServer


class Bridge:
    def __init__(self):
        self.calls = []
        self.locked = False
        self.panel_epoch = 0

    def state(self):
        return {'version': '3.0.0', 'revision': 1, 'locked': self.locked, 'panel_epoch': self.panel_epoch, 'accounts': []}

    def call(self, name, data):
        self.calls.append((name, data))
        return {'account_id': data.get('account_id')}


class HttpTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.web = Path(self.tmp.name)
        (self.web / 'index.html').write_text('<p>本机工作台</p>')
        (self.web / 'app.js').write_text('window.ready=true;')
        self.bridge = Bridge()
        self.server = LocalServer(('127.0.0.1', 0), self.bridge, 'private-token', web_root=self.web)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.origin = self.server.origin
        self.opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(2)
        self.tmp.cleanup()

    def request(self, path, payload=None, headers=None, token=True):
        h = {'X-Local-Token': 'private-token'} if token else {}
        if payload is not None:
            h['Content-Type'] = 'application/json'
        h.update(headers or {})
        body = json.dumps(payload).encode() if payload is not None else None
        return self.opener.open(urllib.request.Request(self.origin + path, data=body, headers=h), timeout=3)

    def test_state_requires_token_and_does_not_publish_token(self):
        with self.assertRaises(urllib.error.HTTPError) as e:
            self.request('/api/state', token=False)
        self.assertEqual(e.exception.code, 403)
        e.exception.close()
        with self.request('/api/state') as response:
            raw = response.read().decode()
        self.assertEqual(json.loads(raw)['version'], '3.0.0')
        self.assertNotIn('private-token', raw)

    def test_wrong_origin_and_host_rejected(self):
        for headers in ({'Origin': 'https://untrusted.invalid'}, {'Host': 'attacker.invalid'}):
            with self.assertRaises(urllib.error.HTTPError) as e:
                self.request('/api/state', headers=headers)
            self.assertEqual(e.exception.code, 403)
            e.exception.close()

    def test_action_keeps_explicit_account_scope(self):
        payload = {'action': 'save_listener', 'account_id': 'B', 'group_ids': [-123]}
        with self.request('/api/action', payload) as response:
            result = json.load(response)
        self.assertTrue(result['ok'])
        self.assertEqual(result['data']['account_id'], 'B')
        self.assertEqual(self.bridge.calls, [('save_listener', payload)])

    def test_static_paths_are_allowlisted(self):
        with self.request('/', token=False) as response:
            self.assertIn('本机工作台', response.read().decode())
            self.assertIn("frame-ancestors 'none'", response.headers['Content-Security-Policy'])
        for path in ('/../accounts.py', '/storage.py', '/%2e%2e/config.json'):
            with self.assertRaises(urllib.error.HTTPError) as e:
                self.request(path, token=False)
            self.assertEqual(e.exception.code, 404)
            e.exception.close()

    def test_lock_prevents_media_and_mutations(self):
        self.bridge.locked = True
        for path, payload in (('/api/action', {'action': 'send_message', 'account_id': 'A'}),
                              ('/api/media?account_id=A&chat_id=1&message_id=2', None)):
            with self.assertRaises(urllib.error.HTTPError) as e:
                self.request(path, payload)
            self.assertEqual(e.exception.code, 423)
            e.exception.close()
        self.assertEqual(self.bridge.calls, [])
        with self.request('/api/action', {'action': 'unlock_panel', 'pin': '1234'}) as response:
            self.assertTrue(json.load(response)['ok'])

    def test_stream_is_authenticated_and_emits_state(self):
        with self.request('/api/stream') as response:
            self.assertIn('application/x-ndjson', response.headers['Content-Type'])
            line = json.loads(response.readline())
            self.assertEqual(line['state']['version'], '3.0.0')

    def test_invalid_json_cannot_reach_action(self):
        request = urllib.request.Request(self.origin + '/api/action', data=b'[]', headers={
            'X-Local-Token': 'private-token', 'Content-Type': 'application/json'})
        with self.assertRaises(urllib.error.HTTPError) as e:
            self.opener.open(request)
        self.assertEqual(e.exception.code, 400)
        e.exception.close()
        self.assertEqual(self.bridge.calls, [])

    def test_action_result_is_not_returned_after_lock_during_request(self):
        def call(name, data):
            self.bridge.locked = True
            self.bridge.panel_epoch += 1
            return {'private_contacts': 'PRIVATE_RESULT'}
        self.bridge.call = call
        with self.assertRaises(urllib.error.HTTPError) as e:
            self.request('/api/action', {'action': 'prepare_clear', 'account_id': 'A'})
        self.assertEqual(e.exception.code, 423)
        self.assertNotIn(b'PRIVATE_RESULT', e.exception.read())
        e.exception.close()

    def test_lock_and_unlock_still_invalidates_an_older_request(self):
        def call(name, data):
            self.bridge.panel_epoch += 2
            return {'private_contacts': 'PRIVATE_RESULT'}
        self.bridge.call = call
        with self.assertRaises(urllib.error.HTTPError) as e:
            self.request('/api/action', {'action': 'list_contacts', 'account_id': 'A'})
        self.assertEqual(e.exception.code, 423)
        self.assertNotIn(b'PRIVATE_RESULT', e.exception.read())
        e.exception.close()

    def test_media_result_is_not_returned_after_lock_during_download(self):
        file = self.web / 'private-media.bin'
        file.write_bytes(b'PRIVATE_MEDIA_BYTES')
        def media(*args):
            self.bridge.locked = True
            self.bridge.panel_epoch += 1
            return {'path': file, 'name': file.name, 'mime': 'application/octet-stream'}
        self.bridge.media = media
        with self.assertRaises(urllib.error.HTTPError) as e:
            self.request('/api/media?account_id=A&chat_id=1&message_id=2')
        self.assertEqual(e.exception.code, 423)
        self.assertNotIn(b'PRIVATE_MEDIA_BYTES', e.exception.read())
        e.exception.close()


if __name__ == '__main__':
    unittest.main()
