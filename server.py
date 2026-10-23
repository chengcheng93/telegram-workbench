"""Authenticated loopback-only HTTP and state stream for the local workbench."""

import argparse
import asyncio
from concurrent.futures import TimeoutError as FutureTimeout
import fcntl
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import logging
import mimetypes
import os
from pathlib import Path
import secrets
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request

from telethon import errors


def public_error(error):
    if isinstance(error, errors.FloodError):
        return f'Telegram 要求等待 {getattr(error, "seconds", "一段时间")} 秒，请稍后检查恢复。'
    if isinstance(error, errors.TakeoutInitDelayError):
        return f'Telegram 联系人导出需要等待 {error.seconds} 秒，当前不支持完整清空，请稍后重新准备。'
    if isinstance(error, errors.PeerFloodError):
        return 'Telegram 限制本账号当前操作，已暂停；需要人工检查账号状态。'
    if isinstance(error, (TimeoutError, FutureTimeout, ConnectionError, OSError)):
        return '连接或本机文件操作未完成，请检查网络和本机存储；未核实的结果不会自动重试。'
    if isinstance(error, errors.RPCError):
        known = {'PhoneCodeInvalidError': '验证码不正确，请重试。',
                 'PhoneCodeExpiredError': '验证码已过期，请重新获取。',
                 'PasswordHashInvalidError': '两步验证密码不正确。',
                 'ApiIdInvalidError': '应用 API ID 或 API Hash 不正确。',
                 'PhoneNumberInvalidError': '手机号格式不正确。',
                 'PhoneNumberBannedError': '此手机号被 Telegram 限制。'}
        return known.get(type(error).__name__, 'Telegram 操作未完成：' + type(error).__name__)
    if isinstance(error, (ValueError, RuntimeError)):
        return str(error)
    return '操作未完成：' + type(error).__name__


class Bridge:
    def __init__(self, root):
        self.root = Path(root)
        self.loop = asyncio.new_event_loop()
        self.ready = threading.Event()
        self.error = None
        self.thread = threading.Thread(target=self._worker, daemon=True)
        self.thread.start()
        if not self.ready.wait(5):
            raise RuntimeError('后台启动超时。')
        if self.error:
            raise RuntimeError('后台初始化失败：' + type(self.error).__name__)

    def _worker(self):
        asyncio.set_event_loop(self.loop)
        try:
            from app import Workbench
            self.app = Workbench(self.root)
            self.loop.create_task(self.app.start())
        except BaseException as error:
            self.error = error
            self.ready.set()
            return
        self.ready.set()
        self.loop.run_forever()
        self.loop.close()

    def _run(self, coro, timeout=50):
        future = asyncio.run_coroutine_threadsafe(coro, self.loop)
        try:
            return future.result(timeout=timeout)
        except FutureTimeout:
            # Cancelling the waiter does not promise that Telegram rolled back
            # a submitted write. Workflow journals retain its unknown boundary.
            future.cancel()
            raise

    def state(self):
        async def read():
            return self.app.snapshot()
        return self._run(read(), 5)

    def call(self, name, data):
        return self._run(self.app.action(name, data))

    def upload(self, account_id, filename, content):
        return self._run(self.app.chats.upload(account_id, filename, content))

    def media(self, account_id, chat_id, message_id):
        return self._run(self.app.chats.media(account_id, chat_id, message_id))

    def close(self):
        try:
            self._run(self.app.close(), 35)
        finally:
            self.loop.call_soon_threadsafe(self.loop.stop)
            self.thread.join(timeout=5)


class LocalServer(ThreadingHTTPServer):
    daemon_threads = True

    def __init__(self, address, bridge, token, web_root=None):
        self.bridge, self.token = bridge, token
        self.web_root = Path(web_root) if web_root else Path(__file__).parent / 'web'
        self.stopping = threading.Event()
        super().__init__(address, Handler)
        self.origin = f'http://127.0.0.1:{self.server_port}'

    def shutdown(self):
        self.stopping.set()
        return super().shutdown()


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *_):
        pass

    def headers_for(self, content_type, *, length=None):
        self.send_header('Content-Type', content_type)
        if length is not None:
            self.send_header('Content-Length', str(length))
        self.send_header('Cache-Control', 'no-store')
        self.send_header('Referrer-Policy', 'no-referrer')
        self.send_header('X-Content-Type-Options', 'nosniff')
        self.send_header('Content-Security-Policy', "default-src 'self'; script-src 'self'; style-src 'self' 'unsafe-inline'; img-src 'self' blob: data:; media-src 'self' blob:; connect-src 'self'; frame-ancestors 'none'; base-uri 'none'; form-action 'self'")

    def respond(self, code, payload, content_type='application/json; charset=utf-8'):
        raw = json.dumps(payload, ensure_ascii=False).encode() if isinstance(payload, dict) else payload
        self.send_response(code)
        self.headers_for(content_type, length=len(raw))
        self.end_headers()
        self.wfile.write(raw)

    def allowed(self, token=False):
        if self.headers.get('Host') != self.server.origin.removeprefix('http://'):
            self.respond(403, {'ok': False, 'error': '只允许本机工作台访问。'})
            return False
        origin = self.headers.get('Origin')
        if origin and origin != self.server.origin:
            self.respond(403, {'ok': False, 'error': '页面来源不匹配。'})
            return False
        if token and not secrets.compare_digest(self.headers.get('X-Local-Token', ''), self.server.token):
            self.respond(403, {'ok': False, 'error': '请通过本机启动入口重新打开工作台。'})
            return False
        return True

    def unlocked(self, state=None):
        state = self.server.bridge.state() if state is None else state
        if state.get('locked'):
            self.respond(423, {'ok': False, 'error': '面板已锁定，请输入解锁密码。'})
            return False
        return True

    def fence_valid(self, before, after=None):
        after = self.server.bridge.state() if after is None else after
        return not after.get('locked') and before.get('panel_epoch', 0) == after.get('panel_epoch', 0)

    def stale_result(self, state=None):
        self.respond(423, {'ok': False, 'error': '面板锁定状态已变化，请解锁后重新查看执行记录；未核实的操作不要重复提交。',
                           'state': self.server.bridge.state() if state is None else state})

    def do_GET(self):
        parsed = urllib.parse.urlsplit(self.path)
        path = parsed.path
        if not self.allowed(token=path.startswith('/api/')):
            return
        try:
            if path == '/api/state':
                self.respond(200, self.server.bridge.state())
            elif path == '/api/stream':
                self.stream()
            elif path == '/api/media':
                before = self.server.bridge.state()
                if not self.unlocked(before):
                    return
                query = urllib.parse.parse_qs(parsed.query)
                account = query.get('account_id', [''])[0]
                chat = int(query.get('chat_id', ['0'])[0])
                message = int(query.get('message_id', ['0'])[0])
                result = self.server.bridge.media(account, chat, message)
                if not self.fence_valid(before):
                    self.stale_result()
                    return
                file = Path(result['path'])
                name = ''.join(c for c in str(result.get('name') or file.name) if ord(c) >= 32)[:200]
                self.send_response(200)
                self.headers_for(result.get('mime') or 'application/octet-stream', length=file.stat().st_size)
                self.send_header('Content-Disposition', "attachment; filename*=UTF-8''" + urllib.parse.quote(name))
                self.end_headers()
                with file.open('rb') as source:
                    while chunk := source.read(65536):
                        if not self.fence_valid(before):
                            return
                        self.wfile.write(chunk)
            elif path in ('/', '/app.js', '/state.js', '/styles.css'):
                name = 'index.html' if path == '/' else path[1:]
                file = self.server.web_root / name
                if not file.is_file():
                    self.respond(404, {'ok': False, 'error': '界面文件不存在。'})
                    return
                mime = 'text/javascript' if name.endswith('.js') else mimetypes.guess_type(name)[0] or 'text/plain'
                self.respond(200, file.read_bytes(), mime + '; charset=utf-8')
            elif path == '/favicon.ico':
                self.respond(204, b'', 'image/x-icon')
            else:
                self.respond(404, {'ok': False, 'error': '页面不存在。'})
        except (BrokenPipeError, ConnectionResetError):
            pass
        except Exception as error:
            self.respond(400, {'ok': False, 'error': public_error(error)})

    def stream(self):
        self.send_response(200)
        self.headers_for('application/x-ndjson; charset=utf-8')
        self.send_header('Connection', 'close')
        self.end_headers()
        previous, heartbeat = None, 0
        while not self.server.stopping.is_set():
            state = self.server.bridge.state()
            revision = (state.get('revision'), state.get('locked'))
            if revision != previous or time.monotonic() - heartbeat > 15:
                body = json.dumps({'type': 'state', 'state': state}, ensure_ascii=False) + '\n'
                self.wfile.write(body.encode())
                self.wfile.flush()
                previous, heartbeat = revision, time.monotonic()
            self.server.stopping.wait(.5)

    def do_POST(self):
        if not self.allowed(token=True):
            return
        parsed = urllib.parse.urlsplit(self.path)
        if parsed.path not in ('/api/action', '/api/upload'):
            self.respond(404, {'ok': False, 'error': '操作不存在。'})
            return
        before = self.server.bridge.state()
        try:
            size = int(self.headers.get('Content-Length', '0'))
            limit = 25 * 1024 * 1024 if parsed.path == '/api/upload' else 1024 * 1024
            if size <= 0 or size > limit:
                self.respond(413, {'ok': False, 'error': '请求为空或超过允许大小。'})
                return
            if parsed.path == '/api/upload':
                if not self.unlocked(before):
                    return
                query = urllib.parse.parse_qs(parsed.query)
                result = self.server.bridge.upload(query.get('account_id', [''])[0],
                                                   query.get('filename', ['attachment'])[0],
                                                   self.rfile.read(size))
                if not self.fence_valid(before):
                    self.stale_result()
                    return
                self.respond(200, {'ok': True, 'data': {'attachment': result}})
                return
            if self.headers.get_content_type() != 'application/json':
                self.respond(415, {'ok': False, 'error': '请求必须使用 JSON。'})
                return
            data = json.loads(self.rfile.read(size))
            if not isinstance(data, dict) or not isinstance(data.get('action'), str):
                raise ValueError('请求格式不正确。')
            if data['action'] != 'unlock_panel':
                if not self.unlocked(before):
                    return
                if not self.fence_valid(before):
                    self.stale_result()
                    return
            result = self.server.bridge.call(data['action'], data)
            after = self.server.bridge.state()
            if data['action'] not in ('lock_panel', 'unlock_panel') and not self.fence_valid(before, after):
                self.stale_result(after)
                return
            self.respond(200, {'ok': True, 'data': result, 'state': after})
            if data['action'] == 'shutdown':
                threading.Thread(target=self.server.shutdown, daemon=True).start()
        except (BrokenPipeError, ConnectionResetError):
            pass
        except Exception as error:
            after = self.server.bridge.state()
            unlocking = isinstance(locals().get('data'), dict) and data.get('action') == 'unlock_panel'
            if not unlocking and not self.fence_valid(before, after):
                self.stale_result(after)
            else:
                self.respond(400, {'ok': False, 'error': public_error(error), 'state': after})


def runtime_root():
    default = Path(__file__).resolve().parents[2] / 'work' / 'telegram-workbench-runtime'
    return Path(os.environ.get('TG_WORKBENCH_STATE_DIR', str(default))).expanduser().resolve()


def open_browser(url):
    if sys.platform == 'darwin':
        subprocess.run(['/usr/bin/open', url], check=True)
    else:
        import webbrowser
        webbrowser.open(url)


def open_existing(root, should_open):
    try:
        descriptor = json.loads((root / 'instance.json').read_text())
        origin = descriptor['origin']
        parsed = urllib.parse.urlsplit(origin)
        if parsed.hostname != '127.0.0.1' or parsed.scheme != 'http' or not parsed.port or parsed.path:
            return False
        req = urllib.request.Request(origin + '/api/state', headers={'X-Local-Token': descriptor['token']})
        opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
        with opener.open(req, timeout=3) as response:
            if json.load(response).get('version') != '3.0.0':
                return False
        if should_open:
            open_browser(origin + '/#' + descriptor['token'])
        return True
    except (OSError, ValueError, KeyError, urllib.error.URLError):
        return False


def main():
    parser = argparse.ArgumentParser(description='TG 多账号工作台')
    parser.add_argument('--no-open', action='store_true')
    args = parser.parse_args()
    os.umask(0o077)
    logging.getLogger('telethon').setLevel(logging.CRITICAL)
    root = runtime_root()
    root.mkdir(parents=True, exist_ok=True, mode=0o700)
    root.chmod(0o700)
    if open_existing(root, not args.no_open):
        return 0
    with (root / 'server.lock').open('a+') as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise SystemExit('工作台已在启动或运行，请稍后重新打开。')
        bridge = Bridge(root)
        token = secrets.token_urlsafe(32)
        server = LocalServer(('127.0.0.1', 0), bridge, token)
        descriptor = root / 'instance.json'
        descriptor.write_text(json.dumps({'origin': server.origin, 'token': token}))
        descriptor.chmod(0o600)
        print('本机工作台已启动：' + server.origin, flush=True)
        if not args.no_open:
            open_browser(server.origin + '/#' + token)
        try:
            server.serve_forever(poll_interval=.25)
        except KeyboardInterrupt:
            pass
        finally:
            server.stopping.set()
            server.server_close()
            bridge.close()
            descriptor.unlink(missing_ok=True)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
