"""Durable, explicitly confirmed account tasks and bounded contact cleanup."""
import asyncio
import builtins
from datetime import datetime, timedelta, timezone
import csv
import hashlib
import hmac
import io
import json
from pathlib import Path
import secrets
import subprocess
from uuid import UUID, uuid4

from telethon import errors, functions

from cleanup import read_full_snapshot, save_backup, validate_backup, input_user
from group_invites import resolve_target, invite_contact, _membership, _fatal
from scheduler import OperationPaused


def _now(): return datetime.now(timezone.utc).isoformat()
def _future(seconds=300): return (datetime.now(timezone.utc)+timedelta(seconds=seconds)).isoformat()
def _hash(value): return hashlib.sha256(json.dumps(value,ensure_ascii=False,sort_keys=True,separators=(',',':')).encode()).hexdigest()
def _expired(value):
    try: return datetime.fromisoformat(value) <= datetime.now(timezone.utc)
    except (TypeError,ValueError): return True

def _csv_safe(value):
    value=str(value if value is not None else '')
    return "'"+value if value.lstrip().startswith(('=','+','-','@','\t','\r','\n')) else value


class _BoundaryPause(RuntimeError): pass


class TaskService:
    TYPES={'add_contacts','delete_contacts','invite','send_message','send_invite_link'}
    FINAL={'invited','already','restricted','unverified','failed','added','deleted','sent'}

    def __init__(self,app):
        self.app=app
        self._locks={}
        self._prepare_locks={}
        self._workers={}
        self._bindings={}
        self._protected={}
        self._closing=False
        self._restore_message_ownership()
        # Recovery is read-only: a lost in-flight boundary is never replayed.
        for task in self.app.store.list('tasks'):
            changed=False
            if task['status'] in {'queued','running','awaiting_confirmation','paused','waiting'}:
                task.update(status='paused',error='程序重新启动，请先检查未核实结果并重新确认。',token='')
                changed=True
            for item in self._items(task):
                if item['status']=='inflight':
                    item.update(status='unverified',kind='unverified',detail='请求曾提交，重启后需只读核实。')
                    self._put_item(task,item)
                    changed=True
            if changed: self._save(task)

    @staticmethod
    def _request_key(value):
        try: return str(UUID(str(value)))
        except (ValueError,TypeError,AttributeError): return None

    def _message_record_matches(self,task,item,row):
        key=self._request_key(item.get('request_id'))
        return (key is not None and row.get('account_id')==task['account_id']
                and self._request_key(row.get('request_id'))==key
                and row.get('chat_id')==item.get('user_id') and row.get('text')==task.get('text')
                and row.get('reply_to') is None and not row.get('attachment_ids'))

    def _restore_message_ownership(self):
        """Bind legacy task outbox evidence before ordinary chat can dispatch it."""
        candidates={}
        for task in self.app.store.list('tasks'):
            if task['type'] not in {'send_message','send_invite_link'}: continue
            for item in self._items(task):
                key=self._request_key(item.get('request_id'))
                if key: candidates.setdefault((task['account_id'],key),[]).append((task,item))
        for row in self.app.store.list('outbox'):
            key=self._request_key(row.get('request_id'))
            owners=candidates.get((row.get('account_id'),key),[])
            if not owners: continue
            row['request_id']=key
            task,item=owners[0]
            expected=(task['id'],item['id'])
            recorded=(row.get('task_id'),row.get('item_id'))
            valid=(len(owners)==1 and self._message_record_matches(task,item,row)
                   and recorded in {(None,None),expected} and not row.get('task_ownership_error'))
            if recorded==(None,None): row.update(task_id=task['id'],item_id=item['id'])
            if not valid:
                row['task_ownership_error']='任务消息归属或参数证据冲突，已阻止重发，请在任务中心人工核对。'
                for owner,entry in owners:
                    entry.update(status='unverified',kind='unverified',detail=row['task_ownership_error'])
                    self._put_item(owner,entry)
                    if owner['status']!='cancelled': owner.update(status='paused',error=row['task_ownership_error'])
                    self._save(owner)
            self.app.store.put('outbox',row['id'],row,account_id=task['account_id'])

    def contact_lock(self,account_id):
        return self._locks.setdefault(account_id,asyncio.Lock())

    def _items(self,task):
        items=[r for r in self.app.store.list('task_items',task['account_id']) if r.get('task_id')==task['id']]
        return sorted(items,key=lambda r:r.get('index',0))

    def _put_item(self,task,item):
        self.app.store.put('task_items',item['id'],item,account_id=task['account_id'])

    def _save(self,task):
        items=self._items(task)
        counts={'total':len(items),'processed':0,'remaining':0,'inflight':0}
        for kind in self.FINAL: counts[kind]=0
        for item in items:
            status=item['status']
            if status in self.FINAL:
                counts['processed']+=1
                counts[status]+=1
            else:
                counts['remaining']+=1
                if status=='inflight': counts['inflight']+=1
        task['counts']=counts
        task['remaining']=counts['remaining']
        self.app.store.put('tasks',task['id'],task,account_id=task['account_id'])
        self.app.bump()
        return task

    def _get(self,task_id,account_id=None):
        task=next((r for r in self.app.store.list('tasks',account_id) if r['id']==task_id),None)
        if not task: raise ValueError('任务不存在或不属于所选账号。')
        return task

    def _expire(self,account_id=None):
        for task in self.app.store.list('tasks',account_id):
            expirable=(task['status']=='awaiting_confirmation' or (task['type']=='clear_contacts' and task['status']=='paused' and not task.get('confirmed_at') and not task.get('drain_pending')))
            if expirable and _expired(task.get('expires_at')):
                task.update(status='expired',token='',error='确认已过期，请重新准备。')
                self._save(task)
                self._release(task)

    def contact_blocked(self,account_id):
        self._expire(account_id)
        profile=self.app.store.get('accounts',account_id,{})
        if account_id in self._protected and (not profile or profile.get('deleted')):
            task_id=self._protected.pop(account_id)
            task=next((t for t in self.app.store.list('tasks',account_id) if t['id']==task_id),None)
            if task:
                task.update(status='cancelled',token='',error='账号已移除，清空保护已释放。')
                self._save(task)
        return account_id in self._protected

    def snapshot(self,account_id=None):
        self._expire(account_id)
        return self.app.store.list('tasks',account_id)

    def _release(self,task):
        if self._protected.get(task['account_id'])==task['id']:
            self._protected.pop(task['account_id'],None)

    def _identity(self,task,*,require_binding=False):
        session=self.app.accounts.require(task['account_id'])
        if session.own_id!=task['own_id']: raise ValueError('账号真实身份已变化，请重新准备任务。')
        binding=self._bindings.get(task['id'])
        if require_binding and (not binding or binding is not session.client):
            raise ValueError('账号连接已变化，请重新检查并准备任务。')
        return session

    def _scope_hash(self,task):
        return _hash({k:task.get(k) for k in ['account_id','own_id','type','selection','entries','target','text','consent_confirmed']})

    def _validate_confirmation(self,task,data,clear=False):
        if data.get('account_id') and data['account_id']!=task['account_id']: raise ValueError('确认账号与任务账号不一致。')
        if task['status']!='awaiting_confirmation': raise ValueError('任务不在待确认状态，请重新准备。')
        if _expired(task.get('expires_at')):
            task.update(status='expired',token='',error='确认已过期，请重新准备。')
            self._save(task)
            self._release(task)
            raise ValueError('确认已过期，请重新准备。')
        supplied=data.get('confirmation_token')
        if not isinstance(supplied,str) or not hmac.compare_digest(supplied,task.get('token','')):
            raise ValueError('确认凭证无效。')
        self._identity(task,require_binding=True)
        if task.get('scope_hash')!=self._scope_hash(task): raise ValueError('任务参数已变化，请重新准备。')
        if clear:
            account=self.app.store.get('accounts',task['account_id'],{})
            if (data.get('account_name')!=account.get('name') or data.get('account_name')!=task.get('account_name')
                    or data.get('understood_backup') is not True or data.get('confirmed_scope') is not True):
                raise ValueError('请输入准确账号名称，并勾选理解备份边界及确认完整范围。')

    def _new(self,account_id,type,data):
        session=self.app.accounts.require(account_id)
        return {'id':uuid4().hex,'account_id':account_id,'own_id':session.own_id,'type':type,
                'name':str(data.get('name') or {'invite':'邀请入群','add_contacts':'添加联系人','delete_contacts':'删除联系人',
                    'send_message':'发送消息','send_invite_link':'发送邀请链接','clear_contacts':'清空全部联系人'}[type])[:200],
                'status':'awaiting_confirmation','created_at':_now(),'confirmed_at':None,'control_version':0,
                'token':secrets.token_urlsafe(32),'expires_at':_future(),'error':'','wait_until':None}

    def _reservation(self,account_id,group_id,user_id,exclude_task=None):
        for task in self.app.store.list('tasks',account_id):
            if task['type']!='invite' or task['id']==exclude_task or task.get('target',{}).get('group_id')!=group_id: continue
            for item in self._items(task):
                if item.get('user_id')!=user_id or item.get('reservation_owner'): continue
                status=item['status']
                if status in {'invited','already'}: return 'already'
                if status in {'inflight','unverified'}: return 'unverified'
                if status=='queued' and task['status'] not in {'expired','cancelled'}: return 'reserved'
        return None

    async def _membership(self,client,target,person):
        try: return _membership(await client.get_permissions(target.entity,person),target.entity,person.id)
        except errors.UserNotParticipantError: return 'absent'
        except errors.RPCError as error:
            if _fatal(error): raise
            return 'unknown'
        except (ValueError,TypeError): return 'unknown'

    async def _prepare(self,data):
        account_id=data.get('account_id')
        if not account_id: raise ValueError('请选择账号。')
        async with self._prepare_locks.setdefault(account_id,asyncio.Lock()):
            self._expire(account_id)
            if self.contact_blocked(account_id): raise ValueError('本账号联系人正在清空保护中，请先取消清空。')
            type=data.get('type')
            if type not in self.TYPES: raise ValueError('任务类型无效。')
            task=self._new(account_id,type,data)
            session=self.app.accounts.require(account_id)
            selected=data.get('user_ids') or []
            entries=data.get('entries') or []
            if not isinstance(selected,list) or not isinstance(entries,list) or len(selected)+len(entries)>10000:
                raise ValueError('联系人名单格式无效或超过 10000 人。')
            if any(not isinstance(e,dict) or (e.get('account') is not None and e.get('account')!=account_id)
                   or (e.get('account_id') is not None and e.get('account_id')!=account_id) for e in entries):
                raise ValueError('导入对象格式无效或不属于当前账号。')
            if any(builtins.type(i) is not int or i<=0 for i in selected): raise ValueError('联系人标识无效。')
            if len(selected)!=len(set(selected)): selected=list(dict.fromkeys(selected))
            if type!='add_contacts' and entries: raise ValueError('该任务只能使用明确选择的当前联系人。')
            if type=='add_contacts' and selected: entries += [{'user_id':i} for i in selected]
            if not entries and not selected: raise ValueError('请明确选择联系人。')
            if type in {'invite','send_message','send_invite_link'} and data.get('consent_confirmed') is not True:
                raise ValueError('请确认已获得所选对象的同意。')
            text=data.get('text','')
            if type in {'send_message','send_invite_link'} and (not isinstance(text,str) or not text.strip() or len(text)>4096):
                raise ValueError('请填写明确的消息或邀请链接正文（最多 4096 字符）。')
            if type=='send_invite_link' and not any(s in text for s in ['https://t.me/','http://t.me/','tg://join']):
                raise ValueError('邀请链接任务需要明确的 Telegram 邀请链接正文。')
            cached={r.get('user_id'):r for r in self.app.contacts.cached(account_id) if r.get('user_id')}
            if type in {'send_message','send_invite_link'} and any(i not in cached or not cached[i].get('eligible') for i in selected):
                raise ValueError('消息任务仅支持明确选择的当前有效联系人。')
            task.update(selection=list(selected),entries=entries,text=text,consent_confirmed=data.get('consent_confirmed') is True,
                        prechecks={'account':True,'permission':None,'membership_verifiable':True},exclusions=[])
            target=None
            if type=='invite':
                group_id=data.get('group_id')
                target=await resolve_target(session.client,group_id,session.own_id)
                task['target']={'group_id':group_id,'title':target.title}
                task['prechecks']['permission']=True
            items=[]
            for index,entry in enumerate(entries if type=='add_contacts' else [{'user_id':i} for i in selected]):
                item={'id':uuid4().hex,'task_id':task['id'],'account_id':account_id,'index':index,
                      'user_id':entry.get('user_id'),'name':entry.get('name',''),'status':'queued','kind':None,'detail':'',
                      'submitted_at':None,'verified_at':None,'entry':entry}
                if type in {'send_message','send_invite_link'}: item['request_id']=str(uuid4())
                try:
                    row=cached.get(item['user_id'])
                    if type!='add_contacts' and (not row or not row.get('eligible')):
                        raise ValueError((row or {}).get('exclusion_reason') or '当前账号中不存在该有效联系人。')
                    person=await self.app.contacts.resolve_person(account_id,entry)
                    item.update(user_id=person.id,name=(row or {}).get('name') or item['name'] or person.first_name or str(person.id))
                    if target:
                        reservation=self._reservation(account_id,target.group_id,person.id)
                        member=await self._membership(session.client,target,person)
                        if member=='member' or reservation=='already':
                            item.update(status='already',kind='already',detail='已核实原已在群或此前成功加入。',verified_at=_now())
                        elif member=='banned': item.update(status='restricted',kind='restricted',detail='对象在目标群受限。')
                        elif reservation:
                            item.update(status='unverified',kind='unverified',detail='已有未完成或未核实邀请任务，请先核实原任务。',reservation_owner=True)
                        elif member=='unknown':
                            task['prechecks']['membership_verifiable']=False
                            item['detail']='当前无法核实成员状态；执行仍会先检查，无法核实则不提交。'
                except (ValueError,TypeError) as error:
                    item.update(status='failed',kind='failed',detail=str(error))
                items.append(item)
                if item['status']!='queued': task['exclusions'].append({'user_id':item.get('user_id'),'kind':item['kind'],'detail':item['detail']})
            task['scope_hash']=self._scope_hash(task)
            # No await between reservation persistence and releasing preparation lock.
            for item in items: self._put_item(task,item)
            self._bindings[task['id']]=session.client
            return {'task':self._save(task)}

    def _kick(self,account_id):
        worker=self._workers.get(account_id)
        if self._closing or (worker and not worker.done()): return
        self._workers[account_id]=asyncio.create_task(self._worker(account_id))

    async def _worker(self,account_id):
        while not self._closing:
            task=next((t for t in self.app.store.list('tasks',account_id) if t['status']=='queued'),None)
            if not task: return
            task['status']='running'
            self._save(task)
            try:
                if task['type']=='clear_contacts': await self._run_clear(task)
                else: await self._run(task)
            except asyncio.CancelledError:
                await self._pause(task,'执行被中断，请核实在途结果。')
                raise
            except Exception as error:
                await self._pause(task,self._error(error))

    def _error(self,error):
        if isinstance(error,errors.FloodWaitError): return f'Telegram 要求等待 {error.seconds} 秒后再检查。'
        if isinstance(error,(ConnectionError,TimeoutError,OSError)): return '网络连接异常，任务已暂停；已提交结果需先核实。'
        if isinstance(error,errors.RPCError): return f'Telegram 账号或目标权限受限，任务已暂停（{builtins.type(error).__name__}）。'
        if isinstance(error,(ValueError,_BoundaryPause)): return str(error)
        return '执行异常，任务已暂停，请检查后再确认。'

    def _boundary(self,task):
        current=self._get(task['id'])
        if task['type']=='clear_contacts' and self._protected.get(task['account_id'])!=task['id']:
            raise _BoundaryPause('清空写入保护已释放，必须重新准备。')
        if self._closing or current['status'] not in {'running','queued'}: raise _BoundaryPause('任务已停止后续提交。')
        if getattr(self.app,'global_paused',False): raise _BoundaryPause('全局已暂停，请检查后再确认。')
        try: session=self._identity(task)
        except ValueError as error: raise _BoundaryPause(str(error)) from error
        return session

    def _journal(self,task,item):
        self._boundary(task)
        item.update(status='inflight',submitted_at=_now(),detail='请求已提交，等待核实。')
        self._put_item(task,item)
        self._save(self._get(task['id']))

    async def _pause(self,task,reason):
        current=self._get(task['id'])
        for item in self._items(current):
            if item['status']=='inflight':
                item.update(status='unverified',kind='unverified',detail='请求结果未知，请只读核实。')
                self._put_item(current,item)
        if current['status'] not in {'cancelled','expired'}:
            current.update(status='paused',error=reason)
        self._save(current)

    async def _resolve_item(self,task,item):
        person=await self.app.contacts.resolve_person(task['account_id'],item['entry'])
        if person.id!=item.get('user_id'):
            raise ValueError('联系人号码或用户名映射已变化，请重新预览并确认原定对象。')
        return person

    async def _record_rpc_error(self,task,item,error):
        if _fatal(error):
            await self._pause_with_wait(task,error)
            return False
        code=getattr(error,'code',None)
        if builtins.type(code) is int and 400 <= code < 500:
            item.update(status='failed',kind='failed',detail=f'Telegram 明确拒绝此对象（{builtins.type(error).__name__}）。')
        elif item['status']=='inflight':
            item.update(status='unverified',kind='unverified',detail='请求已提交，但平台异常未能证明失败；请先只读核实，禁止重复执行。')
        else:
            await self._pause(task,'平台只读检查异常，尚未提交该对象，任务已暂停。')
            return False
        self._put_item(task,item)
        self._save(self._get(task['id']))
        return True

    async def _run(self,task):
        for item in self._items(task):
            if item['status']!='queued': continue
            current=self._get(task['id'])
            if current['status']!='running' or self._closing: return
            if self.contact_blocked(task['account_id']):
                await self._pause(task,'清空联系人保护已阻止本批后续写入。')
                return
            try:
                session=self._boundary(task)
                person=await self._resolve_item(task,item)
                type=task['type']
                if type=='invite':
                    async def operation():
                        self._boundary(task)
                        if self.contact_blocked(task['account_id']): raise _BoundaryPause('清空联系人保护已生效。')
                        try: target=await resolve_target(session.client,task['target']['group_id'],session.own_id)
                        except (ValueError,TypeError) as error:
                            raise _BoundaryPause('目标群或账号权限已变化，请重新检查。') from error
                        reservation=self._reservation(task['account_id'],target.group_id,person.id,exclude_task=task['id'])
                        if reservation=='already': return {'kind':'already','detail':'此前任务已核实加入。'}
                        if reservation in {'reserved','unverified'}: return {'kind':'unverified','detail':'已有其他未完成邀请，已阻止重复提交。'}
                        return await invite_contact(session.client,target,person,before_submit=lambda:self._journal(task,item))
                    async with self.contact_lock(task['account_id']):
                        result=await self.app.execute(task['account_id'],operation,priority=20)
                    kind=result['kind']
                    item.update(status=kind,kind=kind,detail=result['detail'])
                    if kind in {'invited','already'}: item['verified_at']=_now()
                elif type=='add_contacts':
                    async with self.contact_lock(task['account_id']):
                        await self.app.contacts.add_person(task['account_id'],person,source='import' if task.get('entries') else 'manual',
                            before_submit=lambda:self._journal(task,item))
                    item.update(status='added',kind='added',detail='Telegram 已确认添加联系人。',verified_at=_now())
                    metadata={k:v for k,v in item['entry'].items() if k in {'tags','note','consent_record'}}
                    if metadata: await self.app.contacts.action('update_contact',dict(metadata,account_id=task['account_id'],user_id=person.id))
                elif type=='delete_contacts':
                    async def operation():
                        if self.contact_blocked(task['account_id']): raise _BoundaryPause('清空联系人保护已生效。')
                        self._journal(task,item)
                        return await session.client(functions.contacts.DeleteContactsRequest([input_user(person)]))
                    async with self.contact_lock(task['account_id']): await self.app.execute(task['account_id'],operation,priority=20)
                    try:
                        after=await read_full_snapshot(session.client,task['account_id'],session.own_id)
                    except errors.RPCError as error:
                        if _fatal(error):
                            await self._pause_with_wait(task,error)
                            return
                        item.update(status='unverified',kind='unverified',detail='删除请求已接受，但后续只读核实失败，不能当作删除失败或重试。')
                        self._put_item(task,item)
                        self._save(self._get(task['id']))
                        continue
                    if person.id in {p.id for p in after.users}: item.update(status='unverified',kind='unverified',detail='请求已提交，联系人仍在最新列表。')
                    else: item.update(status='deleted',kind='deleted',detail='已核实联系人删除。',verified_at=_now())
                else:
                    # ChatService owns its write queue and durable outbox boundary.
                    result=await self.app.chats.send_confirmed(task['account_id'],person.id,task['text'],request_id=item['request_id'],
                        before_submit=lambda:self._journal(task,item),retry_failed=item.get('retry_failed',False),priority=20,automation=True,
                        task_id=task['id'],item_id=item['id'])
                    status=result.get('status')
                    item['outbox_id']=result.get('id')
                    if status=='queued':
                        item.update(status='queued',kind=None,detail='消息尚未提交，请检查后确认继续。')
                        self._put_item(task,item)
                        await self._pause(task,'消息仍在等待发送，未自动续发；可取消或检查后恢复。')
                        return
                    if status in {'sent','completed'}: item.update(status='sent',kind='sent',detail='消息已发送。',verified_at=_now())
                    elif status=='failed': item.update(status='failed',kind='failed',detail='消息明确发送失败。')
                    else: item.update(status='unverified',kind='unverified',detail='消息结果尚未核实。',outbox_id=result.get('id'))
                self._put_item(task,item)
                self._save(self._get(task['id']))
            except errors.RPCError as error:
                if not await self._record_rpc_error(task,item,error): return
            except OperationPaused as error:
                await self._pause(task,str(error))
                return
            except (ValueError,TypeError) as error:
                if item['status']=='inflight':
                    await self._pause(task,self._error(error))
                    return
                item.update(status='failed',kind='failed',detail=str(error))
                self._put_item(task,item)
                self._save(self._get(task['id']))
            except Exception as error:
                await self._pause_with_wait(task,error)
                return
        current=self._get(task['id'])
        if current['status']=='running':
            counts=self._save(current)['counts']
            current.update(status='partial' if any(counts.get(k) for k in ['failed','restricted','unverified']) else 'completed',error='')
            self._save(current)

    async def _pause_with_wait(self,task,error):
        if isinstance(error,errors.FloodWaitError):
            current=self._get(task['id'])
            current['wait_until']=_future(error.seconds)
            self._save(current)
        await self._pause(task,self._error(error))

    async def _check(self,task):
        checks={}
        session=self._identity(task)
        if task.get('wait_until') and not _expired(task['wait_until']): raise ValueError('平台等待时间尚未结束。')
        if hasattr(session.client,'is_user_authorized') and not await session.client.is_user_authorized():
            raise ValueError('账号授权失效，请重新登录。')
        if not self.app.automation_allowed(task['account_id']): raise ValueError('账号或全局暂停仍未解除，请先检查平台等待状态。')
        if task['type']=='invite': await resolve_target(session.client,task['target']['group_id'],session.own_id)
        if task['type']=='clear_contacts':
            if task.get('drain_pending'):
                await self.app.listeners.stop(task['account_id'],reason='检查清空前在途写入',drain=True)
            if task.get('backup'): validate_backup(self.app.root,task['backup'],task['account_id'])
            snapshot=await read_full_snapshot(session.client,task['account_id'],session.own_id)
            checks.update(checked_scope=snapshot.scope_info,checked_fingerprint=snapshot.fingerprint)
        for item in self._items(task):
            if item['status'] in {'unverified','inflight'}: await self._verify(task,item)
            elif item['status']=='queued': await self._resolve_item(task,item)
        task=self._get(task['id'])
        task.update(checked_at=_now(),error='',**checks)
        self._bindings[task['id']]=session.client
        return self._save(task)

    async def _verify(self,task,item):
        session=self._identity(task)
        type=task['type']
        if item['status'] not in {'unverified','inflight'}: return item
        try:
            if type=='invite':
                person=await self._resolve_item(task,item)
                target=await resolve_target(session.client,task['target']['group_id'],session.own_id)
                membership=await self._membership(session.client,target,person)
                if membership=='member': item.update(status='invited',kind='invited',detail='只读查询已核实成员身份。',verified_at=_now())
                else: item.update(status='unverified',kind='unverified',detail='未核实入群，保留未知状态；不会再次邀请。')
            elif type in {'delete_contacts','clear_contacts','add_contacts'}:
                snapshot=await read_full_snapshot(session.client,task['account_id'],session.own_id)
                present=item['user_id'] in {p.id for p in snapshot.users}
                if (type=='add_contacts' and present) or (type!='add_contacts' and not present):
                    kind='added' if type=='add_contacts' else 'deleted'
                    item.update(status=kind,kind=kind,detail='已通过最新联系人快照核实。',verified_at=_now())
            elif type in {'send_message','send_invite_link'}:
                outbox=self.app.store.get('outbox',item.get('outbox_id',''),account_id=task['account_id'])
                if not outbox:
                    outbox=next((r for r in self.app.store.list('outbox',task['account_id'])
                                 if self._request_key(r.get('request_id'))==self._request_key(item.get('request_id'))),None)
                if (not outbox or not self._message_record_matches(task,item,outbox)
                        or (outbox.get('task_id'),outbox.get('item_id'))!=(task['id'],item['id'])
                        or outbox.get('task_ownership_error')): return item
                item['outbox_id']=outbox['id']
                result=await self.app.chats.action('check_message',{'account_id':task['account_id'],'outbox_id':item['outbox_id']})
                if result.get('status')=='sent': item.update(status='sent',kind='sent',detail='已核实发送。',verified_at=_now())
        except (ValueError,TypeError): pass
        self._put_item(task,item)
        self._save(self._get(task['id']))
        return item

    async def _prepare_clear(self,account_id,old_task=None):
        if not account_id: raise ValueError('请选择账号。')
        async with self._prepare_locks.setdefault(account_id,asyncio.Lock()):
            if self.contact_blocked(account_id) and self._protected.get(account_id)!=(old_task or {}).get('id'):
                raise ValueError('本账号已有清空准备，请先取消。')
            task=self._new(account_id,'clear_contacts',{})
            if old_task:
                old_task.update(status='superseded',token='',error='已重新准备，请使用新的完整确认。')
                self._save(old_task)
            # Logical protection precedes draining any listener/contact work.
            self._protected[account_id]=task['id']
            task.update(status='preparing',affected_task_ids=[],drain_pending=True)
            self._save(task)
            affected=[]
            drained=False
            try:
                for other in self.app.store.list('tasks',account_id):
                    if other['type'] in {'invite','add_contacts','delete_contacts'} and other['status'] in {'queued','running','awaiting_confirmation'}:
                        other.update(status='paused',error='清空联系人准备已暂停本任务，需重新检查。')
                        self._save(other)
                        affected.append(other['id'])
                await self.app.listeners.stop(account_id,reason='准备清空全部联系人',drain=True)
                worker=self._workers.get(account_id)
                if worker and not worker.done():
                    await asyncio.wait_for(asyncio.shield(worker),timeout=30)
                drained=True
                self._preparing_boundary(task)
                async with self.contact_lock(account_id):
                    session=self.app.accounts.require(account_id)
                    snapshot=await read_full_snapshot(session.client,account_id,session.own_id)
                    self._preparing_boundary(task)
                    if not snapshot.scope_info.get('complete'):
                        raise ValueError(snapshot.scope_info.get('error') or '不支持完整清空：无法准确确定保存号码的注册状态。')
                    for person in snapshot.users: input_user(person)
                    backup=save_backup(self.app.root,snapshot)
                    validate_backup(self.app.root,backup,account_id)
                    account=self.app.store.get('accounts',account_id,{})
                    task.update(status='awaiting_confirmation',drain_pending=False,account_name=account.get('name'),selection=[p.id for p in snapshot.users],entries=[],text='',
                        consent_confirmed=False,target=None,scope_info=snapshot.scope_info,fingerprint=snapshot.fingerprint,
                        saved_scope=self._saved_scope(snapshot),backup=backup,backup_path=backup['path'],backup_name=backup['name'],
                        backup_hash=backup['sha256'],backup_count=backup['count'],registered_count=len(snapshot.users),
                        unregistered_count=snapshot.scope_info['unregistered_count'],total_delete_count=len(snapshot.users)+snapshot.scope_info['unregistered_count'],
                        affected_task_ids=affected,prechecks={'listener_stopped':True,'inflight_drained':True,'backup_validated':True},
                        backup_warning='备份仅包含可导出的已注册联系人；不包含会话密钥，首版无一键恢复。',reset_status='pending')
                    task['scope_hash']=self._scope_hash(task)
                    for index,person in enumerate(snapshot.users):
                        self._put_item(task,{'id':uuid4().hex,'task_id':task['id'],'account_id':account_id,'index':index,
                            'user_id':person.id,'name':getattr(person,'first_name',None) or str(person.id),'status':'queued','kind':None,
                            'detail':'','submitted_at':None,'verified_at':None,'entry':{'user_id':person.id}})
                    self._bindings[task['id']]=session.client
                    return {'task':self._save(task)}
            except BaseException as error:
                current=self._get(task['id'])
                if current['status']!='preparing':
                    raise
                if not drained:
                    task.update(status='paused',token='',error='在途联系人写入尚未核实结束，保留写入保护；请检查后重新准备或停止。',drain_pending=True,affected_task_ids=affected)
                    self._save(task)
                else:
                    task.update(status='failed',token='',error=self._error(error),drain_pending=False,affected_task_ids=affected)
                    self._save(task)
                    self._protected.pop(account_id,None)
                raise

    def _preparing_boundary(self,task):
        current=self._get(task['id'])
        if current['status']!='preparing' or self._protected.get(task['account_id'])!=task['id']:
            raise ValueError('清空准备已停止，请重新准备。')

    def _saved_scope(self,snapshot):
        return sorted([{'phone':s.phone,'first_name':s.first_name,'last_name':s.last_name,
                        'date':s.date.isoformat() if s.date else None} for s in snapshot.saved],key=lambda s:(s['phone'],s['first_name'],s['last_name']))

    async def _confirm_clear(self,task,data):
        self._validate_confirmation(task,data,clear=True)
        try:
            if self._protected.get(task['account_id'])!=task['id']: raise ValueError('清空保护已释放，请重新准备。')
            async with self.contact_lock(task['account_id']):
                session=self._identity(task,require_binding=True)
                validate_backup(self.app.root,task['backup'],task['account_id'])
                snapshot=await read_full_snapshot(session.client,task['account_id'],session.own_id)
                task=self._get(task['id'])
                self._validate_confirmation(task,data,clear=True)
                if self._protected.get(task['account_id'])!=task['id']:
                    raise ValueError('清空保护已释放，请重新准备。')
                if not snapshot.scope_info['complete'] or snapshot.fingerprint!=task['fingerprint']:
                    raise ValueError('联系人范围已变化或无法核实，本次确认作废，请重新准备并备份。')
                task.update(status='queued',confirmed_at=_now(),token='')
                self._save(task)
                self._kick(task['account_id'])
                return {'task':task}
        except BaseException:
            task=self._get(task['id'])
            if task['status']=='awaiting_confirmation':
                task.update(status='invalidated',token='',error='范围或备份校验失败，请重新准备。')
                self._save(task)
                self._release(task)
            raise

    async def _run_clear(self,task):
        account_id=task['account_id']
        async with self.contact_lock(account_id):
            session=self._identity(task)
            validate_backup(self.app.root,task['backup'],account_id)
            initial=await read_full_snapshot(session.client,account_id,session.own_id)
            if not initial.scope_info.get('complete') or initial.fingerprint!=task['fingerprint']:
                await self._pause(task,'联系人范围在执行前已变化，请重新准备并完整确认。')
                return
            for item in self._items(task):
                if item['status']!='queued': continue
                if self._get(task['id'])['status']!='running' or self._closing: return
                try:
                    person=await self._resolve_item(task,item)
                    async def operation():
                        self._boundary(task)
                        if not any(i.get('submitted_at') for i in self._items(task)):
                            dispatch_scope=await read_full_snapshot(session.client,account_id,session.own_id)
                            self._boundary(task)
                            if not dispatch_scope.scope_info.get('complete') or dispatch_scope.fingerprint!=task['fingerprint']:
                                raise _BoundaryPause('联系人范围在实际删除前已变化，请重新准备并确认。')
                        self._journal(task,item)
                        return await session.client(functions.contacts.DeleteContactsRequest([input_user(person)]))
                    await self.app.execute(account_id,operation,priority=20)
                    # The response proves acceptance, not final absence.
                    item.update(status='unverified',kind='unverified',detail='删除请求已接受，等待最终联系人核实。')
                    self._put_item(task,item)
                    self._save(self._get(task['id']))
                except errors.RPCError as error:
                    if not await self._record_rpc_error(task,item,error): return
                except Exception as error:
                    await self._pause_with_wait(task,error)
                    return
            if self._get(task['id'])['status']!='running': return
            before_reset=await read_full_snapshot(session.client,account_id,session.own_id)
            original=set(task['selection'])
            original_saved={_hash(s) for s in task['saved_scope']}
            if (not {p.id for p in before_reset.users}.issubset(original)
                    or before_reset.scope_info.get('saved_numbers_count') is None
                    or not {_hash(s) for s in self._saved_scope(before_reset)}.issubset(original_saved)):
                await self._pause(task,'执行期间联系人范围变化或无法核实；已阻止清除保存号码，请重新准备。')
                return
            async def reset():
                self._boundary(task)
                dispatch_scope=await read_full_snapshot(session.client,account_id,session.own_id)
                self._boundary(task)
                if (not {p.id for p in dispatch_scope.users}.issubset(original)
                        or dispatch_scope.scope_info.get('saved_numbers_count') is None
                        or not {_hash(s) for s in self._saved_scope(dispatch_scope)}.issubset(original_saved)):
                    raise _BoundaryPause('保存号码范围在实际提交前变化，已阻止清除，请重新准备。')
                latest=self._get(task['id'])
                latest['reset_status']='inflight'
                latest['reset_submitted_at']=_now()
                self._save(latest)
                result=await session.client(functions.contacts.ResetSavedRequest())
                if result is not True: raise RuntimeError('平台未确认清除保存号码。')
                latest=self._get(task['id'])
                latest['reset_status']='submitted'
                self._save(latest)
            try: await self.app.execute(account_id,reset,priority=20)
            except Exception as error:
                current=self._get(task['id'])
                if current.get('reset_status')=='inflight': current['reset_status']='unverified'
                self._save(current)
                await self._pause_with_wait(task,error)
                return
            after=await read_full_snapshot(session.client,account_id,session.own_id)
            remaining={p.id for p in after.users}
            for item in self._items(task):
                if item['user_id'] not in remaining:
                    item.update(status='deleted',kind='deleted',detail='最终快照已核实删除。',verified_at=_now())
                    self._put_item(task,item)
            current=self._get(task['id'])
            current.update(verified=bool(after.scope_info.get('complete') and not remaining and after.scope_info.get('saved_numbers_count')==0),
                           verified_remaining=len(remaining),final_scope_info=after.scope_info,finished_at=_now())
            current['status']='completed' if current['verified'] else 'partial'
            current['error']='' if current['verified'] else '未核实全部清空；请查看剩余、失败及未核实项目。'
            self._save(current)
            self._release(current)
            await self.app.contacts.refresh(account_id)

    async def pause_account(self,account_id,reason=''):
        for task in self.app.store.list('tasks',account_id):
            if task['status'] in {'queued','running','awaiting_confirmation'}:
                task.update(status='paused',error=reason or '账号任务已暂停，请检查后再确认。',control_version=task.get('control_version',0)+1)
                self._save(task)
        worker=self._workers.get(account_id)
        if worker and not worker.done(): await asyncio.shield(worker)

    async def close(self):
        if self._closing: return
        self._closing=True
        for task in self.app.store.list('tasks'):
            if task['status'] in {'queued','running'}:
                task.update(status='paused',error='程序退出，需检查后重新确认。')
                self._save(task)
        workers=[w for w in self._workers.values() if not w.done()]
        if workers: await asyncio.gather(*workers,return_exceptions=True)
        self._protected.clear()

    async def action(self,name,data):
        if name=='prepare_task': return await self._prepare(data)
        if name=='prepare_clear': return await self._prepare_clear(data.get('account_id'))
        task=self._get(data.get('task_id'),data.get('account_id'))
        if name=='task_detail': return {'task':task,'items':self._items(task)}
        if name=='confirm_clear': return await self._confirm_clear(task,data)
        if name=='confirm_task':
            if task['type']=='clear_contacts': raise ValueError('清空联系人必须填写完整二次确认。')
            self._validate_confirmation(task,data)
            if self.contact_blocked(task['account_id']): raise ValueError('本账号清空保护中，请取消清空后重新检查任务。')
            task.update(status='queued',confirmed_at=_now(),token='')
            self._save(task)
            self._kick(task['account_id'])
            return {'task':task}
        if name=='pause_task':
            if task['status'] in {'completed','cancelled','expired','superseded'}: raise ValueError('该任务不能暂停。')
            task.update(status='paused',error='已暂停后续步骤，在途请求仍可能完成。',control_version=task.get('control_version',0)+1)
            return {'task':self._save(task)}
        if name in {'cancel_task','cancel_clear'}:
            if task['type']=='clear_contacts' and task.get('drain_pending'):
                await self.app.listeners.stop(task['account_id'],reason='取消清空前排空在途写入',drain=True)
            if name=='cancel_task' and data.get('confirmed') is not True: raise ValueError('请确认取消未执行项目。')
            task.update(status='cancelled',token='',error='已取消未执行项目；监听保持停止。',control_version=task.get('control_version',0)+1)
            self._save(task)
            worker=self._workers.get(task['account_id'])
            if worker and not worker.done(): await asyncio.shield(worker)
            self._release(task)
            return {'task':self._get(task['id'])}
        if name=='check_task': return {'task':await self._check(task)}
        if name=='verify_task_item':
            item=next((i for i in self._items(task) if i['id']==data.get('item_id')),None)
            if not item: raise ValueError('任务项目不存在。')
            await self._verify(task,item)
            return {'task':self._get(task['id']),'item':item}
        if name in {'resume_task','retry_task'}:
            if data.get('confirmed') is not True: raise ValueError('请确认检查后继续任务。')
            guard=task.get('control_version',0)
            if task['type']=='clear_contacts':
                if name=='retry_task': raise ValueError('清空不能普通重试，必须重新准备并完整确认。')
                await self._check(task)
                task=self._get(task['id'])
                if task.get('control_version',0)!=guard or task['status'] in {'cancelled','superseded','expired','completed'}:
                    raise ValueError('检查期间任务状态已变化，请重新操作。')
                return await self._prepare_clear(task['account_id'],old_task=task)
            if not task.get('confirmed_at'):
                raise ValueError('任务尚未经确认，请取消旧预览并重新准备后确认。')
            if task['status'] in {'running','queued','awaiting_confirmation','completed','expired','cancelled','superseded'}:
                raise ValueError('当前任务状态不能继续或重试。')
            await self._check(task)
            task=self._get(task['id'])
            if task.get('control_version',0)!=guard or task['status'] in {'cancelled','superseded','expired','completed','running','queued'}:
                raise ValueError('检查期间任务状态已变化，请重新操作。')
            if self.contact_blocked(task['account_id']): raise ValueError('清空联系人保护中，不能恢复本任务。')
            if name=='retry_task':
                failed=[i for i in self._items(task) if i['status']=='failed']
                if not failed: raise ValueError('没有确定失败的项目可重试；未知结果只能核实。')
                for item in failed:
                    item.update(status='queued',kind=None,detail='已明确确认重试确定失败项。',retry_failed=True)
                    self._put_item(task,item)
            if not any(i['status']=='queued' for i in self._items(task)): raise ValueError('没有安全的未执行项目。')
            task.update(status='queued',error='',confirmed_at=_now())
            self._save(task)
            self._kick(task['account_id'])
            return {'task':task}
        if name=='export_task':
            out=io.StringIO(newline='')
            writer=csv.writer(out)
            writer.writerow(['account','task','type','target','user_id','name','status','detail','submitted_at','verified_at'])
            for item in self._items(task):
                writer.writerow([_csv_safe(v) for v in [task['account_id'],task['name'],task['type'],(task.get('target') or {}).get('group_id',''),
                    item.get('user_id'),item['name'],item['status'],item['detail'],item.get('submitted_at'),item.get('verified_at')]])
            return {'csv':out.getvalue(),'filename':f'task-{task["id"]}.csv'}
        if name=='open_backup':
            if task['type']!='clear_contacts': raise ValueError('此任务没有联系人备份。')
            if not task.get('backup'): raise ValueError('尚未生成可验证的联系人备份。')
            validate_backup(self.app.root,task['backup'],task['account_id'])
            parent=Path(task['backup']['path']).resolve().parent
            subprocess.Popen(['open',str(parent)],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
            return {'opened':True}
        raise ValueError('不支持的任务操作。')
