"""Account-scoped contacts and read-only import previews."""
import csv
from datetime import datetime, timezone
import io
import re
from uuid import uuid4

from telethon import errors, functions, types, utils

from cleanup import read_full_snapshot, _phone as _saved_phone


def _now():
    return datetime.now(timezone.utc).isoformat()


def _phone(value):
    value = str(value or '').strip()
    if not value: return ''
    normalized = re.sub(r'[\s()\-]', '', value)
    if not re.fullmatch(r'\+?[1-9][0-9]{6,14}', normalized):
        raise ValueError('手机号应包含国家区号及 7 至 15 位数字。')
    return normalized.lstrip('+')


def _tags(value):
    if isinstance(value, str): value = re.split(r'[,;，；|]', value)
    if not isinstance(value, list): raise ValueError('标签格式无效。')
    result = list(dict.fromkeys(str(t).strip() for t in value if str(t).strip()))
    if len(result)>30 or any(len(t)>80 for t in result): raise ValueError('标签过多或过长。')
    return result


class ContactService:
    def __init__(self, app):
        self.app = app

    def cached(self, account_id):
        return [r for r in self.app.store.list('contacts', account_id)
                if r.get('record_type') != 'scope']

    def scope_info(self, account_id):
        row = self.app.store.get('contacts', '!scope', {}, account_id=account_id)
        defaults={'registered_count':None,'saved_numbers_count':None,'unregistered_count':None,'complete':False,'error':'尚未刷新完整联系人范围。'}
        defaults.update({k:v for k,v in row.items() if k not in ('account_id','record_type')})
        return defaults

    def _row(self, account_id, person, source='original'):
        identifier = getattr(person, 'id', None)
        if type(identifier) is not int or identifier <= 0:
            raise ValueError('联系人标识无效。')
        old = self.app.store.get('contacts', str(identifier), {}, account_id=account_id)
        reason = ''
        if not isinstance(person, types.User): reason = '联系人资料缺失，无法解析。'
        elif person.deleted: reason = '账号已删除。'
        elif person.bot: reason = '机器人不参与批量操作。'
        elif person.is_self or person.id==self.app.accounts.get(account_id).own_id: reason = '不能操作当前账号自己。'
        else:
            try: utils.get_input_peer(person)
            except (ValueError, TypeError): reason = '联系人访问资料不足，无法解析。'
        row = {'id':str(identifier),'account_id':account_id,'user_id':identifier,
               'name':' '.join(filter(None, [getattr(person,'first_name',None),getattr(person,'last_name',None)])) or f'用户 {identifier}',
               'username':getattr(person,'username',None) or '', 'phone':getattr(person,'phone',None),
               'source':old.get('source',source),'tags':old.get('tags',[]),'note':old.get('note',''),
               'consent_record':old.get('consent_record',''),'consent_at':old.get('consent_at'),
               'consent_group_id':old.get('consent_group_id'),'eligible':not reason,
               'exclusion_reason':reason, 'refreshed_at':_now(), 'registration':'registered', 'current':True}
        return row

    async def refresh(self, account_id):
        session = self.app.accounts.require(account_id)
        snapshot = await read_full_snapshot(session.client, account_id, session.own_id)
        seen = set()
        for person in snapshot.users:
            row = self._row(account_id, person)
            seen.add(row['id'])
            self.app.store.put('contacts', row['id'], row, account_id=account_id)
        # GetSaved has no registration bit. Preserve unmatched numbers as unknown.
        registered_phones = {_saved_phone(getattr(p,'phone',None)) for p in snapshot.users if isinstance(p,types.User) and not p.deleted and not p.min}
        for saved in snapshot.saved:
            phone = getattr(saved,'phone','')
            if _saved_phone(phone) is not None and _saved_phone(phone) in registered_phones: continue
            identifier = 'phone:' + phone
            old = self.app.store.get('contacts',identifier,{},account_id=account_id)
            row = {'id':identifier,'account_id':account_id,'name':' '.join(filter(None,[saved.first_name,saved.last_name])) or phone,
                   'user_id':None,'username':'','phone':phone,'source':old.get('source','saved_number'),
                   'tags':old.get('tags',[]),'note':old.get('note',''),'consent_record':old.get('consent_record',''),
                   'consent_at':old.get('consent_at'),'eligible':False,'registration':'unknown',
                   'exclusion_reason':'号码注册状态未知，不能邀请；不支持完整清空。','refreshed_at':_now()}
            seen.add(identifier)
            self.app.store.put('contacts',identifier,row,account_id=account_id)
        for old in self.cached(account_id):
            if old['id'] not in seen:
                old.update(eligible=False,exclusion_reason='已不在最新联系人列表，保留本地记录。', current=False)
                self.app.store.put('contacts',old['id'],old,account_id=account_id)
        scope = dict(snapshot.scope_info, account_id=account_id,record_type='scope',refreshed_at=_now())
        self.app.store.put('contacts','!scope',scope,account_id=account_id)
        self.app.bump()
        return self.cached(account_id)

    async def resolve_person(self, account_id, entry):
        if not isinstance(entry,dict) or 'access_hash' in entry:
            raise ValueError('请使用当前账号的用户标识、用户名或手机号解析联系人。')
        session = self.app.accounts.require(account_id)
        identifier = entry.get('user_id')
        query = None
        if identifier is not None:
            if type(identifier) is not int or identifier<=0: raise ValueError('联系人标识无效。')
            query = identifier
        elif entry.get('username'):
            query = str(entry['username']).strip().lstrip('@')
            if not re.fullmatch(r'[A-Za-z][A-Za-z0-9_]{4,31}',query): raise ValueError('用户名格式无效。')
        elif entry.get('phone'):
            person = await self._resolve_phone(account_id, session, entry['phone'])
        else: raise ValueError('联系人缺少用户标识、用户名或手机号。')
        if query is not None:
            try: person = await session.client.get_entity(query)
            except (TypeError, ValueError) as error: raise ValueError('当前账号无法解析该联系人。') from error
        if (not isinstance(person,types.User) or person.deleted or person.bot or person.is_self
                or type(person.id) is not int or person.id<=0 or person.id==session.own_id or (identifier and person.id != identifier)):
            raise ValueError('联系人资料无效、已删除，或属于机器人/当前账号。')
        try: peer = utils.get_input_peer(person)
        except (ValueError,TypeError) as error: raise ValueError('当前账号没有可用的联系人访问资料。') from error
        if not isinstance(peer,types.InputPeerUser) or peer.user_id != person.id:
            raise ValueError('联系人身份无法核实。')
        return person

    async def _resolve_phone(self, account_id, session, phone):
        if not isinstance(phone, str):
            raise ValueError('手机号格式无效，请填写包含国家区号的号码。')
        normalized = _phone(phone)
        if not normalized:
            raise ValueError('手机号不能为空。')
        client, own_id = session.client, session.own_id
        try:
            response = await client(functions.contacts.ResolvePhoneRequest(phone=normalized))
        except (errors.PhoneNotOccupiedError, errors.PhoneNumberInvalidError, TypeError, ValueError) as error:
            raise ValueError('当前账号无法通过该号码只读解析有效用户。') from error
        current = self.app.accounts.require(account_id)
        if current.client is not client or current.own_id != own_id:
            raise ValueError('账号连接或真实身份已变化，请重新解析联系人。')
        if (type(response) is not types.contacts.ResolvedPeer
                or type(response.peer) is not types.PeerUser
                or type(response.peer.user_id) is not int
                or not 0 < response.peer.user_id <= 2 ** 53 - 1
                or not isinstance(response.users, list)
                or any(type(user) not in (types.User, types.UserEmpty) for user in response.users)):
            raise ValueError('号码解析未返回可核实的用户身份。')
        matching = [user for user in response.users if user.id == response.peer.user_id]
        if len(matching) != 1:
            raise ValueError('号码解析的用户身份缺失、重复或不一致。')
        person = matching[0]
        if (type(person) is not types.User or type(person.id) is not int
                or person.id != response.peer.user_id or person.deleted or person.bot
                or person.is_self or person.id == own_id or person.min
                or type(person.access_hash) is not int or person.access_hash == 0
                or not -(2 ** 63) <= person.access_hash < 2 ** 63):
            raise ValueError('号码解析的用户资料不完整，或属于机器人/当前账号/已删除账号。')
        if person.phone is not None and (
                not isinstance(person.phone, str) or _phone(person.phone) != normalized):
            raise ValueError('号码解析返回的手机号与所查询号码不一致。')
        # Only this request's PeerUser/User mapping supplies the account's hash.
        # Optional phone privacy does not invalidate that mapping; chats are unused.
        return person

    async def add_person(self, account_id, user, source='manual', *, before_submit=None):
        session = self.app.accounts.require(account_id)
        if not isinstance(user,types.User) or user.deleted or user.bot or user.is_self or user.id==session.own_id:
            raise ValueError('联系人资料无效。')
        row = self._row(account_id,user,source)
        if not row['eligible']: raise ValueError(row['exclusion_reason'])
        def merge_metadata():
            latest=self.app.store.get('contacts',row['id'],{},account_id=account_id)
            for key in ['tags','note','consent_record','consent_at','consent_group_id']:
                if key in latest: row[key]=latest[key]
        async def operation():
            merge_metadata()
            if self.app.tasks.contact_blocked(account_id): raise ValueError('本账号正在准备或执行清空，联系人写入已锁定。')
            if self.app.accounts.require(account_id).client is not session.client: raise ValueError('账号连接已变化，请重新检查。')
            if source=='listener':
                if not self.app.listeners.allow_submission(account_id,user.id):
                    raise ValueError('监听配置已变化，取消尚未提交的添加。')
                self.app.listeners.mark_submitted(account_id,user.id)
            if before_submit is not None: before_submit()
            row['eligible']=False
            row['exclusion_reason']='添加请求尚未核实，不能用于批量操作。'
            row['write_status']='inflight'
            row['submitted_at']=_now()
            self.app.store.put('contacts',row['id'],row,account_id=account_id)
            try:
                await session.client(functions.contacts.AddContactRequest(
                    id=user,first_name=user.first_name or '',last_name=user.last_name or '',
                    phone='',add_phone_privacy_exception=False))
            except BaseException:
                merge_metadata()
                row['write_status']='unverified'
                self.app.store.put('contacts',row['id'],row,account_id=account_id)
                raise
            merge_metadata()
            row['write_status']='added'
            row['eligible']=True
            row['exclusion_reason']=''
            self.app.store.put('contacts',row['id'],row,account_id=account_id)
            self.app.bump()
            return row
        return await self.app.execute(account_id,operation,priority=10 if source=='listener' else 20)

    def _preview_csv(self, data):
        content = data.get('csv','')
        if not isinstance(content,str) or len(content.encode('utf-8'))>1024*1024: raise ValueError('CSV 文件为空或超过 1 MiB。')
        aliases = {'账号':'account','账号标识':'account','姓名':'name','名称':'name','手机号':'phone','用户名':'username',
                   '标签':'tags','备注':'note','同意邀请的记录':'consent_record','同意邀请记录':'consent_record','邀请授权记录':'consent_record','授权记录':'consent_record'}
        expected = ['account','name','phone','username','tags','note','consent_record']
        try:
            reader=csv.reader(io.StringIO(content.lstrip('\ufeff')),strict=True)
            headers=[aliases.get(h.strip(),h.strip()) for h in next(reader)]
            if len(headers)!=len(expected) or set(headers)!=set(expected): raise ValueError('CSV 必须包含 account,name,phone,username,tags,note,consent_record 七列。')
            rows, invalid, duplicates, seen=[],[],[],set()
            for line,values in enumerate(reader,2):
                if line>10001: raise ValueError('CSV 最多支持 10000 行。')
                if not any(values): continue
                item={'line':line}
                try:
                    if len(values)!=len(headers): raise ValueError('列数与表头不一致。')
                    item.update({k:v.strip() for k,v in zip(headers,values)})
                    a=item['account']
                    if not a or (data.get('account_id') and a!=data['account_id']): raise ValueError('CSV 账号与当前账号不一致。')
                    if not self.app.store.get('accounts',a): raise ValueError('CSV 账号不存在。')
                    item['phone']=_phone(item['phone'])
                    item['username']=item['username'].lstrip('@')
                    if item['username'] and not re.fullmatch(r'[A-Za-z][A-Za-z0-9_]{4,31}',item['username']): raise ValueError('用户名格式无效。')
                    if not item['phone'] and not item['username']: raise ValueError('手机号或可用用户名至少填写一个。')
                    if any(len(item[k])>4000 for k in ['name','note','consent_record']): raise ValueError('字段内容过长。')
                    item['tags']=_tags(item['tags'])
                    keys={(a,'phone',item['phone'])} if item['phone'] else set()
                    if item['username']: keys.add((a,'username',item['username'].lower()))
                    if keys & seen:
                        duplicates.append(dict(item,reason='CSV 重复联系人。'))
                    else:
                        rows.append(item)
                        seen.update(keys)
                except ValueError as error: invalid.append(dict(item,reason=str(error)))
            return {'rows':rows,'invalid':invalid,'duplicates':duplicates}
        except (csv.Error,StopIteration) as error: raise ValueError('CSV 格式无效。') from error

    async def action(self,name,data):
        account_id=data.get('account_id')
        if name=='preview_import': return self._preview_csv(data)
        if not account_id: raise ValueError('请选择账号。')
        if name=='refresh_contacts':
            rows=await self.refresh(account_id)
            return {'contacts':rows,'refreshed_at':self.scope_info(account_id).get('refreshed_at'),'scope_info':self.scope_info(account_id)}
        if name=='list_contacts':
            rows=self.cached(account_id)
            query=str(data.get('query','')).casefold()
            tag=data.get('tag')
            rows=[r for r in rows if (not query or query in ' '.join(str(r.get(k) or '') for k in ['name','username','phone','note']).casefold()) and (not tag or tag in r['tags'])]
            return {'contacts':rows,'refreshed_at':self.scope_info(account_id).get('refreshed_at'),'scope_info':self.scope_info(account_id)}
        if name=='update_contact':
            identifier=data.get('user_id')
            if type(identifier) is not int or identifier<=0: raise ValueError('联系人标识无效。')
            row=self.app.store.get('contacts',str(identifier),account_id=account_id)
            if not row: raise ValueError('当前账号中不存在该联系人。')
            if 'tags' in data: row['tags']=_tags(data['tags'])
            for key in ['note','consent_record']:
                if key in data:
                    if not isinstance(data[key],str) or len(data[key])>4000: raise ValueError('备注或授权记录过长或格式无效。')
                    row[key]=data[key].strip()
            if 'consent_record' in data:
                row['consent_at']=_now() if row['consent_record'] else None
                group=data.get('consent_group_id')
                if group is not None and (type(group) is not int or group>=0): raise ValueError('授权目标群无效。')
                row['consent_group_id']=group
            self.app.store.put('contacts',str(identifier),row,account_id=account_id)
            self.app.bump()
            return {'contact':row}
        raise ValueError('不支持的联系人操作。')
