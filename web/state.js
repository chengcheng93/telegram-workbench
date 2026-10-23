(function (root, factory) {
  const api = factory();
  if (typeof module === 'object' && module.exports) module.exports = api;
  else root.WorkbenchUI = api;
})(typeof globalThis !== 'undefined' ? globalThis : this, function () {
  'use strict';
  const clone = value => JSON.parse(JSON.stringify(value));
  function escapeHtml(value) { return String(value == null ? '' : value).replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c])); }
  function mediaFilename(disposition) {
    const fallback = 'telegram-attachment';
    if (typeof disposition !== 'string') return fallback;
    const parameters = /(?:^|;)\s*([^=;\s]+)\s*=\s*(?:"((?:\\[\s\S]|[^"\\])*)"|([^;]*))/g;
    let original = '', extended = '', match;
    while ((match = parameters.exec(disposition))) {
      const name = match[1].toLowerCase(), value = match[2] ?? match[3].trim();
      if (name === 'filename') original = value;
      if (name === 'filename*') extended = value;
    }
    const utf8 = /^UTF-8'[^']*'([\s\S]*)$/i.exec(extended);
    let decoded = '';
    if (utf8) {
      try { decoded = decodeURIComponent(utf8[1]); } catch (_) { /* Use the original filename below. */ }
    }
    for (const candidate of [decoded, original]) {
      const safe = candidate.replace(/[\x00-\x1f\x7f/\\]/g, '').trim();
      if (safe && safe !== '.' && safe !== '..') return safe;
    }
    return fallback;
  }
  // Presentation only: public logged_in intentionally means authorized AND connected.
  function accountPresentation(account = {}) {
    const saved = Boolean(account.telegram_id) || account.stage === 'ready';
    const raw = account.status || '';
    let status;
    if (raw === 'needs_login') status = saved ? 'needs_login' : 'pending';
    else if (raw === 'connecting' || raw === 'reconnecting') status = raw;
    else if (raw === 'offline' && saved) status = 'offline';
    else if (raw === 'waiting' || account.wait?.blocked && (account.wait.category === 'platform' || !['network','authorization'].includes(account.wait.category) && (account.wait.until || account.wait.wait_until || account.wait_until))) status = 'waiting';
    else if (account.logged_in === true && account.connected === true) status = 'online';
    else if (raw === 'authorizing' || ['code', 'password'].includes(account.stage)) status = 'authorizing';
    else status = saved ? 'offline' : 'pending';
    const labels = {online:'在线',offline:'离线',connecting:'连接中',reconnecting:'重连中',waiting:'等待平台允许',needs_login:'需要重新登录',pending:'待登录',authorizing:'验证中'};
    let loginStage = ['phone','code','password'].includes(account.stage) ? account.stage : 'phone';
    if (status === 'online') loginStage = 'ready';
    else if (['offline','connecting','reconnecting'].includes(status) || status === 'waiting' && saved) loginStage = 'connection';
    else if (status === 'needs_login') loginStage = 'phone';
    const actionLabel = {online:'重新连接',offline:'重新连接',connecting:'连接中',reconnecting:'重连中',waiting:'查看连接',needs_login:'重新登录',pending:'登录',authorizing:'继续验证'}[status];
    return {status,label:labels[status],kind:status==='online'?'good':['connecting','reconnecting'].includes(status)?'blue':status==='offline'?'neutral':'warning',actionLabel,actionDisabled:['connecting','reconnecting'].includes(status),loginStage,needsLogin:['pending','needs_login','authorizing'].includes(status)};
  }
  function createUiState() {
    return {
      snapshot:{revision:-1,accounts:[],settings:{},tasks:[],events:[]}, accountId:'', page:'listener', chat:null, drafts:{}, configs:{}, epoch:0, requests:{}, sendAttempts:{}, invite:{people:[],target:'',consent:false,step:1,task:null},
      account(id=this.accountId) { return this.snapshot.accounts.find(a=>a.id===id); },
      applySnapshot(snapshot) { if (!snapshot || Number(snapshot.revision ?? 0)<Number(this.snapshot.revision ?? -1)) return false;if(Boolean(snapshot.locked)!==Boolean(this.snapshot.locked))this.epoch++;this.snapshot=snapshot.locked?{revision:snapshot.revision,version:snapshot.version,locked:true,accounts:[],settings:{},tasks:[],events:[],outbox:[]}:{...this.snapshot,...snapshot};if(!snapshot.locked)for(const account of this.snapshot.accounts||[]){if(this.configs[account.id]&&!this.configs[account.id].dirty&&account.listener?.config)this.configs[account.id]={...clone(account.listener.config),dirty:false};}return true; },
      selectAccount(id) { if(id && !this.account(id)) throw new Error('账号身份已失效，请重新选择'); if(this.accountId===id)return; this.accountId=id; this.chat=null; this.epoch++; this.invite={people:[],target:'',consent:false,step:1,task:null}; },
      openChat(accountId,chatId) { if(!this.account(accountId))throw new Error('无法确认会话账号身份'); if(this.accountId && this.accountId!==accountId)throw new Error('会话与所选账号身份不一致'); this.chat={account_id:accountId,chat_id:chatId}; this.epoch++; },
      draftKey() { if(!this.chat)throw new Error('请先打开会话'); return `${this.chat.account_id}:${this.chat.chat_id}`; },
      hasDraft() { return Object.prototype.hasOwnProperty.call(this.drafts,this.draftKey()); },
      hydrateDraft(accountId,chatId,draft) { const key=`${accountId}:${chatId}`;if(!Object.prototype.hasOwnProperty.call(this.drafts,key))this.drafts[key]={text:'',reply_to:null,attachments:[],...clone(draft)}; },
      getDraft() { return clone(this.drafts[this.draftKey()] || {text:'',reply_to:null,attachments:[]}); },
      setDraft(patch) { const key=this.draftKey(); this.drafts[key]={...this.getDraft(),...patch}; },
      prepareSend(requestId) { const draft=this.getDraft(),key=this.draftKey(),payload={account_id:this.chat.account_id,chat_id:this.chat.chat_id,text:draft.text,reply_to:draft.reply_to??null,attachment_ids:(draft.attachments||[]).map(a=>a.id)};const fingerprint=JSON.stringify(payload),old=this.sendAttempts[key];const attempt=old?.fingerprint===fingerprint?old:{fingerprint,request_id:requestId};this.sendAttempts[key]=attempt;return {...payload,request_id:attempt.request_id}; },
      completeSend(payload) { const key=`${payload.account_id}:${payload.chat_id}`,draft=this.drafts[key];if(this.sendAttempts[key]?.request_id===payload.request_id)delete this.sendAttempts[key];if(!draft)return false;const matches=draft.text===payload.text&&(draft.reply_to??null)===(payload.reply_to??null)&&JSON.stringify((draft.attachments||[]).map(a=>a.id))===JSON.stringify(payload.attachment_ids||[]);if(matches)this.drafts[key]={text:'',reply_to:null,attachments:[]};return matches; },
      config() { if(!this.accountId)throw new Error('请先选择一个账号'); if(!this.configs[this.accountId]){const saved=this.account()?.listener?.config || {};this.configs[this.accountId]={group_ids:[],mode:'preview',version:0,...clone(saved),dirty:false};}return this.configs[this.accountId]; },
      editConfig(patch) { Object.assign(this.config(),patch,{dirty:true}); },
      savedConfig(config) { this.configs[this.accountId]={...clone(config),dirty:false}; },
      begin(channel) { const seq=(this.requests[channel]||0)+1;this.requests[channel]=seq;return {channel,seq,epoch:this.epoch,accountId:this.accountId,chat:this.chat ? `${this.chat.account_id}:${this.chat.chat_id}` : ''}; },
      isCurrent(ticket) { return ticket.epoch===this.epoch && ticket.accountId===this.accountId && ticket.seq===this.requests[ticket.channel] && ticket.chat===(this.chat ? `${this.chat.account_id}:${this.chat.chat_id}` : ''); },
      action(name, data={}) { if(this.snapshot.locked){if(name!=='unlock_panel')throw new Error('面板已锁定，请先解锁');return {action:name,account_id:'',...data};}const chatActions=new Set(['send_message','save_draft','get_draft','ai_reply','takeover_chat','edit_message','delete_message','messages','check_message','retry_message','cancel_message',...(name==='adopt_ai_draft'&&data.chat_id!=null?['adopt_ai_draft']:[])]);const aggregate=new Set(['list_chats','add_account','save_settings','pause_all','resume_all','shutdown_preview','shutdown','lock_panel','unlock_panel','clear_cache','backup_data','check_legacy','import_legacy','ai_task']);let id=this.accountId;if(chatActions.has(name)){if(!this.chat)throw new Error('请先打开会话，确认发送身份');id=this.chat.account_id;}if(!id&&!aggregate.has(name))throw new Error('请先选择一个明确账号');if(data.account_id!=null&&data.account_id!==id)throw new Error('操作账号身份不一致');if(id&&!this.account(id))throw new Error('账号身份已失效');return {action:name,account_id:id,...(chatActions.has(name)?{chat_id:this.chat.chat_id}:{}),...data}; },
      manualTaskPayload(form={}) {
        if(this.snapshot.locked)throw new Error('面板已锁定，请先解锁');
        if(!form||typeof form!=='object'||Array.isArray(form))throw new Error('任务参数无效');
        const {account_id,type}=form;
        if(typeof account_id!=='string'||!account_id||!this.account(account_id))throw new Error('请选择一个明确存在的账号');
        if(!['add_contacts','delete_contacts','invite','send_message','send_invite_link'].includes(type))throw new Error('请选择有效的任务类型');
        const ids=form.user_ids===undefined?[]:form.user_ids,rawEntries=form.entries===undefined?[]:form.entries;
        if(!Array.isArray(ids)||Array.from(ids).some(id=>!Number.isSafeInteger(id)||id<=0))throw new Error('用户ID必须全部为正安全整数');
        if(!Array.isArray(rawEntries))throw new Error('联系人条目必须为明确名单');
        if(type!=='add_contacts'&&rawEntries.length)throw new Error('此任务只接受明确选择的联系人ID，不接受导入条目');
        const entries=Array.from(rawEntries,entry=>{
          if(!entry||typeof entry!=='object'||Array.isArray(entry))throw new Error('联系人条目必须包含一种明确身份');
          const keys=Object.keys(entry);
          if(keys.length!==1||!['user_id','username','phone'].includes(keys[0]))throw new Error('联系人条目必须仅包含一种明确身份');
          const key=keys[0],value=entry[key];
          if(key==='user_id'){
            if(!Number.isSafeInteger(value)||value<=0)throw new Error('联系人用户ID必须为正安全整数');
            return {user_id:value};
          }
          if(typeof value!=='string')throw new Error('联系人条目身份格式无效');
          const trimmed=value.trim();
          if(key==='username'&&!/^@?[A-Za-z][A-Za-z0-9_]{4,31}$/.test(trimmed))throw new Error('联系人用户名格式无效');
          if(key==='phone'&&!/^\+?[1-9][0-9]{6,14}$/.test(trimmed.replace(/[\s()\-]/g,'')))throw new Error('手机号应包含国家区号及7至15位数字');
          return {[key]:trimmed};
        });
        const user_ids=[...new Set(ids)];
        if(!user_ids.length&&!entries.length)throw new Error('请明确选择用户或填写联系人名单');
        const payload={account_id,type,user_ids,entries};
        if(form.name!=null){if(typeof form.name!=='string')throw new Error('任务名称格式无效');if(form.name.trim())payload.name=form.name.trim();}
        if(type==='invite'){
          if(!Number.isSafeInteger(form.group_id)||form.group_id>=0)throw new Error('目标群ID必须为负安全整数');
          payload.group_id=form.group_id;
        }
        if(['invite','send_message','send_invite_link'].includes(type)){
          if(form.consent_confirmed!==true)throw new Error('请明确确认已取得本次操作的同意');
          payload.consent_confirmed=true;
        }
        if(type==='send_message'||type==='send_invite_link'){
          if(typeof form.text!=='string'||!form.text.trim()||form.text.length>4096)throw new Error('消息正文不能为空且不能超过4096字符');
          if(type==='send_invite_link'&&!['https://t.me/','http://t.me/','tg://join'].some(link=>form.text.includes(link)))throw new Error('正文必须包含明确的Telegram邀请链接');
          payload.text=form.text;
        }
        return payload;
      },
      setInvitePeople(people) { this.invite.people=[...new Set(people)];this.invite.consent=false;this.invite.task=null; },
      setInviteTarget(target) { this.invite.target=target;this.invite.consent=false;this.invite.task=null; },
      setInviteConsent(value) { this.invite.consent=Boolean(value)&&Boolean(this.accountId)&&Boolean(this.invite.target)&&this.invite.people.length>0; },
      inviteReady() { return Boolean(this.accountId&&this.invite.target&&this.invite.people.length&&this.invite.consent); },
      canConfirmClear(task, form, eventType) { return Boolean(eventType==='click'&&task&&task.account_id===this.accountId&&this.account()?.name===form.name&&form.backup&&form.scope&&(task.confirmation_token||task.token)&&task.expires_at&&Date.parse(task.expires_at)>Date.now()); }
    };
  }
  return {createUiState,escapeHtml,mediaFilename,accountPresentation};
});
