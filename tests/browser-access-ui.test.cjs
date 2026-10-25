const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const UI = require('../web/state.js');

const localOrigin = 'http://127.0.0.1:49000';
const credential = 'fixture-local-access';
const accounts = Array.from({length:6}, (_, i) => ({
  id:`fixture-${i}`, name:`模拟账号 ${i + 1}`, telegram_id:100 + i,
  status:'offline', stage:'ready', logged_in:false, connected:false,
  listener:{state:'stopped', config:{group_ids:[], mode:'preview', version:0}}
}));

function harness({savedToken='', hash='', rows=accounts, failure='', clipboardFailure=false, origin=localOrigin}={}) {
  const source=fs.readFileSync(require.resolve('../web/app.js'),'utf8').replace(
    /\ninit\(\);\n\}\)\(\);\s*$/,
    '\nglobalThis.testUi={S,init,perform,setPage,renderShell,fetchJson,applySnapshot};\n})();'
  );
  const nodes=new Map(), requests=[], copied=[], replaced=[], toasts=[];
  const storage=new Map(savedToken ? [['tg-workbench-token', savedToken]] : []);
  const makeNode=()=>({innerHTML:'',textContent:'',disabled:false,isConnected:true,dataset:{},
    classList:{toggle(){},add(){},remove(){}},replaceChildren(){this.innerHTML='';},append(){}});
  const nav=Array.from({length:7},makeNode), controls=Array.from({length:4},makeNode);
  const document={
    querySelector(selector){if(!nodes.has(selector)){const node=makeNode();if(selector==='#toast-region')node.append=value=>toasts.push(value.textContent);nodes.set(selector,node);}return nodes.get(selector);},
    querySelectorAll(selector){if(selector.includes('[data-page]'))return nav;if(selector.includes('.global-actions'))return controls;return[];},
    addEventListener(){},createElement:makeNode
  };
  const context={WorkbenchUI:UI,document,location:{origin,hash,pathname:'/',search:'',reload(){}},
    history:{replaceState(_state,_title,url){replaced.push(url);}},
    sessionStorage:{getItem:key=>storage.get(key)||'',setItem:(key,value)=>storage.set(key,value)},
    navigator:{clipboard:{async writeText(value){if(clipboardFailure)throw Error('fixture permission denied');copied.push(value);}}},
    URLSearchParams,window:{addEventListener(){}},setTimeout(){},clearTimeout(){},console,Blob,AbortController,
    fetch:async(url,options={})=>{
      requests.push({url,options});
      if(url==='/api/stream')throw Error('fixture stream closed');
      assert.equal(url,'/api/state','access flow must not submit an account action');
      if(failure==='network')throw TypeError('fixture network unavailable');
      const ok=options.headers['X-Local-Token']===credential;
      return {ok,status:ok?200:403,json:async()=>ok?{revision:1,accounts:rows,settings:{},tasks:[],events:[]}:{ok:false,error:'请通过本机启动入口重新打开工作台。'}};
    }};
  vm.runInNewContext(source,context);
  return {ui:context.testUi,nodes,requests,copied,replaced,toasts,storage,nav,controls,context};
}

test('新浏览器缺少访问凭证时明确未授权，不显示空账号或发送请求',async()=>{
  const h=harness();await h.ui.init();
  assert.match(h.nodes.get('#account-count').textContent,/尚未授权/);
  assert.doesNotMatch(h.nodes.get('#account-list').innerHTML,/还没有账号|添加第一个账号|全部账号/);
  assert.match(h.nodes.get('#page').innerHTML,/访问链接|已打开的工作台/);
  assert.equal(h.requests.length,0);
  assert.ok(h.nav.every(node=>node.disabled));
  assert.equal(h.nodes.get('#browser-access').disabled,true);
  const before=h.ui.S.page;h.ui.setPage('accounts');assert.equal(h.ui.S.page,before);
  await assert.rejects(h.ui.perform('add-account',{},{}),/授权|读取/);
});

test('错误访问凭证被拒绝时不冒充零账号',async()=>{
  const h=harness({savedToken:'invalid-fixture-token'});await h.ui.init();
  assert.match(h.nodes.get('#account-count').textContent,/尚未授权/);
  assert.match(h.nodes.get('#page').innerHTML,/失效|未获授权/);
  assert.doesNotMatch(h.nodes.get('#account-list').innerHTML,/还没有账号|添加第一个账号/);
  assert.equal(h.requests[0].options.headers['X-Local-Token'],'invalid-fixture-token');
});

test('首次读取失败显示尚未读取，和确实零账号分开',async()=>{
  const h=harness({savedToken:credential,failure:'network'});await h.ui.init();
  assert.match(h.nodes.get('#account-count').textContent,/尚未读取/);
  assert.doesNotMatch(h.nodes.get('#account-list').innerHTML,/还没有账号|添加第一个账号/);
  assert.equal(h.nodes.get('#browser-access').disabled,true);
});

test('首次成功读取的零账号仍允许添加账号',async()=>{
  const h=harness({savedToken:credential,rows:[]});await h.ui.init();
  assert.equal(h.nodes.get('#account-count').textContent,'0 个账号 · 0 个在线');
  assert.match(h.nodes.get('#page').innerHTML,/添加第一个 Telegram 账号/);
  assert.ok(h.nav.every(node=>!node.disabled));
});

test('已授权页面经明确复制动作让独立浏览器读取同一六账号，页面不展示凭证',async()=>{
  const first=harness({savedToken:credential});await first.ui.init();
  assert.equal(first.nodes.get('#browser-access').disabled,false);
  await first.ui.perform('browser-access',{},{});
  assert.match(first.nodes.get('#modal-root').innerHTML,/复制访问链接/);
  assert.doesNotMatch(first.nodes.get('#modal-root').innerHTML,new RegExp(credential));
  assert.equal(first.copied.length,0);
  await first.ui.perform('copy-access-link',{},{});
  assert.equal(first.copied[0],`${localOrigin}/#token=${credential}`);
  const second=harness({hash:new URL(first.copied[0]).hash});await second.ui.init();
  assert.equal(second.nodes.get('#account-count').textContent,'6 个账号 · 0 个在线');
  assert.equal(second.storage.get('tg-workbench-token'),credential);
  assert.deepEqual(second.replaced,['/']);
  assert.equal(first.ui.S.snapshot.accounts.length,second.ui.S.snapshot.accounts.length);
  assert.ok([...first.requests,...second.requests].every(r=>r.url==='/api/state'||r.url==='/api/stream'));
  assert.ok(first.toasts.length>0);assert.ok(first.toasts.every(message=>!message.includes(credential)));
});

test('未授权或已锁定页面不能复制访问链接',async()=>{
  for(const locked of [false,true]){
    const h=harness({savedToken:credential});
    if(locked)h.ui.S.applySnapshot({revision:1,locked:true});
    await assert.rejects(h.ui.perform('copy-access-link',{},{}),/授权|读取|解锁/);
    assert.equal(h.copied.length,0);
  }
});

test('复制失败保持原页，且不把凭证放进错误信息',async()=>{
  const h=harness({savedToken:credential,clipboardFailure:true});await h.ui.init();
  await h.ui.perform('browser-access',{},{});
  const before=h.nodes.get('#modal-root').innerHTML;
  await assert.rejects(h.ui.perform('copy-access-link',{},{}),error=>/复制/.test(error.message)&&!error.message.includes(credential));
  assert.equal(h.nodes.get('#modal-root').innerHTML,before);
  assert.equal(h.copied.length,0);
});

test('访问链接仅为当前本机来源生成并编码凭证',async()=>{
  const h=harness({savedToken:credential});await h.ui.init();
  h.storage.set('tg-workbench-token','fixture/+&?');
  await h.ui.perform('copy-access-link',{},{});
  assert.equal(h.copied[0],`${localOrigin}/#token=fixture%2F%2B%26%3F`);
  h.context.location.origin='https://example.com';
  await assert.rejects(h.ui.perform('copy-access-link',{},{}),/本机/);
  assert.equal(h.copied.length,1);
});

test('先前成功后访问被拒绝会隐藏旧账号并恢复授权门控',async()=>{
  const h=harness({savedToken:credential});await h.ui.init();
  h.context.fetch=async()=>({ok:false,status:403,json:async()=>({ok:false,error:'fixture access rejected'})});
  await assert.rejects(h.ui.fetchJson('/api/state'));
  assert.match(h.nodes.get('#account-count').textContent,/尚未授权/);
  assert.doesNotMatch(h.nodes.get('#account-list').innerHTML,/模拟账号/);
  assert.equal(h.nodes.get('#browser-access').disabled,true);
  await assert.rejects(h.ui.perform('copy-access-link',{},{}),/授权|读取/);
});

test('锁定面板保持锁定含义并关闭浏览器访问入口',async()=>{
  const h=harness({savedToken:credential});await h.ui.init();
  h.ui.S.applySnapshot({revision:2,locked:true});h.ui.renderShell();
  assert.match(h.nodes.get('#account-count').textContent,/已锁定/);
  assert.doesNotMatch(h.nodes.get('#account-list').innerHTML,/还没有账号|模拟账号/);
  assert.equal(h.nodes.get('#browser-access').disabled,true);
});

test('授权失效后迟到的保存成功响应不能重新画出旧账号',async()=>{
  const h=harness({savedToken:credential});await h.ui.init();
  let release;
  const delayed=new Promise(resolve=>release=resolve);
  h.context.fetch=async url=>url==='/api/action'?delayed:{ok:false,status:403,json:async()=>({ok:false,error:'fixture access rejected'})};
  h.nodes.set('#rename-account-name',{value:'模拟新名称'});
  const pending=h.ui.perform('save-account-name',{dataset:{id:'fixture-0'}},{});
  await assert.rejects(h.ui.fetchJson('/api/state'));
  release({ok:true,status:200,json:async()=>({ok:true,data:{},state:{revision:99,accounts,settings:{},events:[],tasks:[]}})});
  await assert.rejects(pending,/授权/);
  assert.match(h.nodes.get('#page').innerHTML,/授权访问本机工作台/);
  assert.doesNotMatch(h.nodes.get('#page').innerHTML,/模拟账号/);
  assert.equal(h.ui.S.snapshot.revision,1);
});

test('授权失效后迟到的状态流不能应用旧快照',async()=>{
  const h=harness({savedToken:credential});await h.ui.init();
  h.context.fetch=async()=>({ok:false,status:403,json:async()=>({ok:false,error:'fixture access rejected'})});
  await assert.rejects(h.ui.fetchJson('/api/state'));
  h.ui.applySnapshot({revision:99,accounts,settings:{},events:[],tasks:[]});
  assert.equal(h.ui.S.snapshot.revision,1);
  assert.match(h.nodes.get('#account-count').textContent,/尚未授权/);
});
