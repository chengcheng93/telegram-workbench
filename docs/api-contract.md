# TG Workbench internal contract v1

This contract fixes shared names so independent modules can be built safely. User V0.2 is authoritative. Python modules live directly in outputs/telegram-workbench. Use standard library + Telethon from ../../work/telegram-greeter-runtime/venv/bin/python. Tests use unittest and fake clients, never real accounts. No module import connects Telegram or reads legacy runtime. All dates are UTC ISO strings (browser displays local time); Telegram IDs are Python integers / JS safe integers; internal IDs are opaque uuid hex strings.

## Application

`Workbench(root: Path, client_factory=None)` (app.py) owns `.root`, `.store`, `.accounts`, `.listeners`, `.chats`, `.contacts`, `.tasks`, `.ai`, `.global_paused: bool`.

- `async action(name: str, data: dict) -> dict`: every service action returns JSON data. HTTP wraps `{ok:true,data:result,state:app.snapshot()}`; errors `{ok:false,error:public_error,state:...}`. Fields are passed at top level with `action`, `account_id`.
- `snapshot() -> dict`: `{version:'3.0.0', revision, accounts:[...], settings:{...public...}, global_paused, tasks:[...], events:[...]}`. No secrets. Account rows include listener via app snapshot enrichment.
- `bump()` updates revision. `log(account_id,action,detail,**fields)` appends structured event without raw message text or credentials, persists, then bumps.
- `async execute(account_id, operation, *, priority=10, automation=True)` runs callable->awaitable through the shared per-account priority queue. Manual chat=0, listener=10, normal task=20. No preemption of submitted requests. Checks account wait and global pause before dispatch; queued writes cannot bypass a platform wait. Contact locks remain separate; never hold a global mutex across network calls. Network mutation journaling belongs to the operation callback immediately before RPC, not after it. ContactService.add_person and ChatService.send_confirmed schedule their own RPCs; task invite/delete schedule via app.execute; avoid nested execute for same account.
- `automation_allowed(account_id) -> bool` and `wait_state(account_id) -> dict` inspect persistent account wait. Flood seconds have restore timestamp; restriction without known duration remains paused for explicit human check. Per-account write queue holds pending work without blocking other accounts or read-only requests.
- `async on_account_ready(account_id)` attaches listeners event handler, initializes chat events and syncs recent dialogs; no writes to contacts.
- `async on_account_offline(account_id)` gates listener receiving, pauses tasks. Does not affect other accounts.
- `async pause_account(account_id)` stops automation at safe boundary. `async close()` drains and preserves tasks, disconnects all.

## Store (storage.py, owned by accounts worker)

`Store(root:Path)` creates private root and root/workbench.sqlite3. SQLite use in one event loop thread (`check_same_thread=False` allowed for snapshots but writes stay on loop).

- `get(kind:str, key:str, default=None, *, account_id:str='') -> Any`
- `put(kind:str, key:str, value:dict|list|str|int|bool|None, *, account_id:str='') -> None` atomic transaction commit.
- `delete(kind,key,*,account_id='')`
- `list(kind:str, account_id:str|None=None) -> list[Any]` None=all scopes, ''=app only; returns values sorted by key.
- `setting(key,default=None)` / `set_setting(key,value)` wrap app-scoped kind='settings'.
- `event(account_id,action,detail,**fields) -> dict` saves kind='events', generated ID, time, account_id; `events(account_id=None,limit=100)` latest first.
- `close()`; `new_id() -> str` module helper; `now() -> str` UTC helper.

Persistent kind names owned separately: `accounts`, `settings`, `listener_config`, `listener_run`, `listener_processed`, `listener_messages`, `listener_totals`, `dialogs`, `messages`, `drafts`, `outbox`, `contacts`, `tasks`, `task_items`, `ai_drafts`. Every value includes account_id where scoped. Settings secrets NEVER in snapshot.

## AccountManager (accounts.py)

`AccountManager(app, client_factory=None)`. factory signature `(session_path:Path, api_id:int, api_hash:str)` returning Telegram-like client. Production uses sequential_updates=True, catch_up=False, flood_sleep_threshold=0, request_retries=0. Unique per-account flock. Root app does not use old Config.

- `.sessions: dict[str, AccountSession]`; `get(account_id)` gives runtime including `.id`, `.client`, `.own_id` (int|None), `.logged_in`, `.stage`, `.status`, `.wait_until`, `.epoch` (aware datetime raised on connect/reconnect). `require(account_id)` returns it only connected + authorized or raises user-facing ValueError.
- `public() -> list[dict]`: account rows `{id,name,telegram_id,username,phone_masked,status,stage,connected,logged_in,last_sync,wait_until,login_channel,error}`; no phone_hash/code/password/secrets/session data.
- `async action(name,data)` actions:
  `add_account{name?}` => `{account_id}` (max8), `rename_account{account_id,name}`, `connect_account{account_id}`, `send_code{account_id,phone}`, `sign_in{account_id,code}`, `password{account_id,password}`, `cancel_login{account_id}`, `disconnect_account{account_id}`, `logout_account{account_id,confirmed:true}`, `remove_account{account_id,confirmed:true}`.
- `async restore()` reuses valid sessions and calls app hooks; listeners remain stopped. `.close()` disconnects all. Real identity mismatch or duplicate account identity rejects binding and clears untrusted connection; don't overwrite existing profile.
- App-level API creds via `store.setting('telegram_api')` => `{api_id,api_hash}`. Local account profiles kind='accounts', key=internal ID, scope='' include private `session_path`? Prefer deterministic root/accounts/id/account.session. Telegram identity is persisted after validation. Removing profile must not delete task evidence; soft delete account or keep tombstone.

## ListenerService (listeners.py)

`ListenerService(app)`; `async ready(account_id)` attach NEW message handler on shared client; `async offline(account_id)` closes gate. Never attach history/edit to listener.
- `snapshot(account_id) -> {state,mode,config:{group_ids,mode,version},run:{id,version,started_at,mode,counts,...}|null,totals,groups:[{id,title,available,member_count?,can_invite?}],groups_refreshed_at,wait_until,error}`. state defaults stopped; mode preview/live; counts `{added,skipped,preview,failed,pending}`. Persist dedup across restarts. Unavailable selected groups remain visible.
- `async action(name,data)`: `refresh_groups`, `save_listener{group_ids,mode,expected_version,apply_confirmed?}`, `start_listener{mode}`, `stop_listener`, `pause_listener`, `resume_listener`, `reset_processed{user_ids?,all?,confirmed:true}`. Running save uses confirmed=True apply boundary, cancels not-yet-started old pending, increments version; expected_version CAS. Start uses saved config only, no auto on restart. Pause/resume new epoch.
- `async stop(account_id, reason='', drain=True)` safe boundary; called by tasks before cleanup. `async close()` all.
- `async handle_event(account_id,event,*,source='live')` public for tests: checks source, type, timestamp strict epoch, selected group and IDs; same message is deduped, user pending unique per account. Keep contact work queue bounded. Add via `await app.contacts.add_person(account_id, user, source='listener')` inside `app.tasks.contact_lock(account_id)`. Service may access `app.tasks.contact_blocked(account_id)` to gate. Tasks use lock object too; do not hold on chat.
- Before submitting listener add, persist listener_processed record status='inflight'; after known success mark added, after ambiguous error/crash mark unverified (never success). On restart/refresh, read current contacts to reconcile unverified once; absent/unknown stays unverified and does not blindly retry. Advanced reset_processed explicitly allows selected unresolved or processed IDs; account-scoped all reset needs confirmation and displayed impact. Store failed/preview separately from successful dedup.

## ChatService (chats.py)

`ChatService(app)`; `async ready(account_id)` attach chat new/edit/delete handlers; safe to call repeatedly; `async offline(account_id)` optional remove; `async close()`.
- `async action(name,data)`:
  `list_chats{account_id? (empty=all),query?,filter? all/unread/private/group}` => `{dialogs:[{account_id,id,title,last_text,date,unread,is_group,pinned,muted,...}]}` (local; explicit refresh separate).
  `refresh_chats{account_id}` => dialogs; `messages{account_id,chat_id,before_id?}` => `{messages:[{id,account_id,chat_id,text,date,out,reply_to,media?,status,...}],has_more}`; older history never enters listeners.
  `save_draft{account_id,chat_id,text,reply_to?}` => draft; `get_draft` => `{text,reply_to}`.
  `send_message{account_id,chat_id,text,reply_to?,request_id,attachment_ids?}` => outbox entry. request_id UUID idempotency; persist inflight before write, unknown outcomes no blind retry. `check_message{account_id,outbox_id}`, `retry_message{...}` only definite failed; offline queue entry `queued` and cancellation before resume.
  `cancel_message{account_id,outbox_id}`, `edit_message{chat_id,message_id,text}`, `delete_message{chat_id,message_id,confirmed:true}`, `takeover_chat{account_id,chat_id,enabled:true|false}`.
- `async send_confirmed(account_id, chat_id, text, *, request_id, reply_to=None, attachment_ids=None) -> dict` used by explicitly confirmed message tasks; no implicit marketing/recipient creation.
- `async upload(account_id, filename:str, content:bytes) -> dict` saves private media metadata `{id,name,size,mime,...}` <=25MiB first version, progress visible; upload to Telegram happens on send with progress callback; cancel inflight upload via cancel_message.
- `async media(account_id,chat_id,message_id) -> {path:Path,name,mime}` downloads only current account message if accessible, private file cache; no arbitrary browser paths. Store metadata media IDs scoped account. No attachment secrets in state.

## ContactService and TaskService (contacts.py, tasks.py)

`ContactService(app)`:
- `async refresh(account_id) -> list[dict]` complete GetContacts(hash=0) current account, preserve metadata/source; no successful contact hidden. `.cached(account_id)` list.
- Contact rows carry `{id,user_id?,name,username,phone?,source,tags,note,consent_record?,consent_at?,eligible,exclusion_reason?}`. Keep deleted/unresolvable contacts as visible ineligible rows. Explicit refresh may attempt getSaved/takeout; saved-number rows remain visible but ineligible. `.scope_info(account_id)` exposes `{registered_count,saved_numbers_count?,unregistered_count?,complete,error?}`; unknown counts must stay null. GetSaved returns saved phone contacts without registration status: do NOT infer unregistered count from saved_count or subtraction. If phones cannot be classified against verifiable registered references, label registration unknown and refuse full-clear preparation. Empty saved list or fully matched registered phones can establish zero unregistered. Preserve capability/error info instead of silently dropping rows.
- `async add_person(account_id,user,source='manual')` single AddContact(phone='',add_phone_privacy_exception=False), source metadata; caller holds contact lock.
- `async action(name,data)`: `refresh_contacts` => `{contacts,refreshed_at}`, `list_contacts{query?,tag?}` => same cached; `update_contact{user_id,tags?,note?,consent_record?,consent_group_id?}`; `preview_import{csv}` => `{rows,invalid,duplicates}` strict header account,name,phone,username,tags,note,consent_record (accept Chinese header aliases); parse only, don't write Telegram.
- `async resolve_person(account_id,entry)` current account entity from user_id/username/phone; no cross-account access hashes. Manual add also uses task preview.

`TaskService(app)`:
- `.contact_lock(account_id) -> asyncio.Lock` per account; `.contact_blocked(account_id) -> bool` cleanup/prepared-exclusive.
- `.snapshot(account_id=None) -> list[dict]` tasks incl totals/counts/remaining/status; no secrets. `async pause_account(account_id,reason='')`, `async close()`; recovery never auto replays writes.
- `async action(name,data)`:
  `prepare_task{account_id,type,user_ids?,entries?,group_id?,text?,consent_confirmed?}` => `{task:...}`. types `add_contacts`, `delete_contacts`, `invite`, `send_message`, `send_invite_link`. Only explicitly selected existing contacts for batch messages; invitation links explicit text field, no auto fallback. Previews no mutations; for invite read membership/existing completed/unknown tasks, resolve exact target and permission. Output token, expires_at, account+list+target+body+exclusions+prechecks.
  `confirm_task{task_id,confirmation_token}` => task, validates same client/account/TTL scope and persists confirm time. Per account worker queue, RPC inflight persisted before call. Single person error continue; fatal pause. No auto account switch.
  `pause_task{task_id}`, `check_task{task_id}` read auth/network/target+unknown; `resume_task{task_id,confirmed:true}` only safe pending, elapsed waits; `cancel_task{task_id,confirmed:true}`, `retry_task{task_id,confirmed:true}` definite failed only; `verify_task_item{task_id,item_id}` read-only, don't re-invite unknown. `task_detail{task_id}` => `{task,items}`; `export_task{task_id}` => `{csv,filename}` escaping spreadsheet formula prefixes.
  `prepare_clear{account_id}` stops/drains listener, pauses invitations and acquires exclusive contact write protection across preview; reads full registered + unregistered getSaved via takeout (if not available, fail with explicit unsupported full-clear). Atomic backup registered only, checksum and reread validated. Preview task type `clear_contacts` includes counts, snapshot fingerprint, backup_path/name/hash, token TTL5min. No deletion.
  `confirm_clear{task_id,confirmation_token,account_name,understood_backup:true,confirmed_scope:true}` exact name+checks; reread/fingerprint check + backup checksum; mutate only original IDs, per-person results; ResetSaved only if still matching scope. Stop supported, final fresh snapshot verify; can't claim full if unavailable. `cancel_clear{task_id}` stops and releases lock; no listener restart. `open_backup{task_id}` open only task's validated backup parent via macOS open, no browser arbitrary path.
- Stable task value `{id,account_id,type,name,status,created_at,confirmed_at,target?,text?,items:[...] or items separate,counts:{total,processed,remaining,invited,already,restricted,unverified,failed,...},error,wait_until,token?}`. Item `{id,user_id?,name,status,kind?,detail,submitted_at,verified_at}`. Runtime statuses queued/inflight; final invite kinds invited/already/restricted/unverified/failed; counts conservation mandatory. Initial tasks persisted awaiting_confirmation; unfinished on restart paused, inflight unverified; clear never resumes automatically.
- Clear task paused/interrupted recovery cannot use generic resume to continue deleting. `check_task` re-reads remaining scope and verifies backup; `resume_task` for clear must create a fresh read-only preparation + new backup/token and return awaiting_confirmation, followed by the full confirm_clear form. A changed external scope always invalidates old confirmation; don't silently enlarge original task scope. Clear global pause releases at a safe boundary but retains exclusive protection until cancelled/reprepared.
- Invitation reservations persist by account+group+user before confirm/queue; two simultaneous previews cannot both submit the same invitation. Resolve membership again immediately before request. Reservations associated with unverified outcomes block new invites until read-only verification; known final success/already are excluded across tasks. Task message items save one immutable request_id, reused by checks/retries.
- Cleanup protection starts BEFORE draining pending contact writes/invitations; expose affected task IDs in preview. Preparation failure, expiry, cancellation, account removal, and process restart release or rebuild protection deterministically (no stale permanent locks); listener stays stopped. Expired token can never confirm. Suspended clear cannot pass normal retry_task; recovery must reprepare and reconfirm as above.

## AIService (ai.py)

Cross-cutting message idempotency: request_id is scoped to account and bound to immutable hash of chat_id/text/reply_to/attachment_ids. Reusing the same key with different parameters is rejected. Task items persist a fixed key and reuse it on every safe check/retry, not a newly generated key.

`AIService(app)`; `async action(name,data)` actions `ai_reply{account_id,chat_id,context_ids?,instruction?,data_consent:true}`, `ai_task{account_id,prompt,data_consent:true}`, `adopt_ai_draft{account_id,draft_id,chat_id?}`. Returns draft, no execution. OpenAI-compatible configured endpoint/model/api_key in private settings (`ai`); `enabled:false` default. Backend whitelists context fields text,role,time only and max20 current-chat messages; limits output and response schema. AI service/context disclosure via public settings; taking over prevents automatic mode (not implemented phase2), adoption only puts text into manual draft. AI task draft valid schema contains predefined action fields and asks missing account/target, not arbitrary tool/terminal/HTTP commands.

## Browser/HTTP

`GET /api/state` => snapshot. `POST /api/action` => wrapper above. Account_id explicit except account creation/settings/read aggregate. `GET /api/stream` authenticated fetch readable stream NDJSON state events (same wrapper snapshot each revision/heartbeat) updates UI without changing drafts. `POST /api/upload?account_id=...&filename=...` binary -> `{ok,data:{attachment}}`; optional upload JS progress via XHR. `GET /api/media?account_id=...&chat_id=...&message_id=...` bearer header fetch blob, no public arbitrary paths. ≤25MiB. GETs loopback host + matching Origin when present; all /api token X-Local-Token constant time; no access token in returned snapshots. Session token hash fragment removed after load into sessionStorage. Local static files allowlist.

App actions `save_settings{telegram_api?,ai?,enter_to_send?,notifications?,log_retention_days?,panel_pin?}` (no secrets echo); `pause_all`, `resume_all` (resumes only paused eligible listeners, no automatic deletes/invites), `shutdown_preview` => affected list/token, `shutdown{confirmation_token}`; `lock_panel`, `unlock_panel{pin}`, `clear_cache{confirmed:true}`, `backup_data{confirmed:true}` private snapshot path warning credentials; `check_legacy{source_path}` / `import_legacy{confirmation_token,name}` explicit import. Root author owns app/server/migration/launch/docs. UI uses response data, stream only state, never assumes switching accounts changes client.

## Testing and reports

## Integration addenda (final reviewed interfaces)

- Store also provides `backup_to(path:Path)` for a consistent private SQLite backup, and `clear_kind(kind,account_id=None)->int`. Message cache cleanup preserves attachment metadata, drafts, pending outbox, task evidence and deletion tombstones.
- App provides `mark_synced(account_id)` (actual local sync time); snapshot account rows include `unread`. `panel_epoch` advances at both lock and unlock, and sensitive HTTP results are checked again after awaits against that epoch. An old result is withheld even if the panel was locked and then unlocked during the request.
- App wait state merges persistent operation waits and `accounts.profile.wait_restricted`. `acknowledge_wait{account_id,confirmed:true}` cannot shorten a known platform deadline; unknown restrictions require human acknowledgement, do not claim platform clearance, and do not auto-resume work.
- AccountManager `clear_wait(account_id,confirmed=True)` implements the same wait deadline restriction. Completed/ready login `cancel_login` is a no-op that preserves the authorized session; only unfinished challenges are cancelled and erased.
- Listener submission handoff: `allow_submission(account_id,user_id)->bool` and `mark_submitted(account_id,user_id)` are called by ContactService inside the shared queue immediately before a listener RPC. Queued obsolete work is cancellable; a truly submitted write retains its lock until its boundary.
- Listener `inspect_group{group_id}` returns `{group,groups,groups_refreshed_at}`; cached rows carry group_type, can_invite (true/false/null), permission_reason and permission_checked_at. Denied permission reads mean unknown, not confirmed lack of invite rights. Refresh uses available entity data without adding one permission RPC per group.
- ContactService phone-only resolution uses read-only ResolvePhoneRequest and verifies its unique PeerUser/User mapping and current account reference. An add task stores the preview user_id and rejects any later username/phone mapping to a different person.
- ContactService.add_person has optional internal `before_submit` callback. ChatService.send_confirmed adds internal `before_submit`, `retry_failed=False`, `priority=0`, `automation=False`, `task_id=None`, `item_id=None`. These cannot be supplied through ordinary browser chat JSON.
- Task outbox ownership is immutable; task messages always use priority20/automationTrue and their original task item boundary. Ordinary chat send/retry/cancel cannot operate a task-owned outbox. UI redirects those entries to their owning task. Old task outbox associations are reconciled before manual replay can be attempted.
- Submitted 5xx or unclassified RPC errors are unverified, not retryable definite failures. Only explicit recipient/4xx rejection can be failed/restricted. The same rule applies to add/delete/invite/clear; read-only evidence may later resolve the result.
- UI has independent manual task creation for all five task types (AI may remain disabled), task search, actual backend local-history chat search, contact tags filter and controlled notifications. No-account startup opens account management.
- Media UI binds fetch and blob completion to its original account/chat/panel epoch; stale responses cannot open a preview or trigger a download. Content-Disposition filename* uses UTF-8 RFC5987 decoding with a sanitized filename fallback, preserving Chinese names, spaces and file extensions.

Each implementer owns only assigned files and corresponding tests. Write tests first, execute RED before production code, then GREEN; real clients only fakes in tests. Record full report under work/multiaccount/<module>-report.md, mention exact commands+results and interface changes. Avoid git add/commit shared directory concurrently; root owns integration commits. Changes to contract require message to root; do not silently rename actions or signatures. All public error messages Chinese. No direct files or requests to legacy real runtime.
