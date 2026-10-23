"""Single-contact group invitations with permission and membership checks.

The controller owns the explicitly selected contact scope and confirmation.
This module never retries, sends messages, follows invite links, joins groups,
changes contact lists, purchases Premium, or removes a participant's ban.
"""

from dataclasses import dataclass

from telethon import errors, functions, types, utils
from telethon.tl.custom import ParticipantPermissions


@dataclass(frozen=True)
class InviteTarget:
    group_id: int
    title: str
    entity: object


_CHANNEL_MEMBERS = (types.ChannelParticipant, types.ChannelParticipantSelf,
                    types.ChannelParticipantAdmin, types.ChannelParticipantCreator)
_CHAT_MEMBERS = (types.ChatParticipant, types.ChatParticipantAdmin,
                 types.ChatParticipantCreator)

# These describe the account or target group, rather than one recipient.
_FATAL_RPC = (
    errors.PeerFloodError, errors.ChatAdminRequiredError,
    errors.ChatAdminInviteRequiredError, errors.ChatWriteForbiddenError,
    errors.ChannelPrivateError, errors.ChannelInvalidError,
    errors.ChannelIdInvalidError, errors.ChatInvalidError,
    errors.ChatIdInvalidError, errors.ChatForbiddenError,
    errors.ChatRestrictedError, errors.UsersTooMuchError,
    errors.ChannelsTooMuchError,
    errors.UserBannedInChannelError, errors.UserRestrictedError,
    errors.ChatTooBigError, errors.ChannelTooBigError,
)
_RESTRICTED_RPC = (
    errors.UserPrivacyRestrictedError, errors.UserNotMutualContactError,
    errors.UserChannelsTooMuchError,
    errors.UserBlockedError, errors.UserIsBlockedError,
    errors.YouBlockedUserError, errors.UserKickedError,
    errors.PremiumAccountRequiredError,
)


def _fatal(error: errors.RPCError) -> bool:
    return (isinstance(error, (errors.FloodError, errors.UnauthorizedError, *_FATAL_RPC))
            or error.code in (401, 420))


def _result(kind: str, detail: str) -> dict:
    return {'kind': kind, 'detail': detail}


def _validate_entity(entity, group_id: int) -> None:
    if type(group_id) is not int or group_id >= 0:
        raise ValueError('请选择有效的已加入群组。')
    if not isinstance(entity, (types.Chat, types.Channel)):
        raise ValueError('目标必须是已加入的普通群或超级群。')
    if isinstance(entity, types.Channel) and (
            not entity.megagroup or entity.broadcast or entity.monoforum):
        raise ValueError('目标必须是超级群，不能是广播频道或频道私信。')
    if any(getattr(entity, name, False)
           for name in ('left', 'kicked', 'deactivated', 'migrated_to')):
        raise ValueError('目标群已退出、被移除、停用或迁移，请刷新群组。')
    if type(entity.id) is not int or entity.id <= 0 or utils.get_peer_id(entity) != group_id:
        raise ValueError('目标群标识不一致，请重新选择。')


def _membership(permissions, entity, user_id: int) -> str:
    """Return member, absent, banned or unknown without guessing from truthiness.

    ParticipantPermissions.is_banned also covers restricted existing members;
    they are deliberately left alone. Its has_left property only handles the
    separate ChannelParticipantLeft constructor, not banned.left.
    """
    if not isinstance(permissions, ParticipantPermissions):
        return 'unknown'
    value = permissions.participant
    if permissions.is_chat != isinstance(entity, types.Chat):
        return 'unknown'
    if isinstance(entity, types.Channel):
        if isinstance(value, (types.ChannelParticipantBanned,
                              types.ChannelParticipantLeft)):
            if not isinstance(value.peer, types.PeerUser) or value.peer.user_id != user_id:
                return 'unknown'
            return 'banned' if isinstance(value, types.ChannelParticipantBanned) else 'absent'
        allowed = _CHANNEL_MEMBERS
    else:
        allowed = _CHAT_MEMBERS
    if isinstance(value, allowed) and value.user_id == user_id:
        return 'member'
    return 'unknown'


async def resolve_target(client, group_id: int, own_id: int) -> InviteTarget:
    """Read the exact joined group and effective invitation permission."""
    if (type(group_id) is not int or group_id >= 0
            or type(own_id) is not int or own_id <= 0):
        raise ValueError('账号或目标群标识无效。')
    entity = await client.get_entity(group_id)
    _validate_entity(entity, group_id)
    permissions = await client.get_permissions(entity, 'me')
    if _membership(permissions, entity, own_id) != 'member':
        raise ValueError('无法确认当前账号是目标群的有效成员。')
    if permissions.is_creator:
        allowed = True
    elif permissions.is_admin:
        # Telethon 1.45 exposes invite_users, not add_users. It is an
        # administrator-only property and is always False for normal members.
        allowed = permissions.invite_users is True
    else:
        defaults = entity.default_banned_rights
        allowed = (isinstance(defaults, types.ChatBannedRights)
                   and not defaults.invite_users and not defaults.view_messages)
    if not allowed:
        raise ValueError('当前账号没有邀请成员的权限，或权限信息不完整。')
    return InviteTarget(group_id, entity.title, entity)


def _recipient_error(error: errors.RPCError, *, submitted=False) -> dict:
    if _fatal(error):
        raise error
    if type(error.code) is not int or not 400 <= error.code < 500:
        if submitted:
            return _result('unverified', '邀请请求已提交，但平台返回异常，实际结果须先只读核实；不会再次邀请。')
        # A failed read says nothing about one recipient and must pause the batch.
        raise error
    if isinstance(error, errors.UserAlreadyParticipantError):
        return _result('already', '该联系人已经是目标群成员。')
    if isinstance(error, _RESTRICTED_RPC):
        if isinstance(error, errors.UserNotMutualContactError):
            detail = '对方不是双向联系人，Telegram 拒绝邀请。'
        elif isinstance(error, errors.UserChannelsTooMuchError):
            detail = '对方加入的群组或频道已达到 Telegram 限制。'
        elif isinstance(error, errors.UserKickedError):
            detail = '对方已被目标群移除，已跳过；不会解除封禁。'
        elif isinstance(error, errors.PremiumAccountRequiredError):
            detail = 'Telegram 要求 Premium 条件，已跳过。'
        else:
            detail = '对方的隐私、联系人或屏蔽设置不允许邀请。'
        return _result('restricted', detail)
    return _result('failed', f'Telegram 拒绝本次邀请：{type(error).__name__}。')


def _check_response(response, user_id: int) -> dict | None:
    if (not isinstance(response, types.messages.InvitedUsers)
            or not isinstance(response.missing_invitees, list)):
        return _result('unverified', 'Telegram 返回格式不完整，无法确认邀请结果。')
    for missing in response.missing_invitees:
        if (not isinstance(missing, types.MissingInvitee)
                or type(missing.user_id) is not int or missing.user_id <= 0):
            return _result('unverified', 'Telegram 返回的未邀请名单无效，无法确认结果。')
    matching = [item for item in response.missing_invitees if item.user_id == user_id]
    if matching:
        if any(item.premium_would_allow_invite or item.premium_required_for_pm
               for item in matching):
            return _result('restricted', '对方的隐私或 Premium 条件阻止邀请，已跳过。')
        return _result('restricted', 'Telegram 确认未能邀请该联系人，可能受隐私设置限制。')
    if (response.missing_invitees
            or getattr(response.updates, 'SUBCLASS_OF_ID', None) != types.Updates.SUBCLASS_OF_ID):
        return _result('unverified', 'Telegram 返回结果与所选联系人不一致或信息不完整。')
    return None


async def invite_contact(client, target: InviteTarget, user: types.User, *, before_submit=None) -> dict:
    """Attempt one selected user's invitation once and verify membership.

    Account, group permission, flood, authentication, network and cancellation
    errors propagate so that the controller stops the whole batch. A normal
    post-check RPC error produces unverified, never an invented success.
    """
    if not isinstance(target, InviteTarget):
        raise ValueError('目标群资料无效，请重新准备邀请。')
    _validate_entity(target.entity, target.group_id)
    if (not isinstance(user, types.User) or type(user.id) is not int or user.id <= 0
            or user.is_self or user.bot or user.deleted):
        return _result('failed', '联系人资料无效，或属于自己、机器人、已删除账号。')
    try:
        # get_input_peer rejects min users and absent general-purpose hashes;
        # get_input_user alone would silently replace a missing hash with zero.
        input_user = utils.get_input_user(utils.get_input_peer(user))
    except (TypeError, ValueError):
        return _result('failed', '联系人资料不足，无法安全解析该账号。')
    if not isinstance(input_user, types.InputUser) or input_user.user_id != user.id:
        return _result('failed', '联系人标识不一致，已跳过。')
    try:
        before = _membership(await client.get_permissions(target.entity, user), target.entity, user.id)
    except errors.UserNotParticipantError:
        before = 'absent'
    except errors.RPCError as error:
        return _recipient_error(error)
    except (TypeError, ValueError):
        return _result('failed', '无法确认该联系人的当前群成员状态，已跳过。')
    if before == 'member':
        return _result('already', '该联系人已经是目标群成员。')
    if before == 'banned':
        return _result('restricted', '该联系人在目标群受限或被封禁，已跳过；不会解除封禁。')
    if before != 'absent':
        return _result('failed', '未取得明确的群成员状态，已跳过。')
    submitted = False
    try:
        if isinstance(target.entity, types.Channel):
            request = functions.channels.InviteToChannelRequest(
                channel=target.entity, users=[input_user])
        else:
            request = functions.messages.AddChatUserRequest(
                chat_id=target.entity.id, user_id=input_user, fwd_limit=0)
        if before_submit is not None:
            before_submit()
        submitted = True
        response = await client(request)
    except errors.RPCError as error:
        return _recipient_error(error, submitted=submitted)
    except (TypeError, ValueError):
        if submitted:
            return _result('unverified', '邀请请求已提交，但响应或传输资料异常，须先只读核实；不会再次邀请。')
        return _result('failed', '邀请请求的目标或联系人资料无效，未重试。')
    invalid = _check_response(response, user.id)
    if invalid is not None:
        return invalid
    try:
        after = _membership(await client.get_permissions(target.entity, user), target.entity, user.id)
    except errors.RPCError as error:
        if _fatal(error):
            raise
        return _result('unverified', '邀请请求已提交，但暂时无法核实对方是否入群。')
    except (TypeError, ValueError):
        return _result('unverified', '邀请请求已提交，但成员核验资料不完整。')
    if after != 'member':
        return _result('unverified', '邀请请求已提交，尚未确认对方成为群成员。')
    return _result('invited', '已核实该联系人现在是目标群成员。')
