from __future__ import annotations

import json
import random
import shutil
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

# 自定义异常，用于iMessage发送失败时抛出
class IMessageSendError(RuntimeError):
    """当iMessage发送失败时抛出该异常"""

# 发送风控参数配置，控制延迟、批次、每日上限等
@dataclass(slots=True)
class IMessageRiskControl:
    # 启动时初始等待秒数范围（防止批量密集启动）
    startup_delay_min_seconds: int = 20
    startup_delay_max_seconds: int = 90
    # 两次消息之间的最小、最大随机延迟
    min_delay_between_messages_seconds: int = 35
    max_delay_between_messages_seconds: int = 80
    # 每批次最大发送数量，及批次间的暂停范围（秒）
    batch_size: int = 5
    batch_pause_min_seconds: int = 180
    batch_pause_max_seconds: int = 420
    # 每日最多可发送消息数量
    daily_limit: int = 20
    # 同一手机号发送后冷却周期（小时）
    cooldown_hours: int = 72
    # 允许自动发送的时间窗口（小时）
    active_hours_start: int = 9
    active_hours_end: int = 20

# 单个手机号的发送结果
@dataclass(slots=True)
class SendResult:
    phone: str           # 发送手机号
    status: str          # 状态：sent，failed，skipped，dry_run 等
    detail: str          # 详细描述（出错原因、跳过原因、成功标识）

# 对AppleScript字符串进行转义
def _escape_applescript_string(value: str) -> str:
    return value.replace("\\", "\\\\").replace('"', '\\"')

# 加载发送历史，返回历史消息记录字典
def _load_history(state_path: Path) -> dict[str, list[dict[str, str]]]:
    if not state_path.exists():
        return {"messages": []}
    return json.loads(state_path.read_text(encoding="utf-8"))

# 保存发送历史到指定路径
def _save_history(state_path: Path, payload: dict[str, list[dict[str, str]]]) -> None:
    state_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

# 检查当前是否在允许的自动发送窗口内
def _can_send_now(rules: IMessageRiskControl, now: datetime) -> tuple[bool, str]:
    if not (rules.active_hours_start <= now.hour < rules.active_hours_end):
        return False, (
            f"当前本地时间 {now.hour:02d}:00 不在允许自动发送时间段 "
            f"{rules.active_hours_start:02d}:00-{rules.active_hours_end:02d}:00 内。"
        )
    return True, ""

# 从发送历史中提取24小时内消息及正在冷却的手机号集合
def _recent_history_entries(
    history: dict[str, list[dict[str, str]]],
    *,
    now_ts: float,
    rules: IMessageRiskControl,
) -> tuple[list[dict[str, str]], set[str]]:
    recent_day_entries: list[dict[str, str]] = []
    cooling_down_phones: set[str] = set()
    for entry in history.get("messages", []):
        sent_ts = float(entry["timestamp"])
        if now_ts - sent_ts <= 24 * 60 * 60:
            recent_day_entries.append(entry)
        if now_ts - sent_ts <= rules.cooldown_hours * 60 * 60:
            cooling_down_phones.add(entry["phone"])
    return recent_day_entries, cooling_down_phones

# 构造AppleScript脚本用于直接向某手机号发送iMessage
def _build_direct_send_script(phone: str, message: str) -> str:
    escaped_phone = _escape_applescript_string(phone)
    escaped_message = _escape_applescript_string(message)
    return f'''
set targetPhone to "{escaped_phone}"
set messageText to "{escaped_message}"

tell application "Messages"
    activate
    delay 2
    set targetService to first service whose service type = iMessage
    set targetParticipant to participant targetPhone of targetService
    send messageText to targetParticipant
end tell
'''

# 向指定手机号发送一条iMessage（底层调用AppleScript）
def send_imessage_once(phone: str, message: str) -> None:
    if sys.platform != "darwin":
        raise IMessageSendError("仅支持在macOS下由本地脚本自动发送iMessage。")
    if shutil.which("osascript") is None:
        raise IMessageSendError("本机未找到 osascript，无法自动发送iMessage。")

    script = _build_direct_send_script(phone, message)
    completed = subprocess.run(
        ["osascript", "-e", script],
        check=False,
        capture_output=True,
        text=True,
    )
    if completed.returncode != 0:
        # 返回AppleScript错误信息
        stderr = (completed.stderr or completed.stdout or "").strip()
        raise IMessageSendError(stderr or f"osascript 非零退出码: {completed.returncode}")

# 带有防刷风控的iMessage批量发送主流程
def send_imessages_with_risk_control(
    phones: list[str],
    message: str = "Hi",
    *,
    dry_run: bool = False,
    rules: IMessageRiskControl | None = None,
    max_send_count: int | None = None,
    state_path: str | Path = ".imessage_send_history.json",
    normalize_phone_numbers: bool = True,
) -> list[SendResult]:
    # 采用传入的风控规则，否则用默认
    rules = rules or IMessageRiskControl()
    # 首先对手机号去重及规范化
    if normalize_phone_numbers:
        normalized_unique_phones = list(dict.fromkeys(_normalize_phone_number(phone) for phone in phones))
        unique_phones = [phone for phone in normalized_unique_phones if phone]
    else:
        unique_phones = list(dict.fromkeys((phone or "").strip() for phone in phones if (phone or "").strip()))
    state_file = Path(state_path)
    history = _load_history(state_file)
    now = datetime.now()
    # 检查时间窗口
    can_send, reason = _can_send_now(rules, now)
    if not can_send:
        raise IMessageSendError(reason)

    now_ts = time.time()
    recent_day_entries, cooling_down_phones = _recent_history_entries(history, now_ts=now_ts, rules=rules)
    if len(recent_day_entries) >= rules.daily_limit:
        raise IMessageSendError(
            f"今日已达发送上限：过去24小时内已发送{len(recent_day_entries)}条消息。"
        )

    # 计算本次实际可发送数量
    send_budget = rules.daily_limit - len(recent_day_entries)
    if max_send_count is not None:
        send_budget = min(send_budget, max_send_count)

    # 排除正在冷却期的手机号
    queued_phones = [phone for phone in unique_phones if phone not in cooling_down_phones]
    results: list[SendResult] = []

    # dry_run模式，仅输出将会被发送的手机号和内容，不实际执行
    if dry_run:
        return [
            SendResult(phone=phone, status="dry_run", detail=f'将会发送 "{message}"')
            for phone in queued_phones[:send_budget]
        ]

    # 启动初期随机等待，进一步防止批量行为
    time.sleep(random.randint(rules.startup_delay_min_seconds, rules.startup_delay_max_seconds))

    sent_in_this_run = 0
    for index, phone in enumerate(queued_phones):
        if sent_in_this_run >= send_budget:
            results.append(SendResult(phone=phone, status="skipped", detail="跳过，因为本次发送额度已用尽"))
            continue

        # 每达到一个batch，长暂停一次
        if sent_in_this_run and sent_in_this_run % rules.batch_size == 0:
            batch_pause = random.randint(rules.batch_pause_min_seconds, rules.batch_pause_max_seconds)
            time.sleep(batch_pause)

        try:
            send_imessage_once(phone, message)
            sent_in_this_run += 1
            # 记录本次发送历史
            history.setdefault("messages", []).append(
                {
                    "phone": phone,
                    "timestamp": str(time.time()),
                    "message": message,
                }
            )
            _save_history(state_file, history)
            results.append(SendResult(phone=phone, status="sent", detail="发送成功"))
        except Exception as exc:  # noqa: BLE001
            results.append(SendResult(phone=phone, status="failed", detail=str(exc)))

        # 下一个收件人前等待，除非已经满额或最后一个
        if index < len(queued_phones) - 1 and sent_in_this_run < send_budget:
            delay_seconds = random.randint(
                rules.min_delay_between_messages_seconds,
                rules.max_delay_between_messages_seconds,
            )
            time.sleep(delay_seconds)

    return results

# 规范化手机号，仅保留数字，去除特殊字符。长度不足7位为无效号。
def _normalize_phone_number(phone: str | None) -> str | None:
    if not phone:
        return None
    stripped = phone.strip()
    if not stripped:
        return None
    has_plus = stripped.startswith("+")
    digits = "".join(ch for ch in stripped if ch.isdigit())
    if len(digits) < 7:
        return None
    return f"+{digits}" if has_plus else digits
