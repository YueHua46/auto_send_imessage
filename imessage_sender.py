from __future__ import annotations

import json
import random
import shutil
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any


class IMessageSendError(RuntimeError):
    """当 iMessage 发送或 imsg 调用失败时抛出。"""


@dataclass(slots=True)
class IMessageRiskControl:
    startup_delay_min_seconds: int = 20
    startup_delay_max_seconds: int = 90
    min_delay_between_messages_seconds: int = 35
    max_delay_between_messages_seconds: int = 80
    batch_size: int = 5
    batch_pause_min_seconds: int = 180
    batch_pause_max_seconds: int = 420
    daily_limit: int = 100
    cooldown_hours: int = 72
    active_hours_start: int = 9
    active_hours_end: int = 20


@dataclass(slots=True)
class IMsgCLIConfig:
    binary: str = "imsg"
    service: str = "auto"
    region: str = "US"
    send_timeout_seconds: int = 60
    read_timeout_seconds: int = 30
    chats_limit: int = 200
    history_limit: int = 50
    watch_debounce_milliseconds: int = 250


@dataclass(slots=True)
class SendResult:
    phone: str
    status: str
    detail: str
    error: str | None = None


@dataclass(slots=True)
class DeliveryCheckResult:
    status: str
    detail: str
    error: str | None = None
    raw_error: str | None = None


@dataclass(slots=True)
class IMsgCommandResult:
    stdout: str
    stderr: str
    returncode: int | None
    timed_out: bool = False


@dataclass(slots=True)
class IMsgChat:
    id: int
    identifier: str
    service: str
    last_message_at: str | None = None
    name: str | None = None


@dataclass(slots=True)
class IMsgMessage:
    id: int | None
    chat_id: int | None
    guid: str | None
    sender: str | None
    is_from_me: bool
    text: str
    created_at: str | None


@dataclass(slots=True)
class IMsgSendContext:
    chat_id: int | None
    anchor_message_id: int | None
    attempted_at_utc: datetime


def _now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _to_imsg_iso(value: datetime) -> str:
    utc_value = value.astimezone(timezone.utc).replace(microsecond=0)
    return utc_value.isoformat().replace("+00:00", "Z")


def _parse_iso_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    normalized = value.strip()
    if not normalized:
        return None
    normalized = normalized.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _batch_record_key(recipient: str, message: str) -> str:
    return f"{recipient}\n{message}"


def _resolve_batch_paths(
    batch_root_dir: str | Path,
    *,
    batch_date: str | None = None,
) -> tuple[str, Path, Path]:
    if batch_date is None:
        batch_date = datetime.now().strftime("%Y-%m-%d")
    batch_dir = Path(batch_root_dir) / batch_date
    batch_dir.mkdir(parents=True, exist_ok=True)
    return batch_date, batch_dir / "results.json", batch_dir / "events.jsonl"


def _load_batch_results(batch_results_path: Path, batch_date: str) -> dict[str, Any]:
    if not batch_results_path.exists():
        return {"batch_date": batch_date, "updated_at": _now_iso(), "records": []}
    try:
        payload = json.loads(batch_results_path.read_text(encoding="utf-8"))
    except Exception:
        return {"batch_date": batch_date, "updated_at": _now_iso(), "records": []}
    records = payload.get("records", [])
    if not isinstance(records, list):
        records = []
    return {
        "batch_date": str(payload.get("batch_date", batch_date)),
        "updated_at": str(payload.get("updated_at", _now_iso())),
        "records": records,
    }


def _save_batch_results(
    batch_results_path: Path,
    *,
    batch_date: str,
    batch_records_by_key: dict[str, dict[str, Any]],
) -> None:
    records = sorted(
        batch_records_by_key.values(),
        key=lambda item: (
            str(item.get("recipient", "")),
            str(item.get("message", "")),
            str(item.get("updated_at", "")),
        ),
    )
    payload = {
        "batch_date": batch_date,
        "updated_at": _now_iso(),
        "records": records,
    }
    batch_results_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def _append_batch_event(batch_events_path: Path, payload: dict[str, Any]) -> None:
    line = json.dumps({"event_at": _now_iso(), **payload}, ensure_ascii=False)
    with batch_events_path.open("a", encoding="utf-8") as handle:
        handle.write(f"{line}\n")


def _index_batch_records(records: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    indexed: dict[str, dict[str, Any]] = {}
    for record in records:
        recipient = str(record.get("recipient", "")).strip()
        message = str(record.get("message", ""))
        if not recipient:
            continue
        indexed[_batch_record_key(recipient, message)] = record
    return indexed


def _upsert_batch_record(
    batch_records_by_key: dict[str, dict[str, Any]],
    *,
    recipient: str,
    message: str,
    transport_status: str,
    delivery_status: str,
    detail: str,
    attempted_at: str,
    error: str | None = None,
    raw_error: str | None = None,
) -> None:
    key = _batch_record_key(recipient, message)
    existing = batch_records_by_key.get(key, {})
    previous_attempt_count = int(existing.get("attempt_count", 0) or 0)
    batch_records_by_key[key] = {
        "recipient": recipient,
        "message": message,
        "transport_status": transport_status,
        "delivery_status": delivery_status,
        "detail": detail,
        "error": error,
        "raw_error": raw_error,
        "attempted_at": existing.get("attempted_at", attempted_at),
        "last_checked_at": _now_iso(),
        "attempt_count": previous_attempt_count + 1,
        "updated_at": _now_iso(),
    }


def _normalize_identity(value: str | None) -> tuple[str, str]:
    text = (value or "").strip().lower()
    digits = "".join(ch for ch in text if ch.isdigit())
    return text, digits


def _recipient_matches(candidate: str | None, recipient: str) -> bool:
    candidate_text, candidate_digits = _normalize_identity(candidate)
    recipient_text, recipient_digits = _normalize_identity(recipient)
    if candidate_text and candidate_text == recipient_text:
        return True
    if candidate_digits and recipient_digits:
        return (
            candidate_digits == recipient_digits
            or candidate_digits.endswith(recipient_digits)
            or recipient_digits.endswith(candidate_digits)
        )
    return False


def _recipient_identity_key(value: str | None) -> str:
    normalized_text, normalized_digits = _normalize_identity(value)
    return normalized_digits or normalized_text


def _normalize_message_text(value: str | None) -> str:
    return " ".join((value or "").strip().split())


def _coerce_int(value: object) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _coerce_bool(value: object) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value != 0
    if isinstance(value, str):
        return value.strip().lower() not in {"", "0", "false", "none", "null"}
    return bool(value)


def _resolve_cli_binary(binary: str) -> str | None:
    candidate = (binary or "").strip()
    if not candidate:
        return None
    if any(sep in candidate for sep in ("/", "\\")):
        path = Path(candidate).expanduser()
        return str(path) if path.exists() else None
    return shutil.which(candidate)


def _build_imsg_send_command(
    *,
    binary: str,
    recipient: str,
    message: str,
    cli_config: IMsgCLIConfig,
) -> list[str]:
    command = [
        binary,
        "send",
        "--to",
        recipient,
        "--text",
        message,
        "--service",
        cli_config.service,
    ]
    region = cli_config.region.strip()
    if region:
        command.extend(["--region", region])
    return command


def _validate_imsg_cli(cli_config: IMsgCLIConfig) -> str:
    if sys.platform != "darwin":
        raise IMessageSendError("仅支持在 macOS 下通过 imsg 自动发送 iMessage。")

    resolved_binary = _resolve_cli_binary(cli_config.binary)
    if resolved_binary is None:
        raise IMessageSendError(
            f"未找到 imsg 可执行文件：{cli_config.binary}。"
            "请先安装 imsg，或通过 IMESSAGE_IMSG_BINARY 指定路径。"
        )

    try:
        completed = subprocess.run(
            [resolved_binary, "--version"],
            check=False,
            capture_output=True,
            text=True,
            timeout=max(cli_config.send_timeout_seconds, 1),
        )
    except OSError as exc:
        raise IMessageSendError(f"启动 imsg 失败：{exc}") from exc
    except subprocess.TimeoutExpired as exc:
        raise IMessageSendError("检查 imsg 版本超时，请确认命令可正常执行。") from exc

    if completed.returncode != 0:
        stderr = (completed.stderr or completed.stdout or "").strip()
        raise IMessageSendError(stderr or f"imsg --version 非零退出码: {completed.returncode}")
    return resolved_binary


def _run_imsg_command(
    *,
    resolved_binary: str,
    cli_config: IMsgCLIConfig,
    args: list[str],
    timeout_seconds: int,
    allow_timeout: bool = False,
    require_success: bool = True,
) -> IMsgCommandResult:
    command = [resolved_binary, *args]
    try:
        completed = subprocess.run(
            command,
            check=False,
            capture_output=True,
            text=True,
            timeout=max(timeout_seconds, 1),
        )
    except OSError as exc:
        raise IMessageSendError(f"调用 imsg 失败：{exc}") from exc
    except subprocess.TimeoutExpired as exc:
        if allow_timeout:
            return IMsgCommandResult(
                stdout=exc.stdout or "",
                stderr=exc.stderr or "",
                returncode=None,
                timed_out=True,
            )
        raise IMessageSendError(
            f"imsg {' '.join(args[:2])} 超时（>{timeout_seconds} 秒）。"
        ) from exc

    stdout = completed.stdout or ""
    stderr = completed.stderr or ""
    if require_success and completed.returncode != 0:
        detail = stderr.strip() or stdout.strip() or f"非零退出码: {completed.returncode}"
        raise IMessageSendError(f"imsg {' '.join(args[:2])} 失败：{detail}")
    return IMsgCommandResult(
        stdout=stdout,
        stderr=stderr,
        returncode=completed.returncode,
        timed_out=False,
    )


def _parse_ndjson_objects(payload: str) -> list[dict[str, Any]]:
    objects: list[dict[str, Any]] = []
    for raw_line in payload.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        if not line.startswith("{"):
            continue
        try:
            parsed = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(parsed, dict):
            objects.append(parsed)
    return objects


def _parse_imsg_chats(payload: str) -> list[IMsgChat]:
    chats: list[IMsgChat] = []
    for item in _parse_ndjson_objects(payload):
        chat_id = _coerce_int(item.get("id"))
        if chat_id is None:
            continue
        chats.append(
            IMsgChat(
                id=chat_id,
                identifier=str(item.get("identifier", "") or "").strip(),
                service=str(item.get("service", "") or "").strip(),
                last_message_at=str(item.get("last_message_at", "") or "").strip() or None,
                name=str(item.get("name", "") or "").strip() or None,
            )
        )
    return chats


def _parse_imsg_messages(payload: str) -> list[IMsgMessage]:
    messages: list[IMsgMessage] = []
    for item in _parse_ndjson_objects(payload):
        messages.append(
            IMsgMessage(
                id=_coerce_int(item.get("id")),
                chat_id=_coerce_int(item.get("chat_id")),
                guid=str(item.get("guid", "") or "").strip() or None,
                sender=str(item.get("sender", "") or "").strip() or None,
                is_from_me=_coerce_bool(item.get("is_from_me")),
                text=str(item.get("text", "") or ""),
                created_at=str(item.get("created_at", "") or "").strip() or None,
            )
        )
    return messages


def _list_imsg_chats(
    *,
    resolved_binary: str,
    cli_config: IMsgCLIConfig,
) -> list[IMsgChat]:
    result = _run_imsg_command(
        resolved_binary=resolved_binary,
        cli_config=cli_config,
        args=["chats", "--limit", str(max(cli_config.chats_limit, 1)), "--json"],
        timeout_seconds=max(cli_config.read_timeout_seconds, 1),
    )
    return _parse_imsg_chats(result.stdout)


def _list_imsg_history(
    *,
    resolved_binary: str,
    cli_config: IMsgCLIConfig,
    chat_id: int,
    start_at: datetime | None = None,
    limit: int | None = None,
) -> list[IMsgMessage]:
    args = [
        "history",
        "--chat-id",
        str(chat_id),
        "--limit",
        str(max(limit or cli_config.history_limit, 1)),
        "--json",
    ]
    if start_at is not None:
        args.extend(["--start", _to_imsg_iso(start_at)])
    result = _run_imsg_command(
        resolved_binary=resolved_binary,
        cli_config=cli_config,
        args=args,
        timeout_seconds=max(cli_config.read_timeout_seconds, 1),
    )
    return _parse_imsg_messages(result.stdout)


def _select_chat_for_recipient(recipient: str, chats: list[IMsgChat]) -> IMsgChat | None:
    scored: list[tuple[float, float, IMsgChat]] = []
    for chat in chats:
        score = 0.0
        if _recipient_matches(chat.identifier, recipient):
            score += 10
        if chat.service.lower() == "imessage":
            score += 1
        if score <= 0:
            continue
        last_ts = _parse_iso_datetime(chat.last_message_at)
        scored.append((score, last_ts.timestamp() if last_ts is not None else 0.0, chat))
    if not scored:
        return None
    scored.sort(key=lambda item: (item[0], item[1]), reverse=True)
    return scored[0][2]


def _max_message_id(messages: list[IMsgMessage]) -> int | None:
    ids = [message.id for message in messages if message.id is not None]
    return max(ids) if ids else None


def _find_matching_outgoing_message(
    messages: list[IMsgMessage],
    *,
    expected_text: str,
    attempted_after_utc: datetime,
    min_message_id: int | None = None,
) -> IMsgMessage | None:
    expected_normalized = _normalize_message_text(expected_text)
    best_match: tuple[float, IMsgMessage] | None = None
    for message in messages:
        if not message.is_from_me:
            continue
        if min_message_id is not None and message.id is not None and message.id <= min_message_id:
            continue
        created_at = _parse_iso_datetime(message.created_at)
        if created_at is not None and created_at < attempted_after_utc - timedelta(seconds=5):
            continue

        score = 0.0
        candidate_text = _normalize_message_text(message.text)
        if candidate_text == expected_normalized:
            score += 10
        elif candidate_text and expected_normalized and (
            candidate_text in expected_normalized or expected_normalized in candidate_text
        ):
            score += 5
        else:
            continue

        if message.id is not None:
            score += 1
        if created_at is not None:
            score += created_at.timestamp() / 1_000_000_000

        if best_match is None or score > best_match[0]:
            best_match = (score, message)
    return best_match[1] if best_match is not None else None


def _build_send_context(
    recipient: str,
    *,
    resolved_binary: str,
    cli_config: IMsgCLIConfig,
) -> IMsgSendContext:
    attempted_at_utc = _utc_now()
    try:
        chats = _list_imsg_chats(resolved_binary=resolved_binary, cli_config=cli_config)
        chat = _select_chat_for_recipient(recipient, chats)
        if chat is None:
            return IMsgSendContext(chat_id=None, anchor_message_id=None, attempted_at_utc=attempted_at_utc)
        history = _list_imsg_history(
            resolved_binary=resolved_binary,
            cli_config=cli_config,
            chat_id=chat.id,
            limit=1,
        )
        return IMsgSendContext(
            chat_id=chat.id,
            anchor_message_id=_max_message_id(history),
            attempted_at_utc=attempted_at_utc,
        )
    except Exception:
        # 发送前上下文获取失败不阻断发送，发送后再走 history 兜底确认。
        return IMsgSendContext(chat_id=None, anchor_message_id=None, attempted_at_utc=attempted_at_utc)


def _watch_for_confirmation(
    *,
    chat_id: int,
    expected_text: str,
    attempted_after_utc: datetime,
    min_message_id: int | None,
    resolved_binary: str,
    cli_config: IMsgCLIConfig,
    timeout_seconds: int,
) -> DeliveryCheckResult | None:
    args = [
        "watch",
        "--chat-id",
        str(chat_id),
        "--json",
        "--debounce",
        f"{max(cli_config.watch_debounce_milliseconds, 1)}ms",
    ]
    if min_message_id is not None:
        args.extend(["--since-rowid", str(min_message_id)])

    try:
        result = _run_imsg_command(
            resolved_binary=resolved_binary,
            cli_config=cli_config,
            args=args,
            timeout_seconds=max(timeout_seconds, 1),
            allow_timeout=True,
            require_success=False,
        )
    except Exception as exc:
        return DeliveryCheckResult(
            status="sent_not_confirmed",
            detail=f"imsg watch 启动失败：{exc}",
            error=str(exc),
            raw_error=str(exc),
        )

    if result.returncode not in (0, None):
        detail = result.stderr.strip() or result.stdout.strip() or "imsg watch 非零退出"
        return DeliveryCheckResult(
            status="sent_not_confirmed",
            detail=f"imsg watch 执行异常：{detail}",
            error=detail,
            raw_error=detail,
        )

    messages = _parse_imsg_messages(result.stdout)
    matched = _find_matching_outgoing_message(
        messages,
        expected_text=expected_text,
        attempted_after_utc=attempted_after_utc,
        min_message_id=min_message_id,
    )
    if matched is None:
        return None

    detail = f"消息已在 imsg watch 中确认（chat_id={chat_id}"
    if matched.id is not None:
        detail += f", id={matched.id}"
    detail += "）。"
    return DeliveryCheckResult(status="confirmed_in_imsg", detail=detail)


def _confirm_via_history(
    recipient: str,
    message: str,
    *,
    send_context: IMsgSendContext,
    resolved_binary: str,
    cli_config: IMsgCLIConfig,
    timeout_seconds: int,
    interval_seconds: int,
    lookback_seconds: int,
    prior_watch_detail: DeliveryCheckResult | None = None,
) -> DeliveryCheckResult:
    deadline = time.time() + max(timeout_seconds, 1)
    lookback_start = send_context.attempted_at_utc - timedelta(seconds=max(lookback_seconds, 1))
    last_detail = "imsg 尚未返回匹配消息。"
    last_error = prior_watch_detail.error if prior_watch_detail is not None else None
    last_raw_error = prior_watch_detail.raw_error if prior_watch_detail is not None else None

    while True:
        try:
            chats = _list_imsg_chats(resolved_binary=resolved_binary, cli_config=cli_config)
            chat = _select_chat_for_recipient(recipient, chats)
            if chat is None:
                last_detail = "发送后尚未在 imsg chats 中解析到目标会话。"
            else:
                history = _list_imsg_history(
                    resolved_binary=resolved_binary,
                    cli_config=cli_config,
                    chat_id=chat.id,
                    start_at=lookback_start,
                )
                matched = _find_matching_outgoing_message(
                    history,
                    expected_text=message,
                    attempted_after_utc=send_context.attempted_at_utc,
                    min_message_id=send_context.anchor_message_id,
                )
                if matched is not None:
                    detail = f"消息已在 imsg history 中确认（chat_id={chat.id}"
                    if matched.id is not None:
                        detail += f", id={matched.id}"
                    detail += "）。"
                    return DeliveryCheckResult(status="confirmed_in_imsg", detail=detail)
                last_detail = f"已找到 chat_id={chat.id}，但 history 中尚未出现本次外发消息。"
        except Exception as exc:
            last_detail = f"查询 imsg history 失败：{exc}"
            last_error = str(exc)
            last_raw_error = str(exc)

        if time.time() >= deadline:
            break
        time.sleep(max(interval_seconds, 1))

    detail = (
        f"imsg 在 {max(timeout_seconds, 1)} 秒内未确认消息进入 history。"
        f"最后观测：{last_detail}"
    )
    if prior_watch_detail is not None and prior_watch_detail.detail:
        detail = f"{detail}；watch观测：{prior_watch_detail.detail}"
    return DeliveryCheckResult(
        status="sent_not_confirmed",
        detail=detail,
        error=last_error,
        raw_error=last_raw_error,
    )


def _confirm_delivery_status(
    recipient: str,
    message: str,
    *,
    send_context: IMsgSendContext,
    resolved_binary: str,
    cli_config: IMsgCLIConfig,
    timeout_seconds: int,
    interval_seconds: int,
    lookback_seconds: int,
) -> DeliveryCheckResult:
    watch_result: DeliveryCheckResult | None = None
    if send_context.chat_id is not None:
        watch_result = _watch_for_confirmation(
            chat_id=send_context.chat_id,
            expected_text=message,
            attempted_after_utc=send_context.attempted_at_utc,
            min_message_id=send_context.anchor_message_id,
            resolved_binary=resolved_binary,
            cli_config=cli_config,
            timeout_seconds=timeout_seconds,
        )
        if watch_result is not None and watch_result.status == "confirmed_in_imsg":
            return watch_result

    return _confirm_via_history(
        recipient,
        message,
        send_context=send_context,
        resolved_binary=resolved_binary,
        cli_config=cli_config,
        timeout_seconds=timeout_seconds,
        interval_seconds=interval_seconds,
        lookback_seconds=lookback_seconds,
        prior_watch_detail=watch_result,
    )


def _load_history(state_path: Path) -> dict[str, list[dict[str, str]]]:
    if not state_path.exists():
        return {"messages": []}
    return json.loads(state_path.read_text(encoding="utf-8"))


def _save_history(state_path: Path, payload: dict[str, list[dict[str, str]]]) -> None:
    state_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _can_send_now(rules: IMessageRiskControl, now: datetime) -> tuple[bool, str]:
    if not (rules.active_hours_start <= now.hour < rules.active_hours_end):
        return False, (
            f"当前本地时间 {now.hour:02d}:00 不在允许自动发送时间段 "
            f"{rules.active_hours_start:02d}:00-{rules.active_hours_end:02d}:00 内。"
        )
    return True, ""


def _recent_history_entries(
    history: dict[str, list[dict[str, str]]],
    *,
    now_ts: float,
    rules: IMessageRiskControl,
) -> tuple[list[dict[str, str]], set[str]]:
    recent_day_entries: list[dict[str, str]] = []
    cooling_down_recipient_keys: set[str] = set()
    for entry in history.get("messages", []):
        sent_ts = float(entry["timestamp"])
        if now_ts - sent_ts <= 24 * 60 * 60:
            recent_day_entries.append(entry)
        if now_ts - sent_ts <= rules.cooldown_hours * 60 * 60:
            identity_key = _recipient_identity_key(str(entry.get("phone", "")).strip())
            if identity_key:
                cooling_down_recipient_keys.add(identity_key)
    return recent_day_entries, cooling_down_recipient_keys


def _prepare_unique_recipients(
    recipients: list[str],
    *,
    normalize_phone_numbers: bool,
) -> list[str]:
    prepared: list[str] = []
    seen: set[str] = set()
    for raw in recipients:
        recipient = (raw or "").strip()
        if not recipient:
            continue
        if normalize_phone_numbers:
            dedupe_key = _recipient_identity_key(recipient)
        else:
            dedupe_key = recipient
        if not dedupe_key or dedupe_key in seen:
            continue
        seen.add(dedupe_key)
        prepared.append(recipient)
    return prepared


def send_imessage_once(
    phone: str,
    message: str,
    *,
    cli_config: IMsgCLIConfig | None = None,
    resolved_binary: str | None = None,
) -> None:
    cli_config = cli_config or IMsgCLIConfig()
    binary = resolved_binary or _validate_imsg_cli(cli_config)
    command = _build_imsg_send_command(
        binary=binary,
        recipient=phone,
        message=message,
        cli_config=cli_config,
    )
    try:
        completed = subprocess.run(
            command,
            check=False,
            capture_output=True,
            text=True,
            timeout=max(cli_config.send_timeout_seconds, 1),
        )
    except OSError as exc:
        raise IMessageSendError(f"调用 imsg 失败：{exc}") from exc
    except subprocess.TimeoutExpired as exc:
        raise IMessageSendError(
            f"imsg 发送超时（>{cli_config.send_timeout_seconds} 秒）：{phone}"
        ) from exc

    if completed.returncode != 0:
        stderr = (completed.stderr or completed.stdout or "").strip()
        raise IMessageSendError(stderr or f"imsg send 非零退出码: {completed.returncode}")


def send_imessages_with_risk_control(
    phones: list[str],
    message: str = "Hi",
    *,
    dry_run: bool = False,
    rules: IMessageRiskControl | None = None,
    max_send_count: int | None = None,
    state_path: str | Path = ".imessage_send_history.json",
    normalize_phone_numbers: bool = True,
    batch_root_dir: str | Path = "imessage_batches",
    batch_date: str | None = None,
    cli_config: IMsgCLIConfig | None = None,
    delivery_check_timeout_seconds: int = 45,
    delivery_check_interval_seconds: int = 3,
    delivery_check_lookback_seconds: int = 600,
) -> list[SendResult]:
    rules = rules or IMessageRiskControl()
    cli_config = cli_config or IMsgCLIConfig()
    unique_recipients = _prepare_unique_recipients(
        phones,
        normalize_phone_numbers=normalize_phone_numbers,
    )

    state_file = Path(state_path)
    history = _load_history(state_file)
    batch_date, batch_results_path, batch_events_path = _resolve_batch_paths(
        batch_root_dir,
        batch_date=batch_date,
    )
    batch_payload = _load_batch_results(batch_results_path, batch_date)
    batch_records_by_key = _index_batch_records(batch_payload.get("records", []))
    now = datetime.now()
    can_send, reason = _can_send_now(rules, now)
    if not can_send:
        raise IMessageSendError(reason)

    now_ts = time.time()
    recent_day_entries, cooling_down_recipients = _recent_history_entries(
        history,
        now_ts=now_ts,
        rules=rules,
    )
    if len(recent_day_entries) >= rules.daily_limit:
        raise IMessageSendError(
            f"今日已达发送上限：过去24小时内已发送{len(recent_day_entries)}条消息。"
        )

    send_budget = rules.daily_limit - len(recent_day_entries)
    if max_send_count is not None:
        send_budget = min(send_budget, max_send_count)

    queued_recipients = [
        recipient
        for recipient in unique_recipients
        if _recipient_identity_key(recipient) not in cooling_down_recipients
    ]
    skip_statuses_in_batch = {"confirmed_in_imsg", "sent_not_confirmed", "delivered"}
    already_processed_in_batch = {
        _recipient_identity_key(str(record.get("recipient", "")).strip())
        for record in batch_records_by_key.values()
        if str(record.get("delivery_status", "")) in skip_statuses_in_batch
        and str(record.get("message", "")) == message
    }

    results: list[SendResult] = []
    for recipient in queued_recipients:
        if _recipient_identity_key(recipient) in already_processed_in_batch:
            detail = f"跳过：该批次中收件人已存在非失败状态记录（{batch_date}）"
            results.append(SendResult(phone=recipient, status="skipped_processed", detail=detail))
            _append_batch_event(
                batch_events_path,
                {
                    "event_type": "skip_processed",
                    "recipient": recipient,
                    "message": message,
                    "detail": detail,
                },
            )

    queued_recipients = [
        recipient
        for recipient in queued_recipients
        if _recipient_identity_key(recipient) not in already_processed_in_batch
    ]

    if dry_run:
        dry_run_results = [
            SendResult(phone=recipient, status="dry_run", detail=f'将会通过 imsg 发送 "{message}"')
            for recipient in queued_recipients[:send_budget]
        ]
        return results + dry_run_results

    if not queued_recipients:
        return results

    resolved_imsg_binary = _validate_imsg_cli(cli_config)
    time.sleep(random.randint(rules.startup_delay_min_seconds, rules.startup_delay_max_seconds))

    sent_in_this_run = 0
    for index, recipient in enumerate(queued_recipients):
        if sent_in_this_run >= send_budget:
            results.append(
                SendResult(phone=recipient, status="skipped", detail="跳过，因为本次发送额度已用尽")
            )
            continue

        if sent_in_this_run and sent_in_this_run % rules.batch_size == 0:
            batch_pause = random.randint(rules.batch_pause_min_seconds, rules.batch_pause_max_seconds)
            time.sleep(batch_pause)

        attempted_at = _now_iso()
        _append_batch_event(
            batch_events_path,
            {
                "event_type": "send_attempt",
                "recipient": recipient,
                "message": message,
                "attempted_at": attempted_at,
            },
        )

        try:
            send_context = _build_send_context(
                recipient,
                resolved_binary=resolved_imsg_binary,
                cli_config=cli_config,
            )
            send_imessage_once(
                recipient,
                message,
                cli_config=cli_config,
                resolved_binary=resolved_imsg_binary,
            )
            sent_in_this_run += 1
            history.setdefault("messages", []).append(
                {
                    "phone": recipient,
                    "timestamp": str(time.time()),
                    "message": message,
                }
            )
            _save_history(state_file, history)

            delivery_status = _confirm_delivery_status(
                recipient,
                message,
                send_context=send_context,
                resolved_binary=resolved_imsg_binary,
                cli_config=cli_config,
                timeout_seconds=delivery_check_timeout_seconds,
                interval_seconds=delivery_check_interval_seconds,
                lookback_seconds=delivery_check_lookback_seconds,
            )
            results.append(
                SendResult(
                    phone=recipient,
                    status=delivery_status.status,
                    detail=delivery_status.detail,
                    error=delivery_status.error,
                )
            )
            _append_batch_event(
                batch_events_path,
                {
                    "event_type": "delivery_status",
                    "recipient": recipient,
                    "message": message,
                    "delivery_status": delivery_status.status,
                    "detail": delivery_status.detail,
                    "error": delivery_status.error,
                },
            )
            _upsert_batch_record(
                batch_records_by_key,
                recipient=recipient,
                message=message,
                transport_status="sent",
                delivery_status=delivery_status.status,
                detail=delivery_status.detail,
                attempted_at=attempted_at,
                error=delivery_status.error,
                raw_error=delivery_status.raw_error,
            )
            _save_batch_results(
                batch_results_path,
                batch_date=batch_date,
                batch_records_by_key=batch_records_by_key,
            )
        except Exception as exc:  # noqa: BLE001
            error_detail = str(exc)
            results.append(
                SendResult(
                    phone=recipient,
                    status="failed",
                    detail=error_detail,
                    error=error_detail,
                )
            )
            _append_batch_event(
                batch_events_path,
                {
                    "event_type": "send_failed",
                    "recipient": recipient,
                    "message": message,
                    "detail": error_detail,
                },
            )
            _upsert_batch_record(
                batch_records_by_key,
                recipient=recipient,
                message=message,
                transport_status="failed",
                delivery_status="failed",
                detail=error_detail,
                attempted_at=attempted_at,
                error=error_detail,
                raw_error=error_detail,
            )
            _save_batch_results(
                batch_results_path,
                batch_date=batch_date,
                batch_records_by_key=batch_records_by_key,
            )

        if index < len(queued_recipients) - 1 and sent_in_this_run < send_budget:
            delay_seconds = random.randint(
                rules.min_delay_between_messages_seconds,
                rules.max_delay_between_messages_seconds,
            )
            time.sleep(delay_seconds)

    return results
