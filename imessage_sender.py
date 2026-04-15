from __future__ import annotations

import json
import time
import uuid
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import requests


class IMessageSendError(RuntimeError):
    """当 iMessage 发送或 BlueBubbles 调用失败时抛出。"""


@dataclass(slots=True)
class BlueBubblesAPIConfig:
    base_url: str = "http://127.0.0.1:1234"
    password: str = ""
    auth_param_name: str = "guid"
    send_timeout_seconds: int = 20
    read_timeout_seconds: int = 30
    verify_ssl: bool = True
    recent_messages_limit: int = 25


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
class BlueBubblesSendContext:
    recipient: str
    message: str
    attempted_at_ms: int
    temp_guid: str
    message_guid: str | None = None
    transport_error: str | None = None


def _now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")


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
        dedupe_key = _recipient_identity_key(recipient) if normalize_phone_numbers else recipient
        if not dedupe_key or dedupe_key in seen:
            continue
        seen.add(dedupe_key)
        prepared.append(recipient)
    return prepared


def _load_history(state_path: Path) -> dict[str, list[dict[str, str]]]:
    if not state_path.exists():
        return {"messages": []}
    return json.loads(state_path.read_text(encoding="utf-8"))


def _save_history(state_path: Path, payload: dict[str, list[dict[str, str]]]) -> None:
    state_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _normalize_base_url(base_url: str) -> str:
    return base_url.strip().rstrip("/")


def _auth_params(api_config: BlueBubblesAPIConfig) -> dict[str, str]:
    return {api_config.auth_param_name: api_config.password}


def _build_chat_guid(recipient: str) -> str:
    return f"iMessage;-;{recipient.strip()}"


def _request_json(
    *,
    method: str,
    path: str,
    api_config: BlueBubblesAPIConfig,
    timeout_seconds: int,
    params: dict[str, Any] | None = None,
    json_body: dict[str, Any] | None = None,
) -> dict[str, Any]:
    if not api_config.password.strip():
        raise IMessageSendError("缺少 BLUEBUBBLES_PASSWORD，无法调用 BlueBubbles API。")

    merged_params: dict[str, Any] = _auth_params(api_config)
    if params:
        merged_params.update(params)

    url = f"{_normalize_base_url(api_config.base_url)}{path}"
    session = requests.Session()
    hostname = (urlparse(url).hostname or "").strip().lower()
    if hostname in {"127.0.0.1", "localhost", "::1"}:
        session.trust_env = False
    try:
        response = session.request(
            method=method,
            url=url,
            params=merged_params,
            json=json_body,
            timeout=max(timeout_seconds, 1),
            verify=api_config.verify_ssl,
        )
    except requests.RequestException as exc:
        raise IMessageSendError(f"请求 BlueBubbles 失败：{exc}") from exc

    try:
        payload = response.json()
    except ValueError as exc:
        raise IMessageSendError(
            f"BlueBubbles 返回了非 JSON 响应（HTTP {response.status_code}）。"
        ) from exc

    status = _coerce_int(payload.get("status"))
    message = str(payload.get("message", "") or "").strip()
    if response.status_code >= 400 or status is None or status >= 400:
        error_payload = payload.get("error")
        error_text = ""
        if isinstance(error_payload, dict):
            error_text = str(error_payload.get("error", "") or error_payload.get("type", "")).strip()
        detail = error_text or message or f"HTTP {response.status_code}"
        raise IMessageSendError(f"BlueBubbles API 调用失败：{detail}")
    return payload


def _read_recent_log_lines(log_path: Path, *, limit: int = 300) -> list[str]:
    if not log_path.exists():
        return []
    try:
        return log_path.read_text(encoding="utf-8", errors="ignore").splitlines()[-limit:]
    except Exception:
        return []


def _confirm_delivery_in_server_log(
    send_context: BlueBubblesSendContext,
    *,
    log_path: Path | None = None,
) -> DeliveryCheckResult | None:
    effective_log_path = log_path or (Path.home() / "Library/Logs/bluebubbles-server/main.log")
    lines = _read_recent_log_lines(effective_log_path)
    if not lines:
        return None

    await_marker = f"tempGuid: {send_context.temp_guid}"
    last_await_index = -1
    for index, line in enumerate(lines):
        if await_marker in line:
            last_await_index = index

    if last_await_index < 0:
        return None

    for line in lines[last_await_index : last_await_index + 20]:
        if "Delivered message from [You]" in line or "New Message from You" in line:
            return DeliveryCheckResult(
                status="confirmed_in_bluebubbles",
                detail=(
                    "消息已在 BlueBubbles 服务日志中确认。"
                    f" tempGuid={send_context.temp_guid}"
                ),
            )
    return None


def _validate_bluebubbles_server(api_config: BlueBubblesAPIConfig) -> None:
    _request_json(
        method="GET",
        path="/api/v1/ping",
        api_config=api_config,
        timeout_seconds=max(api_config.read_timeout_seconds, 1),
    )


def get_bluebubbles_server_info(
    *,
    api_config: BlueBubblesAPIConfig | None = None,
) -> dict[str, Any]:
    config = api_config or BlueBubblesAPIConfig()
    return _request_json(
        method="GET",
        path="/api/v1/server/info",
        api_config=config,
        timeout_seconds=max(config.read_timeout_seconds, 1),
    )


def _message_handle_address(item: dict[str, Any]) -> str | None:
    handle = item.get("handle")
    if isinstance(handle, dict):
        address = str(handle.get("address", "") or "").strip()
        if address:
            return address

    chats = item.get("chats")
    if isinstance(chats, list):
        for chat in chats:
            if not isinstance(chat, dict):
                continue
            chat_identifier = str(chat.get("chatIdentifier", "") or "").strip()
            if chat_identifier:
                return chat_identifier
    return None


def _find_matching_sent_message(
    messages: list[dict[str, Any]],
    *,
    recipient: str,
    expected_text: str,
    attempted_after_ms: int,
    expected_guid: str | None,
) -> dict[str, Any] | None:
    expected_normalized = _normalize_message_text(expected_text)
    for item in messages:
        if not isinstance(item, dict):
            continue
        if not item.get("isFromMe"):
            continue

        guid = str(item.get("guid", "") or "").strip()
        if expected_guid and guid and guid == expected_guid:
            return item

        created_at = _coerce_int(item.get("dateCreated")) or 0
        if created_at and created_at < attempted_after_ms - 5_000:
            continue

        address = _message_handle_address(item)
        if address and not _recipient_matches(address, recipient):
            continue

        candidate_text = _normalize_message_text(str(item.get("text", "") or ""))
        if candidate_text == expected_normalized:
            return item
    return None


def _confirm_delivery_status(
    recipient: str,
    message: str,
    *,
    send_context: BlueBubblesSendContext,
    api_config: BlueBubblesAPIConfig,
    timeout_seconds: int,
    interval_seconds: int,
) -> DeliveryCheckResult:
    deadline = time.time() + max(timeout_seconds, 1)
    last_error: str | None = None
    last_detail = "尚未在 BlueBubbles 最近消息列表中发现匹配的外发记录。"

    while True:
        log_delivery = _confirm_delivery_in_server_log(send_context)
        if log_delivery is not None:
            return log_delivery

        try:
            payload = _request_json(
                method="GET",
                path="/api/v1/message",
                api_config=api_config,
                timeout_seconds=max(api_config.read_timeout_seconds, 1),
                params={
                    "limit": max(api_config.recent_messages_limit, 1),
                    "offset": 0,
                    "sort": "DESC",
                    "after": max(send_context.attempted_at_ms - 10_000, 0),
                },
            )
            data = payload.get("data")
            messages = data if isinstance(data, list) else []
            matched = _find_matching_sent_message(
                messages,
                recipient=recipient,
                expected_text=message,
                attempted_after_ms=send_context.attempted_at_ms,
                expected_guid=send_context.message_guid,
            )
            if matched is not None:
                guid = str(matched.get("guid", "") or "").strip()
                detail = "消息已在 BlueBubbles recent messages 中确认。"
                if guid:
                    detail = f"{detail} guid={guid}"
                return DeliveryCheckResult(
                    status="confirmed_in_bluebubbles",
                    detail=detail,
                )
        except Exception as exc:  # noqa: BLE001
            last_error = str(exc)
            last_detail = f"轮询 BlueBubbles recent messages 失败：{exc}"

        if time.time() >= deadline:
            break
        time.sleep(max(interval_seconds, 1))

    return DeliveryCheckResult(
        status="sent_not_confirmed",
        detail=(
            f"BlueBubbles 在 {max(timeout_seconds, 1)} 秒内未确认消息出现在 recent messages。"
            f"最后观测：{last_detail}"
        ),
        error=last_error,
        raw_error=last_error,
    )


def send_imessage_once(
    phone: str,
    message: str,
    *,
    api_config: BlueBubblesAPIConfig | None = None,
) -> BlueBubblesSendContext:
    config = api_config or BlueBubblesAPIConfig()
    attempted_at_ms = int(time.time() * 1000)
    temp_guid = f"temp-{uuid.uuid4()}"
    message_guid: str | None = None
    transport_error: str | None = None
    try:
        payload = _request_json(
            method="POST",
            path="/api/v1/message/text",
            api_config=config,
            timeout_seconds=max(config.send_timeout_seconds, 1),
            json_body={
                "chatGuid": _build_chat_guid(phone),
                "tempGuid": temp_guid,
                "message": message,
            },
        )
        data = payload.get("data")
        if isinstance(data, dict):
            raw_guid = str(data.get("guid", "") or "").strip()
            if raw_guid:
                message_guid = raw_guid
    except Exception as exc:  # noqa: BLE001
        transport_error = str(exc)

    return BlueBubblesSendContext(
        recipient=phone,
        message=message,
        attempted_at_ms=attempted_at_ms,
        temp_guid=temp_guid,
        message_guid=message_guid,
        transport_error=transport_error,
    )


def send_imessages(
    phones: list[str],
    message: str = "Hi",
    *,
    state_path: str | Path = ".imessage_send_history.json",
    normalize_phone_numbers: bool = True,
    batch_root_dir: str | Path = "imessage_batches",
    batch_date: str | None = None,
    api_config: BlueBubblesAPIConfig | None = None,
    delivery_check_timeout_seconds: int = 45,
    delivery_check_interval_seconds: int = 3,
) -> list[SendResult]:
    api_config = api_config or BlueBubblesAPIConfig()
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
    queued_recipients = unique_recipients
    skip_statuses_in_batch = {
        "confirmed_in_imsg",
        "confirmed_in_bluebubbles",
        "sent_not_confirmed",
        "delivered",
    }
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

    if not queued_recipients:
        return results

    _validate_bluebubbles_server(api_config)

    for recipient in queued_recipients:
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
            send_context = send_imessage_once(
                recipient,
                message,
                api_config=api_config,
            )

            delivery_status = _confirm_delivery_status(
                recipient,
                message,
                send_context=send_context,
                api_config=api_config,
                timeout_seconds=delivery_check_timeout_seconds,
                interval_seconds=delivery_check_interval_seconds,
            )
            if delivery_status.status == "confirmed_in_bluebubbles":
                history.setdefault("messages", []).append(
                    {
                        "phone": recipient,
                        "timestamp": str(time.time()),
                        "message": message,
                    }
                )
                _save_history(state_file, history)

            result_detail = delivery_status.detail
            result_error = delivery_status.error
            transport_status = "sent"
            raw_error = delivery_status.raw_error
            if send_context.transport_error:
                if delivery_status.status == "confirmed_in_bluebubbles":
                    result_detail = (
                        f"{delivery_status.detail} "
                        f"发送接口曾返回异常，但消息已确认发出：{send_context.transport_error}"
                    )
                    transport_status = "sent_with_api_error"
                else:
                    transport_status = "api_error"
                    result_error = send_context.transport_error
                    raw_error = send_context.transport_error

            results.append(
                SendResult(
                    phone=recipient,
                    status=delivery_status.status,
                    detail=result_detail,
                    error=result_error,
                )
            )
            _append_batch_event(
                batch_events_path,
                {
                    "event_type": "delivery_status",
                    "recipient": recipient,
                    "message": message,
                    "delivery_status": delivery_status.status,
                    "detail": result_detail,
                    "error": result_error,
                    "transport_error": send_context.transport_error,
                },
            )
            _upsert_batch_record(
                batch_records_by_key,
                recipient=recipient,
                message=message,
                transport_status=transport_status,
                delivery_status=delivery_status.status,
                detail=result_detail,
                attempted_at=attempted_at,
                error=result_error,
                raw_error=raw_error,
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

    return results
