from __future__ import annotations

import json
import random
import sys
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import requests

try:
    from openai import OpenAI
except Exception:
    OpenAI = None

class IMessageSendError(RuntimeError):
    """当iMessage发送失败时抛出该异常"""

@dataclass(slots=True)
class IMessageRiskControl:
    startup_delay_min_seconds: int = 20
    startup_delay_max_seconds: int = 90
    min_delay_between_messages_seconds: int = 35
    max_delay_between_messages_seconds: int = 80
    batch_size: int = 5
    batch_pause_min_seconds: int = 180
    batch_pause_max_seconds: int = 420
    daily_limit: int = 20
    cooldown_hours: int = 72
    active_hours_start: int = 9
    active_hours_end: int = 20

@dataclass(slots=True)
class SendResult:
    phone: str
    status: str
    detail: str
    error: str | None = None

class IMessageLLMRewriter:
    def __init__(
        self,
        *,
        base_url: str,
        api_key: str,
        model: str,
        timeout_seconds: int = 20,
        temperature: float = 0.9,
    ) -> None:
        if OpenAI is None:
            raise IMessageSendError("未安装 openai 依赖，请先安装 requirements.txt 中的新依赖。")
        self._client = OpenAI(
            api_key=api_key.strip(),
            base_url=base_url.strip(),
            timeout=max(timeout_seconds, 1),
        )
        self._model = model.strip()
        self._temperature = max(0.0, min(temperature, 1.8))

    def rewrite(self, original_text: str) -> str:
        source = original_text.strip()
        if not source:
            return original_text
        prompt = (
            "你是 iMessage 文案优化助手。请改写下面的消息，要求：\n"
            "1) 保持核心主题 and 意图不变，必须与原文契合；\n"
            "2) 用词、语序、句式随机变化，避免重复模板感；\n"
            "3) 保持自然、礼貌、可直接发送；\n"
            "4) 禁止添加解释、标题、引号、编号、前后缀说明；\n"
            "5) 尽量保持与原文接近的长度与段落结构。\n\n"
            f"原文：\n{source}"
        )
        response = self._client.chat.completions.create(
            model=self._model,
            temperature=self._temperature,
            messages=[
                {"role": "system", "content": "你只输出可直接发送的最终消息正文。"},
                {"role": "user", "content": prompt},
            ],
        )
        rewritten = (response.choices[0].message.content or "").strip()
        rewritten = rewritten.strip().strip('"').strip("'").strip()
        return rewritten or original_text

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
    line = json.dumps(
        {
            "event_at": _now_iso(),
            **payload,
        },
        ensure_ascii=False,
    )
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
    cooling_down_phones: set[str] = set()
    for entry in history.get("messages", []):
        sent_ts = float(entry["timestamp"])
        if now_ts - sent_ts <= 24 * 60 * 60:
            recent_day_entries.append(entry)
        if now_ts - sent_ts <= rules.cooldown_hours * 60 * 60:
            cooling_down_phones.add(entry["phone"])
    return recent_day_entries, cooling_down_phones

def send_via_bluebubbles(
    recipient: str,
    message: str,
    base_url: str,
    password: str,
    auth_param_name: str = "guid",
    verify_ssl: bool = True,
) -> dict[str, Any]:
    """通过 BlueBubbles REST API 发送消息"""
    url = f"{base_url.rstrip('/')}/api/v1/message/text"
    payload = {
        "chatGuid": recipient,  # BlueBubbles 通常需要完整的 chatGuid，但单聊也可以直接传手机号
        "tempGuid": f"temp-{int(time.time()*1000)}",
        "message": message,
        "method": "apple-script" # 或者 "private-api"
    }
    params = {auth_param_name: password}
    
    response = requests.post(url, json=payload, params=params, verify=verify_ssl, timeout=30)
    response.raise_for_status()
    return response.json()

def check_bluebubbles_delivery(
    base_url: str,
    password: str,
    recipient: str,
    message: str,
    auth_param_name: str = "guid",
    verify_ssl: bool = True,
    limit: int = 25
) -> dict[str, Any]:
    """通过 BlueBubbles REST API 检查消息送达状态"""
    url = f"{base_url.rstrip('/')}/api/v1/message"
    params = {
        auth_param_name: password,
        "limit": limit,
        "with_handle": "true"
    }
    
    response = requests.get(url, params=params, verify=verify_ssl, timeout=30)
    response.raise_for_status()
    data = response.json()
    
    messages = data.get("data", [])
    for msg in messages:
        # 简单的匹配逻辑：匹配收件人和消息内容
        handle = msg.get("handle", {})
        msg_recipient = handle.get("address", "")
        msg_text = msg.get("text", "")
        
        if _recipient_matches(msg_recipient, recipient) and (message in msg_text or msg_text in message):
            date_delivered = msg.get("dateDelivered")
            date_read = msg.get("dateRead")
            error = msg.get("error")
            
            if error:
                return {"status": "failed", "detail": f"BlueBubbles 报告错误: {error}"}
            if date_read:
                return {"status": "delivered", "detail": "消息已读"}
            if date_delivered:
                return {"status": "delivered", "detail": "消息已送达"}
            return {"status": "sent_not_confirmed", "detail": "消息已发送，待送达"}
            
    return {"status": "sent_not_confirmed", "detail": "BlueBubbles 记录中未找到匹配消息"}

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
    delivery_check_timeout_seconds: int = 45,
    delivery_check_interval_seconds: int = 3,
    delivery_check_lookback_seconds: int = 600,
    llm_rewrite_enabled: bool = False,
    openai_base_url: str | None = None,
    openai_api_key: str | None = None,
    openai_model: str = "gemini-2.0-flash",
    openai_timeout_seconds: int = 20,
    openai_temperature: float = 0.9,
    # BlueBubbles 相关参数
    bluebubbles_base_url: str | None = None,
    bluebubbles_password: str | None = None,
    bluebubbles_auth_param_name: str = "guid",
    bluebubbles_verify_ssl: bool = True,
) -> list[SendResult]:
    if not bluebubbles_base_url or not bluebubbles_password:
        raise IMessageSendError("未配置 BlueBubbles API 地址或密码。")

    rules = rules or IMessageRiskControl()
    if normalize_phone_numbers:
        normalized_unique_phones = list(dict.fromkeys(_normalize_phone_number(phone) for phone in phones))
        unique_phones = [phone for phone in normalized_unique_phones if phone]
    else:
        unique_phones = list(dict.fromkeys((phone or "").strip() for phone in phones if (phone or "").strip()))
    
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
    recent_day_entries, cooling_down_phones = _recent_history_entries(history, now_ts=now_ts, rules=rules)
    if len(recent_day_entries) >= rules.daily_limit:
        raise IMessageSendError(f"今日已达发送上限：过去24小时内已发送{len(recent_day_entries)}条消息。")

    send_budget = rules.daily_limit - len(recent_day_entries)
    if max_send_count is not None:
        send_budget = min(send_budget, max_send_count)

    rewriter: IMessageLLMRewriter | None = None
    if llm_rewrite_enabled:
        if not openai_base_url or not openai_api_key:
            raise IMessageSendError("已启用 LLM 改写，但 OPENAI_BASE_URL 或 OPENAI_API_KEY 未配置。")
        rewriter = IMessageLLMRewriter(
            base_url=openai_base_url,
            api_key=openai_api_key,
            model=openai_model,
            timeout_seconds=openai_timeout_seconds,
            temperature=openai_temperature,
        )

    queued_phones = [phone for phone in unique_phones if phone not in cooling_down_phones]
    skip_statuses_in_batch = {"delivered", "sent_not_confirmed"}
    already_processed_in_batch = {
        str(record.get("recipient", "")).strip()
        for record in batch_records_by_key.values()
        if str(record.get("delivery_status", "")) in skip_statuses_in_batch
        and (rewriter is not None or str(record.get("message", "")) == message)
    }

    results: list[SendResult] = []
    for phone in queued_phones:
        if phone in already_processed_in_batch:
            detail = f"跳过：该批次中收件人已存在非失败状态记录（{batch_date}）"
            results.append(SendResult(phone=phone, status="skipped_processed", detail=detail))
            _append_batch_event(batch_events_path, {"event_type": "skip_processed", "recipient": phone, "message": message, "detail": detail})

    queued_phones = [phone for phone in queued_phones if phone not in already_processed_in_batch]

    if dry_run:
        dry_run_results: list[SendResult] = []
        for phone in queued_phones[:send_budget]:
            message_to_send = message
            if rewriter is not None:
                try:
                    message_to_send = rewriter.rewrite(message)
                except Exception:
                    message_to_send = message
            dry_run_results.append(SendResult(phone=phone, status="dry_run", detail=f'将会发送 "{message_to_send}"'))
        return results + dry_run_results

    time.sleep(random.randint(rules.startup_delay_min_seconds, rules.startup_delay_max_seconds))

    sent_in_this_run = 0
    for index, phone in enumerate(queued_phones):
        if sent_in_this_run >= send_budget:
            results.append(SendResult(phone=phone, status="skipped", detail="跳过，因为本次发送额度已用尽"))
            continue

        if sent_in_this_run and sent_in_this_run % rules.batch_size == 0:
            batch_pause = random.randint(rules.batch_pause_min_seconds, rules.batch_pause_max_seconds)
            time.sleep(batch_pause)

        attempted_at = _now_iso()
        message_to_send = message
        if rewriter is not None:
            try:
                message_to_send = rewriter.rewrite(message)
            except Exception as rewrite_exc:
                _append_batch_event(batch_events_path, {"event_type": "llm_rewrite_failed", "recipient": phone, "message": message, "detail": f"LLM改写失败，回退原文：{rewrite_exc}"})
                message_to_send = message
        
        _append_batch_event(batch_events_path, {"event_type": "send_attempt", "recipient": phone, "message": message_to_send, "attempted_at": attempted_at})

        try:
            # 调用 BlueBubbles API 发送
            send_via_bluebubbles(
                recipient=phone,
                message=message_to_send,
                base_url=bluebubbles_base_url,
                password=bluebubbles_password,
                auth_param_name=bluebubbles_auth_param_name,
                verify_ssl=bluebubbles_verify_ssl
            )
            
            sent_in_this_run += 1
            history.setdefault("messages", []).append({"phone": phone, "timestamp": str(time.time()), "message": message_to_send})
            _save_history(state_file, history)
            
            # 轮询检查状态
            delivery_status_info = {"status": "sent_not_confirmed", "detail": "尚未开始状态回查"}
            deadline = time.time() + delivery_check_timeout_seconds
            while time.time() < deadline:
                try:
                    status_res = check_bluebubbles_delivery(
                        base_url=bluebubbles_base_url,
                        password=bluebubbles_password,
                        recipient=phone,
                        message=message_to_send,
                        auth_param_name=bluebubbles_auth_param_name,
                        verify_ssl=bluebubbles_verify_ssl
                    )
                    delivery_status_info = status_res
                    if delivery_status_info["status"] in {"delivered", "failed"}:
                        break
                except Exception as e:
                    delivery_status_info = {"status": "sent_not_confirmed", "detail": f"检查状态出错: {e}"}
                time.sleep(delivery_check_interval_seconds)

            results.append(SendResult(phone=phone, status=delivery_status_info["status"], detail=delivery_status_info["detail"]))
            _append_batch_event(batch_events_path, {"event_type": "delivery_status", "recipient": phone, "message": message_to_send, "delivery_status": delivery_status_info["status"], "detail": delivery_status_info["detail"]})
            _upsert_batch_record(batch_records_by_key, recipient=phone, message=message_to_send, transport_status="sent", delivery_status=delivery_status_info["status"], detail=delivery_status_info["detail"], attempted_at=attempted_at)
            _save_batch_results(batch_results_path, batch_date=batch_date, batch_records_by_key=batch_records_by_key)
            
        except Exception as exc:
            error_detail = str(exc)
            results.append(SendResult(phone=phone, status="failed", detail=error_detail, error=error_detail))
            _append_batch_event(batch_events_path, {"event_type": "send_failed", "recipient": phone, "message": message_to_send, "detail": error_detail})
            _upsert_batch_record(batch_records_by_key, recipient=phone, message=message_to_send, transport_status="failed", delivery_status="failed", detail=error_detail, attempted_at=attempted_at, error=error_detail, raw_error=error_detail)
            _save_batch_results(batch_results_path, batch_date=batch_date, batch_records_by_key=batch_records_by_key)

        if index < len(queued_phones) - 1 and sent_in_this_run < send_budget:
            time.sleep(random.randint(rules.min_delay_between_messages_seconds, rules.max_delay_between_messages_seconds))

    return results

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
