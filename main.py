from __future__ import annotations

import argparse
import os
import sys
from dataclasses import replace
from typing import Any

from dotenv import load_dotenv

from api_server import run_api_server
from config import APP_CONFIG
from imessage_sender import send_imessages


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="通过 BlueBubbles 发送 iMessage")
    subparsers = parser.add_subparsers(dest="command")

    send_parser = subparsers.add_parser("send", help="发送单条 iMessage")
    send_parser.add_argument("--recipient", required=True, help="目标号码或 iMessage 标识")
    send_parser.add_argument("--message", default="", help="发送文案；未提供时使用 IMESSAGE_TEXT")
    send_parser.add_argument("--batch-date", default="", help="可选：指定批次目录日期（YYYY-MM-DD）")

    serve_parser = subparsers.add_parser("serve", help="启动本地 HTTP 服务")
    serve_parser.add_argument("--host", default="", help="监听地址；未提供时使用 API_HOST")
    serve_parser.add_argument("--port", default=0, type=int, help="监听端口；未提供时使用 API_PORT")
    return parser


def _normalize_argv(argv: list[str]) -> list[str]:
    if len(argv) <= 1:
        return ["send"]
    first = argv[1]
    if first in {"send", "serve", "-h", "--help"}:
        return argv[1:]
    return ["send", *argv[1:]]


def _get_optional_env(name: str) -> str | None:
    value = os.getenv(name, "").strip()
    return value or None


def _get_optional_int_env(name: str) -> int | None:
    raw = _get_optional_env(name)
    if raw is None:
        return None
    try:
        return int(raw)
    except ValueError as exc:
        raise ValueError(f"{name} 必须是整数，当前值: {raw}") from exc


def _load_runtime_bluebubbles_config(app_config: Any) -> Any:
    base_config = getattr(app_config, "imessage_bluebubbles_config")
    overrides: dict[str, Any] = {}

    base_url = _get_optional_env("BLUEBUBBLES_BASE_URL")
    if base_url:
        overrides["base_url"] = base_url

    password = _get_optional_env("BLUEBUBBLES_PASSWORD")
    if password:
        overrides["password"] = password

    auth_param_name = _get_optional_env("BLUEBUBBLES_AUTH_PARAM_NAME")
    if auth_param_name:
        overrides["auth_param_name"] = auth_param_name

    send_timeout = _get_optional_int_env("BLUEBUBBLES_SEND_TIMEOUT_SECONDS")
    if send_timeout is not None:
        if send_timeout <= 0:
            raise ValueError("BLUEBUBBLES_SEND_TIMEOUT_SECONDS 必须大于 0")
        overrides["send_timeout_seconds"] = send_timeout

    read_timeout = _get_optional_int_env("BLUEBUBBLES_READ_TIMEOUT_SECONDS")
    if read_timeout is not None:
        if read_timeout <= 0:
            raise ValueError("BLUEBUBBLES_READ_TIMEOUT_SECONDS 必须大于 0")
        overrides["read_timeout_seconds"] = read_timeout

    recent_messages_limit = _get_optional_int_env("BLUEBUBBLES_RECENT_MESSAGES_LIMIT")
    if recent_messages_limit is not None:
        if recent_messages_limit <= 0:
            raise ValueError("BLUEBUBBLES_RECENT_MESSAGES_LIMIT 必须大于 0")
        overrides["recent_messages_limit"] = recent_messages_limit

    verify_ssl = _get_optional_env("BLUEBUBBLES_VERIFY_SSL")
    if verify_ssl:
        overrides["verify_ssl"] = verify_ssl.strip().lower() not in {"0", "false", "no", "off"}

    if not overrides:
        return base_config
    return replace(base_config, **overrides)


def _load_runtime_app_config() -> Any:
    overrides: dict[str, Any] = {}

    imessage_text_override = _get_optional_env("IMESSAGE_TEXT")
    if imessage_text_override:
        overrides["imessage_text"] = imessage_text_override.replace("\\n", "\n")

    timeout_override = _get_optional_int_env("IMESSAGE_DELIVERY_CHECK_TIMEOUT_SECONDS")
    if timeout_override is not None:
        if timeout_override <= 0:
            raise ValueError("IMESSAGE_DELIVERY_CHECK_TIMEOUT_SECONDS 必须大于 0")
        overrides["imessage_delivery_check_timeout_seconds"] = timeout_override

    api_host = _get_optional_env("API_HOST")
    if api_host:
        overrides["api_host"] = api_host

    api_port = _get_optional_int_env("API_PORT")
    if api_port is not None:
        if api_port <= 0:
            raise ValueError("API_PORT 必须大于 0")
        overrides["api_port"] = api_port

    bluebubbles_config = _load_runtime_bluebubbles_config(APP_CONFIG)
    if bluebubbles_config != APP_CONFIG.imessage_bluebubbles_config:
        overrides["imessage_bluebubbles_config"] = bluebubbles_config

    if not overrides:
        return APP_CONFIG
    return replace(APP_CONFIG, **overrides)


def _run_send_command(args: argparse.Namespace, app_config: Any) -> int:
    message = args.message.strip() or app_config.imessage_text
    batch_date = args.batch_date.strip() or None
    results = send_imessages(
        [args.recipient.strip()],
        message=message,
        state_path=app_config.imessage_state_path,
        normalize_phone_numbers=False,
        batch_root_dir=app_config.imessage_batch_root_dir,
        batch_date=batch_date,
        api_config=app_config.imessage_bluebubbles_config,
        delivery_check_timeout_seconds=app_config.imessage_delivery_check_timeout_seconds,
        delivery_check_interval_seconds=app_config.imessage_delivery_check_interval_seconds,
    )
    for result in results:
        if result.error:
            print(f"[{result.status}] {result.phone} - {result.detail} (error={result.error})")
        else:
            print(f"[{result.status}] {result.phone} - {result.detail}")
    return 0


def _run_serve_command(args: argparse.Namespace, app_config: Any) -> int:
    overrides: dict[str, Any] = {}
    if args.host.strip():
        overrides["api_host"] = args.host.strip()
    if args.port:
        overrides["api_port"] = args.port
    if overrides:
        app_config = replace(app_config, **overrides)
    run_api_server(app_config)
    return 0


def main(argv: list[str] | None = None) -> int:
    load_dotenv()
    raw_argv = sys.argv if argv is None else ["main.py", *argv]
    parser = _build_parser()
    args = parser.parse_args(_normalize_argv(raw_argv))

    try:
        app_config = _load_runtime_app_config()
        command = args.command or "send"
        if command == "serve":
            return _run_serve_command(args, app_config)
        return _run_send_command(args, app_config)
    except Exception as exc:  # noqa: BLE001
        print(f"执行失败: {exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
