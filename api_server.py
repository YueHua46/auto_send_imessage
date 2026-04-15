from __future__ import annotations

import json
from dataclasses import asdict
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

from config import AppConfig
from imessage_sender import get_bluebubbles_server_info, send_imessages


def _json_bytes(payload: dict[str, Any]) -> bytes:
    return json.dumps(payload, ensure_ascii=False).encode("utf-8")


def _build_runtime_options(app_config: AppConfig) -> dict[str, Any]:
    return {
        "state_path": app_config.imessage_state_path,
        "normalize_phone_numbers": False,
        "batch_root_dir": app_config.imessage_batch_root_dir,
        "api_config": app_config.imessage_bluebubbles_config,
        "delivery_check_timeout_seconds": app_config.imessage_delivery_check_timeout_seconds,
        "delivery_check_interval_seconds": app_config.imessage_delivery_check_interval_seconds,
    }


def _parse_json_body(handler: BaseHTTPRequestHandler) -> dict[str, Any]:
    content_length = int(handler.headers.get("Content-Length", "0") or 0)
    raw_body = handler.rfile.read(content_length) if content_length > 0 else b""
    if not raw_body:
        return {}
    try:
        payload = json.loads(raw_body.decode("utf-8"))
    except json.JSONDecodeError as exc:
        raise ValueError(f"请求体不是合法 JSON：{exc}") from exc
    if not isinstance(payload, dict):
        raise ValueError("请求体必须是 JSON object")
    return payload


def _send_json(handler: BaseHTTPRequestHandler, status: HTTPStatus, payload: dict[str, Any]) -> None:
    body = _json_bytes(payload)
    handler.send_response(status.value)
    handler.send_header("Content-Type", "application/json; charset=utf-8")
    handler.send_header("Content-Length", str(len(body)))
    handler.end_headers()
    handler.wfile.write(body)


def create_app_handler(app_config: AppConfig) -> type[BaseHTTPRequestHandler]:
    runtime_options = _build_runtime_options(app_config)

    class AppHandler(BaseHTTPRequestHandler):
        server_version = "AutoSendIMessageHTTP/1.0"

        def log_message(self, format: str, *args: object) -> None:
            return None

        def do_GET(self) -> None:  # noqa: N802
            if self.path.rstrip("/") == "/health":
                self._handle_health()
                return
            _send_json(
                self,
                HTTPStatus.NOT_FOUND,
                {"ok": False, "error": "未找到对应接口"},
            )

        def do_POST(self) -> None:  # noqa: N802
            if self.path.rstrip("/") == "/send":
                self._handle_send(is_batch=False)
                return
            if self.path.rstrip("/") == "/send/batch":
                self._handle_send(is_batch=True)
                return
            _send_json(
                self,
                HTTPStatus.NOT_FOUND,
                {"ok": False, "error": "未找到对应接口"},
            )

        def _handle_health(self) -> None:
            try:
                info = get_bluebubbles_server_info(
                    api_config=app_config.imessage_bluebubbles_config,
                )
            except Exception as exc:  # noqa: BLE001
                _send_json(
                    self,
                    HTTPStatus.SERVICE_UNAVAILABLE,
                    {"ok": False, "error": str(exc)},
                )
                return

            _send_json(
                self,
                HTTPStatus.OK,
                {
                    "ok": True,
                    "service": {
                        "host": app_config.api_host,
                        "port": app_config.api_port,
                    },
                    "bluebubbles": info,
                },
            )

        def _handle_send(self, *, is_batch: bool) -> None:
            try:
                payload = _parse_json_body(self)
                message = str(payload.get("message", "") or "").strip() or app_config.imessage_text
                batch_date = str(payload.get("batch_date", "") or "").strip() or None
                if is_batch:
                    recipients = payload.get("recipients")
                    if not isinstance(recipients, list):
                        raise ValueError("`recipients` 必须是数组")
                    prepared = [str(item).strip() for item in recipients if str(item).strip()]
                else:
                    recipient = str(payload.get("recipient", "") or "").strip()
                    if not recipient:
                        raise ValueError("`recipient` 不能为空")
                    prepared = [recipient]

                if not prepared:
                    raise ValueError("至少需要一个有效收件人")

                results = send_imessages(
                    prepared,
                    message=message,
                    batch_date=batch_date,
                    **runtime_options,
                )
            except ValueError as exc:
                _send_json(
                    self,
                    HTTPStatus.BAD_REQUEST,
                    {"ok": False, "error": str(exc)},
                )
                return
            except Exception as exc:  # noqa: BLE001
                _send_json(
                    self,
                    HTTPStatus.INTERNAL_SERVER_ERROR,
                    {"ok": False, "error": str(exc)},
                )
                return

            _send_json(
                self,
                HTTPStatus.OK,
                {
                    "ok": True,
                    "message": message,
                    "count": len(results),
                    "results": [asdict(item) for item in results],
                },
            )

    return AppHandler


def run_api_server(app_config: AppConfig) -> None:
    server = ThreadingHTTPServer(
        (app_config.api_host, app_config.api_port),
        create_app_handler(app_config),
    )
    print(
        f"HTTP service listening on http://{app_config.api_host}:{app_config.api_port}",
        flush=True,
    )
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
