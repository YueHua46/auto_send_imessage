from __future__ import annotations

import json
import os
from datetime import datetime, timedelta
from pathlib import Path

from dotenv import load_dotenv

from config import APP_CONFIG
from imessage_sender import send_imessages_with_risk_control
from lingxing_result import (
    create_lingxing_session_from_login,
    export_and_download_order_management_report,
    extract_phone_numbers_from_order_records,
    parse_order_management_export_file,
    switch_lingxing_to_multi_platform,
)


def _get_required_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise ValueError(f"Missing required environment variable: {name}")
    return value


def _get_optional_env(name: str) -> str | None:
    value = os.getenv(name, "").strip()
    return value or None


def _load_lingxing_web_login_config() -> dict[str, str] | None:
    account = _get_optional_env("LINGXING_WEB_ACCOUNT")
    password = _get_optional_env("LINGXING_WEB_PASSWORD")

    if not account and not password:
        return None
    if not account or not password:
        raise ValueError(
            "LINGXING_WEB_ACCOUNT 和 LINGXING_WEB_PASSWORD 需要同时配置，或同时留空"
        )

    return {
        "account": account,
        "password": password,
    }


def _load_imessage_test_usernames() -> list[str]:
    raw = os.getenv("IMESSAGE_TEST_USERNAMES", "").strip()
    if not raw:
        return []
    normalized = raw.replace("，", ",").replace(";", ",").replace("\n", ",")
    usernames = [item.strip() for item in normalized.split(",")]
    return [name for name in usernames if name]


def write_json(path: str | Path, payload: object) -> None:
    Path(path).write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def load_runtime_config() -> dict[str, object]:
    load_dotenv()
    return {
        "web_login": _load_lingxing_web_login_config(),
        "imessage_test_usernames": _load_imessage_test_usernames(),
        "app_config": APP_CONFIG,
    }


def _build_order_export_range(lookback_days: int) -> tuple[str, str]:
    """
    对齐 HAR：
    - 开始时间：向前回溯 N 天的 00:00:00
    - 结束时间：当天 23:59:59
    """
    today = datetime.now()
    days_back = max(lookback_days - 1, 0)
    start_dt = today - timedelta(days=days_back)
    return (
        start_dt.strftime("%Y-%m-%d 00:00:00"),
        today.strftime("%Y-%m-%d 23:59:59"),
    )


def main() -> int:
    try:
        config = load_runtime_config()
    except Exception as exc:  # noqa: BLE001
        print(f"配置读取失败: {exc}")
        return 2

    web_login = config.get("web_login")
    if not isinstance(web_login, dict):
        print(
            "未检测到网页登录配置，请在 .env 中同时配置 "
            "LINGXING_WEB_ACCOUNT 和 LINGXING_WEB_PASSWORD"
        )
        return 2

    try:
        app_config = config["app_config"]  # type: ignore[assignment]
        web_session = create_lingxing_session_from_login(
            account=str(web_login["account"]),
            password=str(web_login["password"]),
            debug=app_config.print_request_debug,
        )
        print(
            "领星网页登录成功，已获取会话Cookie："
            f" {sorted(cookie.name for cookie in web_session.cookies)}"
        )

        switch_lingxing_to_multi_platform(
            web_session,
            debug=app_config.print_request_debug,
        )
        print("领星登录环境切换成功")

        start_time, end_time = _build_order_export_range(app_config.lookback_days)
        export_path = export_and_download_order_management_report(
            session=web_session,
            start_time=start_time,
            end_time=end_time,
            save_path=app_config.order_export_out,
            debug=app_config.print_request_debug,
        )
        print(f"订单管理导出成功: {Path(export_path).resolve()}")

        orders = parse_order_management_export_file(export_path)
        phones = extract_phone_numbers_from_order_records(orders)
        write_json(app_config.orders_out, orders)
        write_json(app_config.phones_out, phones)

        print(f"订单解析完成: {len(orders)} 条")
        print(f"提取到不重复手机号: {len(phones)}")
        print(f"订单JSON已保存至: {Path(app_config.orders_out).resolve()}")
        print(f"手机号JSON已保存至: {Path(app_config.phones_out).resolve()}")

        if not app_config.imessage_send_enabled and not app_config.imessage_dry_run:
            return 0

        imessage_test_usernames = config.get("imessage_test_usernames")
        test_usernames = (
            imessage_test_usernames
            if isinstance(imessage_test_usernames, list)
            else []
        )
        recipients = test_usernames if test_usernames else phones
        if test_usernames:
            print(
                "检测到 IMESSAGE_TEST_USERNAMES，进入测试用户名发送模式，"
                f"将按指定列表发送（{len(test_usernames)}个目标）"
            )

        send_results = send_imessages_with_risk_control(
            recipients,
            message=app_config.imessage_text,
            dry_run=app_config.imessage_dry_run,
            max_send_count=app_config.imessage_max_send_count,
            rules=app_config.imessage_risk_control,
            state_path=app_config.imessage_state_path,
            normalize_phone_numbers=not bool(test_usernames),
        )
        for result in send_results:
            print(f"[{result.status}] {result.phone} - {result.detail}")
        return 0
    except Exception as exc:  # noqa: BLE001
        print(f"领星流程执行失败: {exc}")
        return 1

if __name__ == "__main__":
    raise SystemExit(main())
