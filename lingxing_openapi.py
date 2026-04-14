from __future__ import annotations

import base64
import hashlib
import json
import time
from urllib.parse import urlencode
from dataclasses import dataclass
from typing import Any, Iterator

import orjson
import requests
from Crypto.Cipher import AES

# 领星OpenAPI接口主机地址
OPENAPI_HOST = "https://openapi.lingxing.com"
# 查询时间窗口最大允许跨度（天）
MAX_QUERY_SPAN_DAYS = 31
# 默认每页查询订单数量
DEFAULT_PAGE_SIZE = 200
# 默认待审核和待发货订单状态码
PENDING_REVIEW_AND_SHIPMENT_STATUSES = (4, 5)
# 可能出现手机号的key名
PHONE_KEYWORDS = (
    "phone",
    "mobile",
    "tel",
    "recipientphone",
    "recipient_phone",
    "consigneephone",
    "consignee_phone",
    "customerphone",
    "customer_phone",
    "buyerphone",
    "buyer_phone",
)

class LingxingAPIError(RuntimeError):
    """当领星OpenAPI返回业务级错误时抛出"""

@dataclass(slots=True)
class AccessTokenData:
    """AccessToken数据结构"""
    access_token: str
    refresh_token: str
    expires_in: int

def _do_pad(text: str) -> str:
    """对明文字符串做PKCS7补全，满足AES加密块大小要求"""
    block_size = AES.block_size
    pad_len = block_size - len(text) % block_size
    return text + pad_len * chr(pad_len)

def _aes_encrypt(key: str, data: str) -> str:
    """对字符串进行AES加密后Base64编码"""
    cipher = AES.new(key.encode("utf-8"), AES.MODE_ECB)
    encrypted = cipher.encrypt(_do_pad(data).encode("utf-8"))
    return base64.b64encode(encrypted).decode("utf-8")

def _md5_encrypt(text: str) -> str:
    """对字符串进行MD5加密，输出16进制字符串"""
    return hashlib.md5(text.encode("utf-8")).hexdigest()

def _format_sign_value(value: Any) -> str | None:
    """格式化参与签名的字段值，dict/list严格用orjson序列化"""
    if value == "":
        return None
    if isinstance(value, (dict, list, bool)) or value is None:
        return orjson.dumps(value, option=orjson.OPT_SORT_KEYS).decode("utf-8")
    return str(value)

def build_sign_payload(params: dict[str, Any]) -> str:
    """
    构造参与签名的字符串（key升序拼接，格式为key=value，以&连接）
    """
    canonical_parts: list[str] = []
    for key in sorted(params):
        formatted_value = _format_sign_value(params[key])
        if formatted_value is None:
            continue
        canonical_parts.append(f"{key}={formatted_value}")
    return "&".join(canonical_parts)

def generate_sign(app_key: str, params: dict[str, Any]) -> str:
    """
    按照应用key和所有参数生成签名（MD5后，再使用AES加密）
    """
    canonical_querystring = build_sign_payload(params)
    md5_str = _md5_encrypt(canonical_querystring).upper()
    return _aes_encrypt(app_key, md5_str)

def _normalize_time_window(start_ts: int, end_ts: int) -> tuple[int, int]:
    """
    校验时间窗口是否合法，且不超过最大查询区间
    """
    if start_ts >= end_ts:
        raise ValueError("start_ts 必须早于 end_ts")
    max_seconds = MAX_QUERY_SPAN_DAYS * 24 * 60 * 60
    if end_ts - start_ts > max_seconds:
        raise ValueError("领星订单列表API单次仅允许最长31天时间区间")
    return start_ts, end_ts

def split_time_windows(end_ts: int, lookback_days: int, window_days: int = MAX_QUERY_SPAN_DAYS) -> list[tuple[int, int]]:
    """
    把较长的时间段切分为多个不超过window_days（默认31天）的窗口，便于分批拉取
    :param end_ts: 查询结束时间戳（一般为当前时间）
    :param lookback_days: 需要往前回看天数
    :param window_days: 每个时间窗口最多天数
    :return: 时间窗口元组(start_ts, end_ts)列表，按升序排列
    """
    if lookback_days <= 0:
        raise ValueError("lookback_days 必须大于0")
    if window_days <= 0:
        raise ValueError("window_days 必须大于0")
    windows: list[tuple[int, int]] = []
    cursor_end = end_ts
    earliest_start = end_ts - lookback_days * 24 * 60 * 60
    chunk_seconds = window_days * 24 * 60 * 60

    while cursor_end > earliest_start:
        # 窗口起点不能更早于回溯区间
        cursor_start = max(earliest_start, cursor_end - chunk_seconds + 1)
        windows.append((cursor_start, cursor_end))
        cursor_end = cursor_start - 1

    windows.reverse()
    return windows

class LingxingOpenAPIClient:
    """
    领星OpenAPI客户端，为主要的API功能封装
    """

    def __init__(
        self,
        app_key: str,
        app_secret: str,
        *,
        host: str = OPENAPI_HOST,
        timeout: int = 60,
        print_request_debug: bool = False,
        session: requests.Session | None = None,
    ) -> None:
        self.app_key = app_key
        self.app_secret = app_secret
        self.host = host.rstrip("/")
        self.timeout = timeout
        self.print_request_debug = print_request_debug
        self.session = session or requests.Session()
        self.session.trust_env = False
        self._access_token: AccessTokenData | None = None

    def _debug_print(
        self,
        *,
        method: str,
        url: str,
        query_params: dict[str, Any] | None = None,
        body: dict[str, Any] | None = None,
        response: requests.Response | None = None,
        response_text: str | None = None,
    ) -> None:
        if not self.print_request_debug:
            return

        print("\n================ LINGXING REQUEST DEBUG ================")
        print(f"METHOD: {method.upper()}")
        print(f"URL: {url}")
        if query_params:
            print("QUERY PARAMS:")
            print(json.dumps(query_params, ensure_ascii=False, indent=2))
            print("FULL URL:")
            print(f"{url}?{urlencode(query_params, doseq=True)}")
        if body:
            print("BODY:")
            print(json.dumps(body, ensure_ascii=False, indent=2))
        if query_params is None and body is None:
            print("BODY: <empty>")
        if response is not None:
            print(f"HTTP STATUS: {response.status_code}")
            print("RESPONSE HEADERS:")
            print(dict(response.headers))
        if response_text is not None:
            print("RESPONSE TEXT:")
            print(response_text)
        print("=======================================================\n")

    def generate_access_token(self) -> AccessTokenData:
        """
        获取access token，自动刷新本地token信息
        """
        url = f"{self.host}/api/auth-server/oauth/access-token"
        form_data = {
            "appId": self.app_key,
            "appSecret": self.app_secret,
        }
        response = None
        try:
            response = self.session.post(
                url,
                files={
                    "appId": (None, self.app_key),
                    "appSecret": (None, self.app_secret),
                },
                timeout=self.timeout,
            )
            response_text = response.text
            self._debug_print(
                method="POST",
                url=url,
                body=form_data,
                response=response,
                response_text=response_text,
            )
            response.raise_for_status()
        except Exception:
            self._debug_print(
                method="POST",
                url=url,
                body=form_data,
                response=response,
                response_text=response.text if response is not None else None,
            )
            raise
        payload = response.json()
        if str(payload.get("code")) != "200":
            raise LingxingAPIError(f"获取access token失败: {payload}")

        data = payload["data"]
        token_data = AccessTokenData(
            access_token=data["access_token"],
            refresh_token=data["refresh_token"],
            expires_in=int(data["expires_in"]),
        )
        self._access_token = token_data
        return token_data

    def ensure_access_token(self) -> AccessTokenData:
        """
        确保已经有可用的access_token，没有则自动获取
        """
        if self._access_token is None:
            return self.generate_access_token()
        return self._access_token

    def request(
        self,
        route_name: str,
        *,
        method: str = "POST",
        query_params: dict[str, Any] | None = None,
        body: dict[str, Any] | None = None,
        access_token: str | None = None,
    ) -> dict[str, Any]:
        """
        通用接口请求（自动加签名、token等公共参数）
        """
        token = access_token or self.ensure_access_token().access_token
        query_params = query_params or {}
        body = body or {}

        sign_params = dict(body)
        sign_params.update(query_params)
        public_params = {
            "app_key": self.app_key,
            "access_token": token,
            "timestamp": str(int(time.time())),
        }
        sign_params.update(public_params)
        sign_payload = build_sign_payload(sign_params)
        public_params["sign"] = generate_sign(self.app_key, sign_params)

        all_query_params = dict(query_params)
        all_query_params.update(public_params)
        request_data = orjson.dumps(body, option=orjson.OPT_SORT_KEYS) if body else None
        url = f"{self.host}{route_name}"
        response = None
        try:
            response = self.session.request(
                method=method.upper(),
                url=url,
                params=all_query_params,
                data=request_data,
                headers={"Content-Type": "application/json"} if body else None,
                timeout=self.timeout,
            )
            response_text = response.text
            self._debug_print(
                method=method,
                url=url,
                query_params=all_query_params,
                body=body,
                response=response,
                response_text=response_text,
            )
            if self.print_request_debug:
                print("SIGN PAYLOAD:")
                print(sign_payload)
            response.raise_for_status()
        except Exception:
            self._debug_print(
                method=method,
                url=url,
                query_params=all_query_params,
                body=body,
                response=response,
                response_text=response.text if response is not None else None,
            )
            if self.print_request_debug:
                print("SIGN PAYLOAD:")
                print(sign_payload)
            raise
        payload = response.json()
        if int(payload.get("code", -1)) != 0:
            raise LingxingAPIError(f"领星接口请求失败: {json.dumps(payload, ensure_ascii=False)}")
        return payload

    def query_order_page(
        self,
        *,
        order_status: int,
        start_time: int,
        end_time: int,
        offset: int = 0,
        length: int = DEFAULT_PAGE_SIZE,
        date_type: str = "update_time",
        include_delete: bool = False,
    ) -> dict[str, Any]:
        """
        分页拉取订单列表（单页）
        :param order_status: 订单状态码
        :param start_time: 查询起始时间戳
        :param end_time: 查询结束时间戳
        :param offset: 分页偏移
        :param length: 返回页大小
        :param date_type: 时间类型
        :param include_delete: 是否包含已删除订单
        :return: 接口返回json
        """
        _normalize_time_window(start_time, end_time)
        body = {
            "offset": offset,
            "length": length,
            "date_type": date_type,
            "start_time": start_time,
            "end_time": end_time,
            "order_status": order_status,
            "include_delete": include_delete,
        }
        return self.request("/pb/mp/order/v2/list", method="POST", body=body)

    def iter_orders(
        self,
        *,
        order_status: int,
        start_time: int,
        end_time: int,
        page_size: int = DEFAULT_PAGE_SIZE,
        include_delete: bool = False,
    ) -> Iterator[dict[str, Any]]:
        """
        自动分页遍历一段时间区间内的所有订单
        """
        offset = 0
        while True:
            page = self.query_order_page(
                order_status=order_status,
                start_time=start_time,
                end_time=end_time,
                offset=offset,
                length=page_size,
                include_delete=include_delete,
            )
            items = page.get("data", {}).get("list", []) or []
            for item in items:
                yield item
            if len(items) < page_size:
                break
            offset += page_size

    def fetch_pending_review_shipment_orders(
        self,
        *,
        lookback_days: int = MAX_QUERY_SPAN_DAYS,
        now_ts: int | None = None,
        statuses: tuple[int, ...] = PENDING_REVIEW_AND_SHIPMENT_STATUSES,
        include_delete: bool = False,
        page_size: int = DEFAULT_PAGE_SIZE,
    ) -> list[dict[str, Any]]:
        """
        拉取最近lookback_days天内所有"待审核"/"待发货"订单（支持多状态/分页/去重）
        """
        if now_ts is None:
            now_ts = int(time.time())

        seen_order_ids: set[str] = set()
        results: list[dict[str, Any]] = []
        windows = split_time_windows(now_ts, lookback_days)

        for order_status in statuses:
            for start_time, end_time in windows:
                for order in self.iter_orders(
                    order_status=order_status,
                    start_time=start_time,
                    end_time=end_time,
                    page_size=page_size,
                    include_delete=include_delete,
                ):
                    # 订单唯一去重（优先global_order_no，其次reference_no/id）
                    unique_key = (
                        str(order.get("global_order_no") or "")
                        or str(order.get("reference_no") or "")
                        or str(order.get("id") or "")
                    )
                    if unique_key and unique_key in seen_order_ids:
                        continue
                    if unique_key:
                        seen_order_ids.add(unique_key)
                    results.append(order)
        return results

def _find_phone_values(node: Any) -> Iterator[str]:
    """
    递归查找任意嵌套结构里所有可能的手机号字段（命中key或数组递归下去）
    """
    if isinstance(node, dict):
        for key, value in node.items():
            normalized_key = key.lower().replace("-", "").replace(" ", "")
            if any(keyword in normalized_key for keyword in PHONE_KEYWORDS):
                if value is not None:
                    yield str(value)
            yield from _find_phone_values(value)
    elif isinstance(node, list):
        for item in node:
            yield from _find_phone_values(item)

def normalize_phone_number(phone: str) -> str | None:
    """
    规范化手机号，仅保留数字（保留开头+号），长度不足7位视为无效
    """
    if not phone:
        return None
    phone = phone.strip()
    if not phone:
        return None
    if phone.lower() in {"null", "none", "nan"}:
        return None
    has_plus = phone.startswith("+")
    digits = "".join(ch for ch in phone if ch.isdigit())
    if len(digits) < 7:
        return None
    return f"+{digits}" if has_plus else digits

def extract_customer_phones_from_orders(orders: list[dict[str, Any]]) -> list[str]:
    """
    从订单数据中提取唯一有效的手机号集合（去重，规范化）
    """
    seen: set[str] = set()
    phones: list[str] = []
    for order in orders:
        for phone in _find_phone_values(order):
            normalized = normalize_phone_number(phone)
            if not normalized or normalized in seen:
                continue
            seen.add(normalized)
            phones.append(normalized)
    return phones
