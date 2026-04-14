import base64
import hashlib
import json
import os
import time
import uuid
import zipfile
import xml.etree.ElementTree as ET
from datetime import date, timedelta
from typing import Any, Dict, Iterable, List, Mapping, Optional
from urllib.parse import quote, unquote

import requests
from Crypto.Cipher import AES


BASE_URL = "https://erp.lingxing.com"
GATEWAY_BASE_URL = "https://gw.lingxingerp.com"
GET_REPORT_DATA_PATH = "/api/download/downloadCenterReport/getReportData"
DOWNLOAD_RESOURCE_PATH = "/api/download/downloadCenterReport/downloadResource"
ORDER_LIST_EXPORT_CREATE_PATH = "/api/platforms/oms/order_list/exportCreate"
PROFIT_REPORT_CREATE_URL = (
    f"{GATEWAY_BASE_URL}/cepf-finance-report/FinanceReport/download/createDownload"
)
LOGIN_SECRET_KEY_URL = f"{GATEWAY_BASE_URL}/newadmin/api/passport/getLoginSecretKey"
LOGIN_URL = f"{GATEWAY_BASE_URL}/newadmin/api/passport/login"
SET_LOGIN_ENV_LOG_URL = f"{GATEWAY_BASE_URL}/newadmin/api/user/manage/setLoginEnvLog"
USER_MY_INFO_URL = f"{GATEWAY_BASE_URL}/newadmin/api/user/manage/myInfo"
DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/146.0.0.0 Safari/537.36"
)
ERP_REQUEST_VERSION = "3.8.1.3.0.010"
ORDER_MANAGEMENT_PAGE_URL = f"{BASE_URL}/erp/mmulti/mpOrderManagement"
ORDER_MANAGEMENT_EXPORT_HEADER_FIELDS = (
    "order.global_order_no,"
    "order.platform_code,"
    "order.store_id,"
    "order.global_purchase_time,"
    "order.global_payment_time,"
    "item.platform_order_no,"
    "item.local_sku,"
    "item.local_product_name,"
    "item.msku,"
    "logistics.wid,"
    "logistics.first_mile_type_name,"
    "logistics.first_mile_waybill_no,"
    "logistics.logistics_type_id,"
    "logistics.waybill_no,"
    "logistics.tracking_no,"
    "order.buyer_name,"
    "order.buyer_email,"
    "order.buyer_note,"
    "order.receiver_name,"
    "order.receiver_mobile"
)

# 领星平台码：eBay（对齐 HAR：platformCodeS=["10003"]）
LINGXING_PLATFORM_CODE_EBAY = "10003"
# 登录环境：2 对应 HAR 中“切换为多平台”
LINGXING_LOGIN_ENV_MULTI_PLATFORM = 2


class LingXingDownloadError(Exception):
    """领星报表下载相关异常。"""


class LingXingLoginError(Exception):
    """领星网页登录相关异常。"""


def _build_default_session_headers() -> Dict[str, str]:
    """对齐 HAR / 浏览器环境的基础请求头。"""
    return {
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "zh-CN,zh;q=0.9",
        "Connection": "keep-alive",
        "Referer": f"{BASE_URL}/",
        "Origin": BASE_URL,
        "AK-Client-Type": "web",
        "User-Agent": DEFAULT_USER_AGENT,
    }


def _set_session_cookie(
    session: requests.Session,
    *,
    name: str,
    value: Any,
    domain: str,
    path: str = "/",
) -> None:
    """统一写 cookie，忽略空值。"""
    if value is None:
        return
    value_str = str(value)
    if value_str == "":
        return
    session.cookies.set(name=name, value=value_str, domain=domain, path=path)


def create_lingxing_session_from_cookies(
    cookies: Iterable[Mapping[str, Any]],
    extra_headers: Optional[Mapping[str, str]] = None,
) -> requests.Session:
    """
    根据 RPA 传入的 cookie 列表，初始化一个已登录状态的 requests.Session。

    cookies 结构示例（与你提供的一致）：
    [
        {
            "domain": "erp.lingxing.com",
            "name": "__wpkreporterwid_",
            "path": "/",
            "value": "xxxx",
            ...
        },
        ...
    ]

    :param cookies: RPA 侧传入的 cookie 字典列表
    :param extra_headers: 额外要设置的请求头（可选），例如 {"User-Agent": "..."}
    :return: 已携带 cookies / headers 的 Session，后续直接用于所有接口请求
    """
    session = requests.Session()
    session.trust_env = False

    # 1. 设置常用基础请求头（可按需要再扩展）
    default_headers = _build_default_session_headers()
    session.headers.update(default_headers)
    if extra_headers:
        session.headers.update(extra_headers)

    # 2. 把 RPA 传过来的 Cookie 同步到 Session
    for c in cookies or []:
        name = c.get("name")
        value = c.get("value")
        if not name or value is None:
            continue

        domain = c.get("domain") or "erp.lingxing.com"
        path = c.get("path") or "/"
        # 只要非过期的 cookie，一般浏览器导出的都已是有效的
        session.cookies.set(name=name, value=value, domain=domain, path=path)

    return session


def _pkcs7_pad(text: str, block_size: int = AES.block_size) -> bytes:
    """按 CryptoJS.Pkcs7 规则补齐。"""
    raw = text.encode("utf-8")
    padding_size = block_size - len(raw) % block_size
    return raw + bytes([padding_size]) * padding_size


def _encrypt_login_password(password: str, secret_key: str) -> str:
    """
    对齐前端 CryptoJS.AES.encrypt(password, key, { mode: ECB, padding: Pkcs7 }).
    """
    cipher = AES.new(secret_key.encode("utf-8"), AES.MODE_ECB)
    encrypted = cipher.encrypt(_pkcs7_pad(password))
    return base64.b64encode(encrypted).decode("utf-8")


def _generate_browser_fingerprint(account: str, user_agent: str) -> str:
    """
    前端实际用 Fingerprint2 生成 128bit 指纹，这里生成稳定的 32 位 hex 指纹即可。
    """
    raw = f"{account}|{user_agent}|lingxing-web-login"
    return hashlib.md5(raw.encode("utf-8")).hexdigest()


def _generate_sensors_anonymous_id() -> str:
    """
    模拟前端埋点匿名 ID；该字段主要用于埋点/风控透传，接口对格式校验较宽松。
    """
    millis = int(time.time() * 1000)
    token = uuid.uuid4().hex
    return f"{millis:x}-{token[:16]}-26061d51-2073600-{token[16:]}"


def fetch_lingxing_login_secret(
    session: Optional[requests.Session] = None,
    *,
    extra_headers: Optional[Mapping[str, str]] = None,
    timeout: int = 30,
) -> Dict[str, str]:
    """
    获取领星登录临时密钥。

    HAR / 前端代码都表明流程为：
    1. POST /newadmin/api/passport/getLoginSecretKey
    2. 用返回的 secretKey + secretId 去加密登录密码并提交登录
    """
    request_session = session or create_lingxing_session_from_cookies([], extra_headers=extra_headers)
    if extra_headers:
        request_session.headers.update(extra_headers)

    response = request_session.post(
        LOGIN_SECRET_KEY_URL,
        timeout=timeout,
    )
    response.raise_for_status()
    payload = response.json()

    if payload.get("code") != 1:
        raise LingXingLoginError(f"获取登录密钥失败: {payload}")

    data = payload.get("data") or {}
    secret_key = str(data.get("secretKey") or "").strip()
    secret_id = str(data.get("secretId") or "").strip()
    deadline_timestamp = str(data.get("deadlineTimestamp") or "").strip()
    if not secret_key or not secret_id:
        raise LingXingLoginError(f"登录密钥响应缺少关键字段: {payload}")

    return {
        "secretKey": secret_key,
        "secretId": secret_id,
        "deadlineTimestamp": deadline_timestamp,
    }


def login_lingxing_web(
    account: str,
    password: str,
    *,
    session: Optional[requests.Session] = None,
    auto_login: bool = True,
    verify_code: str = "",
    uuid_value: Optional[str] = None,
    fingerprint: Optional[str] = None,
    sensors_anonymous_id: Optional[str] = None,
    extra_headers: Optional[Mapping[str, str]] = None,
    timeout: int = 30,
    debug: bool = False,
) -> Dict[str, Any]:
    """
    逆向领星网页账号密码登录，并把关键鉴权字段写回 requests.Session cookies。

    返回登录接口的原始 JSON，方便后续继续读取 companyId / envKey / token / uid 等字段。
    """
    if not account or not password:
        raise ValueError("account 和 password 不能为空")

    request_session = session or create_lingxing_session_from_cookies([], extra_headers=extra_headers)
    if extra_headers:
        request_session.headers.update(extra_headers)

    secret_data = fetch_lingxing_login_secret(
        request_session,
        timeout=timeout,
    )
    user_agent = request_session.headers.get("User-Agent", DEFAULT_USER_AGENT)
    browser_fingerprint = fingerprint or _generate_browser_fingerprint(account, user_agent)
    sensors_id = sensors_anonymous_id or _generate_sensors_anonymous_id()
    encrypted_password = _encrypt_login_password(password, secret_data["secretKey"])
    login_uuid = uuid_value or str(uuid.uuid4())

    payload: Dict[str, Any] = {
        "account": account,
        "pwd": encrypted_password,
        "verify_code": verify_code,
        "uuid": login_uuid,
        "auto_login": 1 if auto_login else 0,
        "device": user_agent,
        "fingerprint": browser_fingerprint,
        "sensorsAnonymousId": sensors_id,
        "secretId": secret_data["secretId"],
        "doubleCheckLoginReq": {
            "doubleCheckType": 1,
            "mobileLoginCode": "",
            "loginTick": "",
        },
    }

    headers = {"Content-Type": "application/json;charset=UTF-8"}
    response = request_session.post(
        LOGIN_URL,
        json=payload,
        headers=headers,
        timeout=timeout,
    )
    response.raise_for_status()
    result = response.json()

    if debug:
        print("[LINGXING][login] request payload:", payload)
        print("[LINGXING][login] response:", result)

    code = result.get("code")
    if code != 1:
        raise LingXingLoginError(f"领星登录失败: {result}")

    double_check_config = result.get("doubleCheckConfigRes") or {}
    if double_check_config.get("needDoubleCheck"):
        raise LingXingLoginError(
            "当前账号触发双重验证，需先补充短信验证码登录能力后再继续自动化。"
        )

    data = result.get("data") or result
    token = data.get("token")
    encoded_token = quote(str(token), safe="") if token else None
    seller_auth_erp_url = data.get("sellerAuthErpUrl")
    encoded_seller_auth_erp_url = (
        quote(str(seller_auth_erp_url), safe="") if seller_auth_erp_url else None
    )
    cookie_payload = {
        "company_id": data.get("companyId"),
        "envKey": data.get("envKey"),
        "env_key": data.get("envKey"),
        "authToken": encoded_token,
        "auth-token": encoded_token,
        "token": encoded_token,
        "uid": data.get("uid"),
        "zid": data.get("zid"),
        "isLogin": "true",
        "isNeedReset": data.get("needReset"),
        "isUpdatePwd": 1 if data.get("isPwdNotice") else 0,
        "oauthClientId": data.get("clientId"),
    }
    for name, value in cookie_payload.items():
        _set_session_cookie(
            name=name,
            value=value,
            session=request_session,
            domain="erp.lingxing.com",
        )
    _set_session_cookie(
        session=request_session,
        name="seller-auth-erp-url",
        value=encoded_seller_auth_erp_url,
        domain="lingxing.com",
    )

    return result


def create_lingxing_session_from_login(
    account: str,
    password: str,
    *,
    extra_headers: Optional[Mapping[str, str]] = None,
    timeout: int = 30,
    debug: bool = False,
) -> requests.Session:
    """
    通过“账号密码逆向登录”直接构造后续业务可复用的 Session。
    """
    session = create_lingxing_session_from_cookies([], extra_headers=extra_headers)
    login_lingxing_web(
        account=account,
        password=password,
        session=session,
        timeout=timeout,
        debug=debug,
    )
    return session


def prepare_lingxing_erp_session(
    session: requests.Session,
    *,
    debug: bool = False,
    timeout: int = 30,
) -> Dict[str, Any]:
    """
    把当前 session 补齐成更接近浏览器真实态的 ERP 会话。

    关键点：
    1. gw 登录返回的 token 需要以 URL 编码形式写回 ERP cookies
    2. ERP 页面初始化后会通过 myInfo 写入 info cookie
    3. 多平台订单导出还依赖 is_sellerAuth / seller-auth-erp-url 等上下文 cookie
    """
    raw_token = (
        _get_cookie(session, "auth-token")
        or _get_cookie(session, "authToken")
        or _get_cookie(session, "token")
    )
    if not raw_token:
        raise LingXingLoginError("当前 session 缺少 auth-token，无法准备 ERP 会话")

    encoded_token = quote(unquote(str(raw_token)), safe="")
    for name in ("authToken", "auth-token", "token"):
        _set_session_cookie(
            session=session,
            name=name,
            value=encoded_token,
            domain="erp.lingxing.com",
        )

    _set_session_cookie(
        session=session,
        name="is_sellerAuth",
        value="1",
        domain="erp.lingxing.com",
    )

    seller_auth_erp_url = _get_cookie(session, "seller-auth-erp-url")
    if seller_auth_erp_url:
        _set_session_cookie(
            session=session,
            name="seller-auth-erp-url",
            value=seller_auth_erp_url,
            domain="lingxing.com",
        )

    response = session.get(
        USER_MY_INFO_URL,
        headers=_build_gateway_headers(session),
        timeout=timeout,
    )
    response.raise_for_status()
    payload = response.json()
    if payload.get("code") != 1:
        raise LingXingLoginError(f"获取 myInfo 失败: {payload}")

    data = payload.get("data") or {}
    info_cookie_value = quote(
        json.dumps(data, separators=(",", ":"), ensure_ascii=False),
        safe="",
    )
    _set_session_cookie(
        session=session,
        name="info",
        value=info_cookie_value,
        domain="erp.lingxing.com",
    )

    customer_id = data.get("customer_id") or data.get("customerId") or data.get("zid")
    show_zid = data.get("show_zid") or data.get("zid")
    if customer_id and show_zid:
        _set_session_cookie(
            session=session,
            name="sensor-distinace-id",
            value=f"{customer_id}-{show_zid}",
            domain="lingxing.com",
        )

    if debug:
        print("[LINGXING][prepare_erp_session] myInfo:", data)
        print(
            "[LINGXING][prepare_erp_session] cookies:",
            sorted({cookie.name for cookie in session.cookies}),
        )

    return data


def _cookie_domain_matches_host(cookie_domain: str, host: str) -> bool:
    normalized_domain = cookie_domain.lstrip(".").lower()
    normalized_host = host.lower()
    return normalized_host == normalized_domain or normalized_host.endswith(f".{normalized_domain}")


def _build_cookie_header_for_host(session: requests.Session, host: str) -> str:
    """为指定 host 组装浏览器风格的 Cookie 头。"""
    cookie_parts: List[str] = []
    for cookie in session.cookies:
        if _cookie_domain_matches_host(cookie.domain, host):
            cookie_parts.append(f"{cookie.name}={cookie.value}")
    return "; ".join(cookie_parts)


def _build_erp_headers(
    session: requests.Session,
    *,
    referer: str,
    content_type: Optional[str] = "application/json;charset=UTF-8",
    include_cookie_header: bool = True,
    request_id: Optional[str] = None,
) -> Dict[str, str]:
    """
    构造 ERP 域接口请求头。

    注意：订单管理导出接口实测要求显式带 Cookie 头，单靠 requests 自动带 cookie 不够。
    """
    auth_token = unquote(
        str(
            _get_cookie(session, "auth-token")
            or _get_cookie(session, "authToken")
            or _get_cookie(session, "token")
            or ""
        )
    )
    headers: Dict[str, str] = {
        "AK-Client-Type": "web",
        "AK-Origin": BASE_URL,
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "zh-CN,zh;q=0.9",
        "Origin": BASE_URL,
        "Referer": referer,
        "User-Agent": session.headers.get("User-Agent", DEFAULT_USER_AGENT),
        "X-AK-Company-Id": str(_get_cookie(session, "company_id", "") or ""),
        "X-AK-ENV-KEY": str(
            _get_cookie(session, "env_key") or _get_cookie(session, "envKey") or ""
        ),
        "X-AK-Language": "zh",
        "X-AK-PLATFORM": "2",
        "X-AK-Request-Id": request_id or str(uuid.uuid4()),
        "X-AK-Request-Source": "erp",
        "X-AK-Uid": str(_get_cookie(session, "uid", "") or ""),
        "X-AK-Version": ERP_REQUEST_VERSION,
        "X-AK-Zid": str(_get_cookie(session, "zid", "") or ""),
        "auth-token": auth_token,
    }
    if content_type:
        headers["Content-Type"] = content_type
    if include_cookie_header:
        cookie_header = _build_cookie_header_for_host(session, "erp.lingxing.com")
        if cookie_header:
            headers["Cookie"] = cookie_header
    return headers


def switch_lingxing_login_env(
    session: requests.Session,
    login_env: int,
    *,
    refresh_home: bool = True,
    timeout: int = 30,
    debug: bool = False,
) -> Dict[str, Any]:
    """
    切换领星登录环境。

    依据 HAR「切换为多平台.har」：
    POST /newadmin/api/user/manage/setLoginEnvLog
    body: {"login_env": 2, "req_time_sequence": "/newadmin/api/user/manage/setLoginEnvLog$$1"}

    其中 login_env=2 即“多平台”环境。
    """
    if not _get_cookie(session, "auth-token") and not _get_cookie(session, "authToken"):
        raise LingXingLoginError("当前 session 缺少登录态 token，无法切换登录环境")

    headers = _build_gateway_headers(session)
    headers.update(
        {
            "Content-Type": "application/json;charset=UTF-8",
            "Referer": f"{BASE_URL}/erp/home",
            "Origin": BASE_URL,
        }
    )
    payload = {
        "login_env": int(login_env),
        "req_time_sequence": "/newadmin/api/user/manage/setLoginEnvLog$$1",
    }

    response = session.post(
        SET_LOGIN_ENV_LOG_URL,
        json=payload,
        headers=headers,
        timeout=timeout,
    )
    response.raise_for_status()

    try:
        result: Dict[str, Any] = response.json()
    except Exception:
        result = {
            "code": response.status_code,
            "msg": response.text[:500],
        }

    if debug:
        print("[LINGXING][switch_login_env] request payload:", payload)
        print("[LINGXING][switch_login_env] request headers:", headers)
        print("[LINGXING][switch_login_env] response:", result)

    code = result.get("code")
    if code not in (None, 1, "1", 200, "200"):
        raise LingXingLoginError(f"切换领星登录环境失败: {result}")

    if refresh_home:
        home_response = session.get(
            f"{BASE_URL}/erp/home",
            headers={"Referer": f"{BASE_URL}/erp/home"},
            timeout=timeout,
        )
        home_response.raise_for_status()
        result["home_refresh_status"] = home_response.status_code

    return result


def switch_lingxing_to_multi_platform(
    session: requests.Session,
    *,
    refresh_home: bool = True,
    timeout: int = 30,
    debug: bool = False,
) -> Dict[str, Any]:
    """
    把当前已登录 session 切换到“多平台”环境。
    """
    return switch_lingxing_login_env(
        session=session,
        login_env=LINGXING_LOGIN_ENV_MULTI_PLATFORM,
        refresh_home=refresh_home,
        timeout=timeout,
        debug=debug,
    )


def build_order_management_export_payload(
    start_time: str,
    end_time: str,
    *,
    header_fields: str = ORDER_MANAGEMENT_EXPORT_HEADER_FIELDS,
) -> Dict[str, Any]:
    """
    构造订单管理导出请求体。

    筛选参数对齐 HAR：
    - 待审核发货 `status=4`
    - `status_sub=[5, "<>"]`
    - `is_pending=1`
    - 时间字段 `global_purchase_time`
    - 导出字段必须包含 `order.receiver_name` / `order.receiver_mobile`
    """
    return {
        "sort_type": "desc",
        "sort_field": "global_purchase_time",
        "status": "4",
        "tag_no": "",
        "site_code": [],
        "store_id": [],
        "platform_code": [],
        "search_field": "platform_order_name",
        "search_value": [""],
        "search_field_time": "global_purchase_time",
        "start_time": start_time,
        "end_time": end_time,
        "offset": 0,
        "length": 20,
        "status_sub": [5, "<>"],
        "flow_node": [],
        "is_pending": "1",
        "receiver_country_code": [],
        "order_from": "",
        "order_type": "",
        "order_status": "",
        "global_latest_ship_time_diff_day": "",
        "order_total_amount": [">", ""],
        "quantity": [">", ""],
        "gross_profit_amount": [">", ""],
        "buyer_note_status": "",
        "remark_has": "",
        "platform_status": [],
        "address_type": "",
        "is_marking": "",
        "wid": "",
        "logistics_type_id": "",
        "logistics_provider_id": "",
        "asin_principal_uid": [],
        "included_bundled": "",
        "x_ak_platform": "2",
        "is_merge": True,
        "export_sub_product": True,
        "header_fields": header_fields,
        "req_time_sequence": "/api/platforms/oms/order_list/exportCreate$$1",
    }


def create_order_management_export_task(
    session: requests.Session,
    start_time: str,
    end_time: str,
    *,
    header_fields: str = ORDER_MANAGEMENT_EXPORT_HEADER_FIELDS,
    debug: bool = False,
    timeout: int = 60,
) -> Dict[str, Any]:
    """
    创建订单管理导出任务，返回原始响应 JSON。
    """
    payload = build_order_management_export_payload(
        start_time=start_time,
        end_time=end_time,
        header_fields=header_fields,
    )
    headers = _build_erp_headers(
        session,
        referer=ORDER_MANAGEMENT_PAGE_URL,
    )
    url = BASE_URL + ORDER_LIST_EXPORT_CREATE_PATH

    if debug:
        print(
            "[LINGXING][create_order_management_export_task] 请求参数：",
            {"url": url, "headers": headers, "json": payload},
        )

    response = session.post(
        url,
        json=payload,
        headers=headers,
        timeout=timeout,
    )
    response.raise_for_status()
    result = response.json()

    if debug:
        print(
            "[LINGXING][create_order_management_export_task] 响应：",
            result,
        )

    if result.get("code") != 1:
        raise LingXingDownloadError(f"创建订单管理导出任务失败: {result}")

    nested = result.get("data") or {}
    nested_code = nested.get("code")
    if nested_code == 501:
        raise LingXingDownloadError(str(nested.get("msg") or "请勿频繁点击导出"))
    if nested_code != 1:
        raise LingXingDownloadError(f"创建订单管理导出任务失败: {result}")

    nested_data = nested.get("data") or {}
    report_id = nested_data.get("report_id")
    if not report_id:
        raise LingXingDownloadError(f"订单管理导出任务未返回 report_id: {result}")
    return result


def _find_matching_report(
    reports: Iterable[Mapping[str, Any]],
    *,
    report_id: Optional[str] = None,
    keyword: str = "订单管理",
    date_range: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    candidates: List[Dict[str, Any]] = []
    for report in reports:
        item = dict(report)
        current_report_id = str(item.get("report_id") or "")
        if report_id and current_report_id == str(report_id):
            candidates.append(item)
            continue
        if keyword not in str(item.get("report_name", "")):
            continue
        if date_range and str(item.get("report_date_range", "")) != date_range:
            continue
        candidates.append(item)

    if not candidates:
        return None
    candidates.sort(key=lambda x: str(x.get("gmt_create", "")), reverse=True)
    return candidates[0]


def wait_for_download_report_ready(
    session: requests.Session,
    *,
    start_time: str,
    end_time: str,
    report_id: Optional[str] = None,
    keyword: str = "订单管理",
    date_range: Optional[str] = None,
    timeout: int = 300,
    poll_interval: int = 5,
    debug: bool = False,
) -> Dict[str, Any]:
    """
    轮询下载中心，等待指定报表完成。
    """
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        reports = query_download_center_reports(
            session=session,
            start_time=start_time,
            end_time=end_time,
            debug=debug,
        )
        matched = _find_matching_report(
            reports,
            report_id=report_id,
            keyword=keyword,
            date_range=date_range,
        )
        if debug:
            print(
                "[LINGXING][wait_for_download_report_ready] 匹配到的报表：",
                matched,
            )
        if matched and matched.get("report_status") == 2:
            return matched
        time.sleep(poll_interval)
    raise LingXingDownloadError("下载中心轮询超时，未发现已完成的目标报表")


def export_and_download_order_management_report(
    session: requests.Session,
    start_time: str,
    end_time: str,
    save_path: str,
    *,
    header_fields: str = ORDER_MANAGEMENT_EXPORT_HEADER_FIELDS,
    timeout: int = 300,
    poll_interval: int = 5,
    debug: bool = False,
) -> str:
    """
    完整链路：准备 ERP 会话 -> 创建订单管理导出任务 -> 轮询下载中心 -> 下载文件。
    """
    prepare_lingxing_erp_session(session, debug=debug)
    report_id: Optional[str] = None
    date_range = f"{start_time[:10]} ~ {end_time[:10]}"

    try:
        result = create_order_management_export_task(
            session=session,
            start_time=start_time,
            end_time=end_time,
            header_fields=header_fields,
            debug=debug,
        )
        report_id = str((result.get("data") or {}).get("data", {}).get("report_id") or "")
    except LingXingDownloadError as exc:
        if "请勿频繁点击导出" not in str(exc):
            raise
        if debug:
            print(
                "[LINGXING][export_and_download_order_management_report] 命中导出频控，尝试复用最近一次任务。",
            )

    query_start, query_end = _default_download_query_range_around_today()
    matched = wait_for_download_report_ready(
        session=session,
        start_time=query_start,
        end_time=query_end,
        report_id=report_id or None,
        keyword="订单管理",
        date_range=date_range,
        timeout=timeout,
        poll_interval=poll_interval,
        debug=debug,
    )
    return download_report_resource(
        session=session,
        report_id=str(matched["report_id"]),
        save_path=save_path,
        debug=debug,
    )


def _normalize_phone_number(phone: Any) -> Optional[str]:
    """规范化手机号并统一为美国格式，默认输出 +1 前缀。"""
    if phone is None:
        return None
    raw = str(phone).strip()
    if not raw:
        return None
    # 含脱敏符号的号码（如 828*****77）直接丢弃，避免误拼接成假号码
    if "*" in raw:
        return None
    digits = "".join(ch for ch in raw if ch.isdigit())
    if len(digits) == 10:
        return f"+1{digits}"
    if len(digits) == 11 and digits.startswith("1"):
        return f"+{digits}"
    # 非美国号码长度/格式直接丢弃
    if len(digits) < 7:
        return None
    return None


def _get_xlsx_shared_strings(archive: zipfile.ZipFile) -> List[str]:
    ns = {"a": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}
    if "xl/sharedStrings.xml" not in archive.namelist():
        return []
    root = ET.fromstring(archive.read("xl/sharedStrings.xml"))
    shared_strings: List[str] = []
    for item in root.findall("a:si", ns):
        text = "".join(node.text or "" for node in item.findall(".//a:t", ns))
        shared_strings.append(text)
    return shared_strings


def _column_letters_to_index(reference: str) -> int:
    letters = "".join(ch for ch in reference if ch.isalpha()).upper()
    index = 0
    for ch in letters:
        index = index * 26 + (ord(ch) - ord("A") + 1)
    return max(index - 1, 0)


def _read_xlsx_first_sheet_rows(path: str | os.PathLike[str]) -> List[List[str]]:
    """
    轻量读取 XLSX 第一张表，返回二维字符串数组。
    """
    ns = {"a": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}
    rows: List[List[str]] = []
    with zipfile.ZipFile(path) as archive:
        shared_strings = _get_xlsx_shared_strings(archive)
        sheet_xml = archive.read("xl/worksheets/sheet1.xml")
        root = ET.fromstring(sheet_xml)
        for row in root.findall(".//a:sheetData/a:row", ns):
            current_values: Dict[int, str] = {}
            max_index = -1
            for cell in row.findall("a:c", ns):
                ref = cell.get("r") or ""
                cell_index = _column_letters_to_index(ref)
                max_index = max(max_index, cell_index)
                cell_type = cell.get("t")
                value_node = cell.find("a:v", ns)
                inline_text_node = cell.find("a:is/a:t", ns)
                value = ""
                if inline_text_node is not None:
                    value = inline_text_node.text or ""
                elif value_node is not None and value_node.text is not None:
                    raw_value = value_node.text
                    if cell_type == "s":
                        try:
                            value = shared_strings[int(raw_value)]
                        except Exception:
                            value = raw_value
                    else:
                        value = raw_value
                current_values[cell_index] = value
            if max_index < 0:
                continue
            rows.append([current_values.get(idx, "") for idx in range(max_index + 1)])
    return rows


def parse_order_management_export_file(path: str | os.PathLike[str]) -> List[Dict[str, str]]:
    """
    把订单管理导出的 Excel 解析成行字典列表。
    """
    rows = _read_xlsx_first_sheet_rows(path)
    if not rows:
        return []
    headers = [str(header or "").strip() for header in rows[0]]
    result: List[Dict[str, str]] = []
    for row in rows[1:]:
        if not any(str(cell or "").strip() for cell in row):
            continue
        padded = list(row) + [""] * max(0, len(headers) - len(row))
        result.append(
            {
                headers[idx]: str(padded[idx] or "").strip()
                for idx in range(len(headers))
                if headers[idx]
            }
        )
    return result


def extract_phone_numbers_from_order_records(
    records: Iterable[Mapping[str, Any]],
    *,
    phone_field_candidates: Iterable[str] = ("电话", "收件手机号", "收件人电话", "receiver_mobile"),
) -> List[str]:
    """
    从订单导出记录中提取并去重手机号。
    """
    phones: List[str] = []
    seen: set[str] = set()
    candidates = list(phone_field_candidates)
    for record in records:
        raw_phone = None
        for field in candidates:
            value = record.get(field)
            if value:
                raw_phone = value
                break
        normalized = _normalize_phone_number(raw_phone)
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        phones.append(normalized)
    return phones


def _ensure_parent_dir(path: str) -> None:
    """确保保存路径的父目录存在。"""
    directory = os.path.dirname(os.path.abspath(path))
    if directory and not os.path.exists(directory):
        os.makedirs(directory, exist_ok=True)


def _get_cookie(session: requests.Session, name: str, default: Optional[str] = None) -> Optional[str]:
    """
    从 Session 的 cookies 中安全获取某个名称的值。
    """
    try:
        value = session.cookies.get(name)  # type: ignore[assignment]
    except Exception:
        value = None
    return value or default


def _build_gateway_headers(session: requests.Session) -> Dict[str, str]:
    """
    构造调用网关 gw.lingxingerp.com 所需的一些关键请求头。

    大部分字段可以从 cookies 中推导出来，例如 company_id、uid、env_key 等。
    """
    headers: Dict[str, str] = {
        "AK-Client-Type": "web",
        "AK-Origin": "https://erp.lingxing.com",
    }

    company_id = _get_cookie(session, "company_id")
    env_key = _get_cookie(session, "env_key") or _get_cookie(session, "envKey")
    uid = _get_cookie(session, "uid")
    zid = _get_cookie(session, "zid")

    raw_auth_token = (
        _get_cookie(session, "auth-token")
        or _get_cookie(session, "authToken")
        or _get_cookie(session, "token")
    )
    # 部分来源的 cookie 里 auth-token 可能是 URL 编码过的（包含 %2F、%2B 等），
    # 这里只做百分号解码，不能用 unquote_plus，否则原始 token 里的 "+" 会被错误替换成空格。
    auth_token = unquote(raw_auth_token) if raw_auth_token else None

    if company_id:
        headers["X-AK-Company-Id"] = company_id
    if env_key:
        headers["X-AK-Env-Key"] = env_key
    if uid:
        headers["X-AK-Uid"] = uid
    if zid:
        headers["X-AK-Zid"] = zid
    if auth_token:
        headers["auth-token"] = auth_token

    # 一些网关侧可能依赖的平台/语言信息，按 HAR 固定填写，避免再从页面环境推断
    headers.setdefault("X-AK-Platform", "2")
    headers.setdefault("X-AK-Language", "zh")
    headers.setdefault("X-AK-Request-Source", "erp")

    return headers


def _default_download_query_range_around_today() -> tuple[str, str]:
    """
    自动生成一个“合理”的下载中心查询时间范围：
    以“当前日期”为中心，前后各 1 天，兼容时区/跨日情况。

    返回 (start_date, end_date)，格式 'YYYY-MM-DD'。
    """
    today = date.today()
    start = today - timedelta(days=1)
    end = today + timedelta(days=1)
    return start.isoformat(), end.isoformat()


def create_profit_report_download_task(
    session: requests.Session,
    biz_start_date: str,
    biz_end_date: str,
    *,
    platform_code: str = LINGXING_PLATFORM_CODE_EBAY,
    debug: bool = False,
) -> Dict[str, Any]:
    """
    调用领星网关接口，发起「eBay 平台 - 利润报表（订单维度）」的导出任务。

    这个接口对应 HAR 中的：
    POST https://gw.lingxingerp.com/cepf-finance-report/FinanceReport/download/createDownload

    :param session: 已登录的 requests.Session（需包含 auth-token / company_id / uid 等 cookie）
    :param biz_start_date: 报表业务统计开始日期，例如 '2026-02-01'
    :param biz_end_date: 报表业务统计结束日期，例如 '2026-02-28'
    :param platform_code: 领星平台码，默认 eBay=10003（对齐 HAR）
    :param debug: 是否打印调试信息
    :return: 接口返回的 JSON 数据
    """
    # monthTime 字段使用 YYYY-MM 形式的月份范围
    biz_start_month = biz_start_date[:7]
    biz_end_month = biz_end_date[:7]

    company_id = _get_cookie(session, "company_id")
    uid = _get_cookie(session, "uid")

    payload: Dict[str, Any] = {
        "summaryField": "order",
        "bids": [],
        "cids": [],
        "midList": [],
        "sids": "",
        "currencyCode": "",
        "developers": [],
        # 对齐 HAR：eBay 平台为 10003
        "platformCodeS": [str(platform_code)],
        "searchField": "local_sku",
        "searchValue": "",
        "monthTime": [biz_start_month, biz_end_month],
        "startDate": biz_start_date,
        "endDate": biz_end_date,
        "timeDimension": 1,
        "transactionTypeS": [],
        "searchDateType": "2",
        "sortField": "",
        "offset": 0,
        "length": 20,
        "isIncludeDetail": True,
    }

    # 这些字段在 HAR 中也会在 body 里带一份，直接从 cookie 推导
    if uid:
        payload["uid"] = uid
    if company_id:
        payload["companyId"] = company_id

    headers = _build_gateway_headers(session)

    if debug:
        print(
            "[LINGXING][create_profit_report_download_task] 请求参数：",
            {"url": PROFIT_REPORT_CREATE_URL, "headers": headers, "json": payload},
        )

    resp = session.post(
        PROFIT_REPORT_CREATE_URL,
        json=payload,
        headers=headers,
        timeout=30,
    )
    if debug:
        print(
            "[LINGXING][create_profit_report_download_task] 响应状态码：",
            resp.status_code,
        )
    resp.raise_for_status()

    try:
        data = resp.json()
    except Exception:
        if debug:
            text_preview = resp.text[:500]
            print(
                "[LINGXING][create_profit_report_download_task] JSON 解析失败，文本预览：",
                text_preview,
            )
        raise

    if debug:
        print(
            "[LINGXING][create_profit_report_download_task] 响应 JSON：",
            data,
        )

    # 根据 HAR，code=0 且 msg="请勿频繁点击导出，休息一会儿!" 等属于业务错误
    code = data.get("code")
    if code not in (0, 1):
        # 未知 code，直接抛错方便排查
        raise LingXingDownloadError(f"创建利润报表导出任务异常: {data}")
    if code == 0:
        # 典型报错：请勿频繁点击导出
        msg = data.get("msg") or "创建利润报表导出任务失败"
        raise LingXingDownloadError(msg)

    return data


def query_download_center_reports(
    session: requests.Session,
    start_time: str,
    end_time: str,
    report_time_type: int = 0,
    offset: int = 0,
    length: int = 20,
    debug: bool = False,
) -> List[Dict[str, Any]]:
    """
    调用 HAR 中的接口：
    GET /api/download/downloadCenterReport/getReportData
    查询下载中心的报表队列，返回列表数据。

    :param session: 已登录的 requests.Session（必须带上 Cookie / Token）
    :param start_time: 查询开始日期，格式如 '2026-03-03'
    :param end_time: 查询结束日期，格式如 '2026-03-10'
    :param report_time_type: 时间类型，HAR 中为 0，保持默认即可
    :param offset: 翻页偏移量
    :param length: 每页条数
    :param debug: 是否打印调试信息
    """
    url = BASE_URL + GET_REPORT_DATA_PATH
    params = {
        "offset": offset,
        "length": length,
        "report_time_type": report_time_type,
        "start_time": start_time,
        "end_time": end_time,
    }
    if debug:
        print(
            "[LINGXING][query_download_center_reports] 请求参数：",
            {"url": url, "params": params},
        )

    resp = session.get(url, params=params)
    if debug:
        print(
            "[LINGXING][query_download_center_reports] 响应状态码：",
            resp.status_code,
        )
    resp.raise_for_status()
    try:
        data = resp.json()
    except Exception:
        if debug:
            text_preview = resp.text[:500]
            print(
                "[LINGXING][query_download_center_reports] JSON 解析失败，文本预览：",
                text_preview,
            )
        raise

    if debug:
        print(
            "[LINGXING][query_download_center_reports] 响应 JSON code：",
            data.get("code"),
        )

    if data.get("code") != 1:
        raise LingXingDownloadError(f"查询下载中心失败: {data}")

    result_list = data.get("data", {}).get("list", []) or []
    if debug:
        print(
            "[LINGXING][query_download_center_reports] 返回列表条数：",
            len(result_list),
        )
        print(
            "[LINGXING][query_download_center_reports] 返回列表内容：",
            result_list,
        )
    return result_list


def wait_for_profit_report_ready(
    session: requests.Session,
    start_time: str,
    end_time: str,
    keyword: str = "利润报表订单维度",
    timeout: int = 600,
    poll_interval: int = 10,
    debug: bool = False,
) -> str:
    """
    轮询下载中心队列，等待指定时间范围内的「利润报表」生成完成，并返回 report_id。

    注意：此函数只负责“等+取 report_id”，不负责真正创建/发起导出任务，
    需要你在网页中已点击生成对应的利润报表任务。

    :param session: 已登录的 requests.Session
    :param start_time: 查询开始日期，格式如 '2026-02-01'
    :param end_time: 查询结束日期，格式如 '2026-02-28'
    :param keyword: 用于匹配报表名称的关键字。对齐 HAR 中的命名模式，默认“利润报表订单维度”
    :param timeout: 最长等待秒数
    :param poll_interval: 轮询时间间隔（秒）
    :param debug: 是否打印调试信息
    :return: 找到的 report_id
    """
    deadline = time.monotonic() + timeout

    if debug:
        print(
            "[LINGXING][wait_for_profit_report_ready] 开始轮询下载中心，"
            f"时间范围：{start_time} ~ {end_time}，"
            f"关键字：{keyword}，"
            f"超时时间：{timeout}s，轮询间隔：{poll_interval}s",
        )

    while time.monotonic() < deadline:
        reports = query_download_center_reports(
            session=session,
            start_time=start_time,
            end_time=end_time,
            debug=debug,
        )
        if debug:
            print(
                "[LINGXING][wait_for_profit_report_ready] 本次查询返回记录数：",
                len(reports),
            )
        # 过滤已完成的利润报表（report_status == 2 表示完成，见 HAR 响应示例）
        candidates: List[Dict[str, Any]] = []
        for item in reports:
            name = str(item.get("report_name", ""))
            status = item.get("report_status")
            if keyword in name and status == 2:
                candidates.append(item)

        if candidates:
            # 取最近生成的一个（按创建时间排序，字段 gmt_create 如 '2026-03-10 15:14:51'）
            candidates.sort(key=lambda x: str(x.get("gmt_create", "")), reverse=True)
            report_id = candidates[0].get("report_id")
            if report_id:
                if debug:
                    print(
                        "[LINGXING][wait_for_profit_report_ready] 找到已完成利润报表：",
                        {
                            "report_id": report_id,
                            "report_name": candidates[0].get("report_name"),
                            "gmt_create": candidates[0].get("gmt_create"),
                        },
                    )
                return str(report_id)

        if debug:
            print(
                "[LINGXING][wait_for_profit_report_ready] 未找到已完成的利润报表，"
                f"{poll_interval} 秒后重试……",
            )
        time.sleep(poll_interval)

    raise LingXingDownloadError(
        f"在 {timeout} 秒内未在下载中心发现已完成的利润报表（时间范围 {start_time}~{end_time}）。"
    )


def download_report_resource(
    session: requests.Session,
    report_id: str,
    save_path: str,
    chunk_size: int = 8192,
    debug: bool = False,
) -> str:
    """
    根据 report_id 调用：
    GET /api/download/downloadCenterReport/downloadResource?report_id=xxx
    下载领星生成好的报表到本地指定路径（如存在同名文件则覆盖）。

    :param session: 已登录的 requests.Session
    :param report_id: 从下载队列中获得的 report_id
    :param save_path: 要保存到本地的完整文件路径（可包含中文路径）
    :param chunk_size: 流式下载每次读取的字节数
    :param debug: 是否打印调试信息
    :return: 保存后的文件路径
    """
    if debug:
        print(
            "[LINGXING][download_report_resource] 准备下载报表资源：",
            {"report_id": report_id, "save_path": save_path},
        )

    _ensure_parent_dir(save_path)
    url = BASE_URL + DOWNLOAD_RESOURCE_PATH
    params = {"report_id": report_id}

    if debug:
        print(
            "[LINGXING][download_report_resource] 请求参数：",
            {"url": url, "params": params},
        )

    with session.get(url, params=params, stream=True) as resp:
        if debug:
            print(
                "[LINGXING][download_report_resource] 响应状态码：",
                resp.status_code,
                "Content-Type：",
                resp.headers.get("Content-Type"),
            )
        resp.raise_for_status()
        # 覆盖写入：如果已有同名文件会被替换
        with open(save_path, "wb") as f:
            for chunk in resp.iter_content(chunk_size=chunk_size):
                if chunk:
                    f.write(chunk)
    abs_path = os.path.abspath(save_path)
    if debug:
        print("[LINGXING][download_report_resource] 下载完成，保存路径：", abs_path)
    return abs_path


def export_and_download_lingxing_profit_report(
    session: requests.Session,
    start_time: str,
    end_time: str,
    save_path: str,
    keyword: str = "利润报表订单维度",
    timeout: int = 600,
    poll_interval: int = 10,
    debug: bool = False,
) -> str:
    """
    「一条龙」方法：等待领星下载中心中的利润报表生成完成，然后自动下载到指定路径。

    使用前提：
    - 传入的 session 已完成登录，能直接访问 erp.lingxing.com 与 gw.lingxingerp.com 接口。

    :param session: 已登录的 requests.Session
    :param start_time: 利润报表业务统计开始日期，例如 '2026-02-01'
    :param end_time: 利润报表业务统计结束日期，例如 '2026-02-28'
    :param save_path: 最终保存到本地的文件路径（存在同名文件会被覆盖）
    :param keyword: 下载中心中用于匹配报表名称的关键字，默认“利润报表订单维度”
    :param timeout: 等待报表生成完成的最长时间（秒）
    :param poll_interval: 查询下载中心的间隔时间（秒）
    :param debug: 是否打印调试信息
    :return: 实际保存的本地绝对路径
    """
    if debug:
        print(
            "[LINGXING][export_and_download] 开始一条龙导出下载流程（含创建任务）：",
            {
                "biz_start_date": start_time,
                "biz_end_date": end_time,
                "save_path": save_path,
                "keyword": keyword,
                "timeout": timeout,
                "poll_interval": poll_interval,
            },
        )

    # 第一步：根据业务日期范围，调用网关接口创建“利润报表订单维度”导出任务
    create_profit_report_download_task(
        session=session,
        biz_start_date=start_time,
        biz_end_date=end_time,
        platform_code=LINGXING_PLATFORM_CODE_EBAY,
        debug=debug,
    )

    # 第二步：根据“当前时间”自动生成一个合理的下载中心查询时间范围
    query_start, query_end = _default_download_query_range_around_today()
    if debug:
        print(
            "[LINGXING][export_and_download] 自动计算的下载中心查询时间范围：",
            {"query_start": query_start, "query_end": query_end},
        )

    # 第三步：在下载中心轮询等待对应“利润报表”任务生成完成
    report_id = wait_for_profit_report_ready(
        session=session,
        start_time=query_start,
        end_time=query_end,
        keyword=keyword,
        timeout=timeout,
        poll_interval=poll_interval,
        debug=debug,
    )
    if debug:
        print(
            "[LINGXING][export_and_download] 已获取 report_id，开始下载：",
            report_id,
        )
    return download_report_resource(
        session=session,
        report_id=report_id,
        save_path=save_path,
        debug=debug,
    )


def export_and_download_lingxing_ebay_profit_report(
    session: requests.Session,
    start_date: str,
    end_date: str,
    save_path: str,
    *,
    timeout: int = 600,
    poll_interval: int = 10,
    debug: bool = False,
) -> str:
    """
    eBay 专用「一条龙」导出下载：创建任务 → 轮询下载中心 → 下载到本地。

    返回：下载文件的绝对路径。
    """
    return export_and_download_lingxing_profit_report(
        session=session,
        start_time=start_date,
        end_time=end_date,
        save_path=save_path,
        keyword="利润报表订单维度",
        timeout=timeout,
        poll_interval=poll_interval,
        debug=debug,
    )


def lingxing_export_ebay_profit_report_from_cookies(
    cookies: Iterable[Mapping[str, Any]],
    start_date: str,
    end_date: str,
    save_path: str,
    timeout: int = 600,
    poll_interval: int = 10,
    debug: bool = False,
    extra_headers: Optional[Mapping[str, str]] = None,
) -> str:
    """
    给 RPA/脚本侧直接调用的入口：传 cookies（浏览器导出结构）即可完成 eBay 利润报表导出与下载。

    返回：下载文件的绝对路径。
    """
    session = create_lingxing_session_from_cookies(
        cookies=cookies,
        extra_headers=extra_headers,
    )
    return export_and_download_lingxing_ebay_profit_report(
        session=session,
        start_date=start_date,
        end_date=end_date,
        save_path=save_path,
        timeout=timeout,
        poll_interval=poll_interval,
        debug=debug,
    )


def lingxing_export_ebay_profit_report_from_login(
    account: str,
    password: str,
    start_date: str,
    end_date: str,
    save_path: str,
    timeout: int = 600,
    poll_interval: int = 10,
    debug: bool = False,
    extra_headers: Optional[Mapping[str, str]] = None,
) -> str:
    """
    新入口：直接通过账号密码逆向登录后，继续完成 eBay 利润报表导出与下载。

    返回：下载文件的绝对路径。
    """
    session = create_lingxing_session_from_login(
        account=account,
        password=password,
        extra_headers=extra_headers,
        timeout=30,
        debug=debug,
    )
    return export_and_download_lingxing_ebay_profit_report(
        session=session,
        start_date=start_date,
        end_date=end_date,
        save_path=save_path,
        timeout=timeout,
        poll_interval=poll_interval,
        debug=debug,
    )


# if __name__ == "__main__":
#     # 最小回归入口：提供 cookies JSON 文件路径即可本地跑通。
#     # 用法（示例）：
#     #   set LINGXING_COOKIES_JSON=C:\\path\\to\\cookies.json
#     #   python lingxing_result.py 2026-02-01 2026-02-28 C:\\tmp\\利润报表.xlsx
#     import json as _json
#     import sys as _sys

#     cookies_path = os.environ.get("LINGXING_COOKIES_JSON", "").strip()
#     if not cookies_path or not os.path.exists(cookies_path):
#         print(
#             "[LINGXING][MAIN] 未提供有效的 LINGXING_COOKIES_JSON 环境变量，跳过执行。",
#         )
#         _sys.exit(0)

#     if len(_sys.argv) < 4:
#         print(
#             "[LINGXING][MAIN] 参数不足。需要: start_date end_date save_path",
#         )
#         _sys.exit(2)

#     with open(cookies_path, "r", encoding="utf-8") as f:
#         cookies_obj = _json.load(f)

#     out = lingxing_export_ebay_profit_report_from_cookies(
#         cookies=cookies_obj,
#         start_date=_sys.argv[1],
#         end_date=_sys.argv[2],
#         save_path=_sys.argv[3],
#         debug=True,
#     )
#     print("[LINGXING][MAIN] 下载完成：", out)
