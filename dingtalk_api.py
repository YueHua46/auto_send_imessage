"""
单文件版钉钉消息 SDK
====================

说明：
    这是将项目中核心功能（客户端、消息构造、单聊/群聊发送、
    通讯录查询、媒体上传等）整合到一个文件中的版本。

    你可以直接把本文件的全部内容复制到其他项目中使用，
    只需保证安装了依赖：

        pip install requests>=2.31.0

    使用方式一（推荐 - RPA/外部调用：先初始化单例，再调模块级方法）：

        import main

        # 先初始化单例（必须调用一次，传入权限参数）
        main.init_bot(app_key="xxx", app_secret="yyy", robot_code="zzz")

        # 之后直接调模块暴露的方法即可
        main.send_text_to_user("张三", "你好")
        main.send_file_to_user("李四", "C:/path/to/report.pdf", by_name=True)
        main.send_text_to_group("cidXXX==", "群通知", at_all=True)

    使用方式二（单例对象：get_instance 后调 bot 的方法）：

        from main import DingTalkBot
        bot = DingTalkBot.get_instance(app_key="xxx", app_secret="yyy", robot_code="zzz")
        bot.send_text_to_user("张三", "你好")

    使用方式三（按需自行组装）：

        from main import DingTalkClient, SingleChatSender, TextMessage
        client = DingTalkClient(app_key, app_secret)
        single = SingleChatSender(client, robot_code)
        single.send_to_user_by_name("张三", TextMessage("你好"))
"""

import json
import os
import threading
import time
from abc import ABC, abstractmethod
from pathlib import Path
from typing import (
    Any,
    BinaryIO,
    Dict,
    List,
    Optional,
    Union,
)

import requests


# =========================
# 重试配置（网络不稳定时自动重试）
# =========================
MAX_RETRIES = 5
RETRY_BACKOFF_BASE = 1  # 秒，第 n 次重试前等待 n * RETRY_BACKOFF_BASE 秒


def _request_with_retry(request_func, max_retries: int = MAX_RETRIES, backoff_base: float = RETRY_BACKOFF_BASE):
    """
    执行 request_func()，失败时重试最多 max_retries 次。
    仅对 requests 库的网络异常（超时、连接错误等）重试。
    """
    last_exc = None
    for attempt in range(max_retries):
        try:
            return request_func()
        except requests.exceptions.RequestException as e:
            last_exc = e
            if attempt < max_retries - 1:
                time.sleep(backoff_base * (attempt + 1))
            else:
                raise
    if last_exc is not None:
        raise last_exc


# =========================
# 自定义异常定义
# =========================

class DingTalkAPIException(Exception):
    """
    钉钉API调用失败异常
    """

    def __init__(self, error_code, error_msg, request_id=None):
        self.error_code = error_code
        self.error_msg = error_msg
        self.request_id = request_id

        message = f"钉钉API调用失败 [错误代码: {error_code}] {error_msg}"
        if request_id:
            message += f" (请求ID: {request_id})"

        super().__init__(message)


class TokenRefreshException(DingTalkAPIException):
    """
    Token刷新失败异常
    """

    def __init__(self, error_msg, error_code: str = "TOKEN_REFRESH_FAILED"):
        super().__init__(error_code, error_msg)


class InvalidMessageException(Exception):
    """
    无效消息异常（消息内容/参数不合法）
    """

    def __init__(self, message: str):
        super().__init__(f"消息格式错误: {message}")


class NetworkException(Exception):
    """
    网络请求异常
    """

    def __init__(self, message: str, original_exception: Optional[Exception] = None):
        self.original_exception = original_exception

        if original_exception:
            message = f"{message}: {str(original_exception)}"

        super().__init__(message)


# =========================
# 钉钉客户端（access_token 管理）
# =========================

class DingTalkClient:
    """
    钉钉API客户端

    负责管理access_token的获取、缓存和自动刷新（线程安全）
    """

    # 钉钉API基础URL
    BASE_URL = "https://api.dingtalk.com"

    # Token获取端点
    TOKEN_ENDPOINT = "/v1.0/oauth2/accessToken"

    # Token提前刷新时间（秒），在过期前5分钟刷新
    TOKEN_REFRESH_ADVANCE = 300

    def __init__(self, app_key: str, app_secret: str):
        """
        Args:
            app_key: 应用的AppKey
            app_secret: 应用的AppSecret
        """
        if not app_key or not app_secret:
            raise ValueError("app_key和app_secret不能为空")

        self.app_key = app_key
        self.app_secret = app_secret

        # Token缓存
        self._access_token: Optional[str] = None
        self._token_expires_at: float = 0

        # 线程锁，确保token刷新的线程安全
        self._lock = threading.Lock()

        # 会话对象，复用连接
        self._session = requests.Session()
        self._session.headers.update(
            {
                "Content-Type": "application/json",
                "Accept": "application/json",
            }
        )

    def get_access_token(self) -> str:
        """
        获取有效的access_token（必要时自动刷新）
        """
        current_time = time.time()

        # 检查token是否需要刷新
        if self._need_refresh(current_time):
            with self._lock:
                # 双重检查，避免重复刷新
                if self._need_refresh(current_time):
                    self._refresh_token()

        return self._access_token  # type: ignore[return-value]

    def _need_refresh(self, current_time: float) -> bool:
        """
        检查token是否需要刷新
        """
        return self._access_token is None or current_time >= (
            self._token_expires_at - self.TOKEN_REFRESH_ADVANCE
        )

    def _refresh_token(self) -> None:
        """
        刷新access_token
        """
        url = f"{self.BASE_URL}{self.TOKEN_ENDPOINT}"

        payload = {
            "appKey": self.app_key,
            "appSecret": self.app_secret,
        }

        try:
            response = _request_with_retry(
                lambda: self._session.post(url, json=payload, timeout=10)
            )
            response.raise_for_status()

            data = response.json()

            if "accessToken" not in data:
                error_msg = data.get("message", "未知错误")
                raise TokenRefreshException(f"获取access_token失败: {error_msg}")

            self._access_token = data["accessToken"]
            expires_in = data.get("expireIn", 7200)
            self._token_expires_at = time.time() + expires_in

        except requests.exceptions.RequestException as e:
            raise NetworkException("网络请求失败", e)
        except (KeyError, ValueError) as e:
            raise TokenRefreshException(f"解析响应数据失败: {str(e)}")

    def invalidate_token(self) -> None:
        """
        使当前token失效，下次请求会强制刷新
        """
        with self._lock:
            self._access_token = None
            self._token_expires_at = 0

    def get_headers(self) -> Dict[str, str]:
        """
        获取带有access_token的请求头
        """
        token = self.get_access_token()
        return {
            "x-acs-dingtalk-access-token": token,
            "Content-Type": "application/json",
        }

    def close(self) -> None:
        if self._session:
            self._session.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def __del__(self):
        try:
            self.close()
        except Exception:
            pass


# =========================
# 消息构造器
# =========================

class MessageBuilder(ABC):
    """
    消息构造器抽象基类
    """

    @abstractmethod
    def build(self) -> Dict[str, Any]:
        """
        构建消息体
        """

    @abstractmethod
    def get_msg_key(self) -> str:
        """
        获取消息类型键
        """

    def to_json(self) -> str:
        """
        将消息转换为JSON字符串
        """
        return json.dumps(self.build(), ensure_ascii=False)


class TextMessage(MessageBuilder):
    """
    文本消息
    """

    def __init__(self, content: str):
        if not content or not content.strip():
            raise InvalidMessageException("文本消息内容不能为空")
        self.content = content

    def get_msg_key(self) -> str:
        return "sampleText"

    def build(self) -> Dict[str, Any]:
        return {"content": self.content}


class MarkdownMessage(MessageBuilder):
    """
    Markdown消息
    """

    def __init__(self, title: str, text: str):
        if not title or not title.strip():
            raise InvalidMessageException("Markdown消息标题不能为空")
        if not text or not text.strip():
            raise InvalidMessageException("Markdown消息内容不能为空")
        self.title = title
        self.text = text

    def get_msg_key(self) -> str:
        return "sampleMarkdown"

    def build(self) -> Dict[str, Any]:
        return {"title": self.title, "text": self.text}


class LinkMessage(MessageBuilder):
    """
    链接消息
    """

    def __init__(self, title: str, text: str, message_url: str, pic_url: Optional[str] = None):
        if not title or not title.strip():
            raise InvalidMessageException("链接消息标题不能为空")
        if not text or not text.strip():
            raise InvalidMessageException("链接消息文本不能为空")
        if not message_url or not message_url.strip():
            raise InvalidMessageException("链接消息URL不能为空")

        self.title = title
        self.text = text
        self.message_url = message_url
        self.pic_url = pic_url

    def get_msg_key(self) -> str:
        return "sampleLink"

    def build(self) -> Dict[str, Any]:
        message = {
            "title": self.title,
            "text": self.text,
            "messageUrl": self.message_url,
        }
        if self.pic_url:
            message["picUrl"] = self.pic_url
        return message


class ActionCardMessage(MessageBuilder):
    """
    卡片消息（支持单按钮或多按钮）
    """

    def __init__(
        self,
        title: str,
        text: str,
        single_title: Optional[str] = None,
        single_url: Optional[str] = None,
        btn_orientation: str = "0",
        buttons: Optional[List[Dict[str, str]]] = None,
    ):
        if not title or not title.strip():
            raise InvalidMessageException("卡片消息标题不能为空")
        if not text or not text.strip():
            raise InvalidMessageException("卡片消息内容不能为空")

        # 单按钮模式和多按钮模式二选一
        if single_title and buttons:
            raise InvalidMessageException("不能同时使用单按钮和多按钮模式")
        if not single_title and not buttons:
            raise InvalidMessageException("必须指定单按钮或多按钮")
        if single_title and not single_url:
            raise InvalidMessageException("单按钮模式下必须指定URL")

        self.title = title
        self.text = text
        self.single_title = single_title
        self.single_url = single_url
        self.btn_orientation = btn_orientation
        self.buttons = buttons

    def get_msg_key(self) -> str:
        return "sampleActionCard"

    def build(self) -> Dict[str, Any]:
        message: Dict[str, Any] = {
            "title": self.title,
            "text": self.text,
            "buttonOrientation": self.btn_orientation,
        }

        if self.single_title:
            message["singleTitle"] = self.single_title
            message["singleURL"] = self.single_url
        else:
            message["actionCard"] = self.buttons

        return message


class ImageMessage(MessageBuilder):
    """
    图片消息（URL）
    """

    def __init__(self, photo_url: str):
        if not photo_url or not photo_url.strip():
            raise InvalidMessageException("图片URL不能为空")
        self.photo_url = photo_url

    def get_msg_key(self) -> str:
        return "sampleImageMsg"

    def build(self) -> Dict[str, Any]:
        return {"photoURL": self.photo_url}


class AudioMessage(MessageBuilder):
    """
    语音消息
    """

    def __init__(self, media_id: str, duration: int):
        if not media_id or not media_id.strip():
            raise InvalidMessageException("语音媒体ID不能为空")
        if duration <= 0:
            raise InvalidMessageException("语音时长必须大于0")
        self.media_id = media_id
        self.duration = duration

    def get_msg_key(self) -> str:
        return "sampleAudio"

    def build(self) -> Dict[str, Any]:
        return {
            "mediaId": self.media_id,
            "duration": str(self.duration),
        }


class FileMessage(MessageBuilder):
    """
    文件消息
    """

    def __init__(self, media_id: str, file_name: str, file_type: str):
        if not media_id or not media_id.strip():
            raise InvalidMessageException("文件媒体ID不能为空")
        if not file_name or not file_name.strip():
            raise InvalidMessageException("文件名不能为空")
        if not file_type or not file_type.strip():
            raise InvalidMessageException("文件类型不能为空")
        self.media_id = media_id
        self.file_name = file_name
        self.file_type = file_type

    def get_msg_key(self) -> str:
        return "sampleFile"

    def build(self) -> Dict[str, Any]:
        return {
            "mediaId": self.media_id,
            "fileName": self.file_name,
            "fileType": self.file_type,
        }


# =========================
# 消息发送基类
# =========================

class MessageSender(ABC):
    """
    消息发送基类（被单聊/群聊发送器继承）
    """

    # 钉钉API基础URL
    BASE_URL = "https://api.dingtalk.com"

    def __init__(self, client: DingTalkClient, robot_code: str):
        if not isinstance(client, DingTalkClient):
            raise ValueError("client必须是DingTalkClient实例")
        if not robot_code or not robot_code.strip():
            raise ValueError("robot_code不能为空")

        self.client = client
        self.robot_code = robot_code

        # 会话对象
        self._session = requests.Session()

    @abstractmethod
    def _get_endpoint(self) -> str:
        """
        获取API端点（子类实现）
        """

    def _build_request_body(self, message: MessageBuilder, **kwargs: Any) -> Dict[str, Any]:
        """
        构建请求体
        """
        body: Dict[str, Any] = {
            "robotCode": self.robot_code,
            "msgKey": message.get_msg_key(),
            "msgParam": message.to_json(),
        }
        body.update(kwargs)
        return body

    def _send_request(self, body: Dict[str, Any], timeout: int = 10) -> Dict[str, Any]:
        """
        发送HTTP请求
        """
        url = f"{self.BASE_URL}{self._get_endpoint()}"
        headers = self.client.get_headers()

        try:
            response = _request_with_retry(
                lambda: self._session.post(
                    url,
                    json=body,
                    headers=headers,
                    timeout=timeout,
                )
            )
            response.raise_for_status()

            data = response.json()

            # 钉钉错误码检查
            if "code" in data and data["code"] != 0:
                error_code = data.get("code", "UNKNOWN")
                error_msg = data.get("message", "未知错误")
                request_id = data.get("requestId")
                raise DingTalkAPIException(error_code, error_msg, request_id)

            return data

        except requests.exceptions.Timeout:
            raise NetworkException("请求超时")
        except requests.exceptions.ConnectionError as e:
            raise NetworkException("网络连接失败", e)
        except requests.exceptions.HTTPError as e:
            try:
                error_data = e.response.json()
                error_code = error_data.get("code", e.response.status_code)
                error_msg = error_data.get("message", str(e))
                raise DingTalkAPIException(error_code, error_msg)
            except (ValueError, AttributeError):
                raise NetworkException(f"HTTP错误 {e.response.status_code}", e)
        except requests.exceptions.RequestException as e:
            raise NetworkException("请求失败", e)
        except (ValueError, KeyError) as e:
            raise DingTalkAPIException("PARSE_ERROR", f"解析响应失败: {str(e)}")

    def _validate_message(self, message: Any) -> None:
        if not isinstance(message, MessageBuilder):
            raise ValueError("message必须是MessageBuilder的实例")

    def close(self) -> None:
        if self._session:
            self._session.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def __del__(self):
        try:
            self.close()
        except Exception:
            pass


# =========================
# 通讯录服务
# =========================

class ContactService:
    """
    通讯录服务（用户搜索等）
    """

    BASE_URL = "https://api.dingtalk.com"
    USER_SEARCH_ENDPOINT = "/v1.0/contact/users/search"

    def __init__(self, client: DingTalkClient):
        if not isinstance(client, DingTalkClient):
            raise ValueError("client必须是DingTalkClient实例")
        self.client = client
        self._session = requests.Session()

    def search_users(
        self,
        query: str,
        max_results: int = 20,
        full_match: bool = False,
        debug: bool = False,
    ) -> List[Dict[str, Any]]:
        if not query or not query.strip():
            raise ValueError("搜索关键词不能为空")
        if max_results <= 0 or max_results > 100:
            raise ValueError("max_results必须在1-100之间")

        url = f"{self.BASE_URL}{self.USER_SEARCH_ENDPOINT}"
        headers = self.client.get_headers()

        body: Dict[str, Any] = {
            "queryWord": query.strip(),
            "offset": 0,
            "size": max_results,
        }
        if full_match:
            body["fullMatchField"] = 1

        try:
            response = _request_with_retry(
                lambda: self._session.post(
                    url,
                    json=body,
                    headers=headers,
                    timeout=10,
                )
            )
            response.raise_for_status()

            data = response.json()
            if debug:
                print(
                    "[ContactService 调试] 搜索用户接口原始返回:",
                    json.dumps(data, ensure_ascii=False, indent=2),
                )

            if "code" in data and data["code"] != 0:
                error_code = data.get("code", "UNKNOWN")
                error_msg = data.get("message", "未知错误")
                raise DingTalkAPIException(error_code, error_msg)

            user_ids = data.get("result", {}).get("list") or data.get("list", [])
            if not isinstance(user_ids, list):
                user_ids = []

            users = [{"userId": user_id} for user_id in user_ids]
            return users

        except requests.exceptions.Timeout:
            raise NetworkException("请求超时")
        except requests.exceptions.ConnectionError as e:
            raise NetworkException("网络连接失败", e)
        except requests.exceptions.HTTPError as e:
            try:
                error_data = e.response.json()
                error_code = error_data.get("code", e.response.status_code)
                error_msg = error_data.get("message", str(e))
                raise DingTalkAPIException(error_code, error_msg)
            except (ValueError, AttributeError):
                raise NetworkException(f"HTTP错误 {e.response.status_code}", e)
        except requests.exceptions.RequestException as e:
            raise NetworkException("请求失败", e)
        except (ValueError, KeyError) as e:
            raise DingTalkAPIException("PARSE_ERROR", f"解析响应失败: {str(e)}")

    def search_user_by_name(self, name: str, exact_match: bool = False) -> Optional[str]:
        users = self.search_users(name, max_results=1, full_match=exact_match)
        if users:
            return users[0]["userId"]
        return None

    def batch_search_users(
        self,
        names: List[str],
        exact_match: bool = False,
    ) -> Dict[str, Optional[str]]:
        result: Dict[str, Optional[str]] = {}
        for name in names:
            user_id = self.search_user_by_name(name, exact_match)
            result[name] = user_id
        return result

    def close(self) -> None:
        if self._session:
            self._session.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def __del__(self):
        try:
            self.close()
        except Exception:
            pass


class ConversationService:
    """
    会话服务（当前主要是占位，提醒如何获取 openConversationId）
    """

    def __init__(self, client: DingTalkClient):
        if not isinstance(client, DingTalkClient):
            raise ValueError("client必须是DingTalkClient实例")
        self.client = client
        self._session = requests.Session()

    def get_conversation_info(self, open_conversation_id: str) -> Dict[str, Any]:
        raise NotImplementedError(
            "获取群会话信息需要通过以下方式之一：\n"
            "1. 使用JSAPI Explorer工具（需要前端交互）\n"
            "2. 在机器人收到群消息时从消息体中获取openConversationId\n"
            "3. 创建群时从API响应中获取\n"
            "建议：在应用中缓存已知的openConversationId，或通过群名称维护映射关系"
        )

    def close(self) -> None:
        if self._session:
            self._session.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def __del__(self):
        try:
            self.close()
        except Exception:
            pass


# =========================
# 媒体上传服务
# =========================

MEDIA_TYPE_FILE = "file"
MEDIA_TYPE_IMAGE = "image"
MEDIA_TYPE_VOICE = "voice"
MEDIA_TYPE_VIDEO = "video"

EXT_TO_MEDIA_TYPE = {
    ".jpg": MEDIA_TYPE_IMAGE,
    ".jpeg": MEDIA_TYPE_IMAGE,
    ".png": MEDIA_TYPE_IMAGE,
    ".gif": MEDIA_TYPE_IMAGE,
    ".bmp": MEDIA_TYPE_IMAGE,
    ".webp": MEDIA_TYPE_IMAGE,
    ".amr": MEDIA_TYPE_VOICE,
    ".wav": MEDIA_TYPE_VOICE,
    ".mp3": MEDIA_TYPE_VOICE,
    ".mp4": MEDIA_TYPE_VIDEO,
    ".mov": MEDIA_TYPE_VIDEO,
}
DEFAULT_MEDIA_TYPE = MEDIA_TYPE_FILE


class MediaService:
    """
    媒体文件上传服务（用于发送文件、语音、图片等）
    """

    # 旧版上传端点
    UPLOAD_URL = "https://oapi.dingtalk.com/media/upload"

    def __init__(self, client: DingTalkClient):
        if not isinstance(client, DingTalkClient):
            raise ValueError("client 必须是 DingTalkClient 实例")
        self.client = client
        self._session = requests.Session()

    def _get_media_type_from_path(self, path: str) -> str:
        ext = Path(path).suffix.lower()
        return EXT_TO_MEDIA_TYPE.get(ext, DEFAULT_MEDIA_TYPE)

    def upload_file(
        self,
        file: Union[str, Path, BinaryIO],
        file_name: Optional[str] = None,
        media_type: Optional[str] = None,
        timeout: int = 60,
    ) -> str:
        """
        上传文件并返回 media_id
        """
        need_close = False
        path_str: Optional[str] = None

        if isinstance(file, (str, Path)):
            path_str = str(file)
            if not os.path.isfile(path_str):
                raise ValueError(f"文件不存在: {path_str}")
            file_name = file_name or os.path.basename(path_str)
            media_type = media_type or self._get_media_type_from_path(path_str)
            file_handle = open(path_str, "rb")
            need_close = True
        elif hasattr(file, "read"):
            if file_name is None or not str(file_name).strip():
                raise ValueError("使用文件对象时必须指定 file_name")
            media_type = media_type or self._get_media_type_from_path(str(file_name))
            file_handle = file  # type: ignore[assignment]
        else:
            raise ValueError("file 必须是文件路径（str/Path）或可读文件对象")

        try:
            if hasattr(file_handle, "seek"):
                file_handle.seek(0)
            token = self.client.get_access_token()
            url = f"{self.UPLOAD_URL}?access_token={token}&type={media_type}"

            files = {"media": (file_name, file_handle)}

            def _do_upload():
                if hasattr(file_handle, "seek"):
                    file_handle.seek(0)
                return self._session.post(url, files=files, timeout=timeout)

            response = _request_with_retry(_do_upload)
            response.raise_for_status()

            data = response.json()

            if data.get("errcode", 0) != 0:
                raise DingTalkAPIException(
                    str(data.get("errcode", "UNKNOWN")),
                    data.get("errmsg", "未知错误"),
                )

            media_id = data.get("media_id")
            if not media_id:
                raise DingTalkAPIException("PARSE_ERROR", "响应中无 media_id")

            return media_id

        except requests.exceptions.Timeout:
            raise NetworkException("上传超时")
        except requests.exceptions.ConnectionError as e:
            raise NetworkException("网络连接失败", e)
        except requests.exceptions.HTTPError as e:
            try:
                err = e.response.json()
                raise DingTalkAPIException(
                    str(err.get("errcode", e.response.status_code)),
                    err.get("errmsg", str(e)),
                )
            except (ValueError, AttributeError):
                raise NetworkException(f"HTTP 错误 {e.response.status_code}", e)
        except requests.exceptions.RequestException as e:
            raise NetworkException("请求失败", e)
        finally:
            if need_close and file_handle:
                file_handle.close()

    def upload_and_send_file(
        self,
        file: Union[str, Path, BinaryIO],
        file_name: Optional[str] = None,
        media_type: Optional[str] = None,
        timeout: int = 60,
    ) -> tuple:
        """
        上传文件并返回 (media_id, file_name, file_type)
        """
        media_id = self.upload_file(file, file_name=file_name, media_type=media_type, timeout=timeout)
        if file_name is None and isinstance(file, (str, Path)):
            file_name = os.path.basename(str(file))
        if not file_name:
            file_name = "file"
        ext = Path(file_name).suffix.lstrip(".").lower() or "file"
        return media_id, file_name, ext

    def close(self) -> None:
        if self._session:
            self._session.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def __del__(self):
        try:
            self.close()
        except Exception:
            pass


# =========================
# 单聊发送器
# =========================

class SingleChatSender(MessageSender):
    """
    单聊消息发送器
    """

    SINGLE_CHAT_ENDPOINT = "/v1.0/robot/oToMessages/batchSend"

    def _get_endpoint(self) -> str:
        return self.SINGLE_CHAT_ENDPOINT

    def send_to_user(
        self,
        user_ids: List[str],
        message: MessageBuilder,
        timeout: int = 10,
    ) -> Dict[str, Any]:
        if not user_ids:
            raise ValueError("user_ids不能为空")
        if len(user_ids) > 20:
            raise ValueError("单次最多支持向20个用户发送消息")

        self._validate_message(message)

        body = self._build_request_body(message=message, userIds=user_ids)
        return self._send_request(body, timeout)

    def send_to_single_user(
        self,
        user_id: str,
        message: MessageBuilder,
        timeout: int = 10,
    ) -> Dict[str, Any]:
        if not user_id or not user_id.strip():
            raise ValueError("user_id不能为空")
        return self.send_to_user(user_ids=[user_id], message=message, timeout=timeout)

    def batch_send(
        self,
        user_ids: List[str],
        message: MessageBuilder,
        batch_size: int = 20,
        timeout: int = 10,
    ) -> List[Dict[str, Any]]:
        if not user_ids:
            raise ValueError("user_ids不能为空")
        if batch_size <= 0 or batch_size > 20:
            raise ValueError("batch_size必须在1-20之间")

        self._validate_message(message)

        results: List[Dict[str, Any]] = []
        for i in range(0, len(user_ids), batch_size):
            batch = user_ids[i : i + batch_size]
            result = self.send_to_user(user_ids=batch, message=message, timeout=timeout)
            results.append(result)
        return results

    def send_to_user_by_name(
        self,
        user_name: str,
        message: MessageBuilder,
        exact_match: bool = False,
        timeout: int = 10,
    ) -> Dict[str, Any]:
        if not user_name or not user_name.strip():
            raise ValueError("用户名不能为空")

        contact_service = ContactService(self.client)
        try:
            user_id = contact_service.search_user_by_name(user_name.strip(), exact_match=exact_match)
            if not user_id:
                raise ValueError(f"未找到用户: {user_name}")
            return self.send_to_single_user(user_id=user_id, message=message, timeout=timeout)
        finally:
            contact_service.close()

    def send_to_users_by_names(
        self,
        user_names: List[str],
        message: MessageBuilder,
        exact_match: bool = False,
        skip_not_found: bool = True,
        timeout: int = 10,
    ) -> Dict[str, Any]:
        if not user_names:
            raise ValueError("用户名列表不能为空")

        self._validate_message(message)

        contact_service = ContactService(self.client)
        try:
            search_result = contact_service.batch_search_users(user_names, exact_match=exact_match)

            found_user_ids: List[str] = []
            not_found: List[str] = []

            for name, user_id in search_result.items():
                if user_id:
                    found_user_ids.append(user_id)
                else:
                    not_found.append(name)

            if not found_user_ids:
                if skip_not_found:
                    return {
                        "result": None,
                        "not_found": not_found,
                        "sent_count": 0,
                        "message": "所有用户都未找到",
                    }
                raise ValueError(f"所有用户都未找到: {', '.join(not_found)}")

            if not_found and not skip_not_found:
                raise ValueError(f"以下用户未找到: {', '.join(not_found)}")

            api_result = self.send_to_user(
                user_ids=found_user_ids,
                message=message,
                timeout=timeout,
            )

            return {
                "result": api_result,
                "not_found": not_found,
                "sent_count": len(found_user_ids),
                "message": f"成功发送给{len(found_user_ids)}个用户"
                + (f"，{len(not_found)}个用户未找到" if not_found else ""),
            }
        finally:
            contact_service.close()

    def send_file_to_user(
        self,
        user_id_or_name: str,
        file_path: str,
        file_name: Optional[str] = None,
        by_name: bool = False,
        exact_match: bool = False,
        timeout: int = 60,
    ) -> Dict[str, Any]:
        """
        向单个用户发送文件（可按 userId 或 用户名）
        """
        if by_name:
            return self.send_file_to_user_by_name(
                user_name=user_id_or_name,
                file_path=file_path,
                file_name=file_name,
                exact_match=exact_match,
                timeout=timeout,
            )

        name = file_name or os.path.basename(file_path)
        ext = os.path.splitext(name)[1].lstrip(".").lower() or "file"

        media = MediaService(self.client)
        try:
            media_id = media.upload_file(file_path, file_name=name, timeout=timeout)
            msg = FileMessage(media_id, name, ext)
            return self.send_to_single_user(user_id_or_name, msg, timeout=min(timeout, 10))
        finally:
            media.close()

    def send_file_to_users(
        self,
        user_ids: List[str],
        file_path: str,
        file_name: Optional[str] = None,
        timeout: int = 60,
    ) -> Dict[str, Any]:
        name = file_name or os.path.basename(file_path)
        ext = os.path.splitext(name)[1].lstrip(".").lower() or "file"

        media = MediaService(self.client)
        try:
            media_id = media.upload_file(file_path, file_name=name, timeout=timeout)
            msg = FileMessage(media_id, name, ext)
            return self.send_to_user(user_ids, msg, timeout=min(timeout, 10))
        finally:
            media.close()

    def send_file_to_user_by_name(
        self,
        user_name: str,
        file_path: str,
        file_name: Optional[str] = None,
        exact_match: bool = False,
        timeout: int = 60,
    ) -> Dict[str, Any]:
        if not user_name or not user_name.strip():
            raise ValueError("用户名不能为空")

        contact = ContactService(self.client)
        try:
            uid = contact.search_user_by_name(user_name.strip(), exact_match=exact_match)
            if not uid:
                raise ValueError(f"未找到用户: {user_name}")
            return self.send_file_to_user(uid, file_path, file_name=file_name, timeout=timeout)
        finally:
            contact.close()

    def send_file_to_users_by_names(
        self,
        user_names: List[str],
        file_path: str,
        file_name: Optional[str] = None,
        exact_match: bool = False,
        skip_not_found: bool = True,
        timeout: int = 60,
    ) -> Dict[str, Any]:
        if not user_names:
            raise ValueError("用户名列表不能为空")

        contact = ContactService(self.client)
        try:
            search_result = contact.batch_search_users(user_names, exact_match=exact_match)
            found_user_ids = [uid for uid in search_result.values() if uid]
            not_found = [name for name, uid in search_result.items() if not uid]

            if not found_user_ids:
                if skip_not_found:
                    return {
                        "result": None,
                        "not_found": not_found,
                        "sent_count": 0,
                        "message": "所有用户都未找到",
                    }
                raise ValueError(f"所有用户都未找到: {', '.join(not_found)}")

            if not_found and not skip_not_found:
                raise ValueError(f"以下用户未找到: {', '.join(not_found)}")

            api_result = self.send_file_to_users(
                user_ids=found_user_ids,
                file_path=file_path,
                file_name=file_name,
                timeout=timeout,
            )

            return {
                "result": api_result,
                "not_found": not_found,
                "sent_count": len(found_user_ids),
                "message": f"成功发送给{len(found_user_ids)}个用户"
                + (f"，{len(not_found)}个用户未找到" if not_found else ""),
            }
        finally:
            contact.close()


# =========================
# 群聊发送器
# =========================

class GroupChatSender(MessageSender):
    """
    群聊消息发送器
    """

    GROUP_CHAT_ENDPOINT = "/v1.0/robot/groupMessages/send"

    def _get_endpoint(self) -> str:
        return self.GROUP_CHAT_ENDPOINT

    def send_to_group(
        self,
        conversation_id: str,
        message: MessageBuilder,
        at_users: Optional[List[str]] = None,
        at_all: bool = False,
        timeout: int = 10,
    ) -> Dict[str, Any]:
        if not conversation_id or not conversation_id.strip():
            raise ValueError("conversation_id不能为空")

        self._validate_message(message)

        body = self._build_request_body(
            message=message,
            openConversationId=conversation_id,
        )

        if at_users or at_all:
            at_info: Dict[str, Any] = {}
            if at_users:
                at_info["atUserIds"] = at_users
            if at_all:
                at_info["isAtAll"] = True

            msg_param = json.loads(body["msgParam"])
            msg_param["at"] = at_info
            body["msgParam"] = json.dumps(msg_param, ensure_ascii=False)

        return self._send_request(body, timeout)

    def send_text(
        self,
        conversation_id: str,
        content: str,
        at_users: Optional[List[str]] = None,
        at_all: bool = False,
        timeout: int = 10,
    ) -> Dict[str, Any]:
        msg = TextMessage(content)
        return self.send_to_group(
            conversation_id=conversation_id,
            message=msg,
            at_users=at_users,
            at_all=at_all,
            timeout=timeout,
        )

    def send_markdown(
        self,
        conversation_id: str,
        title: str,
        text: str,
        at_users: Optional[List[str]] = None,
        at_all: bool = False,
        timeout: int = 10,
    ) -> Dict[str, Any]:
        msg = MarkdownMessage(title, text)
        return self.send_to_group(
            conversation_id=conversation_id,
            message=msg,
            at_users=at_users,
            at_all=at_all,
            timeout=timeout,
        )

    def send_actioncard(
        self,
        conversation_id: str,
        title: str,
        text: str,
        single_title: str,
        single_url: str,
        at_users: Optional[List[str]] = None,
        at_all: bool = False,
        timeout: int = 10,
    ) -> Dict[str, Any]:
        msg = ActionCardMessage(
            title=title,
            text=text,
            single_title=single_title,
            single_url=single_url,
        )
        return self.send_to_group(
            conversation_id=conversation_id,
            message=msg,
            at_users=at_users,
            at_all=at_all,
            timeout=timeout,
        )

    def send_image(
        self,
        conversation_id: str,
        photo_url: str,
        at_users: Optional[List[str]] = None,
        at_all: bool = False,
        timeout: int = 10,
    ) -> Dict[str, Any]:
        msg = ImageMessage(photo_url)
        return self.send_to_group(
            conversation_id=conversation_id,
            message=msg,
            at_users=at_users,
            at_all=at_all,
            timeout=timeout,
        )

    def batch_send_to_groups(
        self,
        conversation_ids: List[str],
        message: MessageBuilder,
        at_users: Optional[List[str]] = None,
        at_all: bool = False,
        timeout: int = 10,
    ) -> List[Dict[str, Any]]:
        if not conversation_ids:
            raise ValueError("conversation_ids不能为空")

        self._validate_message(message)

        results: List[Dict[str, Any]] = []
        for cid in conversation_ids:
            try:
                result = self.send_to_group(
                    conversation_id=cid,
                    message=message,
                    at_users=at_users,
                    at_all=at_all,
                    timeout=timeout,
                )
                results.append({"conversation_id": cid, "success": True, "data": result})
            except Exception as e:
                results.append({"conversation_id": cid, "success": False, "error": str(e)})
        return results

    def send_file_to_group(
        self,
        conversation_id: str,
        file_path: str,
        file_name: Optional[str] = None,
        at_users: Optional[List[str]] = None,
        at_all: bool = False,
        timeout: int = 60,
    ) -> Dict[str, Any]:
        """
        向群组发送文件（先上传再发送）
        """
        name = file_name or os.path.basename(file_path)
        ext = os.path.splitext(name)[1].lstrip(".").lower() or "file"

        media = MediaService(self.client)
        try:
            media_id = media.upload_file(file_path, file_name=name, timeout=timeout)
            msg = FileMessage(media_id, name, ext)
            return self.send_to_group(
                conversation_id=conversation_id,
                message=msg,
                at_users=at_users,
                at_all=at_all,
                timeout=min(timeout, 10),
            )
        finally:
            media.close()


# =========================
# 单例门面：一次配置，直接调方法传参
# =========================

class DingTalkBot:
    """
    钉钉机器人单例门面。

    用法：
        在应用启动时用 get_instance 创建一次（同 app_key/app_secret/robot_code 会复用同一实例），
        之后只需调用方法、传入参数即可，无需再管 client/sender 等。

    示例：
        bot = DingTalkBot.get_instance(
            app_key="xxx",
            app_secret="yyy",
            robot_code="zzz",
        )
        bot.send_text_to_user("张三", "你好")
        bot.send_file_to_user("李四", "/path/to/file.pdf", by_name=True)
        bot.send_text_to_group("cidXXX==", "群通知", at_all=True)
    """

    _instances: Dict[tuple, "DingTalkBot"] = {}

    def __init__(self, app_key: str, app_secret: str, robot_code: str):
        if not app_key or not app_secret or not robot_code:
            raise ValueError("app_key、app_secret、robot_code 均不能为空")
        self._app_key = app_key
        self._app_secret = app_secret
        self._robot_code = robot_code
        self._client = DingTalkClient(app_key, app_secret)
        self._single = SingleChatSender(self._client, robot_code)
        self._group = GroupChatSender(self._client, robot_code)

    @classmethod
    def get_instance(
        cls,
        app_key: str,
        app_secret: str,
        robot_code: str,
    ) -> "DingTalkBot":
        """
        获取单例实例。相同 (app_key, app_secret, robot_code) 返回同一实例。
        """
        key = (app_key, app_secret, robot_code)
        if key not in cls._instances:
            cls._instances[key] = cls(app_key=app_key, app_secret=app_secret, robot_code=robot_code)
        return cls._instances[key]

    # ---------- 单聊：按用户发消息 ----------

    def send_text_to_user(
        self,
        user_id_or_name: str,
        content: str,
        by_name: bool = True,
        exact_match: bool = False,
        timeout: int = 10,
    ) -> Dict[str, Any]:
        """
        向单个用户发文本。默认按用户名查找（by_name=True）。
        """
        msg = TextMessage(content)
        if by_name:
            return self._single.send_to_user_by_name(
                user_name=user_id_or_name,
                message=msg,
                exact_match=exact_match,
                timeout=timeout,
            )
        return self._single.send_to_single_user(user_id_or_name, msg, timeout=timeout)

    def send_markdown_to_user(
        self,
        user_id_or_name: str,
        title: str,
        text: str,
        by_name: bool = True,
        exact_match: bool = False,
        timeout: int = 10,
    ) -> Dict[str, Any]:
        """向单个用户发 Markdown。"""
        msg = MarkdownMessage(title, text)
        if by_name:
            return self._single.send_to_user_by_name(
                user_name=user_id_or_name,
                message=msg,
                exact_match=exact_match,
                timeout=timeout,
            )
        return self._single.send_to_single_user(user_id_or_name, msg, timeout=timeout)

    def send_file_to_user(
        self,
        user_id_or_name: str,
        file_path: str,
        file_name: Optional[str] = None,
        by_name: bool = True,
        exact_match: bool = False,
        timeout: int = 60,
    ) -> Dict[str, Any]:
        """
        向单个用户发文件。默认按用户名查找（by_name=True）。
        """
        return self._single.send_file_to_user(
            user_id_or_name=user_id_or_name,
            file_path=file_path,
            file_name=file_name,
            by_name=by_name,
            exact_match=exact_match,
            timeout=timeout,
        )

    def send_to_users(
        self,
        user_ids: List[str],
        content: str,
        timeout: int = 10,
    ) -> Dict[str, Any]:
        """向多个用户 ID 发同一文本。"""
        return self._single.send_to_user(
            user_ids=user_ids,
            message=TextMessage(content),
            timeout=timeout,
        )

    def send_to_users_by_names(
        self,
        user_names: List[str],
        content: str,
        exact_match: bool = False,
        skip_not_found: bool = True,
        timeout: int = 10,
    ) -> Dict[str, Any]:
        """按用户名列表批量发文本。"""
        return self._single.send_to_users_by_names(
            user_names=user_names,
            message=TextMessage(content),
            exact_match=exact_match,
            skip_not_found=skip_not_found,
            timeout=timeout,
        )

    # ---------- 群聊 ----------

    def send_text_to_group(
        self,
        conversation_id: str,
        content: str,
        at_all: bool = False,
        at_users: Optional[List[str]] = None,
        timeout: int = 10,
    ) -> Dict[str, Any]:
        """向群发文本。"""
        return self._group.send_text(
            conversation_id=conversation_id,
            content=content,
            at_all=at_all,
            at_users=at_users,
            timeout=timeout,
        )

    def send_markdown_to_group(
        self,
        conversation_id: str,
        title: str,
        text: str,
        at_all: bool = False,
        at_users: Optional[List[str]] = None,
        timeout: int = 10,
    ) -> Dict[str, Any]:
        """向群发 Markdown。"""
        return self._group.send_markdown(
            conversation_id=conversation_id,
            title=title,
            text=text,
            at_all=at_all,
            at_users=at_users,
            timeout=timeout,
        )

    def send_file_to_group(
        self,
        conversation_id: str,
        file_path: str,
        file_name: Optional[str] = None,
        at_all: bool = False,
        at_users: Optional[List[str]] = None,
        timeout: int = 60,
    ) -> Dict[str, Any]:
        """向群发文件。"""
        return self._group.send_file_to_group(
            conversation_id=conversation_id,
            file_path=file_path,
            file_name=file_name,
            at_all=at_all,
            at_users=at_users,
            timeout=timeout,
        )

    # ---------- 通讯录（按需） ----------

    def search_user(self, name: str, exact_match: bool = False) -> Optional[str]:
        """根据用户名查 userId，未找到返回 None。"""
        contact = ContactService(self._client)
        try:
            return contact.search_user_by_name(name, exact_match=exact_match)
        finally:
            contact.close()

    def close(self) -> None:
        """释放连接等资源（一般可忽略，进程退出时会自动释放）。"""
        self._single.close()
        self._group.close()
        self._client.close()

    def __enter__(self) -> "DingTalkBot":
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.close()


# =========================
# 模块级暴露：供 RPA/外部先 init_bot，再直接调这些方法
# =========================

_current_bot: Optional[DingTalkBot] = None


def init_bot(app_key: str, app_secret: str, robot_code: str) -> DingTalkBot:
    """
    初始化钉钉单例并设为当前实例（必须先调用，再使用下方暴露的 send_* / search_user 等方法）。
    相同 (app_key, app_secret, robot_code) 会复用同一实例。
    """
    global _current_bot
    _current_bot = DingTalkBot.get_instance(
        app_key=app_key,
        app_secret=app_secret,
        robot_code=robot_code,
    )
    return _current_bot


def get_bot() -> DingTalkBot:
    """
    获取当前已初始化的单例。若未调用过 init_bot，会抛出 RuntimeError。
    """
    if _current_bot is None:
        raise RuntimeError("请先调用 init_bot(app_key, app_secret, robot_code) 初始化")
    return _current_bot


# ---------- 单聊：发送消息、文件 ----------

def send_text_to_user(
    user_id_or_name: str,
    content: str,
    by_name: bool = True,
    exact_match: bool = False,
    timeout: int = 10,
) -> Dict[str, Any]:
    """向单个用户发文本。默认按用户名查找（by_name=True）。"""
    return get_bot().send_text_to_user(
        user_id_or_name=user_id_or_name,
        content=content,
        by_name=by_name,
        exact_match=exact_match,
        timeout=timeout,
    )


def send_markdown_to_user(
    user_id_or_name: str,
    title: str,
    text: str,
    by_name: bool = True,
    exact_match: bool = False,
    timeout: int = 10,
) -> Dict[str, Any]:
    """向单个用户发 Markdown。"""
    return get_bot().send_markdown_to_user(
        user_id_or_name=user_id_or_name,
        title=title,
        text=text,
        by_name=by_name,
        exact_match=exact_match,
        timeout=timeout,
    )


def send_file_to_user(
    user_id_or_name: str,
    file_path: str,
    file_name: Optional[str] = None,
    by_name: bool = True,
    exact_match: bool = False,
    timeout: int = 60,
) -> Dict[str, Any]:
    """向单个用户发文件。默认按用户名查找（by_name=True）。"""
    return get_bot().send_file_to_user(
        user_id_or_name=user_id_or_name,
        file_path=file_path,
        file_name=file_name,
        by_name=by_name,
        exact_match=exact_match,
        timeout=timeout,
    )


def send_to_users(
    user_ids: List[str],
    content: str,
    timeout: int = 10,
) -> Dict[str, Any]:
    """向多个用户 ID 发同一文本。"""
    return get_bot().send_to_users(user_ids=user_ids, content=content, timeout=timeout)


def send_to_users_by_names(
    user_names: List[str],
    content: str,
    exact_match: bool = False,
    skip_not_found: bool = True,
    timeout: int = 10,
) -> Dict[str, Any]:
    """按用户名列表批量发文本。"""
    return get_bot().send_to_users_by_names(
        user_names=user_names,
        content=content,
        exact_match=exact_match,
        skip_not_found=skip_not_found,
        timeout=timeout,
    )


# ---------- 群聊 ----------

def send_text_to_group(
    conversation_id: str,
    content: str,
    at_all: bool = False,
    at_users: Optional[List[str]] = None,
    timeout: int = 10,
) -> Dict[str, Any]:
    """向群发文本。"""
    return get_bot().send_text_to_group(
        conversation_id=conversation_id,
        content=content,
        at_all=at_all,
        at_users=at_users,
        timeout=timeout,
    )


def send_markdown_to_group(
    conversation_id: str,
    title: str,
    text: str,
    at_all: bool = False,
    at_users: Optional[List[str]] = None,
    timeout: int = 10,
) -> Dict[str, Any]:
    """向群发 Markdown。"""
    return get_bot().send_markdown_to_group(
        conversation_id=conversation_id,
        title=title,
        text=text,
        at_all=at_all,
        at_users=at_users,
        timeout=timeout,
    )


def send_file_to_group(
    conversation_id: str,
    file_path: str,
    file_name: Optional[str] = None,
    at_all: bool = False,
    at_users: Optional[List[str]] = None,
    timeout: int = 60,
) -> Dict[str, Any]:
    """向群发文件。"""
    return get_bot().send_file_to_group(
        conversation_id=conversation_id,
        file_path=file_path,
        file_name=file_name,
        at_all=at_all,
        at_users=at_users,
        timeout=timeout,
    )


# ---------- 通讯录 ----------

def search_user(name: str, exact_match: bool = False) -> Optional[str]:
    """根据用户名查 userId，未找到返回 None。"""
    return get_bot().search_user(name=name, exact_match=exact_match)