from __future__ import annotations

from dataclasses import dataclass, field

from imessage_sender import BlueBubblesAPIConfig


@dataclass(frozen=True)
class AppConfig:
    imessage_text: str = "Hi"
    imessage_default_mode: str = "image"
    imessage_default_image_path: str = "img/send_msg.png"
    imessage_state_path: str = ".bluebubbles_send_history.json"
    imessage_batch_root_dir: str = "imessage_batches"
    api_host: str = "127.0.0.1"
    api_port: int = 8787
    imessage_bluebubbles_config: BlueBubblesAPIConfig = field(default_factory=BlueBubblesAPIConfig)
    imessage_delivery_check_timeout_seconds: int = 90
    imessage_delivery_check_interval_seconds: int = 3


APP_CONFIG = AppConfig()
