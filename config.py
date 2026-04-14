from __future__ import annotations

from dataclasses import dataclass, field

from imessage_sender import IMessageRiskControl


@dataclass(frozen=True)
class AppConfig:
    lookback_days: int = 31
    order_statuses: tuple[int, ...] = (4, 5)
    orders_out: str = "领星待审核待发货订单.json"
    phones_out: str = "领星待审核待发货手机号.json"
    order_export_out: str = "订单管理导出.xlsx"
    print_request_debug: bool = True
    imessage_text: str = (
        "Hello, I am a seller of Jennov brand security cameras on TikTok. You purchased a security "
        "camera from our store. If you need a video tutorial on how to install it, please feel free to "
        "contact me.\n"
        "After receiving your order, we are here to help. Don't hesitate to contact us if you have any "
        "questions. Please don't give bad review. Thank you.\n"
        "If you want to return the camera, please select \"No longer needed\" as the reason for the return. "
        "We will prioritize your refund.\n"
        "Hola, soy vendedor de cámaras de seguridad de la marca Jennov en TikTok. Usted compró una cámara "
        "de seguridad en nuestra tienda. Si necesita un video tutorial sobre cómo instalarla, no dude en "
        "contactarme.\n"
        "Después de recibir su pedido, estamos aquí para ayudarle. No dude en contactarnos si tiene alguna "
        "pregunta. Le agradeceríamos que evite dejar una mala reseña. Gracias.\n"
        "Si desea devolver la cámara, por favor seleccione \"Ya no lo necesito\" como motivo de la "
        "devolución. Así priorizaremos su reembolso."
    )
    imessage_send_enabled: bool = False
    imessage_dry_run: bool = False
    imessage_max_send_count: int | None = None
    imessage_state_path: str = ".领星待审核待发货iMessage发送历史.json"
    imessage_batch_root_dir: str = "imessage_batches"
    imessage_delivery_check_timeout_seconds: int = 45
    imessage_delivery_check_interval_seconds: int = 3
    imessage_delivery_check_lookback_seconds: int = 600
    imessage_risk_control: IMessageRiskControl = field(default_factory=IMessageRiskControl)


APP_CONFIG = AppConfig()
