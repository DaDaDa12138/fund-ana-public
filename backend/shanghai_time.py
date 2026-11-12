"""Time contract for new operator-facing timestamps; legacy evidence is immutable."""
from datetime import datetime
from zoneinfo import ZoneInfo

SHANGHAI = ZoneInfo("Asia/Shanghai")


def now_shanghai() -> datetime:
    return datetime.now(SHANGHAI)


def display_shanghai(value: str | None) -> str:
    if not value:
        return "未提供"
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    # Existing publication wall clocks were produced under TZ=Asia/Shanghai.
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=SHANGHAI)
    return parsed.astimezone(SHANGHAI).strftime("%Y-%m-%d %H:%M:%S +08:00")
