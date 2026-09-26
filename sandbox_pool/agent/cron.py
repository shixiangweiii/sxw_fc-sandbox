"""定时任务的触发时间计算：标准 5 段 cron 表达式（分 时 日 月 周）+ 时区。

- 每段支持 `*`、`n`、`a-b`、`*/s`、`a-b/s` 以及逗号分隔的组合；周的 0 和 7 都表示周日。
- 日与周都不是 `*` 时按「或」匹配（与 Vixie cron 一致）。
- 按分钟粒度计算，返回严格晚于给定时刻的下一次触发时间（epoch 秒）。
"""

from dataclasses import dataclass
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

_FIELDS = (("minute", 0, 59), ("hour", 0, 23), ("day", 1, 31), ("month", 1, 12), ("weekday", 0, 7))
# 搜索范围：9 年，覆盖 2 月 29 日这类最稀疏的表达式（跨世纪时两个闰年最多相隔 8 年，如 2096 → 2104）。
# 按日期截止：永不触发的表达式（如 2 月 31 日）几百步就能判定，不会长时间占住事件循环
_MAX_SEARCH_DAYS = 9 * 366
# 步数兜底（正常表达式每年最多几千步）
_MAX_STEPS = 200_000


@dataclass(frozen=True)
class CronSpec:
    minutes: frozenset
    hours: frozenset
    days: frozenset
    months: frozenset
    weekdays: frozenset  # 0 = 周日 … 6 = 周六
    day_any: bool
    weekday_any: bool

    def day_matches(self, dt: datetime) -> bool:
        weekday = (dt.weekday() + 1) % 7  # datetime：周一为 0 → cron：周日为 0
        if self.day_any and self.weekday_any:
            return True
        if self.day_any:
            return weekday in self.weekdays
        if self.weekday_any:
            return dt.day in self.days
        return dt.day in self.days or weekday in self.weekdays


def _parse_field(text: str, name: str, lo: int, hi: int) -> tuple[frozenset, bool]:
    values: set[int] = set()
    any_ = text == "*"
    for part in text.split(","):
        part = part.strip()
        if not part:
            raise ValueError(f"cron {name}: empty item")
        base, _, step_text = part.partition("/")
        step = 1
        if step_text:
            if not step_text.isdigit() or int(step_text) == 0:
                raise ValueError(f"cron {name}: invalid step {step_text!r}")
            step = int(step_text)
        if base == "*":
            start, end = lo, hi
        elif "-" in base:
            a, _, b = base.partition("-")
            if not (a.isdigit() and b.isdigit()):
                raise ValueError(f"cron {name}: invalid range {base!r}")
            start, end = int(a), int(b)
        elif base.isdigit():
            start = end = int(base)
            if step_text:
                end = hi
        else:
            raise ValueError(f"cron {name}: invalid value {base!r}")
        if start < lo or end > hi or start > end:
            raise ValueError(f"cron {name}: {part!r} out of range {lo}-{hi}")
        values.update(range(start, end + 1, step))
    return frozenset(values), any_


def parse(expr: str) -> CronSpec:
    parts = expr.split()
    if len(parts) != 5:
        raise ValueError("cron expression must have 5 fields: minute hour day month weekday")
    parsed = [_parse_field(p, name, lo, hi) for p, (name, lo, hi) in zip(parts, _FIELDS)]
    weekdays = frozenset(d % 7 for d in parsed[4][0])
    return CronSpec(
        minutes=parsed[0][0],
        hours=parsed[1][0],
        days=parsed[2][0],
        months=parsed[3][0],
        weekdays=weekdays,
        day_any=parsed[2][1],
        weekday_any=parsed[4][1],
    )


def zone(name: str) -> ZoneInfo:
    try:
        return ZoneInfo(name)
    except (ZoneInfoNotFoundError, ValueError) as e:
        raise ValueError(f"unknown timezone {name!r}") from e


def next_fire(expr: str, tz: str, after: float) -> float:
    """严格晚于 after 的下一次触发时间（epoch 秒）。在时区的本地时间上逐级跳跃搜索。"""
    spec = parse(expr)
    tzinfo = zone(tz)
    local = datetime.fromtimestamp(after, tzinfo).replace(tzinfo=None, second=0, microsecond=0) + timedelta(minutes=1)
    limit = local + timedelta(days=_MAX_SEARCH_DAYS)
    for _ in range(_MAX_STEPS):
        if local > limit:
            break
        if local.month not in spec.months:
            # 跳到下个月 1 日 0 点
            year, month = (local.year + 1, 1) if local.month == 12 else (local.year, local.month + 1)
            local = datetime(year, month, 1)
            continue
        if not spec.day_matches(local):
            local = datetime(local.year, local.month, local.day) + timedelta(days=1)
            continue
        if local.hour not in spec.hours:
            local = datetime(local.year, local.month, local.day, local.hour) + timedelta(hours=1)
            continue
        if local.minute not in spec.minutes:
            local += timedelta(minutes=1)
            continue
        ts = local.replace(tzinfo=tzinfo).timestamp()
        if ts > after:
            return ts
        local += timedelta(minutes=1)
    raise ValueError(f"cron expression {expr!r} never fires")
