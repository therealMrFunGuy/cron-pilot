"""Cron expression utilities using croniter."""

from datetime import datetime, timezone
from croniter import croniter


def validate_cron(expression: str) -> bool:
    """Check if a cron expression is valid."""
    try:
        croniter(expression)
        return True
    except (ValueError, KeyError, TypeError):
        return False


def next_run_time(expression: str, base: datetime | None = None) -> datetime:
    """Get the next run time for a cron expression."""
    if base is None:
        base = datetime.now(timezone.utc)
    cron = croniter(expression, base)
    return cron.get_next(datetime).replace(tzinfo=timezone.utc)


def prev_run_time(expression: str, base: datetime | None = None) -> datetime:
    """Get the previous run time for a cron expression."""
    if base is None:
        base = datetime.now(timezone.utc)
    cron = croniter(expression, base)
    return cron.get_prev(datetime).replace(tzinfo=timezone.utc)


def next_n_runs(expression: str, n: int = 5, base: datetime | None = None) -> list[datetime]:
    """Get the next N run times for a cron expression."""
    if base is None:
        base = datetime.now(timezone.utc)
    cron = croniter(expression, base)
    return [cron.get_next(datetime).replace(tzinfo=timezone.utc) for _ in range(n)]


def describe_cron(expression: str) -> str:
    """Return a human-readable description of a cron expression."""
    parts = expression.strip().split()
    if len(parts) != 5:
        return expression

    minute, hour, dom, month, dow = parts

    # Common patterns
    if expression == "* * * * *":
        return "Every minute"
    if minute.startswith("*/"):
        interval = minute[2:]
        if hour == "*" and dom == "*" and month == "*" and dow == "*":
            return f"Every {interval} minutes"
    if minute != "*" and hour != "*" and dom == "*" and month == "*" and dow == "*":
        return f"Daily at {hour.zfill(2)}:{minute.zfill(2)} UTC"
    if minute != "*" and hour != "*" and dom == "*" and month == "*" and dow != "*":
        return f"At {hour.zfill(2)}:{minute.zfill(2)} UTC on {dow}"
    if minute == "0" and hour == "0" and dom == "1" and month == "*" and dow == "*":
        return "Monthly on the 1st at midnight UTC"

    return expression
