"""
✝ THE FALLEN ✝ - Shared Utilities
Common helper functions used across all cogs.
"""

import discord
import datetime
import asyncio
from config import (
    STAFF_ROLE_NAME, HIGH_STAFF_ROLES, LOG_CHANNEL_NAME,
    API_CALL_DELAY, BULK_OPERATION_DELAY, MAX_BULK_ACTIONS_PER_MINUTE,
    RAID_RANKS
)

# Rate limit tracker
_api_tracker = {"last_call": 0, "calls_this_minute": 0, "minute_start": 0}


def is_staff(user) -> bool:
    """Check if a member has staff permissions."""
    if user.guild_permissions.administrator:
        return True
    user_role_names = [role.name for role in user.roles]
    if STAFF_ROLE_NAME in user_role_names:
        return True
    return any(role in user_role_names for role in HIGH_STAFF_ROLES)


def is_high_staff(user) -> bool:
    """Check if a member has high staff permissions."""
    if user.guild_permissions.administrator:
        return True
    user_role_names = [role.name for role in user.roles]
    return any(role in user_role_names for role in HIGH_STAFF_ROLES)


async def log_action(guild, title: str, description: str, color: int = 0x3498db):
    """Log an action to the log channel."""
    channel = discord.utils.get(guild.text_channels, name=LOG_CHANNEL_NAME)
    if channel:
        embed = discord.Embed(
            title=title, description=description, color=color,
            timestamp=datetime.datetime.now(datetime.timezone.utc)
        )
        try:
            await channel.send(embed=embed)
        except Exception:
            pass


async def rate_limited_action(coro, delay=API_CALL_DELAY):
    """Execute an action with rate limit protection."""
    global _api_tracker
    now = datetime.datetime.now().timestamp()
    
    if now - _api_tracker["minute_start"] > 60:
        _api_tracker["calls_this_minute"] = 0
        _api_tracker["minute_start"] = now
    
    if _api_tracker["calls_this_minute"] >= MAX_BULK_ACTIONS_PER_MINUTE:
        wait_time = 60 - (now - _api_tracker["minute_start"])
        if wait_time > 0:
            await asyncio.sleep(wait_time)
        _api_tracker["calls_this_minute"] = 0
        _api_tracker["minute_start"] = datetime.datetime.now().timestamp()
    
    time_since_last = now - _api_tracker["last_call"]
    if time_since_last < delay:
        await asyncio.sleep(delay - time_since_last)
    
    _api_tracker["last_call"] = datetime.datetime.now().timestamp()
    _api_tracker["calls_this_minute"] += 1
    
    return await coro


def get_raid_rank(total_raids: int) -> dict:
    """Get raid rank based on total raids completed."""
    current_rank = RAID_RANKS[0]
    for threshold, rank_info in sorted(RAID_RANKS.items()):
        if total_raids >= threshold:
            current_rank = rank_info
    return current_rank


def format_duration(seconds: int) -> str:
    """Format seconds into a human-readable duration."""
    if seconds < 60:
        return f"{seconds}s"
    elif seconds < 3600:
        return f"{seconds // 60}m {seconds % 60}s"
    else:
        hours = seconds // 3600
        minutes = (seconds % 3600) // 60
        return f"{hours}h {minutes}m"


def progress_bar(current: int, maximum: int, length: int = 10) -> str:
    """Create a text-based progress bar."""
    if maximum == 0:
        return "░" * length
    filled = int(length * current / maximum)
    return "█" * filled + "░" * (length - filled)


def ordinal(n: int) -> str:
    """Convert number to ordinal string (1st, 2nd, 3rd, etc.)."""
    if 11 <= (n % 100) <= 13:
        suffix = "th"
    else:
        suffix = {1: "st", 2: "nd", 3: "rd"}.get(n % 10, "th")
    return f"{n}{suffix}"
