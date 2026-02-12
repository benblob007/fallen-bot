"""
✝ THE FALLEN ✝ - Guardian System
Global anti-abuse, structured logging, command audit trail, and protection layer.

This cog acts as a global middleware that:
  - Rate-limits ALL commands per user (even ones without @commands.cooldown)
  - Detects and auto-punishes command spam / button spam
  - Logs every command execution with context to file + Discord channel
  - Captures and reports errors with full tracebacks
  - Tracks per-user abuse scores that decay over time
  - Provides !auditlog, !abuselog, !botlogs commands for staff
"""

import discord
from discord.ext import commands, tasks
import datetime
import asyncio
import traceback
import os
import json
from collections import defaultdict
from io import StringIO

# ==========================================
# CONFIGURATION
# ==========================================

# Global rate limits (commands per window)
GLOBAL_CMD_LIMIT = 8           # Max commands per user in the window
GLOBAL_CMD_WINDOW = 10         # Window in seconds
STRICT_CMD_LIMIT = 3           # Max of the SAME command in the window
STRICT_CMD_WINDOW = 15         # Window for same-command spam

# Interaction rate limits (buttons/modals/selects)
INTERACTION_LIMIT = 12         # Max interactions per window
INTERACTION_WINDOW = 10        # Window in seconds

# Abuse escalation thresholds
WARN_THRESHOLD = 3             # Violations before warning DM
RESTRICT_THRESHOLD = 6         # Violations before temp-restrict (commands ignored)
STAFF_ALERT_THRESHOLD = 8     # Violations before alerting staff channel
AUTO_TIMEOUT_THRESHOLD = 12    # Violations before auto-timeout

# Restriction and decay
RESTRICT_DURATION = 60         # Seconds commands are ignored after hitting threshold
ABUSE_DECAY_INTERVAL = 300     # Seconds between abuse score decay (5 min)
ABUSE_DECAY_AMOUNT = 2         # Points removed per decay tick

# Auto-timeout duration (Discord timeout)
AUTO_TIMEOUT_MINUTES = 5

# Log file settings
LOG_DIR = "logs"
LOG_FILE = "bot_commands.log"
ERROR_LOG_FILE = "bot_errors.log"
AUDIT_LOG_FILE = "bot_audit.log"
MAX_LOG_SIZE_MB = 10

# Channels (will fall back to "fallen-logs" if not found)
GUARDIAN_LOG_CHANNEL = "fallen-logs"
ERROR_ALERT_CHANNEL = "fallen-logs"

# Commands exempt from rate limiting (staff/admin setup commands)
EXEMPT_COMMANDS = {
    "migrate_db", "loadcog", "unloadcog", "reloadcog", "dbstatus",
    "sync", "syncglobal", "clearsync", "clearglobal",
    "setup_permissions", "fix_muted",
}

# Staff role names (users with these bypass rate limits)
STAFF_ROLE_NAMES = [
    "Staff",
    "The Fallen Sovereign〢Owner",
    "The Fallen Right Hand〢Co-Owner",
    "The Fallen Marshal〢Head of Staff",
]


# ==========================================
# FILE LOGGER
# ==========================================

class FileLogger:
    """Thread-safe file logger with rotation."""
    
    def __init__(self):
        os.makedirs(LOG_DIR, exist_ok=True)
        self._cmd_path = os.path.join(LOG_DIR, LOG_FILE)
        self._err_path = os.path.join(LOG_DIR, ERROR_LOG_FILE)
        self._audit_path = os.path.join(LOG_DIR, AUDIT_LOG_FILE)
    
    def _rotate_if_needed(self, path):
        """Rotate log file if it exceeds max size."""
        try:
            if os.path.exists(path) and os.path.getsize(path) > MAX_LOG_SIZE_MB * 1024 * 1024:
                backup = path + ".old"
                if os.path.exists(backup):
                    os.remove(backup)
                os.rename(path, backup)
        except Exception:
            pass
    
    def _write(self, path, line):
        """Write a timestamped line to a log file."""
        self._rotate_if_needed(path)
        ts = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        try:
            with open(path, "a", encoding="utf-8") as f:
                f.write(f"[{ts}] {line}\n")
        except Exception:
            pass
    
    def command(self, user_id, username, command_name, channel, guild, args=""):
        """Log a command execution."""
        self._write(self._cmd_path,
            f"CMD | {username} ({user_id}) | !{command_name} {args} | #{channel} | {guild}")
    
    def error(self, command_name, user_id, username, error_type, error_msg, tb=""):
        """Log an error with optional traceback."""
        self._write(self._err_path,
            f"ERR | {username} ({user_id}) | !{command_name} | {error_type}: {error_msg}")
        if tb:
            self._write(self._err_path, f"     TRACEBACK:\n{tb}")
    
    def audit(self, action, user_id, username, details=""):
        """Log an audit event (abuse detection, restrictions, etc.)."""
        self._write(self._audit_path,
            f"AUDIT | {action} | {username} ({user_id}) | {details}")
    
    def get_recent(self, path, lines=25):
        """Get the last N lines from a log file."""
        full_path = os.path.join(LOG_DIR, path) if not os.path.isabs(path) else path
        if not os.path.exists(full_path):
            return "No logs found."
        try:
            with open(full_path, "r", encoding="utf-8") as f:
                all_lines = f.readlines()
            return "".join(all_lines[-lines:])
        except Exception as e:
            return f"Error reading logs: {e}"


# ==========================================
# ANTI-ABUSE TRACKER
# ==========================================

class AbuseTracker:
    """Tracks per-user command usage and abuse scores."""
    
    def __init__(self):
        # {user_id: [timestamps]} — all commands
        self.cmd_history = defaultdict(list)
        # {user_id: {command_name: [timestamps]}} — per-command
        self.cmd_specific = defaultdict(lambda: defaultdict(list))
        # {user_id: [timestamps]} — interactions (buttons/modals)
        self.interaction_history = defaultdict(list)
        # {user_id: int} — abuse score (decays over time)
        self.abuse_scores = defaultdict(int)
        # {user_id: float} — when restriction expires
        self.restricted_until = {}
        # {user_id: set} — what thresholds we've already acted on
        self.acted_thresholds = defaultdict(set)
        # {user_id: int} — total commands today (for audit)
        self.daily_usage = defaultdict(int)
    
    def _clean_old(self, timestamps, window):
        """Remove timestamps older than the window."""
        cutoff = datetime.datetime.now(datetime.timezone.utc).timestamp() - window
        return [t for t in timestamps if t > cutoff]
    
    def is_staff(self, member):
        """Check if member has any staff role."""
        if not hasattr(member, 'roles'):
            return False
        return any(r.name in STAFF_ROLE_NAMES for r in member.roles)
    
    def check_command(self, user_id, command_name):
        """
        Check if a command should be allowed.
        Returns: (allowed: bool, reason: str or None)
        """
        now = datetime.datetime.now(datetime.timezone.utc).timestamp()
        
        # Check if user is currently restricted
        if user_id in self.restricted_until:
            if now < self.restricted_until[user_id]:
                remaining = int(self.restricted_until[user_id] - now)
                return False, f"You're temporarily restricted. Try again in {remaining}s."
            else:
                del self.restricted_until[user_id]
        
        # Clean and check global command rate
        self.cmd_history[user_id] = self._clean_old(self.cmd_history[user_id], GLOBAL_CMD_WINDOW)
        if len(self.cmd_history[user_id]) >= GLOBAL_CMD_LIMIT:
            self.abuse_scores[user_id] += 1
            return False, f"You're sending commands too fast! Slow down."
        
        # Clean and check same-command spam
        self.cmd_specific[user_id][command_name] = self._clean_old(
            self.cmd_specific[user_id][command_name], STRICT_CMD_WINDOW)
        if len(self.cmd_specific[user_id][command_name]) >= STRICT_CMD_LIMIT:
            self.abuse_scores[user_id] += 1
            return False, f"You're using `!{command_name}` too frequently. Wait a moment."
        
        # Record this command
        self.cmd_history[user_id].append(now)
        self.cmd_specific[user_id][command_name].append(now)
        self.daily_usage[user_id] += 1
        
        return True, None
    
    def check_interaction(self, user_id):
        """
        Check if an interaction (button/modal) should be allowed.
        Returns: (allowed: bool, reason: str or None)
        """
        now = datetime.datetime.now(datetime.timezone.utc).timestamp()
        
        if user_id in self.restricted_until and now < self.restricted_until[user_id]:
            return False, "You're temporarily restricted."
        
        self.interaction_history[user_id] = self._clean_old(
            self.interaction_history[user_id], INTERACTION_WINDOW)
        if len(self.interaction_history[user_id]) >= INTERACTION_LIMIT:
            self.abuse_scores[user_id] += 1
            return False, "You're clicking too fast! Slow down."
        
        self.interaction_history[user_id].append(now)
        return True, None
    
    def get_escalation_action(self, user_id):
        """
        Based on abuse score, determine what action to take.
        Returns: action string or None
        """
        score = self.abuse_scores[user_id]
        acted = self.acted_thresholds[user_id]
        
        if score >= AUTO_TIMEOUT_THRESHOLD and "timeout" not in acted:
            acted.add("timeout")
            return "timeout"
        elif score >= STAFF_ALERT_THRESHOLD and "staff_alert" not in acted:
            acted.add("staff_alert")
            return "staff_alert"
        elif score >= RESTRICT_THRESHOLD and "restrict" not in acted:
            acted.add("restrict")
            now = datetime.datetime.now(datetime.timezone.utc).timestamp()
            self.restricted_until[user_id] = now + RESTRICT_DURATION
            return "restrict"
        elif score >= WARN_THRESHOLD and "warn" not in acted:
            acted.add("warn")
            return "warn"
        
        return None
    
    def decay_scores(self):
        """Reduce all abuse scores (called periodically)."""
        to_remove = []
        for uid in self.abuse_scores:
            self.abuse_scores[uid] = max(0, self.abuse_scores[uid] - ABUSE_DECAY_AMOUNT)
            if self.abuse_scores[uid] == 0:
                to_remove.append(uid)
                self.acted_thresholds[uid].clear()
        for uid in to_remove:
            del self.abuse_scores[uid]
    
    def reset_daily(self):
        """Reset daily usage counters."""
        self.daily_usage.clear()
    
    def get_top_users(self, n=10):
        """Get top N command users today."""
        sorted_users = sorted(self.daily_usage.items(), key=lambda x: x[1], reverse=True)
        return sorted_users[:n]
    
    def get_abuse_report(self):
        """Get current abuse scores above 0."""
        return {uid: score for uid, score in self.abuse_scores.items() if score > 0}


# ==========================================
# GUARDIAN COG
# ==========================================

class GuardianCog(commands.Cog, name="Guardian"):
    """Global anti-abuse, logging, and command protection system."""
    
    def __init__(self, bot):
        self.bot = bot
        self.logger = FileLogger()
        self.tracker = AbuseTracker()
        self._error_count_today = 0
        self._commands_today = 0
    
    async def cog_load(self):
        """Start background tasks when cog loads."""
        self.decay_loop.start()
        self.daily_reset_loop.start()
        print("✅ Guardian system loaded — anti-abuse & logging active!")
    
    async def cog_unload(self):
        """Stop background tasks when cog unloads."""
        self.decay_loop.cancel()
        self.daily_reset_loop.cancel()
    
    # --- Background Tasks ---
    
    @tasks.loop(seconds=ABUSE_DECAY_INTERVAL)
    async def decay_loop(self):
        """Periodically decay abuse scores so users aren't punished forever."""
        self.tracker.decay_scores()
    
    @decay_loop.before_loop
    async def before_decay(self):
        await self.bot.wait_until_ready()
    
    @tasks.loop(hours=24)
    async def daily_reset_loop(self):
        """Reset daily counters and post daily summary."""
        if self._commands_today > 0:
            await self._post_daily_summary()
        self.tracker.reset_daily()
        self._error_count_today = 0
        self._commands_today = 0
    
    @daily_reset_loop.before_loop
    async def before_daily_reset(self):
        await self.bot.wait_until_ready()
        # Wait until midnight UTC
        now = datetime.datetime.now(datetime.timezone.utc)
        midnight = now.replace(hour=0, minute=0, second=0, microsecond=0) + datetime.timedelta(days=1)
        await asyncio.sleep((midnight - now).total_seconds())
    
    # --- Helper Methods ---
    
    def _get_log_channel(self, guild, channel_name=GUARDIAN_LOG_CHANNEL):
        """Find the logging channel in a guild."""
        if not guild:
            return None
        return discord.utils.get(guild.text_channels, name=channel_name)
    
    async def _send_log_embed(self, guild, embed, channel_name=GUARDIAN_LOG_CHANNEL):
        """Send an embed to the log channel."""
        ch = self._get_log_channel(guild, channel_name)
        if ch:
            try:
                await ch.send(embed=embed)
            except Exception:
                pass
    
    async def _handle_escalation(self, user, guild, command_name):
        """Handle abuse escalation actions."""
        action = self.tracker.get_escalation_action(user.id)
        if not action:
            return
        
        score = self.tracker.abuse_scores.get(user.id, 0)
        
        if action == "warn":
            self.logger.audit("WARN", user.id, str(user), f"Abuse score: {score}")
            try:
                await user.send(
                    f"⚠️ **Warning from The Fallen Bot**\n\n"
                    f"You're using commands too quickly. Please slow down to avoid being "
                    f"temporarily restricted.\n\n"
                    f"*Continued spam will result in a temporary timeout.*"
                )
            except discord.Forbidden:
                pass
        
        elif action == "restrict":
            self.logger.audit("RESTRICT", user.id, str(user), 
                              f"Abuse score: {score} | Duration: {RESTRICT_DURATION}s")
            embed = discord.Embed(
                title="🛡️ Guardian — User Restricted",
                description=f"{user.mention} has been temporarily restricted from commands.",
                color=0xf39c12,
                timestamp=datetime.datetime.now(datetime.timezone.utc)
            )
            embed.add_field(name="Abuse Score", value=str(score), inline=True)
            embed.add_field(name="Duration", value=f"{RESTRICT_DURATION}s", inline=True)
            embed.add_field(name="Last Command", value=f"`!{command_name}`", inline=True)
            embed.set_footer(text="✝ THE FALLEN ✝ Guardian System")
            await self._send_log_embed(guild, embed)
        
        elif action == "staff_alert":
            self.logger.audit("STAFF_ALERT", user.id, str(user), f"Abuse score: {score}")
            embed = discord.Embed(
                title="🚨 Guardian — Staff Alert",
                description=(
                    f"**{user.mention}** is repeatedly spamming commands.\n\n"
                    f"**Abuse Score:** {score}\n"
                    f"**Action taken:** Temporarily restricted\n"
                    f"**Recommendation:** Review this user's activity"
                ),
                color=0xe74c3c,
                timestamp=datetime.datetime.now(datetime.timezone.utc)
            )
            embed.set_footer(text="✝ THE FALLEN ✝ Guardian System")
            await self._send_log_embed(guild, embed)
        
        elif action == "timeout":
            self.logger.audit("AUTO_TIMEOUT", user.id, str(user), 
                              f"Abuse score: {score} | Duration: {AUTO_TIMEOUT_MINUTES}m")
            try:
                await user.timeout(
                    datetime.timedelta(minutes=AUTO_TIMEOUT_MINUTES),
                    reason=f"Guardian auto-timeout: Command spam (abuse score: {score})"
                )
            except (discord.Forbidden, discord.HTTPException):
                pass
            
            embed = discord.Embed(
                title="⛔ Guardian — Auto-Timeout",
                description=(
                    f"**{user.mention}** has been timed out for **{AUTO_TIMEOUT_MINUTES} minutes**.\n\n"
                    f"**Reason:** Repeated command spam\n"
                    f"**Abuse Score:** {score}"
                ),
                color=0x8B0000,
                timestamp=datetime.datetime.now(datetime.timezone.utc)
            )
            embed.set_footer(text="✝ THE FALLEN ✝ Guardian System")
            await self._send_log_embed(guild, embed)
    
    async def _post_daily_summary(self):
        """Post a daily summary to the log channel."""
        for guild in self.bot.guilds:
            ch = self._get_log_channel(guild)
            if not ch:
                continue
            
            top_users = self.tracker.get_top_users(5)
            top_str = ""
            for uid, count in top_users:
                member = guild.get_member(uid)
                name = member.display_name if member else f"Unknown ({uid})"
                top_str += f"**{name}:** {count} commands\n"
            
            embed = discord.Embed(
                title="📊 Daily Bot Summary",
                color=0x2ecc71,
                timestamp=datetime.datetime.now(datetime.timezone.utc)
            )
            embed.add_field(name="Commands Processed", value=str(self._commands_today), inline=True)
            embed.add_field(name="Errors", value=str(self._error_count_today), inline=True)
            embed.add_field(name="Active Abuse Flags", 
                            value=str(len(self.tracker.get_abuse_report())), inline=True)
            if top_str:
                embed.add_field(name="Top Command Users", value=top_str, inline=False)
            embed.set_footer(text="✝ THE FALLEN ✝ Guardian System")
            
            try:
                await ch.send(embed=embed)
            except Exception:
                pass
    
    # --- Event Listeners (Global Middleware) ---
    
    @commands.Cog.listener()
    async def on_command(self, ctx):
        """
        Fires BEFORE every prefix command executes.
        This is our global rate limiter and logger.
        """
        if not ctx.guild or ctx.author.bot:
            return
        
        cmd_name = ctx.command.name if ctx.command else "unknown"
        
        # Log the command
        args_str = ctx.message.content[len(ctx.prefix or "!"):].strip()
        self.logger.command(ctx.author.id, str(ctx.author), cmd_name, 
                           ctx.channel.name, ctx.guild.name, args_str)
        self._commands_today += 1
        
        # Skip rate limiting for staff and exempt commands
        if cmd_name in EXEMPT_COMMANDS:
            return
        if self.tracker.is_staff(ctx.author):
            return
        
        # Check global rate limit
        allowed, reason = self.tracker.check_command(ctx.author.id, cmd_name)
        if not allowed:
            # Cancel the command by raising an error the error handler will catch
            self.logger.audit("RATE_LIMITED", ctx.author.id, str(ctx.author), 
                              f"Command: !{cmd_name} | Reason: {reason}")
            
            embed = discord.Embed(
                title="🛡️ Slow Down!",
                description=reason,
                color=0xf39c12
            )
            embed.set_footer(text="✝ THE FALLEN ✝ • Anti-spam protection")
            try:
                await ctx.send(embed=embed, delete_after=8)
            except Exception:
                pass
            
            # Handle escalation
            await self._handle_escalation(ctx.author, ctx.guild, cmd_name)
            
            # Prevent the command from executing
            raise commands.CheckFailure("Guardian rate limit")
    
    @commands.Cog.listener()
    async def on_interaction(self, interaction: discord.Interaction):
        """
        Fires on every interaction (buttons, modals, selects).
        Rate-limits button/component spam.
        """
        if not interaction.guild or interaction.user.bot:
            return
        
        # Only rate limit component interactions (buttons, selects, modals)
        if interaction.type not in (
            discord.InteractionType.component,
            discord.InteractionType.modal_submit,
        ):
            return
        
        # Skip staff
        if self.tracker.is_staff(interaction.user):
            return
        
        allowed, reason = self.tracker.check_interaction(interaction.user.id)
        if not allowed:
            self.logger.audit("INTERACTION_LIMITED", interaction.user.id, 
                              str(interaction.user), f"Reason: {reason}")
            try:
                if not interaction.response.is_done():
                    await interaction.response.send_message(
                        f"🛡️ {reason}", ephemeral=True
                    )
            except Exception:
                pass
            
            await self._handle_escalation(interaction.user, interaction.guild, "interaction")
    
    @commands.Cog.listener()
    async def on_command_error(self, ctx, error):
        """
        Enhanced error handler that logs everything properly.
        This supplements (not replaces) the main error handler in main.py.
        """
        if not ctx.guild or ctx.author.bot:
            return
        
        # Don't re-log CheckFailure from our own rate limiter
        if isinstance(error, commands.CheckFailure) and "Guardian rate limit" in str(error):
            return
        
        # Don't log command not found
        if isinstance(error, commands.CommandNotFound):
            return
        
        # Don't log cooldown (already handled nicely)
        if isinstance(error, commands.CommandOnCooldown):
            return
        
        cmd_name = ctx.command.name if ctx.command else "unknown"
        
        # Get the original error for CommandInvokeError
        original = error.original if isinstance(error, commands.CommandInvokeError) else error
        
        # Get traceback
        tb = ""
        if isinstance(error, commands.CommandInvokeError):
            tb = "".join(traceback.format_exception(type(original), original, original.__traceback__))
        
        # Log to file
        self.logger.error(
            cmd_name, ctx.author.id, str(ctx.author),
            type(original).__name__, str(original), tb
        )
        self._error_count_today += 1
        
        # For serious errors (not user-input errors), alert in log channel
        user_errors = (
            commands.MissingPermissions, commands.MissingRole, commands.MissingAnyRole,
            commands.BotMissingPermissions, commands.MemberNotFound, commands.RoleNotFound,
            commands.ChannelNotFound, commands.BadArgument, commands.MissingRequiredArgument,
            commands.CheckFailure,
        )
        
        if not isinstance(error, user_errors):
            embed = discord.Embed(
                title="⚠️ Command Error Logged",
                color=0xe74c3c,
                timestamp=datetime.datetime.now(datetime.timezone.utc)
            )
            embed.add_field(name="Command", value=f"`!{cmd_name}`", inline=True)
            embed.add_field(name="User", value=str(ctx.author), inline=True)
            embed.add_field(name="Channel", value=f"#{ctx.channel.name}", inline=True)
            
            err_str = str(original)[:500]
            embed.add_field(name="Error", value=f"```{err_str}```", inline=False)
            
            embed.set_footer(text="✝ THE FALLEN ✝ • Check logs/bot_errors.log for full traceback")
            await self._send_log_embed(ctx.guild, embed, ERROR_ALERT_CHANNEL)
    
    @commands.Cog.listener()
    async def on_app_command_completion(self, interaction: discord.Interaction, command):
        """Log slash command completions."""
        if not interaction.guild:
            return
        
        self.logger.command(
            interaction.user.id, str(interaction.user), 
            f"/{command.name}", interaction.channel.name, interaction.guild.name
        )
        self._commands_today += 1
    
    # --- Staff Commands ---
    
    @commands.command(name="botlogs")
    async def botlogs_cmd(self, ctx, log_type: str = "commands", lines: int = 25):
        """
        View recent bot logs. Staff only.
        Usage: !botlogs [commands|errors|audit] [lines]
        """
        if not self.tracker.is_staff(ctx.author):
            return await ctx.send("❌ Staff only.", delete_after=8)
        
        lines = min(lines, 50)
        
        file_map = {
            "commands": LOG_FILE,
            "cmd": LOG_FILE,
            "errors": ERROR_LOG_FILE,
            "err": ERROR_LOG_FILE,
            "audit": AUDIT_LOG_FILE,
        }
        
        filename = file_map.get(log_type.lower())
        if not filename:
            return await ctx.send(
                f"❌ Unknown log type. Use: `commands`, `errors`, or `audit`", 
                delete_after=10
            )
        
        content = self.logger.get_recent(os.path.join(LOG_DIR, filename), lines)
        
        if len(content) > 1900:
            # Send as file attachment
            buf = StringIO(content)
            file = discord.File(buf, filename=f"{log_type}_logs.txt")
            await ctx.send(f"📋 Last {lines} `{log_type}` log entries:", file=file)
        else:
            embed = discord.Embed(
                title=f"📋 Bot Logs — {log_type.title()}",
                description=f"```\n{content[-1900:]}\n```",
                color=0x3498db,
                timestamp=datetime.datetime.now(datetime.timezone.utc)
            )
            embed.set_footer(text=f"Last {lines} entries • ✝ THE FALLEN ✝")
            await ctx.send(embed=embed)
    
    @commands.command(name="auditlog")
    async def auditlog_cmd(self, ctx, lines: int = 20):
        """View recent audit/abuse events. Staff only. Usage: !auditlog [lines]"""
        if not self.tracker.is_staff(ctx.author):
            return await ctx.send("❌ Staff only.", delete_after=8)
        
        content = self.logger.get_recent(os.path.join(LOG_DIR, AUDIT_LOG_FILE), min(lines, 50))
        
        if not content.strip() or content == "No logs found.":
            return await ctx.send("✅ No abuse events logged. All clear!")
        
        if len(content) > 1900:
            buf = StringIO(content)
            file = discord.File(buf, filename="audit_log.txt")
            await ctx.send(f"🛡️ Last {lines} audit entries:", file=file)
        else:
            embed = discord.Embed(
                title="🛡️ Audit Log — Abuse Events",
                description=f"```\n{content[-1900:]}\n```",
                color=0xf39c12,
                timestamp=datetime.datetime.now(datetime.timezone.utc)
            )
            embed.set_footer(text=f"✝ THE FALLEN ✝ Guardian System")
            await ctx.send(embed=embed)
    
    @commands.command(name="abusereport")
    async def abusereport_cmd(self, ctx):
        """View current abuse scores. Staff only."""
        if not self.tracker.is_staff(ctx.author):
            return await ctx.send("❌ Staff only.", delete_after=8)
        
        report = self.tracker.get_abuse_report()
        
        if not report:
            return await ctx.send("✅ No active abuse flags. All clear!")
        
        embed = discord.Embed(
            title="🚨 Active Abuse Flags",
            color=0xe74c3c,
            timestamp=datetime.datetime.now(datetime.timezone.utc)
        )
        
        for uid, score in sorted(report.items(), key=lambda x: x[1], reverse=True):
            member = ctx.guild.get_member(uid)
            name = member.display_name if member else f"Unknown ({uid})"
            
            # Visual severity
            if score >= AUTO_TIMEOUT_THRESHOLD:
                severity = "⛔ CRITICAL"
            elif score >= STAFF_ALERT_THRESHOLD:
                severity = "🚨 HIGH"
            elif score >= RESTRICT_THRESHOLD:
                severity = "⚠️ MEDIUM"
            else:
                severity = "🔸 LOW"
            
            restricted = uid in self.tracker.restricted_until
            status = " (RESTRICTED)" if restricted else ""
            
            embed.add_field(
                name=f"{severity} — {name}{status}",
                value=f"Score: **{score}** | Commands today: {self.tracker.daily_usage.get(uid, 0)}",
                inline=False
            )
        
        embed.set_footer(text="Scores decay every 5 minutes • ✝ THE FALLEN ✝")
        await ctx.send(embed=embed)
    
    @commands.command(name="clearabuse")
    @commands.has_permissions(administrator=True)
    async def clearabuse_cmd(self, ctx, member: discord.Member = None):
        """Clear abuse flags for a user or all. Admin only. Usage: !clearabuse [@user]"""
        if member:
            self.tracker.abuse_scores.pop(member.id, None)
            self.tracker.acted_thresholds.pop(member.id, None)
            self.tracker.restricted_until.pop(member.id, None)
            self.logger.audit("CLEAR_ABUSE", ctx.author.id, str(ctx.author), 
                              f"Cleared for {member} ({member.id})")
            await ctx.send(f"✅ Cleared abuse flags for {member.mention}")
        else:
            count = len(self.tracker.abuse_scores)
            self.tracker.abuse_scores.clear()
            self.tracker.acted_thresholds.clear()
            self.tracker.restricted_until.clear()
            self.logger.audit("CLEAR_ALL_ABUSE", ctx.author.id, str(ctx.author), 
                              f"Cleared {count} abuse records")
            await ctx.send(f"✅ Cleared all abuse flags ({count} users)")
    
    @commands.command(name="guardianstatus")
    async def guardianstatus_cmd(self, ctx):
        """View Guardian system status. Staff only."""
        if not self.tracker.is_staff(ctx.author):
            return await ctx.send("❌ Staff only.", delete_after=8)
        
        embed = discord.Embed(
            title="🛡️ Guardian System Status",
            color=0x2ecc71,
            timestamp=datetime.datetime.now(datetime.timezone.utc)
        )
        
        embed.add_field(name="Commands Today", value=str(self._commands_today), inline=True)
        embed.add_field(name="Errors Today", value=str(self._error_count_today), inline=True)
        embed.add_field(name="Active Flags", 
                        value=str(len(self.tracker.get_abuse_report())), inline=True)
        embed.add_field(name="Users Restricted", 
                        value=str(len(self.tracker.restricted_until)), inline=True)
        
        # Settings summary
        settings = (
            f"Global limit: {GLOBAL_CMD_LIMIT} cmds / {GLOBAL_CMD_WINDOW}s\n"
            f"Same-cmd limit: {STRICT_CMD_LIMIT} / {STRICT_CMD_WINDOW}s\n"
            f"Interaction limit: {INTERACTION_LIMIT} / {INTERACTION_WINDOW}s\n"
            f"Restrict at score: {RESTRICT_THRESHOLD}\n"
            f"Auto-timeout at score: {AUTO_TIMEOUT_THRESHOLD}\n"
            f"Score decay: -{ABUSE_DECAY_AMOUNT} every {ABUSE_DECAY_INTERVAL // 60}min"
        )
        embed.add_field(name="Configuration", value=f"```\n{settings}\n```", inline=False)
        
        # Top users today
        top = self.tracker.get_top_users(5)
        if top:
            top_str = ""
            for uid, count in top:
                member = ctx.guild.get_member(uid)
                name = member.display_name if member else f"({uid})"
                top_str += f"**{name}:** {count}\n"
            embed.add_field(name="Top Users Today", value=top_str, inline=False)
        
        embed.set_footer(text="✝ THE FALLEN ✝ Guardian System")
        await ctx.send(embed=embed)


async def setup(bot):
    await bot.add_cog(GuardianCog(bot))
