"""
✝ THE FALLEN ✝ - Raid & War Tracking System
Comprehensive raid logging, war management, attendance tracking,
performance stats, visual result cards, and seasonal leaderboards.

Commands:
  !raid start <type> [target]  - Start a raid session (Staff)
  !raid join                   - Join the active raid
  !raid leave                  - Leave the active raid
  !raid end <our_score> <their_score> [mvp_mention]  - End & score raid (Staff)
  !raid cancel                 - Cancel active raid (Staff)
  !raid stats [@user]          - View raid stats
  !raid leaderboard [field]    - Raid leaderboard
  !raid history [count]        - Recent raid history
  !raid panel                  - Post raid management panel (Staff)
  
  !war declare <clan> [best_of] - Declare war (High Staff)
  !war status                   - View active wars
  !war score <war_id> <our> <their> - Log war match (Staff)
  !war history                  - War record history
  !war record                   - Overall war W/L record
"""

import discord
from discord import app_commands
from discord.ext import commands
import datetime
import asyncio
from io import BytesIO

try:
    from PIL import Image, ImageDraw, ImageFont, ImageFilter
    PIL_AVAILABLE = True
except ImportError:
    PIL_AVAILABLE = False

from database import db
from utils import is_staff, is_high_staff, log_action, get_raid_rank, progress_bar
from config import RAID_TYPES, RAID_RANKS, RAID_LOG_CHANNEL_NAME, RAID_RESULTS_CHANNEL_NAME


# =========================================================
# RAID RESULT CARD GENERATOR
# =========================================================

async def generate_raid_result_card(raid_data: dict, guild: discord.Guild, 
                                      participants_data: list = None) -> BytesIO:
    """Generate a visual raid result card image."""
    if not PIL_AVAILABLE:
        return None
    
    width, height = 900, 500
    img = Image.new("RGB", (width, height), (15, 15, 20))
    draw = ImageDraw.Draw(img)
    
    # Try to load a font
    try:
        title_font = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf", 32)
        header_font = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf", 22)
        body_font = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf", 18)
        small_font = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf", 14)
    except Exception:
        title_font = ImageFont.load_default()
        header_font = title_font
        body_font = title_font
        small_font = title_font
    
    # Background gradient effect
    for y in range(height):
        r = int(15 + (25 * y / height))
        g = int(0 + (5 * y / height))
        b = int(20 + (15 * y / height))
        draw.line([(0, y), (width, y)], fill=(r, g, b))
    
    # Top accent line
    result = raid_data.get("result", "unknown")
    accent_color = (0, 200, 80) if result == "win" else (200, 30, 30) if result == "loss" else (200, 200, 50)
    draw.rectangle([(0, 0), (width, 4)], fill=accent_color)
    
    # Raid type info
    raid_type = raid_data.get("raid_type", "standard")
    type_info = RAID_TYPES.get(raid_type, RAID_TYPES["standard"])
    
    # Title
    result_text = "VICTORY" if result == "win" else "DEFEAT" if result == "loss" else "DRAW"
    draw.text((width // 2, 35), f"✝ {type_info['name'].upper()} RESULT ✝", 
              fill=(200, 200, 200), font=title_font, anchor="mt")
    
    # Result badge
    badge_y = 80
    draw.text((width // 2, badge_y), result_text, fill=accent_color, font=title_font, anchor="mt")
    
    # Score display
    our_score = raid_data.get("our_score", 0)
    their_score = raid_data.get("their_score", 0)
    score_y = 135
    
    # "THE FALLEN" vs target
    target = raid_data.get("target_clan", "Enemy Clan") or "Enemy Clan"
    draw.text((width // 4, score_y), "THE FALLEN", fill=(180, 180, 200), font=header_font, anchor="mt")
    draw.text((3 * width // 4, score_y), target.upper(), fill=(180, 180, 200), font=header_font, anchor="mt")
    
    # Big score numbers
    score_num_y = score_y + 45
    draw.text((width // 4, score_num_y), str(our_score), fill=(255, 255, 255), font=title_font, anchor="mt")
    draw.text((width // 2, score_num_y), "—", fill=(100, 100, 100), font=title_font, anchor="mt")
    draw.text((3 * width // 4, score_num_y), str(their_score), fill=(255, 255, 255), font=title_font, anchor="mt")
    
    # Divider
    div_y = score_num_y + 50
    draw.line([(50, div_y), (width - 50, div_y)], fill=(60, 60, 70), width=2)
    
    # Stats section
    stats_y = div_y + 20
    participants = raid_data.get("participants", [])
    num_participants = len(participants) if isinstance(participants, list) else 0
    
    # MVP
    mvp_id = raid_data.get("mvp_id")
    mvp_text = "None"
    if mvp_id and guild:
        mvp_member = guild.get_member(mvp_id)
        if mvp_member:
            mvp_text = mvp_member.display_name
    
    # Left column stats
    left_x = 80
    draw.text((left_x, stats_y), "PARTICIPANTS", fill=(120, 120, 140), font=small_font)
    draw.text((left_x, stats_y + 22), str(num_participants), fill=(255, 255, 255), font=header_font)
    
    draw.text((left_x, stats_y + 60), "MVP", fill=(120, 120, 140), font=small_font)
    draw.text((left_x, stats_y + 82), f"⭐ {mvp_text}", fill=(255, 215, 0), font=body_font)
    
    # Right column stats
    right_x = width // 2 + 50
    xp_awarded = raid_data.get("xp_awarded", 0)
    coins_awarded = raid_data.get("coins_awarded", 0)
    
    draw.text((right_x, stats_y), "XP AWARDED", fill=(120, 120, 140), font=small_font)
    draw.text((right_x, stats_y + 22), f"+{xp_awarded} XP", fill=(100, 200, 255), font=header_font)
    
    draw.text((right_x, stats_y + 60), "COINS AWARDED", fill=(120, 120, 140), font=small_font)
    draw.text((right_x, stats_y + 82), f"+{coins_awarded} 🪙", fill=(255, 215, 0), font=body_font)
    
    # Participant names at bottom
    names_y = height - 80
    draw.line([(50, names_y - 10), (width - 50, names_y - 10)], fill=(60, 60, 70), width=1)
    
    if guild and participants:
        member_names = []
        for pid in participants[:12]:  # Max 12 names
            m = guild.get_member(pid)
            if m:
                member_names.append(m.display_name)
        
        if member_names:
            names_text = " • ".join(member_names)
            if len(participants) > 12:
                names_text += f" (+{len(participants) - 12} more)"
            # Truncate if too long
            if len(names_text) > 80:
                names_text = names_text[:77] + "..."
            draw.text((width // 2, names_y + 5), names_text, fill=(100, 100, 120), font=small_font, anchor="mt")
    
    # Footer
    draw.text((width // 2, height - 20), "✝ THE FALLEN ✝", fill=(80, 80, 90), font=small_font, anchor="mt")
    
    # Border
    draw.rectangle([(0, 0), (width - 1, height - 1)], outline=(60, 60, 70), width=2)
    
    buf = BytesIO()
    img.save(buf, format="PNG", quality=95)
    buf.seek(0)
    return buf


async def generate_raid_stats_card(user: discord.Member, stats: dict) -> BytesIO:
    """Generate a visual raid stats card for a user."""
    if not PIL_AVAILABLE:
        return None
    
    width, height = 700, 400
    img = Image.new("RGB", (width, height), (18, 18, 25))
    draw = ImageDraw.Draw(img)
    
    try:
        title_font = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf", 26)
        header_font = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf", 20)
        body_font = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf", 16)
        small_font = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf", 13)
    except Exception:
        title_font = ImageFont.load_default()
        header_font = title_font
        body_font = title_font
        small_font = title_font
    
    # Background gradient
    for y in range(height):
        r = int(18 + (12 * y / height))
        g = int(0 + (5 * y / height))
        b = int(25 + (10 * y / height))
        draw.line([(0, y), (width, y)], fill=(r, g, b))
    
    # Top accent
    draw.rectangle([(0, 0), (width, 3)], fill=(139, 0, 0))
    
    # Title
    draw.text((width // 2, 30), f"⚔️ RAID PROFILE", fill=(200, 200, 200), font=title_font, anchor="mt")
    draw.text((width // 2, 62), user.display_name, fill=(255, 255, 255), font=header_font, anchor="mt")
    
    # Raid rank
    total_raids = stats.get("total_raids", 0)
    rank_info = get_raid_rank(total_raids)
    draw.text((width // 2, 90), f"{rank_info['emoji']} {rank_info['name']}", 
              fill=(200, 170, 100), font=body_font, anchor="mt")
    
    # Divider
    draw.line([(40, 115), (width - 40, 115)], fill=(60, 60, 70), width=2)
    
    # Stats grid
    won = stats.get("raids_won", 0)
    lost = stats.get("raids_lost", 0)
    winrate = (won / total_raids * 100) if total_raids > 0 else 0
    
    grid_y = 135
    col1_x = 60
    col2_x = width // 2 + 30
    row_h = 50
    
    stat_items = [
        ("TOTAL RAIDS", str(total_raids), col1_x),
        ("WIN RATE", f"{winrate:.1f}%", col2_x),
        ("WINS / LOSSES", f"{won}W - {lost}L", col1_x),
        ("CURRENT STREAK", f"🔥 {stats.get('current_streak', 0)}", col2_x),
        ("TOTAL KILLS", str(stats.get("total_kills", 0)), col1_x),
        ("TOTAL DEATHS", str(stats.get("total_deaths", 0)), col2_x),
        ("MVP AWARDS", f"⭐ {stats.get('mvp_count', 0)}", col1_x),
        ("BEST STREAK", f"🏆 {stats.get('best_streak', 0)}", col2_x),
    ]
    
    for i, (label, value, x) in enumerate(stat_items):
        y = grid_y + (i // 2) * row_h
        draw.text((x, y), label, fill=(100, 100, 120), font=small_font)
        draw.text((x, y + 17), value, fill=(255, 255, 255), font=body_font)
    
    # Progress to next rank
    next_rank = None
    for threshold in sorted(RAID_RANKS.keys()):
        if total_raids < threshold:
            next_rank = (threshold, RAID_RANKS[threshold])
            break
    
    if next_rank:
        prog_y = height - 65
        draw.line([(40, prog_y - 10), (width - 40, prog_y - 10)], fill=(60, 60, 70), width=1)
        draw.text((50, prog_y), f"Next: {next_rank[1]['emoji']} {next_rank[1]['name']} ({next_rank[0]} raids)", 
                  fill=(120, 120, 140), font=small_font)
        
        # Progress bar
        bar_y = prog_y + 20
        bar_width = width - 100
        prev_threshold = 0
        for t in sorted(RAID_RANKS.keys()):
            if t < next_rank[0]:
                prev_threshold = t
        progress = (total_raids - prev_threshold) / max(next_rank[0] - prev_threshold, 1)
        filled_w = int(bar_width * min(progress, 1.0))
        
        draw.rectangle([(50, bar_y), (50 + bar_width, bar_y + 12)], fill=(40, 40, 50))
        if filled_w > 0:
            draw.rectangle([(50, bar_y), (50 + filled_w, bar_y + 12)], fill=(139, 0, 0))
        draw.text((50 + bar_width + 8, bar_y - 2), f"{total_raids}/{next_rank[0]}", 
                  fill=(150, 150, 160), font=small_font)
    
    # Footer
    draw.text((width // 2, height - 15), "✝ THE FALLEN ✝", fill=(80, 80, 90), font=small_font, anchor="mt")
    draw.rectangle([(0, 0), (width - 1, height - 1)], outline=(60, 60, 70), width=2)
    
    buf = BytesIO()
    img.save(buf, format="PNG", quality=95)
    buf.seek(0)
    return buf


# =========================================================
# RAID PANEL VIEW (Persistent buttons)
# =========================================================

class RaidJoinView(discord.ui.View):
    """Persistent view for joining/leaving an active raid."""
    
    def __init__(self, raid_id: int):
        super().__init__(timeout=None)
        self.raid_id = raid_id
    
    @discord.ui.button(label="⚔️ Join Raid", style=discord.ButtonStyle.green, custom_id="raid_join")
    async def join_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await db.add_raid_participant(self.raid_id, interaction.user.id)
        raid = await db.get_raid(self.raid_id)
        count = len(raid.get("participants", [])) if raid else 0
        
        await interaction.response.send_message(
            f"✅ You've joined the raid! ({count} participants)", ephemeral=True
        )
        
        # Update embed with new count
        try:
            embed = interaction.message.embeds[0] if interaction.message.embeds else None
            if embed:
                for i, field in enumerate(embed.fields):
                    if "Participants" in field.name:
                        # Build participant list
                        members = []
                        for pid in (raid.get("participants", []) if raid else []):
                            m = interaction.guild.get_member(pid)
                            if m:
                                members.append(m.display_name)
                        member_text = ", ".join(members) if members else "None yet"
                        if len(member_text) > 200:
                            member_text = member_text[:197] + "..."
                        embed.set_field_at(i, name=f"👥 Participants ({count})", value=member_text, inline=False)
                        break
                await interaction.message.edit(embed=embed)
        except Exception:
            pass
    
    @discord.ui.button(label="🚪 Leave Raid", style=discord.ButtonStyle.grey, custom_id="raid_leave")
    async def leave_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await db.remove_raid_participant(self.raid_id, interaction.user.id)
        await interaction.response.send_message("You've left the raid.", ephemeral=True)


class RaidEndModal(discord.ui.Modal, title="End Raid - Enter Scores"):
    """Modal for entering raid results."""
    
    our_score = discord.ui.TextInput(
        label="Our Score", placeholder="e.g. 5", max_length=5, required=True
    )
    their_score = discord.ui.TextInput(
        label="Their Score", placeholder="e.g. 3", max_length=5, required=True
    )
    mvp_id_input = discord.ui.TextInput(
        label="MVP User ID (optional)", placeholder="Right-click user > Copy ID", 
        required=False, max_length=20
    )
    notes = discord.ui.TextInput(
        label="Notes (optional)", placeholder="Any notes about this raid...",
        style=discord.TextStyle.paragraph, required=False, max_length=500
    )
    
    def __init__(self, raid_id: int):
        super().__init__()
        self.raid_id = raid_id
    
    async def on_submit(self, interaction: discord.Interaction):
        try:
            our = int(self.our_score.value)
            their = int(self.their_score.value)
        except ValueError:
            return await interaction.response.send_message("❌ Scores must be numbers!", ephemeral=True)
        
        mvp_id = None
        if self.mvp_id_input.value:
            try:
                mvp_id = int(self.mvp_id_input.value)
            except ValueError:
                pass
        
        result = "win" if our > their else ("loss" if their > our else "draw")
        
        # Complete the raid
        await db.complete_raid(self.raid_id, result, our, their, mvp_id)
        
        raid = await db.get_raid(self.raid_id)
        if not raid:
            return await interaction.response.send_message("❌ Raid not found!", ephemeral=True)
        
        # Update notes
        if self.notes.value:
            await db.update_raid(self.raid_id, notes=self.notes.value)
        
        # Calculate rewards
        raid_type = raid.get("raid_type", "standard")
        type_info = RAID_TYPES.get(raid_type, RAID_TYPES["standard"])
        base_xp = type_info["xp_reward"]
        
        win_multiplier = 1.5 if result == "win" else (0.5 if result == "loss" else 1.0)
        xp_reward = int(base_xp * win_multiplier)
        coin_reward = int(xp_reward * 0.5)
        
        await db.update_raid(self.raid_id, xp_awarded=xp_reward, coins_awarded=coin_reward)
        
        # Award participants
        participants = raid.get("participants", [])
        for pid in participants:
            won = result == "win"
            is_mvp = pid == mvp_id
            
            mvp_bonus = 1.25 if is_mvp else 1.0
            user_xp = int(xp_reward * mvp_bonus)
            user_coins = int(coin_reward * mvp_bonus)
            
            await db.increment_user(pid, xp=user_xp, coins=user_coins, raid_participation=1)
            if won:
                await db.increment_user(pid, raid_wins=1)
            else:
                await db.increment_user(pid, raid_losses=1)
            
            # Update raid stats
            await db.update_raid_stats(pid, won=won, is_mvp=is_mvp)
        
        # Generate result card
        raid_data = await db.get_raid(self.raid_id)
        raid_data["xp_awarded"] = xp_reward
        raid_data["coins_awarded"] = coin_reward
        
        await interaction.response.send_message("✅ Processing raid results...", ephemeral=True)
        
        # Post result card
        card_buf = await generate_raid_result_card(raid_data, interaction.guild)
        
        result_emoji = "🏆" if result == "win" else "💀" if result == "loss" else "🤝"
        
        embed = discord.Embed(
            title=f"{result_emoji} Raid Complete — {result.upper()}",
            description=(
                f"**{type_info['emoji']} {type_info['name']}** vs **{raid.get('target_clan', 'Enemy')}**\n"
                f"Score: **{our}** - **{their}**\n\n"
                f"💰 Rewards: **+{xp_reward} XP** | **+{coin_reward} coins**\n"
                f"👥 Participants: **{len(participants)}**"
            ),
            color=0x00C853 if result == "win" else 0xD32F2F if result == "loss" else 0xFFB300,
            timestamp=datetime.datetime.now(datetime.timezone.utc)
        )
        
        if mvp_id:
            mvp_member = interaction.guild.get_member(mvp_id)
            if mvp_member:
                embed.add_field(name="⭐ MVP", value=mvp_member.mention, inline=True)
        
        embed.set_footer(text=f"✝ THE FALLEN ✝ | Raid #{self.raid_id}")
        
        files = []
        if card_buf:
            files.append(discord.File(card_buf, filename="raid_result.png"))
            embed.set_image(url="attachment://raid_result.png")
        
        # Post to raid results channel (or current channel)
        results_channel = discord.utils.get(
            interaction.guild.text_channels, name=RAID_RESULTS_CHANNEL_NAME
        )
        target_channel = results_channel or interaction.channel
        
        await target_channel.send(embed=embed, files=files)
        
        # Log it
        await log_action(
            interaction.guild,
            f"⚔️ Raid #{self.raid_id} Completed",
            f"**Result:** {result.upper()} ({our}-{their})\n"
            f"**Type:** {type_info['name']}\n"
            f"**Participants:** {len(participants)}\n"
            f"**Led by:** <@{raid.get('leader_id')}>",
            color=0x00C853 if result == "win" else 0xD32F2F
        )


class RaidStaffControlView(discord.ui.View):
    """Staff controls for managing an active raid."""
    
    def __init__(self, raid_id: int):
        super().__init__(timeout=None)
        self.raid_id = raid_id
    
    @discord.ui.button(label="🏁 End Raid", style=discord.ButtonStyle.red, custom_id="raid_end_btn")
    async def end_raid(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not is_staff(interaction.user):
            return await interaction.response.send_message("❌ Staff only!", ephemeral=True)
        await interaction.response.send_modal(RaidEndModal(self.raid_id))
    
    @discord.ui.button(label="❌ Cancel Raid", style=discord.ButtonStyle.grey, custom_id="raid_cancel_btn")
    async def cancel_raid(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not is_staff(interaction.user):
            return await interaction.response.send_message("❌ Staff only!", ephemeral=True)
        
        await db.update_raid(self.raid_id, status="cancelled")
        
        embed = discord.Embed(
            title="❌ Raid Cancelled",
            description="This raid has been cancelled by staff.",
            color=0x666666
        )
        await interaction.response.edit_message(embed=embed, view=None)


# =========================================================
# RAID COG
# =========================================================

class RaidCog(commands.Cog, name="Raids"):
    """⚔️ Raid & War Tracking System"""
    
    def __init__(self, bot):
        self.bot = bot
    
    # --- RAID COMMANDS ---
    
    @commands.group(name="raid", invoke_without_command=True)
    async def raid_group(self, ctx):
        """Raid management commands. Use !raid help for info."""
        embed = discord.Embed(
            title="⚔️ Raid Commands",
            description=(
                "**Starting Raids (Staff):**\n"
                "`!raid start <type> [target_clan]` — Start a raid\n"
                "`!raid end` — End active raid with scores\n"
                "`!raid cancel` — Cancel active raid\n"
                "`!raid panel` — Post raid management panel\n\n"
                "**Joining:**\n"
                "`!raid join` — Join the active raid\n"
                "`!raid leave` — Leave the active raid\n\n"
                "**Stats:**\n"
                "`!raid stats [@user]` — View raid stats\n"
                "`!raid leaderboard [field]` — Raid leaderboard\n"
                "`!raid history [count]` — Recent raid history\n\n"
                f"**Raid Types:** {', '.join(f'{v['emoji']} {v['name']}' for v in RAID_TYPES.values())}"
            ),
            color=0x8B0000
        )
        embed.set_footer(text="✝ THE FALLEN ✝")
        await ctx.send(embed=embed)
    
    @raid_group.command(name="start")
    async def raid_start(self, ctx, raid_type: str = "standard", *, target_clan: str = None):
        """Start a new raid session. Staff only."""
        if not is_staff(ctx.author):
            return await ctx.send("❌ Staff only!", delete_after=5)
        
        # Validate raid type
        if raid_type not in RAID_TYPES:
            types_list = ", ".join(f"`{k}`" for k in RAID_TYPES.keys())
            return await ctx.send(f"❌ Invalid raid type. Options: {types_list}")
        
        # Check for active raids
        active = await db.get_active_raids()
        if active:
            return await ctx.send("❌ There's already an active raid! End or cancel it first.")
        
        type_info = RAID_TYPES[raid_type]
        
        # Create raid in database
        raid_id = await db.create_raid(raid_type, ctx.author.id, target_clan)
        
        # Auto-join the leader
        await db.add_raid_participant(raid_id, ctx.author.id)
        
        target_text = f" vs **{target_clan}**" if target_clan else ""
        
        embed = discord.Embed(
            title=f"{type_info['emoji']} {type_info['name']} Starting!{target_text}",
            description=(
                f"**Led by:** {ctx.author.mention}\n"
                f"**Min Players:** {type_info['min_players']}\n"
                f"**XP Reward:** {type_info['xp_reward']} base XP\n\n"
                "Click **⚔️ Join Raid** to participate!"
            ),
            color=0x8B0000,
            timestamp=datetime.datetime.now(datetime.timezone.utc)
        )
        embed.add_field(name="👥 Participants (1)", value=ctx.author.display_name, inline=False)
        embed.set_footer(text=f"✝ THE FALLEN ✝ | Raid #{raid_id}")
        
        # Send with join view for members
        join_msg = await ctx.send(embed=embed, view=RaidJoinView(raid_id))
        
        # Send staff controls
        staff_embed = discord.Embed(
            title="🔧 Staff Controls",
            description=f"Raid #{raid_id} controls. Only staff can use these.",
            color=0x333333
        )
        await ctx.send(embed=staff_embed, view=RaidStaffControlView(raid_id))
        
        # Update raid with message ID
        await db.update_raid(raid_id, channel_id=ctx.channel.id, message_id=join_msg.id)
        
        await log_action(
            ctx.guild, f"⚔️ Raid #{raid_id} Started",
            f"**Type:** {type_info['name']}\n**Leader:** {ctx.author.mention}\n**Target:** {target_clan or 'N/A'}",
            color=0x8B0000
        )
    
    @raid_group.command(name="join")
    async def raid_join(self, ctx):
        """Join the active raid."""
        active = await db.get_active_raids()
        if not active:
            return await ctx.send("❌ No active raid right now!")
        
        raid = active[0]
        await db.add_raid_participant(raid["id"], ctx.author.id)
        
        participants = raid.get("participants", [])
        if ctx.author.id not in participants:
            count = len(participants) + 1
        else:
            count = len(participants)
        
        await ctx.send(f"✅ {ctx.author.mention} joined the raid! ({count} participants)", delete_after=10)
    
    @raid_group.command(name="leave")
    async def raid_leave(self, ctx):
        """Leave the active raid."""
        active = await db.get_active_raids()
        if not active:
            return await ctx.send("❌ No active raid right now!")
        
        await db.remove_raid_participant(active[0]["id"], ctx.author.id)
        await ctx.send(f"🚪 {ctx.author.mention} left the raid.", delete_after=10)
    
    @raid_group.command(name="end")
    async def raid_end(self, ctx, our_score: int = None, their_score: int = None, mvp: discord.Member = None):
        """End the active raid with scores. Staff only."""
        if not is_staff(ctx.author):
            return await ctx.send("❌ Staff only!", delete_after=5)
        
        active = await db.get_active_raids()
        if not active:
            return await ctx.send("❌ No active raid to end!")
        
        raid = active[0]
        
        # If no scores provided, show modal-like prompt
        if our_score is None or their_score is None:
            return await ctx.send(
                "**Usage:** `!raid end <our_score> <their_score> [@mvp]`\n"
                "Example: `!raid end 5 3 @ShadowKing`"
            )
        
        result = "win" if our_score > their_score else ("loss" if their_score > our_score else "draw")
        mvp_id = mvp.id if mvp else None
        
        # Complete raid
        await db.complete_raid(raid["id"], result, our_score, their_score, mvp_id)
        
        # Calculate rewards
        type_info = RAID_TYPES.get(raid.get("raid_type", "standard"), RAID_TYPES["standard"])
        base_xp = type_info["xp_reward"]
        win_mult = 1.5 if result == "win" else (0.5 if result == "loss" else 1.0)
        xp_reward = int(base_xp * win_mult)
        coin_reward = int(xp_reward * 0.5)
        
        await db.update_raid(raid["id"], xp_awarded=xp_reward, coins_awarded=coin_reward)
        
        # Award participants
        participants = raid.get("participants", [])
        for pid in participants:
            won = result == "win"
            is_mvp = pid == mvp_id
            mvp_bonus = 1.25 if is_mvp else 1.0
            
            await db.increment_user(pid, xp=int(xp_reward * mvp_bonus), coins=int(coin_reward * mvp_bonus), raid_participation=1)
            if won:
                await db.increment_user(pid, raid_wins=1)
            else:
                await db.increment_user(pid, raid_losses=1)
            
            await db.update_raid_stats(pid, won=won, is_mvp=is_mvp)
        
        # Generate and post result card
        raid_data = await db.get_raid(raid["id"])
        raid_data["xp_awarded"] = xp_reward
        raid_data["coins_awarded"] = coin_reward
        
        card_buf = await generate_raid_result_card(raid_data, ctx.guild)
        
        result_emoji = "🏆" if result == "win" else "💀" if result == "loss" else "🤝"
        
        embed = discord.Embed(
            title=f"{result_emoji} Raid Complete — {result.upper()}",
            description=(
                f"**{type_info['emoji']} {type_info['name']}** vs **{raid.get('target_clan', 'Enemy')}**\n"
                f"Score: **{our_score}** - **{their_score}**\n\n"
                f"💰 **+{xp_reward} XP** | **+{coin_reward} coins** per participant\n"
                f"👥 **{len(participants)}** participants rewarded"
            ),
            color=0x00C853 if result == "win" else 0xD32F2F if result == "loss" else 0xFFB300,
            timestamp=datetime.datetime.now(datetime.timezone.utc)
        )
        
        if mvp:
            embed.add_field(name="⭐ MVP", value=f"{mvp.mention} (+25% bonus)", inline=True)
        
        embed.set_footer(text=f"✝ THE FALLEN ✝ | Raid #{raid['id']}")
        
        files = []
        if card_buf:
            files.append(discord.File(card_buf, filename="raid_result.png"))
            embed.set_image(url="attachment://raid_result.png")
        
        results_channel = discord.utils.get(ctx.guild.text_channels, name=RAID_RESULTS_CHANNEL_NAME)
        target_channel = results_channel or ctx.channel
        await target_channel.send(embed=embed, files=files)
        
        if target_channel != ctx.channel:
            await ctx.send(f"✅ Raid ended! Results posted in {target_channel.mention}")
    
    @raid_group.command(name="cancel")
    async def raid_cancel(self, ctx):
        """Cancel the active raid. Staff only."""
        if not is_staff(ctx.author):
            return await ctx.send("❌ Staff only!", delete_after=5)
        
        active = await db.get_active_raids()
        if not active:
            return await ctx.send("❌ No active raid to cancel!")
        
        await db.update_raid(active[0]["id"], status="cancelled")
        await ctx.send("❌ Raid cancelled. No rewards distributed.")
    
    @raid_group.command(name="stats")
    async def raid_stats(self, ctx, member: discord.Member = None):
        """View raid stats for yourself or another member."""
        target = member or ctx.author
        stats = await db.get_raid_stats(target.id)
        
        if not stats or stats.get("total_raids", 0) == 0:
            return await ctx.send(f"📊 {target.display_name} hasn't participated in any raids yet!")
        
        # Try to generate image card
        card_buf = await generate_raid_stats_card(target, stats)
        
        if card_buf:
            file = discord.File(card_buf, filename="raid_stats.png")
            await ctx.send(file=file)
        else:
            # Fallback embed
            rank_info = get_raid_rank(stats.get("total_raids", 0))
            total = stats.get("total_raids", 0)
            won = stats.get("raids_won", 0)
            lost = stats.get("raids_lost", 0)
            winrate = (won / total * 100) if total > 0 else 0
            
            embed = discord.Embed(
                title=f"⚔️ Raid Stats — {target.display_name}",
                description=f"{rank_info['emoji']} **{rank_info['name']}**",
                color=0x8B0000
            )
            embed.add_field(name="Total Raids", value=str(total), inline=True)
            embed.add_field(name="Win Rate", value=f"{winrate:.1f}%", inline=True)
            embed.add_field(name="W/L", value=f"{won}W - {lost}L", inline=True)
            embed.add_field(name="Kills", value=str(stats.get("total_kills", 0)), inline=True)
            embed.add_field(name="MVPs", value=f"⭐ {stats.get('mvp_count', 0)}", inline=True)
            embed.add_field(name="Best Streak", value=f"🔥 {stats.get('best_streak', 0)}", inline=True)
            embed.set_footer(text="✝ THE FALLEN ✝")
            await ctx.send(embed=embed)
    
    @raid_group.command(name="leaderboard", aliases=["lb"])
    async def raid_leaderboard(self, ctx, field: str = "total_raids"):
        """View the raid leaderboard."""
        valid_fields = {
            "raids": "total_raids", "wins": "raids_won", "kills": "total_kills",
            "damage": "total_damage", "mvp": "mvp_count", "streak": "best_streak"
        }
        
        db_field = valid_fields.get(field, field)
        if db_field not in valid_fields.values():
            db_field = "total_raids"
        
        leaders = await db.get_raid_leaderboard(db_field, 10)
        
        if not leaders:
            return await ctx.send("📊 No raid data yet! Start some raids with `!raid start`")
        
        field_labels = {
            "total_raids": "Total Raids", "raids_won": "Wins",
            "total_kills": "Kills", "total_damage": "Damage",
            "mvp_count": "MVP Awards", "best_streak": "Best Streak"
        }
        
        embed = discord.Embed(
            title=f"⚔️ Raid Leaderboard — {field_labels.get(db_field, db_field)}",
            color=0x8B0000
        )
        
        desc = ""
        medals = ["🥇", "🥈", "🥉"]
        for i, entry in enumerate(leaders):
            medal = medals[i] if i < 3 else f"**#{i+1}**"
            member = ctx.guild.get_member(entry["user_id"])
            name = member.display_name if member else f"User {entry['user_id']}"
            value = entry.get(db_field, 0)
            rank_info = get_raid_rank(entry.get("total_raids", 0))
            desc += f"{medal} {rank_info['emoji']} **{name}** — {value}\n"
        
        embed.description = desc
        embed.set_footer(text="✝ THE FALLEN ✝ | Sort by: raids, wins, kills, damage, mvp, streak")
        await ctx.send(embed=embed)
    
    @raid_group.command(name="history")
    async def raid_history(self, ctx, count: int = 5):
        """View recent raid history."""
        count = min(count, 15)
        history = await db.get_raid_history(count)
        
        if not history:
            return await ctx.send("📜 No raid history yet!")
        
        embed = discord.Embed(
            title="📜 Recent Raid History",
            color=0x8B0000
        )
        
        for raid in history:
            type_info = RAID_TYPES.get(raid.get("raid_type", "standard"), RAID_TYPES["standard"])
            result = raid.get("result", "unknown")
            result_emoji = "🏆" if result == "win" else "💀" if result == "loss" else "🤝"
            target = raid.get("target_clan", "Unknown")
            our = raid.get("our_score", 0)
            their = raid.get("their_score", 0)
            participants = raid.get("participants", [])
            num_p = len(participants) if isinstance(participants, list) else 0
            
            completed = raid.get("completed_at")
            time_str = ""
            if completed:
                if isinstance(completed, str):
                    completed = datetime.datetime.fromisoformat(completed)
                time_str = f" — <t:{int(completed.timestamp())}:R>"
            
            embed.add_field(
                name=f"{result_emoji} {type_info['emoji']} vs {target} ({our}-{their})",
                value=f"👥 {num_p} players | +{raid.get('xp_awarded', 0)} XP{time_str}",
                inline=False
            )
        
        embed.set_footer(text="✝ THE FALLEN ✝")
        await ctx.send(embed=embed)
    
    @raid_group.command(name="panel")
    async def raid_panel(self, ctx):
        """Post a raid management panel. Staff only."""
        if not is_staff(ctx.author):
            return await ctx.send("❌ Staff only!", delete_after=5)
        
        embed = discord.Embed(
            title="⚔️ Raid Management Panel",
            description=(
                "Use these commands to manage raids:\n\n"
                "**Start a Raid:**\n"
                "`!raid start standard [clan_name]`\n"
                "`!raid start mega [clan_name]`\n"
                "`!raid start war [clan_name]`\n"
                "`!raid start defense [clan_name]`\n"
                "`!raid start scrimmage [clan_name]`\n\n"
                "**End/Cancel:**\n"
                "`!raid end <our_score> <their_score> [@mvp]`\n"
                "`!raid cancel`\n\n"
                "**View Data:**\n"
                "`!raid history` | `!raid leaderboard` | `!raid stats`"
            ),
            color=0x8B0000
        )
        
        # Show current raid types
        types_text = ""
        for key, info in RAID_TYPES.items():
            types_text += f"{info['emoji']} **{info['name']}** (`{key}`) — Min {info['min_players']} players, {info['xp_reward']} base XP\n"
        embed.add_field(name="📋 Raid Types", value=types_text, inline=False)
        embed.set_footer(text="✝ THE FALLEN ✝")
        
        await ctx.send(embed=embed)
    
    # --- WAR COMMANDS ---
    
    @commands.group(name="war", invoke_without_command=True)
    async def war_group(self, ctx):
        """War management commands."""
        embed = discord.Embed(
            title="🏴 War Commands",
            description=(
                "`!war declare <clan> [best_of]` — Declare war (High Staff)\n"
                "`!war status` — View active wars\n"
                "`!war score <war_id> <our_score> <their_score>` — Log match (Staff)\n"
                "`!war history` — Past war results\n"
                "`!war record` — Overall W/L record"
            ),
            color=0x8B0000
        )
        embed.set_footer(text="✝ THE FALLEN ✝")
        await ctx.send(embed=embed)
    
    @war_group.command(name="declare")
    async def war_declare(self, ctx, clan_name: str, best_of: int = 3):
        """Declare war on another clan. High Staff only."""
        if not is_high_staff(ctx.author):
            return await ctx.send("❌ High Staff only!", delete_after=5)
        
        if best_of not in [1, 3, 5, 7]:
            return await ctx.send("❌ Best of must be 1, 3, 5, or 7!")
        
        war_id = await db.declare_war(clan_name, ctx.author.id, best_of)
        
        embed = discord.Embed(
            title="🏴 WAR DECLARED!",
            description=(
                f"**The Fallen** ⚔️ **{clan_name}**\n\n"
                f"**Format:** Best of {best_of}\n"
                f"**Wins Needed:** {(best_of // 2) + 1}\n"
                f"**Declared by:** {ctx.author.mention}\n\n"
                f"Use `!war score {war_id} <our_score> <their_score>` to log matches."
            ),
            color=0xFF0000,
            timestamp=datetime.datetime.now(datetime.timezone.utc)
        )
        embed.set_footer(text=f"✝ THE FALLEN ✝ | War #{war_id}")
        
        await ctx.send(embed=embed)
        
        await log_action(
            ctx.guild, "🏴 War Declared!",
            f"**Enemy:** {clan_name}\n**Format:** Bo{best_of}\n**By:** {ctx.author.mention}",
            color=0xFF0000
        )
    
    @war_group.command(name="status")
    async def war_status(self, ctx):
        """View active wars."""
        active_wars = await db.get_active_wars()
        
        if not active_wars:
            return await ctx.send("☮️ No active wars at the moment.")
        
        embed = discord.Embed(title="🏴 Active Wars", color=0x8B0000)
        
        for war in active_wars:
            wins_needed = (war['best_of'] // 2) + 1
            our_w = war.get('our_wins', 0)
            our_l = war.get('our_losses', 0)
            
            bar = ""
            for i in range(war['best_of']):
                if i < our_w:
                    bar += "🟢"
                elif i < our_w + our_l:
                    bar += "🔴"
                else:
                    bar += "⬛"
            
            embed.add_field(
                name=f"⚔️ vs {war['enemy_clan']} (War #{war['id']})",
                value=(
                    f"**Score:** The Fallen **{our_w}** - **{our_l}** {war['enemy_clan']}\n"
                    f"**Format:** Best of {war['best_of']} (need {wins_needed})\n"
                    f"{bar}\n"
                    f"Log match: `!war score {war['id']} <our> <their>`"
                ),
                inline=False
            )
        
        embed.set_footer(text="✝ THE FALLEN ✝")
        await ctx.send(embed=embed)
    
    @war_group.command(name="score")
    async def war_score(self, ctx, war_id: int, our_score: int, their_score: int):
        """Log a war match result. Staff only."""
        if not is_staff(ctx.author):
            return await ctx.send("❌ Staff only!", delete_after=5)
        
        war = await db.get_war(war_id)
        if not war:
            return await ctx.send(f"❌ War #{war_id} not found!")
        
        if war.get("status") == "completed":
            return await ctx.send("❌ This war is already completed!")
        
        match_number = war.get("our_wins", 0) + war.get("our_losses", 0) + 1
        match_id = await db.log_war_match(war_id, match_number, our_score, their_score)
        
        result = "win" if our_score > their_score else ("loss" if their_score > our_score else "draw")
        result_emoji = "🏆" if result == "win" else "💀" if result == "loss" else "🤝"
        
        # Re-fetch war for updated scores
        war = await db.get_war(war_id)
        
        embed = discord.Embed(
            title=f"{result_emoji} War Match #{match_number}",
            description=(
                f"**vs {war['enemy_clan']}** — {our_score} to {their_score}\n\n"
                f"**War Score:** The Fallen **{war['our_wins']}** - **{war['our_losses']}** {war['enemy_clan']}"
            ),
            color=0x00C853 if result == "win" else 0xD32F2F if result == "loss" else 0xFFB300
        )
        
        if war.get("status") == "completed":
            war_result = war.get("result", "unknown")
            if war_result == "victory":
                embed.add_field(
                    name="🏆 WAR WON!", 
                    value=f"The Fallen have defeated {war['enemy_clan']}!", 
                    inline=False
                )
            else:
                embed.add_field(
                    name="💀 War Lost", 
                    value=f"{war['enemy_clan']} has won the war.", 
                    inline=False
                )
        
        embed.set_footer(text=f"✝ THE FALLEN ✝ | War #{war_id}")
        await ctx.send(embed=embed)
    
    @war_group.command(name="record")
    async def war_record(self, ctx):
        """View overall war win/loss record."""
        record = await db.get_war_record()
        
        total = record.get("total", 0)
        wins = record.get("wins", 0)
        losses = record.get("losses", 0)
        winrate = (wins / total * 100) if total > 0 else 0
        
        embed = discord.Embed(
            title="🏴 The Fallen — War Record",
            description=(
                f"**Total Wars:** {total}\n"
                f"**Victories:** {wins} 🏆\n"
                f"**Defeats:** {losses} 💀\n"
                f"**Win Rate:** {winrate:.1f}%\n\n"
                f"{progress_bar(wins, total, 15)} {wins}/{total}"
            ),
            color=0x8B0000
        )
        embed.set_footer(text="✝ THE FALLEN ✝")
        await ctx.send(embed=embed)
    
    @war_group.command(name="history")
    async def war_history(self, ctx):
        """View past war results."""
        if not db.pool:
            return await ctx.send("❌ Database not connected!")
        
        async with db.pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT * FROM wars WHERE status = 'completed' ORDER BY completed_at DESC LIMIT 10"
            )
        
        if not rows:
            return await ctx.send("📜 No completed wars yet!")
        
        embed = discord.Embed(title="📜 War History", color=0x8B0000)
        
        for war in rows:
            result = war.get("result", "unknown")
            emoji = "🏆" if result == "victory" else "💀"
            
            embed.add_field(
                name=f"{emoji} vs {war['enemy_clan']}",
                value=f"**{war['our_wins']}** - **{war['our_losses']}** (Bo{war['best_of']})",
                inline=True
            )
        
        embed.set_footer(text="✝ THE FALLEN ✝")
        await ctx.send(embed=embed)


async def setup(bot):
    """Load the Raid cog."""
    await bot.add_cog(RaidCog(bot))
    print("✅ Raid & War cog loaded!")
