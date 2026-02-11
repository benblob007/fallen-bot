"""
✝ THE FALLEN ✝ - Recruitment Pipeline System
Full internal recruitment board with application flow, interview scheduling,
trial periods, staff review panels, and pipeline overview.

Commands:
  !recruit post <position_key>    - Post an open position (High Staff)
  !recruit close <position_id>    - Close a position (High Staff)
  !recruit board                  - View all open positions
  !recruit apply <position_id>    - Apply for a position
  !recruit myapps                 - View your applications
  !recruit pipeline               - Pipeline overview (Staff)
  !recruit review <app_id>        - Review an application (Staff)
  !recruit advance <app_id> <stage> [notes] - Move app stage (Staff)
  !recruit deny <app_id> [reason] - Deny an application (Staff)
  !recruit accept <app_id>        - Accept an application (Staff)
  !recruit panel                  - Post recruitment panel (Staff)
"""

import discord
from discord import app_commands
from discord.ext import commands
import datetime
import asyncio
import json

from database import db
from utils import is_staff, is_high_staff, log_action
from config import (
    RECRUITMENT_POSITIONS, RECRUITMENT_STAGES, RECRUITMENT_STAGE_EMOJIS,
    RECRUITMENT_CHANNEL_NAME
)


# =========================================================
# APPLICATION MODAL
# =========================================================

class RecruitmentApplicationModal(discord.ui.Modal):
    """Modal for applying to a position."""
    
    def __init__(self, position_id: int, position_data: dict):
        title_text = f"Apply: {position_data.get('title', 'Position')}"
        if len(title_text) > 45:
            title_text = title_text[:42] + "..."
        super().__init__(title=title_text)
        self.position_id = position_id
        self.position_data = position_data
        
        self.experience = discord.ui.TextInput(
            label="Relevant Experience",
            placeholder="Describe your experience related to this role...",
            style=discord.TextStyle.paragraph,
            required=True,
            max_length=1000
        )
        self.add_item(self.experience)
        
        self.availability = discord.ui.TextInput(
            label="Availability (days/times)",
            placeholder="e.g. Mon-Fri evenings, weekends all day",
            required=True,
            max_length=200
        )
        self.add_item(self.availability)
        
        self.why_you = discord.ui.TextInput(
            label="Why should we pick you?",
            placeholder="What makes you a good fit for this role?",
            style=discord.TextStyle.paragraph,
            required=True,
            max_length=1000
        )
        self.add_item(self.why_you)
        
        self.additional = discord.ui.TextInput(
            label="Anything else to add? (optional)",
            placeholder="Additional info, links, etc.",
            style=discord.TextStyle.paragraph,
            required=False,
            max_length=500
        )
        self.add_item(self.additional)
    
    async def on_submit(self, interaction: discord.Interaction):
        answers = {
            "experience": self.experience.value,
            "availability": self.availability.value,
            "why_you": self.why_you.value,
            "additional": self.additional.value or "N/A"
        }
        
        app_id = await db.apply_for_position(self.position_id, interaction.user.id, answers)
        
        if app_id is None:
            return await interaction.response.send_message(
                "❌ You've already applied for this position!", ephemeral=True
            )
        
        pos_title = self.position_data.get("title", "Unknown Position")
        
        await interaction.response.send_message(
            f"✅ Application submitted for **{pos_title}**! (Application #{app_id})\n"
            "You'll be notified when staff reviews your application.",
            ephemeral=True
        )
        
        # Notify staff
        await _notify_staff_new_application(interaction.guild, interaction.user, app_id, self.position_data)
        
        await log_action(
            interaction.guild,
            "📋 New Recruitment Application",
            f"**Applicant:** {interaction.user.mention}\n"
            f"**Position:** {pos_title}\n"
            f"**App ID:** #{app_id}",
            color=0x2196F3
        )


class ReviewNotesModal(discord.ui.Modal, title="Review Notes"):
    """Modal for adding review notes to an application."""
    
    notes = discord.ui.TextInput(
        label="Review Notes",
        placeholder="Your assessment of this applicant...",
        style=discord.TextStyle.paragraph,
        required=True,
        max_length=1000
    )
    
    def __init__(self, app_id: int, new_stage: str):
        super().__init__()
        self.app_id = app_id
        self.new_stage = new_stage
    
    async def on_submit(self, interaction: discord.Interaction):
        await db.advance_application(self.app_id, self.new_stage, interaction.user.id, self.notes.value)
        
        app = await db.get_application(self.app_id)
        stage_emoji = RECRUITMENT_STAGE_EMOJIS.get(self.new_stage, "📋")
        
        await interaction.response.send_message(
            f"{stage_emoji} Application #{self.app_id} moved to **{self.new_stage}**.\n"
            f"Notes: {self.notes.value[:200]}",
            ephemeral=True
        )
        
        # Notify the applicant
        if app:
            try:
                applicant = interaction.guild.get_member(app.get("user_id"))
                if applicant:
                    stage_messages = {
                        "under_review": "Your application is now **under review** by our staff.",
                        "interview": "You've been selected for an **interview**! A staff member will reach out.",
                        "trial": "Congratulations! You've been approved for a **trial period**!",
                        "accepted": "🎉 You've been **accepted**! Welcome to the team!",
                        "denied": "Unfortunately, your application has been **denied**.",
                    }
                    msg = stage_messages.get(self.new_stage, f"Your application status changed to: {self.new_stage}")
                    
                    dm_embed = discord.Embed(
                        title=f"{stage_emoji} Application Update",
                        description=msg,
                        color=0x00C853 if self.new_stage == "accepted" else 0xD32F2F if self.new_stage == "denied" else 0x2196F3
                    )
                    dm_embed.set_footer(text="✝ THE FALLEN ✝")
                    await applicant.send(embed=dm_embed)
            except Exception:
                pass


# =========================================================
# RECRUITMENT BOARD VIEW (Persistent)
# =========================================================

class RecruitmentBoardView(discord.ui.View):
    """Persistent view for the recruitment board."""
    
    def __init__(self):
        super().__init__(timeout=None)
    
    @discord.ui.button(label="📋 View Open Positions", style=discord.ButtonStyle.blurple, custom_id="recruit_view_positions")
    async def view_positions(self, interaction: discord.Interaction, button: discord.ui.Button):
        positions = await db.get_open_positions()
        
        if not positions:
            return await interaction.response.send_message(
                "No open positions right now. Check back later!", ephemeral=True
            )
        
        embed = discord.Embed(
            title="📋 Open Positions",
            description="Use `!recruit apply <position_id>` to apply.",
            color=0x8B0000
        )
        
        for pos in positions:
            key = pos.get("position_key", "")
            config = RECRUITMENT_POSITIONS.get(key, {})
            emoji = config.get("emoji", "📋")
            reqs = pos.get("requirements", [])
            if isinstance(reqs, list):
                req_text = "\n".join(f"  • {r}" for r in reqs[:3])
            else:
                req_text = "See description"
            
            filled = pos.get("slots_filled", 0)
            available = pos.get("slots_available", 1)
            
            embed.add_field(
                name=f"{emoji} {pos.get('title', 'Unknown')} (ID: {pos['id']})",
                value=(
                    f"{pos.get('description', 'No description')}\n\n"
                    f"**Requirements:**\n{req_text}\n"
                    f"**Slots:** {filled}/{available} filled"
                ),
                inline=False
            )
        
        embed.set_footer(text="✝ THE FALLEN ✝")
        await interaction.response.send_message(embed=embed, ephemeral=True)
    
    @discord.ui.button(label="📊 My Applications", style=discord.ButtonStyle.grey, custom_id="recruit_my_apps")
    async def my_apps(self, interaction: discord.Interaction, button: discord.ui.Button):
        apps = await db.get_user_applications(interaction.user.id)
        
        if not apps:
            return await interaction.response.send_message(
                "You haven't applied for any positions yet!", ephemeral=True
            )
        
        embed = discord.Embed(
            title="📊 Your Applications",
            color=0x8B0000
        )
        
        for app in apps:
            stage = app.get("stage", "applied")
            stage_emoji = RECRUITMENT_STAGE_EMOJIS.get(stage, "📋")
            pos_title = app.get("position_title", "Unknown")
            
            embed.add_field(
                name=f"{stage_emoji} {pos_title} (App #{app['id']})",
                value=f"**Status:** {stage.replace('_', ' ').title()}",
                inline=False
            )
        
        embed.set_footer(text="✝ THE FALLEN ✝")
        await interaction.response.send_message(embed=embed, ephemeral=True)


class ApplicationReviewView(discord.ui.View):
    """Staff review controls for an application."""
    
    def __init__(self, app_id: int):
        super().__init__(timeout=300)
        self.app_id = app_id
    
    @discord.ui.button(label="🔍 Under Review", style=discord.ButtonStyle.blurple)
    async def under_review(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not is_staff(interaction.user):
            return await interaction.response.send_message("❌ Staff only!", ephemeral=True)
        await interaction.response.send_modal(ReviewNotesModal(self.app_id, "under_review"))
    
    @discord.ui.button(label="🎤 Interview", style=discord.ButtonStyle.blurple)
    async def interview(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not is_staff(interaction.user):
            return await interaction.response.send_message("❌ Staff only!", ephemeral=True)
        await interaction.response.send_modal(ReviewNotesModal(self.app_id, "interview"))
    
    @discord.ui.button(label="⚔️ Trial", style=discord.ButtonStyle.blurple)
    async def trial(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not is_staff(interaction.user):
            return await interaction.response.send_message("❌ Staff only!", ephemeral=True)
        await interaction.response.send_modal(ReviewNotesModal(self.app_id, "trial"))
    
    @discord.ui.button(label="✅ Accept", style=discord.ButtonStyle.green)
    async def accept(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not is_staff(interaction.user):
            return await interaction.response.send_message("❌ Staff only!", ephemeral=True)
        await interaction.response.send_modal(ReviewNotesModal(self.app_id, "accepted"))
    
    @discord.ui.button(label="❌ Deny", style=discord.ButtonStyle.red)
    async def deny(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not is_staff(interaction.user):
            return await interaction.response.send_message("❌ Staff only!", ephemeral=True)
        await interaction.response.send_modal(ReviewNotesModal(self.app_id, "denied"))


# =========================================================
# HELPER FUNCTIONS
# =========================================================

async def _notify_staff_new_application(guild: discord.Guild, applicant: discord.Member, 
                                          app_id: int, position_data: dict):
    """Notify staff about a new application."""
    # Try to find recruitment channel or log channel
    channel = discord.utils.get(guild.text_channels, name=RECRUITMENT_CHANNEL_NAME)
    if not channel:
        channel = discord.utils.get(guild.text_channels, name="fallen-logs")
    if not channel:
        return
    
    embed = discord.Embed(
        title="📋 New Application Received!",
        description=(
            f"**Applicant:** {applicant.mention} ({applicant.display_name})\n"
            f"**Position:** {position_data.get('title', 'Unknown')}\n"
            f"**Application ID:** #{app_id}\n\n"
            f"Use `!recruit review {app_id}` to review."
        ),
        color=0x2196F3,
        timestamp=datetime.datetime.now(datetime.timezone.utc)
    )
    embed.set_thumbnail(url=applicant.display_avatar.url if applicant.display_avatar else None)
    embed.set_footer(text="✝ THE FALLEN ✝")
    
    try:
        await channel.send(embed=embed)
    except Exception:
        pass


def _build_pipeline_visual(overview: dict) -> str:
    """Build a visual pipeline representation."""
    if not overview:
        return "No active applications."
    
    lines = []
    for position, stages in overview.items():
        lines.append(f"**{position}**")
        
        stage_bar = ""
        for stage in RECRUITMENT_STAGES:
            count = stages.get(stage, 0)
            emoji = RECRUITMENT_STAGE_EMOJIS.get(stage, "📋")
            if count > 0:
                stage_bar += f"{emoji} {stage.replace('_', ' ').title()}: **{count}** → "
        
        if stage_bar:
            lines.append(stage_bar.rstrip(" → "))
        lines.append("")
    
    return "\n".join(lines)


# =========================================================
# RECRUITMENT COG
# =========================================================

class RecruitmentCog(commands.Cog, name="Recruitment"):
    """📋 Recruitment Pipeline System"""
    
    def __init__(self, bot):
        self.bot = bot
    
    @commands.group(name="recruit", invoke_without_command=True)
    async def recruit_group(self, ctx):
        """Recruitment system commands."""
        embed = discord.Embed(
            title="📋 Recruitment Commands",
            description=(
                "**For Members:**\n"
                "`!recruit board` — View open positions\n"
                "`!recruit apply <position_id>` — Apply for a position\n"
                "`!recruit myapps` — View your applications\n\n"
                "**For Staff:**\n"
                "`!recruit post <position_key>` — Post a position\n"
                "`!recruit close <position_id>` — Close a position\n"
                "`!recruit pipeline` — Pipeline overview\n"
                "`!recruit review <app_id>` — Review application\n"
                "`!recruit advance <app_id> <stage>` — Move stage\n"
                "`!recruit accept <app_id>` — Accept\n"
                "`!recruit deny <app_id> [reason]` — Deny\n"
                "`!recruit panel` — Post recruitment board\n\n"
                f"**Available Positions:** {', '.join(f'`{k}`' for k in RECRUITMENT_POSITIONS.keys())}"
            ),
            color=0x8B0000
        )
        embed.set_footer(text="✝ THE FALLEN ✝")
        await ctx.send(embed=embed)
    
    @recruit_group.command(name="post")
    async def recruit_post(self, ctx, position_key: str, slots: int = 1):
        """Post an open position. High Staff only."""
        if not is_high_staff(ctx.author):
            return await ctx.send("❌ High Staff only!", delete_after=5)
        
        if position_key not in RECRUITMENT_POSITIONS:
            keys = ", ".join(f"`{k}`" for k in RECRUITMENT_POSITIONS.keys())
            return await ctx.send(f"❌ Invalid position key. Options: {keys}")
        
        config = RECRUITMENT_POSITIONS[position_key]
        
        pos_id = await db.create_position(
            position_key=position_key,
            title=config["name"],
            description=config["description"],
            requirements=config.get("requirements", []),
            posted_by=ctx.author.id,
            review_role=config.get("review_role"),
            slots=slots
        )
        
        embed = discord.Embed(
            title=f"{config['emoji']} Position Open: {config['name']}",
            description=(
                f"{config['description']}\n\n"
                f"**Requirements:**\n" +
                "\n".join(f"• {r}" for r in config.get("requirements", [])) +
                f"\n\n**Slots Available:** {slots}\n"
                f"**Position ID:** {pos_id}\n\n"
                f"Apply with: `!recruit apply {pos_id}`"
            ),
            color=0x2196F3,
            timestamp=datetime.datetime.now(datetime.timezone.utc)
        )
        embed.set_footer(text="✝ THE FALLEN ✝")
        
        msg = await ctx.send(embed=embed)
        
        # Update position with message info
        if db.pool:
            await db.update_position(pos_id, channel_id=ctx.channel.id, message_id=msg.id)
        
        await log_action(
            ctx.guild, "📋 Position Posted",
            f"**Position:** {config['name']}\n**Slots:** {slots}\n**Posted by:** {ctx.author.mention}\n**ID:** {pos_id}",
            color=0x2196F3
        )
    
    @recruit_group.command(name="close")
    async def recruit_close(self, ctx, position_id: int):
        """Close an open position. High Staff only."""
        if not is_high_staff(ctx.author):
            return await ctx.send("❌ High Staff only!", delete_after=5)
        
        position = await db.get_position(position_id)
        if not position:
            return await ctx.send(f"❌ Position #{position_id} not found!")
        
        if db.pool:
            await db.update_position(position_id, status="closed", closed_at=datetime.datetime.now(datetime.timezone.utc))
        
        await ctx.send(f"✅ Position **{position.get('title', 'Unknown')}** (#{position_id}) has been closed.")
    
    @recruit_group.command(name="board")
    async def recruit_board(self, ctx):
        """View all open positions."""
        positions = await db.get_open_positions()
        
        if not positions:
            return await ctx.send("📋 No open positions right now. Check back later!")
        
        embed = discord.Embed(
            title="📋 Recruitment Board — Open Positions",
            description="Apply for any position using `!recruit apply <position_id>`",
            color=0x8B0000
        )
        
        for pos in positions:
            key = pos.get("position_key", "")
            config = RECRUITMENT_POSITIONS.get(key, {})
            emoji = config.get("emoji", "📋")
            
            reqs = pos.get("requirements", [])
            if isinstance(reqs, list) and reqs:
                req_text = " | ".join(reqs[:3])
            else:
                req_text = "See description"
            
            filled = pos.get("slots_filled", 0)
            available = pos.get("slots_available", 1)
            remaining = available - filled
            
            embed.add_field(
                name=f"{emoji} {pos.get('title', 'Unknown')} — ID: {pos['id']}",
                value=(
                    f"{pos.get('description', 'No description')}\n"
                    f"📌 {req_text}\n"
                    f"🪑 **{remaining}** slot{'s' if remaining != 1 else ''} remaining"
                ),
                inline=False
            )
        
        embed.set_footer(text="✝ THE FALLEN ✝ | !recruit apply <id> to apply")
        await ctx.send(embed=embed)
    
    @recruit_group.command(name="apply")
    async def recruit_apply(self, ctx, position_id: int):
        """Apply for an open position."""
        position = await db.get_position(position_id)
        
        if not position:
            return await ctx.send(f"❌ Position #{position_id} not found!")
        
        if position.get("status") != "open":
            return await ctx.send("❌ This position is no longer open!")
        
        # Check if already applied
        existing = await db.get_user_applications(ctx.author.id)
        for app in existing:
            if app.get("position_id") == position_id and app.get("stage") not in ["denied"]:
                return await ctx.send("❌ You already have an active application for this position!")
        
        # Show application modal (slash commands) or text flow (prefix)
        if ctx.interaction:
            await ctx.interaction.response.send_modal(
                RecruitmentApplicationModal(position_id, position)
            )
        else:
            # For prefix commands, use a simpler text-based flow
            embed = discord.Embed(
                title=f"📋 Applying for: {position.get('title', 'Unknown')}",
                description=(
                    "Please answer the following in **one message** separated by `|`:\n\n"
                    "1. **Relevant Experience**\n"
                    "2. **Availability (days/times)**\n"
                    "3. **Why should we pick you?**\n\n"
                    "Example: `I've led raids for 2 years | Mon-Fri evenings | I'm dedicated and skilled`\n\n"
                    "Type your answers or `cancel` to abort."
                ),
                color=0x2196F3
            )
            await ctx.send(embed=embed)
            
            def check(m):
                return m.author == ctx.author and m.channel == ctx.channel
            
            try:
                msg = await self.bot.wait_for("message", timeout=300, check=check)
                
                if msg.content.lower() == "cancel":
                    return await ctx.send("❌ Application cancelled.")
                
                parts = [p.strip() for p in msg.content.split("|")]
                if len(parts) < 3:
                    return await ctx.send("❌ Please provide all 3 answers separated by `|`.")
                
                answers = {
                    "experience": parts[0],
                    "availability": parts[1],
                    "why_you": parts[2],
                    "additional": parts[3] if len(parts) > 3 else "N/A"
                }
                
                app_id = await db.apply_for_position(position_id, ctx.author.id, answers)
                
                if app_id is None:
                    return await ctx.send("❌ You've already applied for this position!")
                
                await ctx.send(
                    f"✅ Application submitted! (#{app_id}) You'll be notified when staff reviews it."
                )
                
                await _notify_staff_new_application(ctx.guild, ctx.author, app_id, position)
                
            except asyncio.TimeoutError:
                await ctx.send("⏰ Application timed out.")
    
    @recruit_group.command(name="myapps")
    async def recruit_myapps(self, ctx):
        """View your applications."""
        apps = await db.get_user_applications(ctx.author.id)
        
        if not apps:
            return await ctx.send("📋 You haven't applied for any positions yet!")
        
        embed = discord.Embed(
            title=f"📊 Your Applications — {ctx.author.display_name}",
            color=0x8B0000
        )
        
        for app in apps:
            stage = app.get("stage", "applied")
            stage_emoji = RECRUITMENT_STAGE_EMOJIS.get(stage, "📋")
            pos_title = app.get("position_title", "Unknown")
            
            created = app.get("created_at")
            time_str = ""
            if created:
                if isinstance(created, str):
                    created = datetime.datetime.fromisoformat(created)
                time_str = f"Applied: <t:{int(created.timestamp())}:R>"
            
            embed.add_field(
                name=f"{stage_emoji} {pos_title} (#{app['id']})",
                value=f"**Status:** {stage.replace('_', ' ').title()}\n{time_str}",
                inline=False
            )
        
        embed.set_footer(text="✝ THE FALLEN ✝")
        await ctx.send(embed=embed)
    
    @recruit_group.command(name="pipeline")
    async def recruit_pipeline(self, ctx):
        """View the recruitment pipeline overview. Staff only."""
        if not is_staff(ctx.author):
            return await ctx.send("❌ Staff only!", delete_after=5)
        
        overview = await db.get_pipeline_overview()
        
        embed = discord.Embed(
            title="📊 Recruitment Pipeline",
            color=0x8B0000
        )
        
        if not overview:
            embed.description = "No active applications in the pipeline."
        else:
            for position, stages in overview.items():
                visual = ""
                total = sum(stages.values())
                
                for stage in RECRUITMENT_STAGES:
                    count = stages.get(stage, 0)
                    if count > 0:
                        emoji = RECRUITMENT_STAGE_EMOJIS.get(stage, "📋")
                        visual += f"{emoji} **{count}** {stage.replace('_', ' ').title()}\n"
                
                embed.add_field(
                    name=f"📋 {position} ({total} total)",
                    value=visual or "No applications",
                    inline=False
                )
        
        # Add pipeline stage legend
        legend = " → ".join(f"{RECRUITMENT_STAGE_EMOJIS[s]} {s.replace('_', ' ').title()}" for s in RECRUITMENT_STAGES)
        embed.add_field(name="📍 Pipeline Stages", value=legend, inline=False)
        
        embed.set_footer(text="✝ THE FALLEN ✝ | !recruit review <app_id> to review")
        await ctx.send(embed=embed)
    
    @recruit_group.command(name="review")
    async def recruit_review(self, ctx, app_id: int):
        """Review a specific application. Staff only."""
        if not is_staff(ctx.author):
            return await ctx.send("❌ Staff only!", delete_after=5)
        
        app = await db.get_application(app_id)
        if not app:
            return await ctx.send(f"❌ Application #{app_id} not found!")
        
        # Get position info
        pos_id = app.get("position_id")
        position = await db.get_position(pos_id) if pos_id else None
        
        applicant = ctx.guild.get_member(app.get("user_id"))
        applicant_name = applicant.display_name if applicant else f"User {app.get('user_id')}"
        
        stage = app.get("stage", "applied")
        stage_emoji = RECRUITMENT_STAGE_EMOJIS.get(stage, "📋")
        
        embed = discord.Embed(
            title=f"{stage_emoji} Application #{app_id} — {applicant_name}",
            color=0x00C853 if stage == "accepted" else 0xD32F2F if stage == "denied" else 0x2196F3
        )
        
        if position:
            embed.add_field(name="📋 Position", value=position.get("title", "Unknown"), inline=True)
        
        embed.add_field(name="📊 Stage", value=stage.replace("_", " ").title(), inline=True)
        
        if applicant:
            # Show some user stats
            user_data = await db.get_user(applicant.id)
            if user_data:
                embed.add_field(
                    name="📈 Member Stats",
                    value=(
                        f"Level: {user_data.get('level', 0)}\n"
                        f"Raids: {user_data.get('raid_participation', 0)}\n"
                        f"Joined: {applicant.joined_at.strftime('%b %d, %Y') if applicant.joined_at else 'Unknown'}"
                    ),
                    inline=True
                )
        
        # Show answers
        answers = app.get("answers", {})
        if isinstance(answers, str):
            try:
                answers = json.loads(answers)
            except Exception:
                answers = {}
        
        if answers:
            for key, value in answers.items():
                label = key.replace("_", " ").title()
                display_value = str(value)[:1024] if value else "N/A"
                embed.add_field(name=f"💬 {label}", value=display_value, inline=False)
        
        # Show review notes if any
        if app.get("review_notes"):
            embed.add_field(name="📝 Review Notes", value=app["review_notes"][:1024], inline=False)
        
        # Show reviewer
        if app.get("reviewer_id"):
            reviewer = ctx.guild.get_member(app["reviewer_id"])
            if reviewer:
                embed.add_field(name="👤 Reviewer", value=reviewer.mention, inline=True)
        
        embed.set_footer(text="✝ THE FALLEN ✝ | Use buttons below to update stage")
        
        if applicant and applicant.display_avatar:
            embed.set_thumbnail(url=applicant.display_avatar.url)
        
        # Show with review controls
        view = ApplicationReviewView(app_id) if stage not in ["accepted", "denied"] else None
        await ctx.send(embed=embed, view=view)
    
    @recruit_group.command(name="advance")
    async def recruit_advance(self, ctx, app_id: int, stage: str, *, notes: str = None):
        """Move an application to a new stage. Staff only."""
        if not is_staff(ctx.author):
            return await ctx.send("❌ Staff only!", delete_after=5)
        
        if stage not in RECRUITMENT_STAGES:
            stages = ", ".join(f"`{s}`" for s in RECRUITMENT_STAGES)
            return await ctx.send(f"❌ Invalid stage. Options: {stages}")
        
        app = await db.get_application(app_id)
        if not app:
            return await ctx.send(f"❌ Application #{app_id} not found!")
        
        await db.advance_application(app_id, stage, ctx.author.id, notes)
        
        stage_emoji = RECRUITMENT_STAGE_EMOJIS.get(stage, "📋")
        await ctx.send(f"{stage_emoji} Application #{app_id} moved to **{stage.replace('_', ' ').title()}**.")
        
        # Notify applicant
        try:
            applicant = ctx.guild.get_member(app.get("user_id"))
            if applicant:
                stage_messages = {
                    "under_review": "Your application is now **under review**.",
                    "interview": "You've been selected for an **interview**!",
                    "trial": "You've been approved for a **trial period**!",
                    "accepted": "🎉 You've been **accepted**! Welcome to the team!",
                    "denied": "Your application has been **denied**.",
                }
                msg = stage_messages.get(stage, f"Status: {stage}")
                
                dm_embed = discord.Embed(
                    title=f"{stage_emoji} Application Update",
                    description=msg,
                    color=0x00C853 if stage == "accepted" else 0xD32F2F if stage == "denied" else 0x2196F3
                )
                if notes:
                    dm_embed.add_field(name="Notes", value=notes[:500])
                dm_embed.set_footer(text="✝ THE FALLEN ✝")
                await applicant.send(embed=dm_embed)
        except Exception:
            pass
    
    @recruit_group.command(name="accept")
    async def recruit_accept(self, ctx, app_id: int, *, notes: str = None):
        """Accept an application. Staff only."""
        if not is_staff(ctx.author):
            return await ctx.send("❌ Staff only!", delete_after=5)
        
        app = await db.get_application(app_id)
        if not app:
            return await ctx.send(f"❌ Application #{app_id} not found!")
        
        await db.advance_application(app_id, "accepted", ctx.author.id, notes or "Accepted!")
        
        # Update position filled count
        pos_id = app.get("position_id")
        if pos_id and db.pool:
            async with db.pool.acquire() as conn:
                await conn.execute(
                    "UPDATE recruitment_positions SET slots_filled = slots_filled + 1 WHERE id = $1",
                    pos_id
                )
        
        position = await db.get_position(pos_id) if pos_id else None
        pos_title = position.get("title", "Unknown") if position else "Unknown"
        
        applicant = ctx.guild.get_member(app.get("user_id"))
        applicant_name = applicant.mention if applicant else f"User {app.get('user_id')}"
        
        embed = discord.Embed(
            title="✅ Application Accepted!",
            description=(
                f"**Applicant:** {applicant_name}\n"
                f"**Position:** {pos_title}\n"
                f"**Accepted by:** {ctx.author.mention}"
            ),
            color=0x00C853,
            timestamp=datetime.datetime.now(datetime.timezone.utc)
        )
        embed.set_footer(text="✝ THE FALLEN ✝")
        await ctx.send(embed=embed)
        
        # DM the applicant
        if applicant:
            try:
                dm_embed = discord.Embed(
                    title="🎉 Congratulations!",
                    description=(
                        f"Your application for **{pos_title}** has been **accepted**!\n\n"
                        "A staff member will be in touch with next steps."
                    ),
                    color=0x00C853
                )
                if notes:
                    dm_embed.add_field(name="Notes", value=notes[:500])
                dm_embed.set_footer(text="✝ THE FALLEN ✝")
                await applicant.send(embed=dm_embed)
            except Exception:
                pass
        
        await log_action(
            ctx.guild, "✅ Recruitment Accepted",
            f"**Applicant:** {applicant_name}\n**Position:** {pos_title}\n**By:** {ctx.author.mention}",
            color=0x00C853
        )
    
    @recruit_group.command(name="deny")
    async def recruit_deny(self, ctx, app_id: int, *, reason: str = None):
        """Deny an application. Staff only."""
        if not is_staff(ctx.author):
            return await ctx.send("❌ Staff only!", delete_after=5)
        
        app = await db.get_application(app_id)
        if not app:
            return await ctx.send(f"❌ Application #{app_id} not found!")
        
        await db.advance_application(app_id, "denied", ctx.author.id, reason or "Application denied.")
        
        await ctx.send(f"❌ Application #{app_id} has been denied.")
        
        # DM the applicant
        applicant = ctx.guild.get_member(app.get("user_id"))
        if applicant:
            try:
                dm_embed = discord.Embed(
                    title="❌ Application Update",
                    description="Unfortunately, your application has been denied.",
                    color=0xD32F2F
                )
                if reason:
                    dm_embed.add_field(name="Reason", value=reason[:500])
                dm_embed.add_field(
                    name="💡", 
                    value="Don't give up! You can re-apply in the future.",
                    inline=False
                )
                dm_embed.set_footer(text="✝ THE FALLEN ✝")
                await applicant.send(embed=dm_embed)
            except Exception:
                pass
    
    @recruit_group.command(name="panel")
    async def recruit_panel(self, ctx):
        """Post a recruitment panel. Staff only."""
        if not is_staff(ctx.author):
            return await ctx.send("❌ Staff only!", delete_after=5)
        
        embed = discord.Embed(
            title="✝ THE FALLEN — RECRUITMENT ✝",
            description=(
                "We're looking for dedicated members to fill key roles!\n\n"
                "Click **📋 View Open Positions** to see what's available, "
                "or use `!recruit board` to browse.\n\n"
                "*Through ruin lies the seed of power.*"
            ),
            color=0x8B0000,
            timestamp=datetime.datetime.now(datetime.timezone.utc)
        )
        
        # List current positions
        positions = await db.get_open_positions()
        if positions:
            pos_text = ""
            for pos in positions:
                key = pos.get("position_key", "")
                config = RECRUITMENT_POSITIONS.get(key, {})
                emoji = config.get("emoji", "📋")
                remaining = pos.get("slots_available", 1) - pos.get("slots_filled", 0)
                pos_text += f"{emoji} **{pos.get('title', 'Unknown')}** — {remaining} slot{'s' if remaining != 1 else ''}\n"
            embed.add_field(name="🔥 Currently Hiring", value=pos_text, inline=False)
        
        embed.set_footer(text="✝ THE FALLEN ✝ | Apply today!")
        
        await ctx.send(embed=embed, view=RecruitmentBoardView())


async def setup(bot):
    """Load the Recruitment cog."""
    await bot.add_cog(RecruitmentCog(bot))
    # Register persistent view
    bot.add_view(RecruitmentBoardView())
    print("✅ Recruitment Pipeline cog loaded!")
