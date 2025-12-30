import discord
from discord import app_commands
from discord.ext import commands, tasks
import json
import os
import asyncio
import datetime
import random
import re
from io import BytesIO

# Try to import PIL for level cards (optional)
try:
    from PIL import Image, ImageDraw, ImageFont, ImageFilter
    PIL_AVAILABLE = True
except ImportError:
    PIL_AVAILABLE = False
    print("PIL not installed - using embed-based level cards. Install with: pip install Pillow")

# --- CONFIGURATION ---
# Get token from environment variable (set in Render dashboard)
TOKEN = os.getenv("DISCORD_TOKEN") 

# --- ROLE SETTINGS ---
REQUIRED_ROLE_NAME = "Mainer"         
STAFF_ROLE_NAME = "Staff"             
UNVERIFIED_ROLE_NAME = "Unverified"
VERIFIED_ROLE_NAME = "Verified"
MEMBER_ROLE_NAME = "Abyssbound" 

# The High Staff roles:
HIGH_STAFF_ROLES = [
    "The Fallen Sovereign〢Owner", 
    "The Fallen Right Hand〢Co-Owner", 
    "The Fallen Marshal〢Head of Staff"
] 

ANNOUNCEMENT_ROLE_NAME = "Set Ping" 

# --- APPLICATION SETTINGS ---
REQUIRED_APP_ROLES = ["Stage 2〢FALLEN ASCENDANT", "High", "Stable"] 
TRYOUT_HOST_ROLE = "The Abyssal Overseer〢Tryout Host"

# --- CHANNEL SETTINGS ---
LEADERBOARD_FILE = "leaderboard.json"
ANNOUNCEMENT_CHANNEL_NAME = "♰・set-annc"     
LOG_CHANNEL_NAME = "fallen-logs"               
SET_RESULTS_CHANNEL_NAME = "♰・set-score"              
TOURNAMENT_RESULTS_CHANNEL_NAME = "╰・tournament-results" 
LEVEL_UP_CHANNEL_NAME = "♰・level"        
SHOP_CHANNEL_NAME = "♰・fallen-shop"      

# --- LEVEL CARD SETTINGS ---
# Option 1: Set URL to your background image (RECOMMENDED for Discloud)
LEVEL_CARD_BACKGROUND = "https://blogger.googleusercontent.com/img/a/AVvXsEhrGLEr5QVeeFYmbJfJMATHt7C7Kgnoe_tPc3TVls9xqg8m-4slNuG4vHdnhB_yjrM0_X0jkp4dHetJEPCliukqbw3DLpsUcOluAHWsCty7Jc9616REfIkd1P6da_67MSyVB-qDDQ9plnT_ICRcY6HnFnRmTs3t4sv88Clu-RP0r7pEn1bskrlny8qH3teL"

# Option 2: Local file - will check multiple locations for Discloud compatibility
LEVEL_CARD_FILE = "level_background.png"

# Possible file locations (for different hosts)
LEVEL_CARD_PATHS = [
    "level_background.png",
    "./level_background.png",
    "/opt/render/project/src/level_background.png",  # Render path
    "/home/user_discloud/level_background.png",
    "/app/level_background.png",
    os.path.join(os.path.dirname(__file__), "level_background.png"),
]

# --- ECONOMY SETTINGS ---
MAX_COINS = 1000000 

# --- LEVELING SYSTEM CONFIG ---
LEVEL_CONFIG = {
    5: {"xp": 1500, "role": "Faint Emberling", "coins": 500},
    10: {"xp": 4000, "role": "Initiate of Shadows", "coins": 1000},
    20: {"xp": 12000, "role": "Abysswalk Student", "coins": 2000},
    30: {"xp": 25000, "role": "Twilight Disciple", "coins": 3000},
    40: {"xp": 45000, "role": "Duskforged Aspirant", "coins": 4000},
    50: {"xp": 70000, "role": "Bearer of Abyssal Echo", "coins": 5000},
    60: {"xp": 100000, "role": "Nightwoven Adept", "coins": 6000},
    70: {"xp": 140000, "role": "Veilmarked Veteran", "coins": 7000},
    80: {"xp": 190000, "role": "Shadowborn Ascendant", "coins": 8000},
    100: {"xp": 300000, "role": "Abyssforged Warden", "coins": 15000},
    120: {"xp": 450000, "role": "Eclipsed Oathbearer", "coins": 20000},
    140: {"xp": 650000, "role": "Harbinger of Dusk", "coins": 30000},
    160: {"xp": 900000, "role": "Ascended Dreadkeeper", "coins": 40000},
    200: {"xp": 1500000, "role": "Eternal Shadow Sovereign", "coins": 100000},
}

XP_TEXT_RANGE = (1, 10) 
XP_VOICE_RANGE = (10, 20) 
XP_REACTION_RANGE = (1, 5) 
COOLDOWN_SECONDS = 60 

# --- SHOP CONFIG ---
SHOP_ITEMS = [
    {"id": "private_tryout", "name": "⚔️ Private Tryout Ticket", "price": 500, "desc": "Opens a private channel with hosts."},
    {"id": "custom_role", "name": "🎨 Custom Role Request", "price": 2000, "desc": "Request a custom colored role."}
]

# --- TOURNAMENT STATE ---
tournament_state = {
    "active": False,
    "title": "",
    "players": [],       
    "next_round": [],    
    "losers_stack": [],  
    "match_count": 0,    
    "finished_matches": 0,
    "ranked_mode": True
}

# --- DEFAULT THEME SETTINGS ---
DEFAULT_THEME = {
    "title": "✝ FALLEN ✝ - The Fallen Saints",
    "description": (
        "> *Through shattered skies and broken crowns,*\n"
        "> *The descent carves its mark.*\n"
        "> *Fallen endures — not erased, but remade.*\n"
        "> *In ruin lies the seed of power.*"
    ),
    "image": "https://blogger.googleusercontent.com/img/b/R29vZ2xl/AVvXsEivz3D1KuVNJC5NJgJS8d4EowFMqP7ba8SqUkLZT5K9m2BrtHHPQir6r6oGK1lw6h18GJcttTt57xwfgCcRQgjYKn3rXEnOmCTQwPieDbvHPifZ3EHPVsL7wrkmYKNSma1ADPeeUUFPTqHPe5S7eZlz4KAEpPq0NWzIZIXeFgk_AaU8iLeMeE8_4aaaGsah/s320/%E8%90%BD%E3%81%A1%E3%81%9F.png",
    "color": 0x2b2d31 
}

# --- DATA MANAGEMENT ---
def load_data():
    if not os.path.exists(LEADERBOARD_FILE):
        return {"roster": [None]*10, "theme": DEFAULT_THEME, "users": {}}
    with open(LEADERBOARD_FILE, "r") as f:
        try:
            data = json.load(f)
            if "users" not in data: data["users"] = {}
            if "roster" not in data: data["roster"] = [None]*10
            if "theme" not in data: data["theme"] = DEFAULT_THEME
            return data
        except Exception as e:
            print(f"Error loading data: {e}")
            return {"roster": [None]*10, "theme": DEFAULT_THEME, "users": {}}

def save_data(data):
    with open(LEADERBOARD_FILE, "w") as f:
        json.dump(data, f, indent=4)

def reset_all_data():
    """Complete data wipe - resets everything"""
    fresh_data = {
        "roster": [None]*10, 
        "theme": DEFAULT_THEME, 
        "users": {}
    }
    save_data(fresh_data)
    return True

def ensure_user_structure(data, uid):
    defaults = {
        "xp": 0, "level": 0, "coins": 0, "last_xp": 0,
        "weekly_xp": 0, "monthly_xp": 0, "voice_time": 0,
        "roblox_username": None, "roblox_id": None, "verified": False,
        "wins": 0, "losses": 0,
        "raid_wins": 0, "raid_losses": 0, "raid_participation": 0,
        "training_attendance": 0, "tryout_passes": 0, "tryout_fails": 0,
        "warnings": [], "last_daily": None, "daily_streak": 0,
        "last_active": datetime.datetime.now(datetime.timezone.utc).isoformat()
    }
    if uid not in data["users"]:
        data["users"][uid] = defaults.copy()
    else:
        for k, v in defaults.items():
            if k not in data["users"][uid]:
                data["users"][uid][k] = v
    return data

# --- USER DATA HELPERS ---
def get_user_data(user_id):
    data = load_data()
    uid = str(user_id)
    data = ensure_user_structure(data, uid)
    save_data(data)
    return data["users"][uid]

def update_user_data(user_id, key, value):
    data = load_data()
    uid = str(user_id)
    data = ensure_user_structure(data, uid)
    data["users"][uid][key] = value
    save_data(data)

def add_user_stat(user_id, key, amount):
    data = load_data()
    uid = str(user_id)
    data = ensure_user_structure(data, uid)
    current = data["users"][uid].get(key, 0)
    new_val = current + amount
    if key == "coins":
        if new_val > MAX_COINS: new_val = MAX_COINS
        if new_val < 0: new_val = 0
    data["users"][uid][key] = new_val
    save_data(data)
    return new_val

def add_xp_to_user(user_id, amount):
    data = load_data()
    uid = str(user_id)
    data = ensure_user_structure(data, uid)
    data["users"][uid]["xp"] += amount
    data["users"][uid]["weekly_xp"] += amount
    data["users"][uid]["monthly_xp"] += amount
    save_data(data)
    return data["users"][uid]["xp"]

def calculate_next_level_xp(level):
    for lvl in sorted(LEVEL_CONFIG.keys()):
        if lvl > level: return LEVEL_CONFIG[lvl]["xp"]
    return (level + 1) * 5000 

def get_level_rank(user_id):
    data = load_data()
    users = data["users"]
    sorted_users = sorted(users.items(), key=lambda x: x[1]['xp'], reverse=True)
    for rank, (uid, stats) in enumerate(sorted_users, 1):
        if uid == str(user_id): return rank
    return len(users)

def format_number(num):
    """Format numbers like Arcane does (1.5K, 2.3M, etc.)"""
    if num >= 1000000:
        return f"{num/1000000:.1f}M"
    elif num >= 1000:
        return f"{num/1000:.1f}K"
    return str(num)

# --- HELPERS ---
def load_leaderboard(): return load_data()["roster"]
def save_leaderboard(roster_list): d=load_data(); d["roster"]=roster_list; save_data(d)
def save_theme(new_theme): d=load_data(); d["theme"].update(new_theme); save_data(d)
def get_rank(user_id):
    roster = load_leaderboard()
    return roster.index(user_id) + 1 if user_id in roster else None

def is_staff(user):
    if user.guild_permissions.administrator: return True
    user_role_names = [role.name for role in user.roles]
    if STAFF_ROLE_NAME in user_role_names: return True
    return any(role in user_role_names for role in HIGH_STAFF_ROLES)

def is_high_staff(user):
    if user.guild_permissions.administrator: return True
    user_role_names = [role.name for role in user.roles]
    return any(role in user_role_names for role in HIGH_STAFF_ROLES)

def check_role_hierarchy(member, allowed_roles_names):
    if member.guild_permissions.administrator: return True
    allowed_roles = []
    for r in member.guild.roles:
        if r.name in allowed_roles_names: allowed_roles.append(r)
    if not allowed_roles: return False 
    min_req_position = min([r.position for r in allowed_roles])
    return member.top_role.position >= min_req_position

async def log_action(guild, title, description, color=0x3498db):
    channel = discord.utils.get(guild.text_channels, name=LOG_CHANNEL_NAME)
    if channel:
        embed = discord.Embed(title=title, description=description, color=color, timestamp=datetime.datetime.now(datetime.timezone.utc))
        await channel.send(embed=embed)

async def post_result(guild, channel_name, title, description, color=0xF1C40F):
    channel = discord.utils.get(guild.text_channels, name=channel_name)
    if not channel: channel = discord.utils.get(guild.text_channels, name=LOG_CHANNEL_NAME)
    if channel:
        embed = discord.Embed(title=title, description=description, color=color, timestamp=discord.utils.utcnow())
        await channel.send(embed=embed)

# --- RANK LOGIC ---
def process_rank_update(winner_id, loser_id):
    data = load_leaderboard()
    w_idx = data.index(winner_id) if winner_id in data else -1
    l_idx = data.index(loser_id) if loser_id in data else -1
    updated = False
    if w_idx != -1 and l_idx != -1:
        if w_idx > l_idx: data.pop(w_idx); data.insert(l_idx, winner_id); updated = True
    elif w_idx == -1 and l_idx != -1:
        data.insert(l_idx, winner_id); data = data[:10]; updated = True
    if updated: save_leaderboard(data)
    return updated

# --- EMBED GENERATOR ---
def create_leaderboard_embed(guild):
    full_data = load_data()
    roster = full_data["roster"]
    theme = full_data["theme"]
    embed = discord.Embed(title=theme.get("title"), description=theme.get("description"), color=theme.get("color"))
    img_url = theme.get("image")
    if img_url: embed.set_image(url=img_url)
    roster_str = ""
    for index, user_id in enumerate(roster):
        rank = index + 1
        if user_id:
            member = guild.get_member(user_id)
            name = member.display_name if member else "Unknown"
            mention = member.mention if member else f"<@{user_id}>"
            roster_str += f"**Rank {rank}: ✝ {name} 🕊️**\n| {mention} |\n\n"
        else:
            roster_str += f"**Rank {rank}: ✝ VACANT ✝ 🕊️**\n\n"
    embed.add_field(name="🏆 EU ROSTER", value=roster_str, inline=False)
    embed.set_footer(text="Updated"); embed.timestamp = discord.utils.utcnow()
    return embed

# --- ARCANE-STYLE LEVEL CARD ---
async def generate_level_card_url(member, user_data, rank):
    """Generate a level card image using external API"""
    lvl = user_data['level']
    xp = user_data['xp']
    req = calculate_next_level_xp(lvl)
    progress = min(100, int((xp / req) * 100)) if req > 0 else 0
    
    # Get static avatar URL (not animated)
    avatar_url = member.display_avatar.with_format('png').with_size(256).url
    
    # Use vacefron API for rank card (free, no auth needed)
    # Or create custom HTML card
    
    # Build custom card using quickchart.io (supports custom HTML)
    username = member.display_name.replace(" ", "%20")
    
    # Alternative: Use a simpler approach with embed + custom formatting
    return None

def create_arcane_level_embed(member, user_data, rank):
    """Create a beautiful Fallen-themed level card embed"""
    lvl = user_data['level']
    xp = user_data['xp']
    req = calculate_next_level_xp(lvl)
    coins = user_data.get('coins', 0)
    roblox = user_data.get('roblox_username', None)
    
    progress_percent = min(100, int((xp / req) * 100)) if req > 0 else 0
    
    # Create visual progress bar with better characters
    bar_length = 12
    filled = int(bar_length * (progress_percent / 100))
    empty = bar_length - filled
    progress_bar = "🟥" * filled + "⬛" * empty
    
    # Dark red theme for Fallen
    embed = discord.Embed(color=0x8B0000)
    
    # Header with avatar
    embed.set_author(
        name=f"{member.display_name}",
        icon_url=member.display_avatar.url
    )
    
    # Main description with all stats formatted nicely
    embed.description = f"**@{member.name}**"
    
    # Stats in a clean layout
    embed.add_field(
        name="📊 LEVEL",
        value=f"**{lvl}**",
        inline=True
    )
    embed.add_field(
        name="🏆 RANK", 
        value=f"**#{rank}**",
        inline=True
    )
    embed.add_field(
        name="💰 COINS",
        value=f"**{coins:,}**",
        inline=True
    )
    
    # XP Progress
    embed.add_field(
        name=f"✨ XP Progress ({progress_percent}%)",
        value=f"{progress_bar}\n**{format_number(xp)}** / **{format_number(req)}**",
        inline=False
    )
    
    # Roblox if linked
    if roblox:
        embed.add_field(name="🎮 Roblox", value=f"**{roblox}**", inline=True)
    
    # Avatar on side
    embed.set_thumbnail(url=member.display_avatar.with_format('png').url)
    
    # Fallen banner at bottom
    if LEVEL_CARD_BACKGROUND:
        embed.set_image(url=LEVEL_CARD_BACKGROUND)
    
    # Footer
    embed.set_footer(text="✝ The Fallen ✝ • 落ちた")
    
    return embed
    return embed

async def create_level_card_image(member, user_data, rank):
    """Create a custom image-based level card with background"""
    if not PIL_AVAILABLE:
        print("PIL not available for level card")
        return None
    
    lvl = user_data['level']
    xp = user_data['xp']
    req = calculate_next_level_xp(lvl)
    progress = min(1.0, xp / req) if req > 0 else 0
    
    # Card dimensions
    width, height = 934, 282
    
    # Try to load custom background
    background = None
    
    # Check for local file in multiple locations (Discloud compatibility)
    for path in LEVEL_CARD_PATHS:
        if os.path.exists(path):
            try:
                background = Image.open(path).convert("RGBA")
                background = background.resize((width, height), Image.Resampling.LANCZOS)
                print(f"Loaded level background from: {path}")
                break
            except Exception as e:
                print(f"Failed to load {path}: {e}")
    
    # Check for URL if no local file found
    if background is None and LEVEL_CARD_BACKGROUND:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(LEVEL_CARD_BACKGROUND) as resp:
                    if resp.status == 200:
                        img_data = await resp.read()
                        background = Image.open(BytesIO(img_data)).convert("RGBA")
                        background = background.resize((width, height), Image.Resampling.LANCZOS)
                        print(f"Loaded level background from URL")
        except Exception as e:
            print(f"Failed to load background URL: {e}")
    
    # If still no background, return None to use embed fallback
    if background is None:
        print("No level background found, using embed fallback")
        return None
    
    # Create base card with background
    card = background.copy()
    # Add dark overlay for text readability
    overlay = Image.new("RGBA", (width, height), (0, 0, 0, 100))
    card = Image.alpha_composite(card, overlay)
    
    draw = ImageDraw.Draw(card)
    
    # Try to load fonts (fallback to default if not available)
    try:
        font_large = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf", 40)
        font_medium = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf", 28)
        font_small = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf", 22)
    except:
        font_large = ImageFont.load_default()
        font_medium = ImageFont.load_default()
        font_small = ImageFont.load_default()
    
    # Download and add avatar
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(str(member.display_avatar.url)) as resp:
                if resp.status == 200:
                    avatar_data = await resp.read()
                    avatar = Image.open(BytesIO(avatar_data)).convert("RGBA")
                    avatar = avatar.resize((180, 180), Image.Resampling.LANCZOS)
                    
                    # Create circular mask
                    mask = Image.new("L", (180, 180), 0)
                    mask_draw = ImageDraw.Draw(mask)
                    mask_draw.ellipse((0, 0, 180, 180), fill=255)
                    
                    # Add border
                    border_size = 186
                    border = Image.new("RGBA", (border_size, border_size), (0, 0, 0, 0))
                    border_draw = ImageDraw.Draw(border)
                    border_draw.ellipse((0, 0, border_size, border_size), fill=(255, 255, 255, 255))
                    
                    # Position avatar (left side)
                    card.paste(border, (47, 48), border)
                    card.paste(avatar, (50, 51), mask)
    except:
        pass
    
    # Username
    draw.text((260, 60), member.display_name, font=font_large, fill=(255, 255, 255))
    draw.text((260, 110), f"@{member.name}", font=font_small, fill=(180, 180, 180))
    
    # Stats
    draw.text((260, 160), f"LEVEL", font=font_small, fill=(150, 150, 150))
    draw.text((260, 185), str(lvl), font=font_large, fill=(255, 255, 255))
    
    draw.text((400, 160), f"RANK", font=font_small, fill=(150, 150, 150))
    draw.text((400, 185), f"#{rank}", font=font_large, fill=(255, 255, 255))
    
    draw.text((550, 160), f"XP", font=font_small, fill=(150, 150, 150))
    draw.text((550, 185), f"{format_number(xp)} / {format_number(req)}", font=font_medium, fill=(255, 255, 255))
    
    # Progress bar
    bar_x, bar_y = 260, 240
    bar_width, bar_height = 620, 25
    bar_radius = 12
    
    # Background bar
    draw.rounded_rectangle(
        [(bar_x, bar_y), (bar_x + bar_width, bar_y + bar_height)],
        radius=bar_radius,
        fill=(60, 60, 65)
    )
    
    # Progress fill
    fill_width = int(bar_width * progress)
    if fill_width > 0:
        draw.rounded_rectangle(
            [(bar_x, bar_y), (bar_x + fill_width, bar_y + bar_height)],
            radius=bar_radius,
            fill=(114, 137, 218)  # Discord blurple
        )
    
    # Progress percentage
    percent_text = f"{int(progress * 100)}%"
    draw.text((bar_x + bar_width - 60, bar_y + 2), percent_text, font=font_small, fill=(255, 255, 255))
    
    # Save to bytes
    output = BytesIO()
    card.save(output, format="PNG")
    output.seek(0)
    
    return output

# --- ARCANE-STYLE XP LEADERBOARD ---
def create_arcane_leaderboard_embed(guild, users_data, sort_key="xp", title_suffix="Overall XP"):
    """Create an Arcane-style leaderboard embed"""
    sorted_users = sorted(users_data.items(), key=lambda x: x[1].get(sort_key, 0), reverse=True)[:10]
    
    embed = discord.Embed(
        title=f"The Fallen | {guild.member_count}",
        color=0x2F3136
    )
    
    description_lines = []
    
    for i, (uid, stats) in enumerate(sorted_users, 1):
        member = guild.get_member(int(uid)) if guild else None
        username = f"@{member.name}" if member else f"@user_{uid[:8]}"
        lvl = stats.get('level', 0)
        xp_value = stats.get(sort_key, 0)
        
        if i == 1:
            rank_display = "🥇"
        elif i == 2:
            rank_display = "🥈"
        elif i == 3:
            rank_display = "🥉"
        else:
            rank_display = f"**#{i}**"
        
        # Show both level and XP for the selected category
        line = f"{rank_display} • {username} • LVL: {lvl} • {format_number(xp_value)} XP"
        description_lines.append(line)
    
    embed.description = "\n".join(description_lines) if description_lines else "No users found."
    
    # Add footer showing which leaderboard type
    embed.set_footer(text=f"📊 {title_suffix}")
    
    return embed

async def create_leaderboard_image(guild, users_data, sort_key="xp", title_suffix="Overall XP"):
    """Create a stylish image-based leaderboard with Fallen theme and avatars"""
    if not PIL_AVAILABLE:
        return None
    
    sorted_users = sorted(users_data.items(), key=lambda x: x[1].get(sort_key, 0), reverse=True)[:10]
    
    # Calculate height based on number of users
    num_users = len(sorted_users)
    row_height = 60  # Increased for avatars
    header_height = 140
    footer_height = 50
    height = header_height + (num_users * row_height) + footer_height
    width = 950  # Slightly wider for avatars
    
    # Try to load background
    background = None
    
    for path in LEVEL_CARD_PATHS:
        if os.path.exists(path):
            try:
                background = Image.open(path).convert("RGBA")
                break
            except:
                pass
    
    if background is None and LEVEL_CARD_BACKGROUND:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(LEVEL_CARD_BACKGROUND) as resp:
                    if resp.status == 200:
                        img_data = await resp.read()
                        background = Image.open(BytesIO(img_data)).convert("RGBA")
        except:
            pass
    
    if background is None:
        background = Image.new("RGBA", (width, height), (20, 20, 30, 255))
    else:
        background = background.resize((width, height), Image.Resampling.LANCZOS)
    
    card = background.copy()
    
    # Dark overlay
    overlay = Image.new("RGBA", (width, height), (0, 0, 0, 200))
    card = Image.alpha_composite(card, overlay)
    
    draw = ImageDraw.Draw(card)
    
    # Load fonts
    try:
        font_title = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf", 36)
        font_subtitle = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf", 18)
        font_header = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf", 14)
        font_name = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf", 18)
        font_stats = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf", 16)
        font_small = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf", 12)
    except:
        font_title = font_subtitle = font_header = font_name = font_stats = font_small = ImageFont.load_default()
    
    # === HEADER SECTION ===
    draw.rectangle([(20, 15), (width - 20, 20)], fill=(139, 0, 0))
    
    title = "LEADERBOARD"
    title_bbox = draw.textbbox((0, 0), title, font=font_title)
    title_width = title_bbox[2] - title_bbox[0]
    title_x = (width - title_width) // 2
    
    draw.text((title_x - 50, 35), "*", font=font_title, fill=(255, 215, 0))
    draw.text((title_x + title_width + 20, 35), "*", font=font_title, fill=(255, 215, 0))
    draw.text((title_x, 35), title, font=font_title, fill=(255, 255, 255))
    
    subtitle = f"{title_suffix} Rankings"
    sub_bbox = draw.textbbox((0, 0), subtitle, font=font_subtitle)
    sub_width = sub_bbox[2] - sub_bbox[0]
    draw.text(((width - sub_width) // 2, 80), subtitle, font=font_subtitle, fill=(200, 180, 180))
    
    # === COLUMN HEADERS ===
    header_y = 115
    draw.rectangle([(30, header_y - 5), (width - 30, header_y + 20)], fill=(60, 20, 20))
    
    draw.text((50, header_y), "RANK", font=font_header, fill=(200, 200, 200))
    draw.text((140, header_y), "USER", font=font_header, fill=(200, 200, 200))
    draw.text((520, header_y), "LEVEL", font=font_header, fill=(200, 200, 200))
    draw.text((640, header_y), "XP", font=font_header, fill=(200, 200, 200))
    draw.text((800, header_y), "COINS", font=font_header, fill=(200, 200, 200))
    
    # === LEADERBOARD ROWS ===
    y_start = header_y + 35
    avatar_size = 40
    
    rank_colors = {
        0: (255, 215, 0),    # Gold
        1: (192, 192, 192),  # Silver  
        2: (205, 127, 50),   # Bronze
    }
    
    # Download all avatars first
    avatars = {}
    async with aiohttp.ClientSession() as session:
        for uid, stats in sorted_users:
            member = guild.get_member(int(uid)) if guild else None
            if member:
                try:
                    avatar_url = member.display_avatar.with_format('png').with_size(64).url
                    async with session.get(avatar_url) as resp:
                        if resp.status == 200:
                            avatar_data = await resp.read()
                            avatar_img = Image.open(BytesIO(avatar_data)).convert("RGBA")
                            avatar_img = avatar_img.resize((avatar_size, avatar_size), Image.Resampling.LANCZOS)
                            avatars[uid] = avatar_img
                except:
                    pass
    
    for i, (uid, stats) in enumerate(sorted_users):
        member = guild.get_member(int(uid)) if guild else None
        username = member.display_name if member else f"User_{uid[:6]}"
        lvl = stats.get('level', 0)
        xp_value = stats.get(sort_key, 0)
        coins = stats.get('coins', 0)
        
        y = y_start + (i * row_height)
        
        # Row background
        if i < 3:
            row_color = (*rank_colors[i], 60)
        elif i % 2 == 0:
            row_color = (80, 20, 20, 150)
        else:
            row_color = (60, 15, 15, 150)
        
        row_bg = Image.new("RGBA", (width - 60, row_height - 5), row_color)
        card.paste(row_bg, (30, y), row_bg)
        draw = ImageDraw.Draw(card)
        
        # Left accent bar for top 3
        if i < 3:
            draw.rectangle([(30, y), (38, y + row_height - 5)], fill=rank_colors[i])
        
        # Rank number
        rank_color = rank_colors.get(i, (180, 180, 180))
        rank_text = f"#{i + 1}"
        draw.text((55, y + 17), rank_text, font=font_name, fill=rank_color)
        
        # Avatar with circular mask and border
        avatar_x = 120
        avatar_y = y + 7
        
        # Draw avatar border (diamond shape for top 3, circle for others)
        if i < 3:
            # Diamond border for top 3
            border_size = avatar_size // 2 + 5
            center_x = avatar_x + avatar_size // 2
            center_y = avatar_y + avatar_size // 2
            diamond_points = [
                (center_x, center_y - border_size),
                (center_x + border_size, center_y),
                (center_x, center_y + border_size),
                (center_x - border_size, center_y)
            ]
            draw.polygon(diamond_points, fill=rank_colors[i])
        else:
            # Circle border for others
            draw.ellipse(
                [avatar_x - 3, avatar_y - 3, avatar_x + avatar_size + 3, avatar_y + avatar_size + 3],
                fill=(100, 100, 100)
            )
        
        # Paste avatar with circular mask
        if uid in avatars:
            avatar_img = avatars[uid]
            
            # Create circular mask
            mask = Image.new("L", (avatar_size, avatar_size), 0)
            mask_draw = ImageDraw.Draw(mask)
            mask_draw.ellipse((0, 0, avatar_size, avatar_size), fill=255)
            
            # Paste avatar
            card.paste(avatar_img, (avatar_x, avatar_y), mask)
            draw = ImageDraw.Draw(card)
        else:
            # Draw placeholder circle
            draw.ellipse(
                [avatar_x, avatar_y, avatar_x + avatar_size, avatar_y + avatar_size],
                fill=(60, 60, 70)
            )
        
        # Username (shifted right for avatar)
        display_name = username[:18] + "..." if len(username) > 18 else username
        draw.text((180, y + 17), display_name, font=font_name, fill=(255, 255, 255))
        
        # Level
        draw.text((540, y + 17), str(lvl), font=font_stats, fill=(100, 200, 255))
        
        # XP
        draw.text((660, y + 17), format_number(xp_value), font=font_stats, fill=(255, 200, 100))
        
        # Coins
        draw.text((815, y + 17), format_number(coins), font=font_stats, fill=(255, 215, 0))
    
    # === FOOTER ===
    footer_y = height - 35
    draw.rectangle([(20, footer_y - 10), (width - 20, footer_y - 5)], fill=(139, 0, 0))
    
    footer = f"The Fallen | {guild.member_count} Members"
    footer_bbox = draw.textbbox((0, 0), footer, font=font_small)
    footer_width = footer_bbox[2] - footer_bbox[0]
    draw.text(((width - footer_width) // 2, footer_y), footer, font=font_small, fill=(150, 150, 150))
    
    # Save to bytes
    output = BytesIO()
    card.save(output, format="PNG")
    output.seek(0)
    
    return output

# --- LEVELING CHECKER ---
async def check_level_up(user_id, guild):
    data = load_data()
    uid = str(user_id)
    data = ensure_user_structure(data, uid) 
    user_data = data["users"][uid]
    
    xp = user_data["xp"]
    current_level = user_data["level"]
    
    next_milestone_level = None
    for lvl in sorted(LEVEL_CONFIG.keys()):
        if lvl > current_level:
            next_milestone_level = lvl
            break
            
    if not next_milestone_level: return 
    req_xp = LEVEL_CONFIG[next_milestone_level]["xp"]
    
    if xp >= req_xp:
        update_user_data(user_id, "level", next_milestone_level)
        reward_data = LEVEL_CONFIG[next_milestone_level]
        coins = reward_data["coins"]
        role_name = reward_data["role"]
        add_user_stat(user_id, "coins", coins)
        role_msg = ""
        member = guild.get_member(user_id)
        if member and role_name:
            role = discord.utils.get(guild.roles, name=role_name)
            if role:
                try: 
                    await member.add_roles(role)
                    role_msg = f"\n🎭 **Role Unlocked:** {role.mention}"
                except Exception as e:
                    print(f"Role assign error: {e}")
                    role_msg = "\n❌ Role assign failed (Hierarchy)."
        channel = discord.utils.get(guild.text_channels, name=LEVEL_UP_CHANNEL_NAME)
        if channel:
            embed = discord.Embed(title="✨ LEVEL UP!", description=f"<@{user_id}> has reached **Level {next_milestone_level}**!", color=0xDC143C)
            embed.add_field(name="Rewards", value=f"💰 +{coins} Fallen Coins{role_msg}")
            if member: embed.set_thumbnail(url=member.display_avatar.url)
            await channel.send(embed=embed)

# --- TOURNAMENT FUNCTIONS ---
async def generate_matchups(channel):
    players = tournament_state["players"].copy()
    random.shuffle(players)
    
    if len(players) % 2 == 1:
        bye_player = players.pop()
        tournament_state["next_round"].append(bye_player)
        await channel.send(f"<@{bye_player}> receives a **BYE** this round!")
    
    tournament_state["match_count"] = len(players) // 2
    tournament_state["finished_matches"] = 0
    
    match_num = 1
    for i in range(0, len(players), 2):
        p1, p2 = players[i], players[i + 1]
        embed = discord.Embed(
            title=f"⚔️ Match {match_num}",
            description=f"<@{p1}> 🆚 <@{p2}>",
            color=0xFF4500
        )
        await channel.send(embed=embed, view=MatchButtonView(p1, p2))
        match_num += 1

async def advance_round(interaction):
    channel = interaction.channel
    
    if len(tournament_state["next_round"]) == 1:
        winner_id = tournament_state["next_round"][0]
        embed = discord.Embed(
            title="🏆 TOURNAMENT COMPLETE!",
            description=f"**{tournament_state['title']}**\n\n🥇 **CHAMPION:** <@{winner_id}>",
            color=0xFFD700
        )
        
        if tournament_state["losers_stack"]:
            placements = ""
            if len(tournament_state["losers_stack"]) >= 1:
                placements += f"🥈 **2nd Place:** <@{tournament_state['losers_stack'][-1]}>\n"
            if len(tournament_state["losers_stack"]) >= 2:
                placements += f"🥉 **3rd Place:** <@{tournament_state['losers_stack'][-2]}>\n"
            if placements:
                embed.add_field(name="Placements", value=placements, inline=False)
        
        await channel.send(embed=embed)
        
        await post_result(
            interaction.guild, 
            TOURNAMENT_RESULTS_CHANNEL_NAME, 
            f"🏆 {tournament_state['title']} Results",
            f"**Champion:** <@{winner_id}>",
            0xFFD700
        )
        
        if tournament_state["ranked_mode"] and tournament_state["losers_stack"]:
            finalist = tournament_state["losers_stack"][-1]
            process_rank_update(winner_id, finalist)
        
        tournament_state["active"] = False
        tournament_state["players"] = []
        tournament_state["next_round"] = []
        tournament_state["losers_stack"] = []
        tournament_state["match_count"] = 0
        tournament_state["finished_matches"] = 0
        
    else:
        round_num = len(tournament_state["losers_stack"]) // max(1, tournament_state["match_count"]) + 1
        await channel.send(embed=discord.Embed(
            title=f"📢 Round {round_num} Complete!",
            description=f"Advancing {len(tournament_state['next_round'])} players to the next round...",
            color=0x3498db
        ))
        
        tournament_state["players"] = tournament_state["next_round"].copy()
        tournament_state["next_round"] = []
        tournament_state["finished_matches"] = 0
        
        await asyncio.sleep(2)
        await generate_matchups(channel)

# --------------------------
# --- UI VIEW CLASSES ---
# --------------------------

class TicketReasonModal(discord.ui.Modal, title="Closing Ticket"):
    reason = discord.ui.TextInput(label="Reason / Role Details", style=discord.TextStyle.paragraph, placeholder="e.g. Granted 'Crimson King' role hex #FF0000", required=True)
    
    async def on_submit(self, interaction: discord.Interaction):
        await log_action(interaction.guild, "🎫 Ticket Closed (Custom Role)", f"Closed by: {interaction.user.mention}\nDetails: {self.reason.value}", 0xe74c3c)
        await interaction.response.send_message("✅ Logged. Deleting channel in 3s...")
        await asyncio.sleep(3)
        await interaction.channel.delete()

class TicketControlView(discord.ui.View):
    def __init__(self, ticket_type):
        super().__init__(timeout=None)
        self.ticket_type = ticket_type

    @discord.ui.button(label="🔒 Close Ticket", style=discord.ButtonStyle.danger, custom_id="ticket_close_btn")
    async def close_ticket(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not is_staff(interaction.user):
            return await interaction.response.send_message("❌ Staff only.", ephemeral=True)

        if self.ticket_type == "role":
            await interaction.response.send_modal(TicketReasonModal())
        else:
            await log_action(interaction.guild, "🎫 Ticket Closed", f"Closed by: {interaction.user.mention}\nChannel: {interaction.channel.name}", 0xe74c3c)
            await interaction.response.send_message("🔒 Closing ticket in 3s...")
            await asyncio.sleep(3)
            await interaction.channel.delete()

# ==========================================
# SUPPORT TICKET SYSTEM (Separate from Applications)
# ==========================================

SUPPORT_TICKET_TYPES = {
    "support": {
        "name": "Support",
        "emoji": "🎫",
        "description": "General help & questions",
        "color": 0x3498db,
        "prefix": "support"
    },
    "report": {
        "name": "Report User",
        "emoji": "🚨",
        "description": "Report a rule breaker",
        "color": 0xe74c3c,
        "prefix": "report"
    },
    "suggestion": {
        "name": "Suggestion",
        "emoji": "💡",
        "description": "Submit an idea",
        "color": 0xf1c40f,
        "prefix": "suggest"
    }
}

# Store support tickets: {channel_id: {"type", "user_id", "created", "status", "claimed_by", "transcript"}}
support_tickets = {}

class SupportTicketPanelView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)
    
    @discord.ui.button(label="🎫 Support", style=discord.ButtonStyle.primary, custom_id="ticket_support_btn", row=0)
    async def support_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.create_ticket(interaction, "support")
    
    @discord.ui.button(label="🚨 Report", style=discord.ButtonStyle.danger, custom_id="ticket_report_btn", row=0)
    async def report_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.create_ticket(interaction, "report")
    
    @discord.ui.button(label="💡 Suggestion", style=discord.ButtonStyle.success, custom_id="ticket_suggestion_btn", row=0)
    async def suggestion_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.create_ticket(interaction, "suggestion")
    
    async def create_ticket(self, interaction: discord.Interaction, ticket_type: str):
        config = SUPPORT_TICKET_TYPES[ticket_type]
        
        # Check for existing open ticket
        for ch_id, ticket in support_tickets.items():
            if ticket["user_id"] == interaction.user.id and ticket["status"] == "open":
                existing_ch = interaction.guild.get_channel(ch_id)
                if existing_ch:
                    return await interaction.response.send_message(
                        f"❌ You already have an open ticket: {existing_ch.mention}",
                        ephemeral=True
                    )
        
        await interaction.response.defer(ephemeral=True)
        
        # Create channel overwrites
        overwrites = {
            interaction.guild.default_role: discord.PermissionOverwrite(read_messages=False),
            interaction.user: discord.PermissionOverwrite(read_messages=True, send_messages=True, attach_files=True, embed_links=True),
            interaction.guild.me: discord.PermissionOverwrite(read_messages=True, send_messages=True, manage_channels=True)
        }
        
        # Add staff access
        staff_role = discord.utils.get(interaction.guild.roles, name=STAFF_ROLE_NAME)
        if staff_role:
            overwrites[staff_role] = discord.PermissionOverwrite(read_messages=True, send_messages=True)
        
        for role_name in HIGH_STAFF_ROLES:
            role = discord.utils.get(interaction.guild.roles, name=role_name)
            if role:
                overwrites[role] = discord.PermissionOverwrite(read_messages=True, send_messages=True)
        
        # Get or create category
        cat = discord.utils.get(interaction.guild.categories, name="Support Tickets")
        if not cat:
            cat = await interaction.guild.create_category("Support Tickets", overwrites={
                interaction.guild.default_role: discord.PermissionOverwrite(read_messages=False)
            })
        
        # Create channel
        channel = await interaction.guild.create_text_channel(
            name=f"{config['prefix']}-{interaction.user.name}",
            category=cat,
            overwrites=overwrites
        )
        
        # Store ticket data
        support_tickets[channel.id] = {
            "type": ticket_type,
            "user_id": interaction.user.id,
            "created": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            "status": "open",
            "claimed_by": None,
            "transcript": []
        }
        
        # Create ticket embed
        embed = discord.Embed(
            title=f"{config['emoji']} {config['name']} Ticket",
            description=f"Thank you for creating a ticket, {interaction.user.mention}!\n\nA staff member will assist you shortly.",
            color=config["color"],
            timestamp=datetime.datetime.now(datetime.timezone.utc)
        )
        embed.add_field(name="👤 Created By", value=interaction.user.mention, inline=True)
        embed.add_field(name="📁 Type", value=config["name"], inline=True)
        embed.add_field(name="📊 Status", value="🟢 Open", inline=True)
        embed.set_footer(text=f"Ticket ID: {channel.id}")
        
        # Type-specific instructions
        if ticket_type == "support":
            embed.add_field(
                name="📝 How can we help?",
                value="Please describe your issue or question in detail.",
                inline=False
            )
        elif ticket_type == "report":
            embed.add_field(
                name="📝 Report Information",
                value=(
                    "Please provide:\n"
                    "• **Who** are you reporting?\n"
                    "• **What** did they do?\n"
                    "• **When** did it happen?\n"
                    "• **Evidence** (screenshots if possible)"
                ),
                inline=False
            )
        elif ticket_type == "suggestion":
            embed.add_field(
                name="📝 Suggestion Details",
                value=(
                    "Please describe:\n"
                    "• **What** is your idea?\n"
                    "• **Why** would it benefit the server?\n"
                    "• **How** should it work?"
                ),
                inline=False
            )
        
        await channel.send(
            content=f"{interaction.user.mention} | {staff_role.mention if staff_role else 'Staff'}",
            embed=embed,
            view=SupportTicketActionsView(ticket_type, interaction.user.id)
        )
        
        await interaction.followup.send(f"✅ Ticket created! Go to {channel.mention}", ephemeral=True)
        await log_action(interaction.guild, f"{config['emoji']} Ticket Opened", f"**Type:** {config['name']}\n**User:** {interaction.user.mention}\n**Channel:** {channel.mention}", config["color"])

class SupportTicketActionsView(discord.ui.View):
    def __init__(self, ticket_type: str, creator_id: int):
        super().__init__(timeout=None)
        self.ticket_type = ticket_type
        self.creator_id = creator_id
    
    @discord.ui.button(label="🔒 Close", style=discord.ButtonStyle.danger, custom_id="sticket_close")
    async def close_ticket(self, interaction: discord.Interaction, button: discord.ui.Button):
        # Both staff and ticket creator can close
        if not is_staff(interaction.user) and interaction.user.id != self.creator_id:
            return await interaction.response.send_message("❌ You can't close this ticket.", ephemeral=True)
        
        await interaction.response.send_message(
            "Are you sure you want to close this ticket?",
            view=ConfirmCloseView(self.ticket_type, self.creator_id),
            ephemeral=True
        )
    
    @discord.ui.button(label="🙋 Claim", style=discord.ButtonStyle.success, custom_id="sticket_claim")
    async def claim_ticket(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not is_staff(interaction.user):
            return await interaction.response.send_message("❌ Staff only.", ephemeral=True)
        
        if interaction.channel.id in support_tickets:
            ticket = support_tickets[interaction.channel.id]
            if ticket["claimed_by"]:
                claimer = interaction.guild.get_member(ticket["claimed_by"])
                return await interaction.response.send_message(
                    f"❌ Already claimed by {claimer.mention if claimer else 'someone'}.",
                    ephemeral=True
                )
            
            ticket["claimed_by"] = interaction.user.id
            
            embed = discord.Embed(
                title="🙋 Ticket Claimed",
                description=f"This ticket is now being handled by {interaction.user.mention}",
                color=0x2ecc71
            )
            await interaction.response.send_message(embed=embed)
            
            # Update original embed
            if interaction.message.embeds:
                original_embed = interaction.message.embeds[0]
                for i, field in enumerate(original_embed.fields):
                    if field.name == "📊 Status":
                        original_embed.set_field_at(i, name="📊 Status", value=f"🟡 Claimed by {interaction.user.display_name}", inline=True)
                        break
                await interaction.message.edit(embed=original_embed)
    
    @discord.ui.button(label="📌 Add User", style=discord.ButtonStyle.secondary, custom_id="sticket_adduser")
    async def add_user(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not is_staff(interaction.user):
            return await interaction.response.send_message("❌ Staff only.", ephemeral=True)
        await interaction.response.send_modal(AddUserToTicketModal())

class ConfirmCloseView(discord.ui.View):
    def __init__(self, ticket_type: str, creator_id: int):
        super().__init__(timeout=60)
        self.ticket_type = ticket_type
        self.creator_id = creator_id
    
    @discord.ui.button(label="✅ Yes, Close", style=discord.ButtonStyle.danger)
    async def confirm_close(self, interaction: discord.Interaction, button: discord.ui.Button):
        config = SUPPORT_TICKET_TYPES.get(self.ticket_type, SUPPORT_TICKET_TYPES["support"])
        
        # Generate transcript
        transcript = []
        async for msg in interaction.channel.history(limit=100, oldest_first=True):
            if not msg.author.bot or msg.embeds:
                transcript.append(f"[{msg.created_at.strftime('%Y-%m-%d %H:%M')}] {msg.author.name}: {msg.content or '[Embed/Attachment]'}")
        
        # Update ticket status
        if interaction.channel.id in support_tickets:
            support_tickets[interaction.channel.id]["status"] = "closed"
            support_tickets[interaction.channel.id]["transcript"] = transcript
        
        # DM creator with transcript summary
        creator = interaction.guild.get_member(self.creator_id)
        if creator:
            try:
                dm_embed = discord.Embed(
                    title=f"{config['emoji']} Ticket Closed",
                    description=f"Your **{config['name']}** ticket has been closed.\n\nThank you for contacting us!",
                    color=config["color"]
                )
                dm_embed.add_field(name="Server", value=interaction.guild.name, inline=True)
                dm_embed.add_field(name="Closed By", value=interaction.user.display_name, inline=True)
                await creator.send(embed=dm_embed)
            except:
                pass
        
        await log_action(
            interaction.guild,
            f"{config['emoji']} Ticket Closed",
            f"**Type:** {config['name']}\n**User:** <@{self.creator_id}>\n**Closed By:** {interaction.user.mention}\n**Messages:** {len(transcript)}",
            0xe74c3c
        )
        
        await interaction.response.edit_message(content="🔒 Closing ticket in 5 seconds...", view=None)
        await asyncio.sleep(5)
        await interaction.channel.delete()
    
    @discord.ui.button(label="❌ Cancel", style=discord.ButtonStyle.secondary)
    async def cancel_close(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.edit_message(content="❌ Cancelled.", view=None)

class AddUserToTicketModal(discord.ui.Modal, title="Add User to Ticket"):
    user_id = discord.ui.TextInput(
        label="User ID or @mention",
        placeholder="Enter user ID (e.g., 123456789012345678)",
        max_length=30
    )
    
    async def on_submit(self, interaction: discord.Interaction):
        # Parse user ID
        user_id_str = self.user_id.value.strip().replace("<@", "").replace(">", "").replace("!", "")
        try:
            user_id = int(user_id_str)
            member = interaction.guild.get_member(user_id)
            if not member:
                return await interaction.response.send_message("❌ User not found in this server.", ephemeral=True)
            
            # Add user to channel
            await interaction.channel.set_permissions(member, read_messages=True, send_messages=True)
            
            embed = discord.Embed(
                title="👤 User Added",
                description=f"{member.mention} has been added to this ticket by {interaction.user.mention}",
                color=0x3498db
            )
            await interaction.response.send_message(embed=embed)
        except ValueError:
            await interaction.response.send_message("❌ Invalid user ID.", ephemeral=True)

# ==========================================
# ADVANCED STATS SYSTEM
# ==========================================

def get_server_stats(guild):
    """Calculate comprehensive server statistics"""
    data = load_data()
    users = data.get("users", {})
    now = datetime.datetime.now(datetime.timezone.utc)
    
    stats = {
        "total_members": guild.member_count,
        "online_members": sum(1 for m in guild.members if m.status != discord.Status.offline),
        "bot_count": sum(1 for m in guild.members if m.bot),
        "total_xp": sum(u.get("xp", 0) for u in users.values()),
        "total_coins": sum(u.get("coins", 0) for u in users.values()),
        "total_messages": len(users),  # Approximation
        "active_today": 0,
        "active_week": 0,
        "active_month": 0,
        "top_level": 0,
        "avg_level": 0,
        "total_wins": sum(u.get("wins", 0) for u in users.values()),
        "total_losses": sum(u.get("losses", 0) for u in users.values()),
        "total_raids": sum(u.get("raid_participation", 0) for u in users.values()),
        "total_trainings": sum(u.get("training_attendance", 0) for u in users.values()),
    }
    
    levels = []
    for uid, udata in users.items():
        levels.append(udata.get("level", 0))
        
        last_active = udata.get("last_active")
        if last_active:
            try:
                last_dt = datetime.datetime.fromisoformat(last_active)
                if last_dt.tzinfo is None:
                    last_dt = last_dt.replace(tzinfo=datetime.timezone.utc)
                diff = (now - last_dt).days
                if diff < 1:
                    stats["active_today"] += 1
                if diff < 7:
                    stats["active_week"] += 1
                if diff < 30:
                    stats["active_month"] += 1
            except:
                pass
    
    if levels:
        stats["top_level"] = max(levels)
        stats["avg_level"] = round(sum(levels) / len(levels), 1)
    
    return stats

def get_user_activity_stats(user_id):
    """Get detailed activity stats for a user"""
    data = get_user_data(user_id)
    
    return {
        "level": data.get("level", 0),
        "xp": data.get("xp", 0),
        "coins": data.get("coins", 0),
        "weekly_xp": data.get("weekly_xp", 0),
        "monthly_xp": data.get("monthly_xp", 0),
        "wins": data.get("wins", 0),
        "losses": data.get("losses", 0),
        "raid_wins": data.get("raid_wins", 0),
        "raid_losses": data.get("raid_losses", 0),
        "raid_participation": data.get("raid_participation", 0),
        "training_attendance": data.get("training_attendance", 0),
        "daily_streak": data.get("daily_streak", 0),
        "warnings": len(data.get("warnings", [])),
        "roblox": data.get("roblox_username", "Not linked"),
        "last_active": data.get("last_active"),
    }

def get_top_active_users(guild, days=7, limit=10):
    """Get most active users in the past X days based on XP gains"""
    data = load_data()
    users = data.get("users", {})
    
    # Use weekly_xp for 7 days, monthly_xp for 30 days
    sort_key = "weekly_xp" if days <= 7 else "monthly_xp"
    
    sorted_users = sorted(
        [(uid, udata.get(sort_key, 0)) for uid, udata in users.items()],
        key=lambda x: x[1],
        reverse=True
    )[:limit]
    
    result = []
    for uid, xp in sorted_users:
        member = guild.get_member(int(uid))
        if member and xp > 0:
            result.append((member, xp))
    
    return result

def get_activity_by_hour(guild):
    """Analyze when members are most active (simplified)"""
    # This would need message tracking over time for accuracy
    # For now, return current online distribution
    online_count = sum(1 for m in guild.members if m.status == discord.Status.online)
    idle_count = sum(1 for m in guild.members if m.status == discord.Status.idle)
    dnd_count = sum(1 for m in guild.members if m.status == discord.Status.dnd)
    offline_count = sum(1 for m in guild.members if m.status == discord.Status.offline)
    
    return {
        "online": online_count,
        "idle": idle_count,
        "dnd": dnd_count,
        "offline": offline_count
    }

class LeaderboardSelect(discord.ui.Select):
    def __init__(self, current_selection="xp"):
        options = [
            discord.SelectOption(label="Overall XP", emoji="🌎", value="xp", default=(current_selection == "xp")),
            discord.SelectOption(label="Monthly XP", emoji="📅", value="monthly_xp", default=(current_selection == "monthly_xp")),
            discord.SelectOption(label="Weekly XP", emoji="📆", value="weekly_xp", default=(current_selection == "weekly_xp"))
        ]
        super().__init__(placeholder="Overall XP", min_values=1, max_values=1, options=options)

    async def callback(self, interaction: discord.Interaction):
        sort_key = self.values[0]
        users = load_data()["users"]
        title_map = {"xp": "Overall XP", "monthly_xp": "Monthly XP", "weekly_xp": "Weekly XP"}
        title = title_map[sort_key]
        
        # Try to create image leaderboard
        if PIL_AVAILABLE:
            try:
                lb_image = await create_leaderboard_image(interaction.guild, users, sort_key, title)
                if lb_image:
                    file = discord.File(lb_image, filename="leaderboard.png")
                    # Need to remove old attachments and add new one
                    await interaction.response.edit_message(attachments=[file], embed=None, view=LeaderboardViewUI(default_val=sort_key))
                    return
            except Exception as e:
                print(f"Leaderboard select image error: {e}")
        
        # Fallback to embed
        embed = create_arcane_leaderboard_embed(
            interaction.guild, 
            users, 
            sort_key=sort_key,
            title_suffix=title
        )
        
        await interaction.response.edit_message(embed=embed, attachments=[], view=LeaderboardViewUI(default_val=sort_key))

class LeaderboardViewUI(discord.ui.View):
    def __init__(self, default_val="xp"):
        super().__init__(timeout=180)
        self.add_item(LeaderboardSelect(current_selection=default_val))
    
    @discord.ui.button(label="View leaderboard", style=discord.ButtonStyle.secondary, emoji="↗️")
    async def view_full(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_message("Full leaderboard coming soon!", ephemeral=True)

class ShopView(discord.ui.View):
    def __init__(self): 
        super().__init__(timeout=None)

    async def buy_item(self, interaction: discord.Interaction, item_id: str):
        item = next((i for i in SHOP_ITEMS if i["id"] == item_id), None)
        if not item: 
            return await interaction.response.send_message("❌ Item not found.", ephemeral=True)
        
        user_data = get_user_data(interaction.user.id)
        if user_data["coins"] < item["price"]:
            return await interaction.response.send_message(f"❌ **Insufficient Funds.**\nYou need {item['price']} coins, you have {user_data['coins']}.", ephemeral=True)
        
        add_user_stat(interaction.user.id, "coins", -item["price"])
        
        ticket_type = "tryout" if item_id == "private_tryout" else "role"
        title = "⚔️ Private Tryout" if ticket_type == "tryout" else "🎨 Custom Role Request"
        await self.open_shop_ticket(interaction, ticket_type, title)

    async def open_shop_ticket(self, interaction: discord.Interaction, prefix: str, title: str):
        overwrites = {
            interaction.guild.default_role: discord.PermissionOverwrite(read_messages=False), 
            interaction.user: discord.PermissionOverwrite(read_messages=True, send_messages=True), 
            interaction.guild.me: discord.PermissionOverwrite(read_messages=True)
        }
        staff = discord.utils.get(interaction.guild.roles, name=STAFF_ROLE_NAME)
        if staff: 
            overwrites[staff] = discord.PermissionOverwrite(read_messages=True, send_messages=True)
        
        cat = discord.utils.get(interaction.guild.categories, name="Purchases")
        if not cat:
            cat = await interaction.guild.create_category("Purchases")
        
        ch = await interaction.guild.create_text_channel(
            name=f"{prefix}-{interaction.user.name}", 
            category=cat, 
            overwrites=overwrites
        )
        
        embed = discord.Embed(
            title=title, 
            description=f"Purchase confirmed by {interaction.user.mention}.\nStaff will assist you shortly.", 
            color=0x2ecc71
        )
        await ch.send(f"{staff.mention if staff else ''}", embed=embed, view=TicketControlView(prefix))
        await interaction.response.send_message(f"✅ **Purchased!** Check {ch.mention}", ephemeral=True)
        await log_action(interaction.guild, "🛒 Purchase", f"User: {interaction.user.mention}\nItem: {title}", 0xF1C40F)

    @discord.ui.button(label="Buy Private Tryout (500 💰)", style=discord.ButtonStyle.primary, custom_id="shop_tryout")
    async def buy_tryout(self, interaction: discord.Interaction, button: discord.ui.Button): 
        await self.buy_item(interaction, "private_tryout")
    
    @discord.ui.button(label="Buy Custom Role (2000 💰)", style=discord.ButtonStyle.secondary, custom_id="shop_role")
    async def buy_role(self, interaction: discord.Interaction, button: discord.ui.Button): 
        await self.buy_item(interaction, "custom_role")

class HelpSelect(discord.ui.Select):
    def __init__(self):
        options = [
            discord.SelectOption(label="Member", emoji="👤", description="Basic commands"),
            discord.SelectOption(label="Economy", emoji="💰", description="Coins & rewards"),
            discord.SelectOption(label="Tickets", emoji="🎫", description="Support tickets"),
            discord.SelectOption(label="Stats", emoji="📊", description="Server & user stats"),
            discord.SelectOption(label="Applications", emoji="📋", description="Staff applications"),
            discord.SelectOption(label="Raids & Wars", emoji="🏴‍☠️", description="Clan battles"),
            discord.SelectOption(label="Training", emoji="📚", description="Events & tryouts"),
            discord.SelectOption(label="Staff", emoji="🛡️", description="Moderation"),
            discord.SelectOption(label="Admin", emoji="⚙️", description="Setup & management"),
        ]
        super().__init__(placeholder="Select a category...", min_values=1, max_values=1, options=options)
    
    async def callback(self, interaction: discord.Interaction):
        e = discord.Embed(color=0x3498db)
        if self.values[0] == "Member": 
            e.title="👤 Member Commands"
            e.description=(
                "**All commands work with both `!` and `/`**\n\n"
                "**🔗 Secure Verification:**\n"
                "`verify` - Verify with Roblox (code in profile)\n"
                "`link_roblox` - Re-verify with different account\n"
                "`update_roblox` - Update via popup form\n\n"
                "**📊 Stats & Profile:**\n"
                "`level [@user]` - Check level & XP\n"
                "`leaderboard` / `lb` - View XP rankings\n"
                "`profile [@user]` - View full profile\n"
                "`stats [@user]` - View combat stats (W/L)\n"
                "`fcoins` - Check your coin balance\n\n"
                "**🎁 Rewards:**\n"
                "`daily` - Claim daily reward\n"
                "`schedule` - View upcoming events\n"
                "`help` - Show this help menu"
            )
        elif self.values[0] == "Economy": 
            e.title="💰 Economy Commands"
            e.description=(
                "**Earning Coins & XP:**\n"
                "• Chat and be active\n"
                "• Join voice channels\n"
                "• Claim daily rewards (`daily`)\n"
                "• Level up milestones\n"
                "• Attend trainings\n"
                "• Participate in raids\n\n"
                "**Spending Coins:**\n"
                "• Visit the shop channel\n"
                "• Buy tryouts & custom roles\n\n"
                "`fcoins` - Check your balance\n"
                "`daily` - Claim daily (streak bonus!)"
            )
        elif self.values[0] == "Tickets":
            e.title="🎫 Support Ticket System"
            e.description=(
                "**Create a Ticket:**\n"
                "Go to the tickets channel and click:\n"
                "• 🎫 **Support** - General help & questions\n"
                "• 🚨 **Report** - Report a rule breaker\n"
                "• 💡 **Suggestion** - Submit an idea\n\n"
                "**Commands:**\n"
                "`close_ticket` - Close your current ticket\n\n"
                "**How it Works:**\n"
                "1. Click a button to create a ticket\n"
                "2. A private channel is created\n"
                "3. Describe your issue\n"
                "4. Staff will assist you\n"
                "5. Ticket is closed when resolved\n\n"
                "*You can only have one open ticket at a time*"
            )
        elif self.values[0] == "Stats":
            e.title="📊 Statistics Commands"
            e.description=(
                "**Personal Stats:**\n"
                "`mystats [@user]` - Detailed activity stats\n"
                "`profile [@user]` - View full profile\n"
                "`stats [@user]` - Combat W/L stats\n"
                "`compare @user` - Compare with someone\n\n"
                "**Server Stats:**\n"
                "`serverstats` - Server-wide statistics\n"
                "`topactive [7/30]` - Most active members\n"
                "`leaderboards` - View all leaderboards\n\n"
                "**Leaderboards:**\n"
                "`leaderboard` - XP rankings\n"
                "`raid_lb` - Raid leaderboard"
            )
        elif self.values[0] == "Applications":
            e.title="📋 Application System"
            e.description=(
                "**Apply for Staff Positions:**\n"
                "🎯 **Tryout Host** - Host member tryouts\n"
                "🛡️ **Moderator** - Moderate the server\n"
                "📚 **Training Host** - Host trainings\n\n"
                "**Commands:**\n"
                "`app_status` - Check your application status\n\n"
                "**How to Apply:**\n"
                "1. Go to the applications channel\n"
                "2. Click 'Apply Now'\n"
                "3. Select position & check requirements\n"
                "4. Fill out the form\n"
                "5. Wait for staff votes!\n\n"
                "**Requirements Checked:**\n"
                "• Minimum level\n"
                "• Days in server\n"
                "• Warning count\n"
                "• Verified status\n"
                "• Cooldown (if reapplying)"
            )
        elif self.values[0] == "Raids & Wars":
            e.title="🏴‍☠️ Raids & Wars Commands"
            e.description=(
                "**Member Commands:**\n"
                "`raid_lb` - Raid leaderboard\n"
                "`wars` - View all clan wars\n"
                "`war_record <clan>` - Record vs clan\n\n"
                "**Staff Commands:**\n"
                "`raid_call <target> <time> [reqs]` - Call raid\n"
                "`raid_log <win/loss> @users` - Log result\n"
                "`war_declare <clan>` - Declare war\n"
                "`war_result <clan> <win/loss>` - Log war\n"
                "`scrim <opponent> <time>` - Schedule scrim"
            )
        elif self.values[0] == "Training":
            e.title="📚 Training & Tryouts"
            e.description=(
                "**Member Commands:**\n"
                "`schedule` - View upcoming events\n\n"
                "**Staff Commands:**\n"
                "`schedule_training <type> <time>` - Schedule training\n"
                "`training_log @users` - Log attendance (+50 XP)\n"
                "`schedule_tryout <type> <time>` - Schedule tryout\n"
                "`tryout_result @user <pass/fail>` - Log result"
            )
        elif self.values[0] == "Staff": 
            e.title="🛡️ Staff Commands"
            e.description=(
                "**XP Management:**\n"
                "`addxp @user amount` - Add XP\n"
                "`removexp @user amount` - Remove XP\n"
                "`levelchange @user level` - Set level\n"
                "`checklevel @user` - View user stats\n\n"
                "**Economy:**\n"
                "`addfcoins @user amount` - Add coins\n"
                "`removefcoins @user amount` - Remove coins\n\n"
                "**Matches:**\n"
                "`report_set @winner @loser` - Report result\n"
                "`tstart [title]` - Start tournament\n\n"
                "**Moderation:**\n"
                "`warn @user [reason]` - Warn user\n"
                "`warnings @user` - View warnings\n"
                "`clearwarnings @user` - Clear warnings\n"
                "`promote @user @role` - Give role\n"
                "`demote @user @role` - Remove role\n"
                "`announce #channel message` - Announcement"
            )
        elif self.values[0] == "Admin":
            e.title="⚙️ Admin Commands"
            e.description=(
                "**Setup Panels:**\n"
                "`setup_verify [#channel]` - Verification panel\n"
                "`setup_tickets [#channel]` - Support tickets panel\n"
                "`setup_shop` - Shop panel\n"
                "`setup_roster` - Roster panel\n"
                "`ticket_panel` - Challenge panel\n"
                "`apply_panel` - Application panel\n\n"
                "**Roster Management:**\n"
                "`roster_add @user <1-10>` - Add to roster\n"
                "`roster_remove <1-10>` - Clear position\n\n"
                "**Maintenance:**\n"
                "`inactive_check [days]` - Find inactive\n"
                "`reset_weekly` - Reset weekly XP\n"
                "`reset_monthly` - Reset monthly XP\n"
                "`wipedata` - Reset ALL data (Owner)\n"
                "`!sync` - Sync slash commands"
            )
        await interaction.response.edit_message(embed=e)

class HelpView(discord.ui.View):
    def __init__(self): 
        super().__init__(timeout=180)
        self.add_item(HelpSelect())

class ChallengeModal(discord.ui.Modal, title="Challenge Request"):
    claimed_rank = discord.ui.TextInput(label="Your Rank", max_length=5)
    opponent_name = discord.ui.TextInput(label="Opponent Username", max_length=32)
    
    async def on_submit(self, interaction: discord.Interaction):
        guild, user = interaction.guild, interaction.user
        opponent = discord.utils.get(guild.members, name=self.opponent_name.value)
        if not opponent: 
            return await interaction.response.send_message("❌ User not found. Make sure you typed their exact username.", ephemeral=True)
        
        my_rank = get_rank(user.id)
        opp_rank = get_rank(opponent.id)
        
        if not my_rank or not opp_rank: 
            return await interaction.response.send_message("❌ Both players must be on the leaderboard.", ephemeral=True)
        if str(my_rank) != self.claimed_rank.value.strip(): 
            return await interaction.response.send_message(f"❌ Rank mismatch. Your actual rank is {my_rank}.", ephemeral=True)
        if (my_rank - opp_rank) != 1: 
            return await interaction.response.send_message(f"❌ You can only challenge Rank {my_rank - 1}.", ephemeral=True)
        
        overwrites = {
            guild.default_role: discord.PermissionOverwrite(read_messages=False), 
            user: discord.PermissionOverwrite(read_messages=True, send_messages=True), 
            guild.me: discord.PermissionOverwrite(read_messages=True)
        }
        staff = discord.utils.get(guild.roles, name=STAFF_ROLE_NAME)
        if staff: 
            overwrites[staff] = discord.PermissionOverwrite(read_messages=True, send_messages=True)
        
        cat = discord.utils.get(guild.categories, name="Challenges")
        if not cat:
            cat = await guild.create_category("Challenges")
        
        ch = await guild.create_text_channel(
            name=f"chal-{my_rank}-vs-{opp_rank}", 
            category=cat, 
            overwrites=overwrites
        )
        
        embed = discord.Embed(
            title="⚔️ Challenge Request", 
            description=f"{user.mention} (Rank {my_rank}) vs {opponent.mention} (Rank {opp_rank})", 
            color=0xE74C3C
        )
        await ch.send(f"{staff.mention if staff else ''}", embed=embed, view=StaffApprovalView(user, opponent))
        await interaction.response.send_message(f"✅ Challenge ticket created: {ch.mention}", ephemeral=True)

class StaffApprovalView(discord.ui.View):
    def __init__(self, challenger=None, opponent=None): 
        super().__init__(timeout=None)
        self.challenger = challenger
        self.opponent = opponent
    
    @discord.ui.button(label="✅ Approve", style=discord.ButtonStyle.success, custom_id="s_app")
    async def approve(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not is_staff(interaction.user):
            return await interaction.response.send_message("❌ Staff only.", ephemeral=True)
        await interaction.response.send_modal(MatchDetailsModal(self.challenger, self.opponent))
    
    @discord.ui.button(label="❌ Deny", style=discord.ButtonStyle.danger, custom_id="s_deny")
    async def deny(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not is_staff(interaction.user):
            return await interaction.response.send_message("❌ Staff only.", ephemeral=True)
        await interaction.response.send_message("🔒 Challenge denied. Closing in 3s...")
        await asyncio.sleep(3)
        await interaction.channel.delete()

class MatchDetailsModal(discord.ui.Modal, title="Finalize Match"):
    match_time = discord.ui.TextInput(label="Match Time", placeholder="e.g. Today 8PM EST")
    referee = discord.ui.TextInput(label="Referee", placeholder="Who will referee?")
    
    def __init__(self, challenger, opponent): 
        super().__init__()
        self.challenger = challenger
        self.opponent = opponent
    
    async def on_submit(self, interaction: discord.Interaction):
        ch = discord.utils.get(interaction.guild.text_channels, name=ANNOUNCEMENT_CHANNEL_NAME)
        if not ch: 
            return await interaction.response.send_message("❌ Announcement channel not found.", ephemeral=True)
        
        role = discord.utils.get(interaction.guild.roles, name=ANNOUNCEMENT_ROLE_NAME)
        embed = discord.Embed(
            title="🔥 OFFICIAL MATCH ANNOUNCEMENT", 
            description=f"**{self.challenger.mention}** 🆚 **{self.opponent.mention}**", 
            color=0xFF4500
        )
        embed.add_field(name="📅 Time", value=self.match_time.value, inline=True)
        embed.add_field(name="👮 Referee", value=self.referee.value, inline=True)
        
        await ch.send(content=role.mention if role else "", embed=embed, view=MatchAnnouncementView())
        await interaction.response.send_message("✅ Match announced! Closing ticket in 3s...")
        await asyncio.sleep(3)
        await interaction.channel.delete()

class MatchAnnouncementView(discord.ui.View):
    def __init__(self): 
        super().__init__(timeout=None)
    
    @discord.ui.button(label="📊 Report Result (High Staff)", style=discord.ButtonStyle.success, custom_id="res_btn")
    async def report(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not is_high_staff(interaction.user): 
            return await interaction.response.send_message("❌ High Staff only.", ephemeral=True)
        
        ids = re.findall(r'<@!?(\d+)>', interaction.message.embeds[0].description)
        if len(ids) >= 2:
            p1 = interaction.guild.get_member(int(ids[0]))
            p2 = interaction.guild.get_member(int(ids[1]))
            if p1 and p2:
                await interaction.response.send_message("Who won?", view=MatchResultSelectView(p1, p2, interaction), ephemeral=True)
            else:
                await interaction.response.send_message("❌ Could not find players.", ephemeral=True)
        else:
            await interaction.response.send_message("❌ Could not parse players from announcement.", ephemeral=True)

class MatchResultSelectView(discord.ui.View):
    def __init__(self, p1, p2, origin): 
        super().__init__(timeout=60)
        self.p1 = p1
        self.p2 = p2
        self.origin = origin
    
    @discord.ui.button(label="P1 Won", style=discord.ButtonStyle.primary)
    async def w1(self, interaction: discord.Interaction, button: discord.ui.Button): 
        await self.finalize(interaction, self.p1, self.p2)
    
    @discord.ui.button(label="P2 Won", style=discord.ButtonStyle.secondary)
    async def w2(self, interaction: discord.Interaction, button: discord.ui.Button): 
        await self.finalize(interaction, self.p2, self.p1)
    
    async def finalize(self, interaction: discord.Interaction, winner, loser):
        changed = process_rank_update(winner.id, loser.id)
        desc = f"🏆 **Winner:** {winner.mention}\n💀 **Loser:** {loser.mention}" + ("\n🚨 **RANK SWAP!**" if changed else "")
        await post_result(interaction.guild, SET_RESULTS_CHANNEL_NAME, "⚔️ Set Result", desc)
        
        try: 
            original_embed = self.origin.message.embeds[0]
            original_embed.add_field(name="📊 Result", value=f"Winner: {winner.mention}", inline=False)
            original_embed.color = 0x2ecc71
            await self.origin.message.edit(embed=original_embed, view=None)
        except Exception as e:
            print(f"Could not edit original message: {e}")
        
        await interaction.response.send_message("✅ Result recorded!", ephemeral=True)

# ==========================================
# APPLICATION SYSTEM - COMPREHENSIVE
# ==========================================

# Application Configuration
APPLICATION_TYPES = {
    "tryout_host": {
        "name": "Tryout Host",
        "emoji": "🎯",
        "role": "The Abyssal Overseer〢Tryout Host",
        "color": 0x9b59b6,
        "min_level": 10,
        "min_days": 7,
        "max_warnings": 1,
        "require_verified": True,
        "cooldown_days": 7,
        "votes_required": 2,
        "questions": [
            {"label": "What is your timezone?", "placeholder": "e.g. EST, PST, GMT+1", "style": "short"},
            {"label": "How many tryouts can you host per week?", "placeholder": "e.g. 3-5 tryouts", "style": "short"},
            {"label": "Describe your hosting experience", "placeholder": "Previous experience hosting tryouts, events, etc.", "style": "long"},
            {"label": "What games/modes are you experienced in?", "placeholder": "e.g. DA Hood, Blade Ball, etc.", "style": "long"},
        ]
    },
    "moderator": {
        "name": "Moderator",
        "emoji": "🛡️",
        "role": "Moderator",
        "color": 0x3498db,
        "min_level": 20,
        "min_days": 14,
        "max_warnings": 0,
        "require_verified": True,
        "cooldown_days": 14,
        "votes_required": 3,
        "questions": [
            {"label": "What is your timezone & availability?", "placeholder": "e.g. EST, available 6-10 PM weekdays", "style": "short"},
            {"label": "Why do you want to be a Moderator?", "placeholder": "Tell us your motivation", "style": "long"},
            {"label": "How would you handle a rule breaker?", "placeholder": "Describe your approach to moderation", "style": "long"},
            {"label": "Describe your moderation experience", "placeholder": "Previous mod experience in other servers/games", "style": "long"},
        ]
    },
    "training_host": {
        "name": "Training Host",
        "emoji": "📚",
        "role": "Training Host",
        "color": 0x2ecc71,
        "min_level": 15,
        "min_days": 7,
        "max_warnings": 1,
        "require_verified": True,
        "cooldown_days": 7,
        "votes_required": 2,
        "questions": [
            {"label": "What is your timezone?", "placeholder": "e.g. EST, PST, GMT+1", "style": "short"},
            {"label": "How many trainings can you host per week?", "placeholder": "e.g. 2-3 sessions", "style": "short"},
            {"label": "What skills/techniques can you teach?", "placeholder": "e.g. Combat basics, advanced combos, etc.", "style": "long"},
            {"label": "Describe your teaching/training experience", "placeholder": "Previous experience teaching or training others", "style": "long"},
        ]
    }
}

# Store applications: {user_id: {"type": str, "status": str, "votes": {}, "created": str, "channel_id": int, "answers": []}}
# Status: pending, under_review, interview, accepted, denied
applications_data = {}

# Store cooldowns: {user_id: {"type": last_application_date}}
application_cooldowns = {}

def check_application_requirements(member, app_type):
    """Check if user meets application requirements. Returns (passed, results_dict)"""
    config = APPLICATION_TYPES[app_type]
    user_data = get_user_data(member.id)
    results = {}
    
    # Check level
    user_level = user_data.get("level", 0)
    results["level"] = {
        "required": config["min_level"],
        "current": user_level,
        "passed": user_level >= config["min_level"]
    }
    
    # Check days in server
    joined = member.joined_at
    if joined:
        days_in_server = (datetime.datetime.now(datetime.timezone.utc) - joined).days
    else:
        days_in_server = 0
    results["days"] = {
        "required": config["min_days"],
        "current": days_in_server,
        "passed": days_in_server >= config["min_days"]
    }
    
    # Check warnings
    warnings_count = len(user_data.get("warnings", []))
    results["warnings"] = {
        "required": f"≤{config['max_warnings']}",
        "current": warnings_count,
        "passed": warnings_count <= config["max_warnings"]
    }
    
    # Check verified
    if config["require_verified"]:
        is_verified = user_data.get("verified", False) or user_data.get("roblox_username") is not None
        results["verified"] = {
            "required": "Yes",
            "current": "Yes" if is_verified else "No",
            "passed": is_verified
        }
    
    # Check cooldown
    if member.id in application_cooldowns:
        last_app = application_cooldowns[member.id].get(app_type)
        if last_app:
            try:
                last_date = datetime.datetime.fromisoformat(last_app)
                days_since = (datetime.datetime.now(datetime.timezone.utc) - last_date).days
                cooldown_passed = days_since >= config["cooldown_days"]
                results["cooldown"] = {
                    "required": f"{config['cooldown_days']} days since last",
                    "current": f"{days_since} days ago" if not cooldown_passed else "Ready",
                    "passed": cooldown_passed
                }
            except:
                results["cooldown"] = {"required": "None", "current": "Ready", "passed": True}
        else:
            results["cooldown"] = {"required": "None", "current": "Ready", "passed": True}
    else:
        results["cooldown"] = {"required": "None", "current": "Ready", "passed": True}
    
    # Check if already has pending application
    if member.id in applications_data:
        existing = applications_data[member.id]
        if existing.get("status") in ["pending", "under_review", "interview"]:
            results["existing"] = {
                "required": "No pending application",
                "current": f"Pending ({existing.get('type', 'unknown')})",
                "passed": False
            }
        else:
            results["existing"] = {"required": "None", "current": "None", "passed": True}
    else:
        results["existing"] = {"required": "None", "current": "None", "passed": True}
    
    all_passed = all(r["passed"] for r in results.values())
    return all_passed, results

def format_requirements_embed(member, app_type):
    """Create an embed showing requirement status"""
    config = APPLICATION_TYPES[app_type]
    passed, results = check_application_requirements(member, app_type)
    
    embed = discord.Embed(
        title=f"{config['emoji']} {config['name']} Application Requirements",
        color=0x2ecc71 if passed else 0xe74c3c
    )
    
    req_text = ""
    for key, result in results.items():
        icon = "✅" if result["passed"] else "❌"
        label = key.replace("_", " ").title()
        req_text += f"{icon} **{label}:** {result['current']} (Required: {result['required']})\n"
    
    embed.description = req_text
    
    if passed:
        embed.set_footer(text="✅ You meet all requirements! Click Apply to continue.")
    else:
        embed.set_footer(text="❌ You don't meet all requirements yet.")
    
    return embed, passed

# Dynamic Modal Generator
class DynamicApplicationModal(discord.ui.Modal):
    def __init__(self, app_type: str, applicant_id: int):
        self.app_type = app_type
        self.applicant_id = applicant_id
        config = APPLICATION_TYPES[app_type]
        super().__init__(title=f"{config['emoji']} {config['name']} Application")
        
        # Add questions dynamically
        self.answers = []
        for i, q in enumerate(config["questions"][:4]):  # Max 4 questions for modal
            style = discord.TextStyle.paragraph if q.get("style") == "long" else discord.TextStyle.short
            text_input = discord.ui.TextInput(
                label=q["label"][:45],  # Label max 45 chars
                placeholder=q.get("placeholder", ""),
                style=style,
                required=True
            )
            self.add_item(text_input)
    
    async def on_submit(self, interaction: discord.Interaction):
        config = APPLICATION_TYPES[self.app_type]
        
        # Collect answers
        answers = []
        for i, child in enumerate(self.children):
            if isinstance(child, discord.ui.TextInput):
                answers.append({
                    "question": config["questions"][i]["label"] if i < len(config["questions"]) else f"Q{i+1}",
                    "answer": child.value
                })
        
        # Store application data
        applications_data[interaction.user.id] = {
            "type": self.app_type,
            "status": "pending",
            "votes": {"approve": [], "deny": []},
            "created": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            "channel_id": interaction.channel.id,
            "answers": answers,
            "notes": []
        }
        
        # Set cooldown
        if interaction.user.id not in application_cooldowns:
            application_cooldowns[interaction.user.id] = {}
        application_cooldowns[interaction.user.id][self.app_type] = datetime.datetime.now(datetime.timezone.utc).isoformat()
        
        # Create application embed
        embed = discord.Embed(
            title=f"📝 {config['name']} Application",
            color=config["color"],
            timestamp=datetime.datetime.now(datetime.timezone.utc)
        )
        embed.set_author(name=interaction.user.display_name, icon_url=interaction.user.display_avatar.url)
        
        # Add user info
        user_data = get_user_data(interaction.user.id)
        embed.add_field(
            name="👤 Applicant Info",
            value=f"**Level:** {user_data.get('level', 0)}\n"
                  f"**Roblox:** {user_data.get('roblox_username', 'Not linked')}\n"
                  f"**Warnings:** {len(user_data.get('warnings', []))}/3",
            inline=True
        )
        embed.add_field(
            name="📊 Status",
            value=f"**Status:** 🟡 Pending\n"
                  f"**Votes:** ✅ 0 | ❌ 0\n"
                  f"**Required:** {config['votes_required']} votes",
            inline=True
        )
        
        # Add answers
        for ans in answers:
            embed.add_field(name=ans["question"], value=ans["answer"][:1024], inline=False)
        
        embed.set_footer(text=f"User ID: {interaction.user.id} | App Type: {self.app_type}")
        
        await interaction.response.send_message("✅ Application submitted! Staff will review it soon.", ephemeral=True)
        
        # Send to channel with review buttons
        staff = discord.utils.get(interaction.guild.roles, name=STAFF_ROLE_NAME)
        await interaction.channel.send(
            content=f"{staff.mention if staff else ''}",
            embed=embed,
            view=ApplicationReviewView(interaction.user, self.app_type)
        )
        
        # DM the user
        try:
            dm_embed = discord.Embed(
                title=f"📝 Application Submitted!",
                description=f"Your **{config['name']}** application has been submitted!\n\n"
                           f"**Status:** 🟡 Pending Review\n\n"
                           f"You'll receive a DM when staff reviews your application.",
                color=config["color"]
            )
            await interaction.user.send(embed=dm_embed)
        except:
            pass

class ApplicationTypeSelect(discord.ui.Select):
    def __init__(self):
        options = []
        for app_id, config in APPLICATION_TYPES.items():
            options.append(discord.SelectOption(
                label=config["name"],
                value=app_id,
                emoji=config["emoji"],
                description=f"Apply for {config['name']} role"
            ))
        super().__init__(placeholder="Select application type...", options=options)
    
    async def callback(self, interaction: discord.Interaction):
        app_type = self.values[0]
        config = APPLICATION_TYPES[app_type]
        
        # Check requirements
        embed, passed = format_requirements_embed(interaction.user, app_type)
        
        if passed:
            view = ApplicationConfirmView(app_type)
        else:
            view = None
        
        await interaction.response.send_message(embed=embed, view=view, ephemeral=True)

class ApplicationConfirmView(discord.ui.View):
    def __init__(self, app_type: str):
        super().__init__(timeout=120)
        self.app_type = app_type
    
    @discord.ui.button(label="📝 Start Application", style=discord.ButtonStyle.success)
    async def start_app(self, interaction: discord.Interaction, button: discord.ui.Button):
        config = APPLICATION_TYPES[self.app_type]
        
        # Double check requirements
        passed, _ = check_application_requirements(interaction.user, self.app_type)
        if not passed:
            return await interaction.response.send_message("❌ You no longer meet the requirements.", ephemeral=True)
        
        # Create private channel
        overwrites = {
            interaction.guild.default_role: discord.PermissionOverwrite(read_messages=False),
            interaction.user: discord.PermissionOverwrite(read_messages=True, send_messages=True),
            interaction.guild.me: discord.PermissionOverwrite(read_messages=True, send_messages=True)
        }
        staff = discord.utils.get(interaction.guild.roles, name=STAFF_ROLE_NAME)
        if staff:
            overwrites[staff] = discord.PermissionOverwrite(read_messages=True, send_messages=True)
        
        # Add high staff
        for role_name in HIGH_STAFF_ROLES:
            role = discord.utils.get(interaction.guild.roles, name=role_name)
            if role:
                overwrites[role] = discord.PermissionOverwrite(read_messages=True, send_messages=True)
        
        cat = discord.utils.get(interaction.guild.categories, name="Applications")
        if not cat:
            cat = await interaction.guild.create_category("Applications")
        
        ch = await interaction.guild.create_text_channel(
            name=f"{config['emoji']}-{interaction.user.name}-app",
            category=cat,
            overwrites=overwrites
        )
        
        embed = discord.Embed(
            title=f"{config['emoji']} {config['name']} Application",
            description=f"Welcome {interaction.user.mention}!\n\n"
                       f"Click the button below to fill out your application.\n\n"
                       f"**Please answer honestly and thoroughly!**",
            color=config["color"]
        )
        embed.add_field(name="📋 Questions", value="\n".join([f"• {q['label']}" for q in config["questions"]]), inline=False)
        
        await ch.send(embed=embed, view=ApplicationFormButtonView(self.app_type, interaction.user.id))
        await interaction.response.send_message(f"✅ Application channel created! Go to {ch.mention}", ephemeral=True)

class ApplicationFormButtonView(discord.ui.View):
    def __init__(self, app_type: str, applicant_id: int):
        super().__init__(timeout=None)
        self.app_type = app_type
        self.applicant_id = applicant_id
    
    @discord.ui.button(label="📝 Fill Application", style=discord.ButtonStyle.primary, custom_id="fill_application")
    async def fill_app(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user.id != self.applicant_id:
            return await interaction.response.send_message("❌ This isn't your application!", ephemeral=True)
        await interaction.response.send_modal(DynamicApplicationModal(self.app_type, self.applicant_id))

class ApplicationReviewView(discord.ui.View):
    def __init__(self, applicant=None, app_type="tryout_host"):
        super().__init__(timeout=None)
        self.applicant = applicant
        self.app_type = app_type
    
    @discord.ui.button(label="✅ Approve", style=discord.ButtonStyle.success, custom_id="app_vote_approve")
    async def approve(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not is_staff(interaction.user):
            return await interaction.response.send_message("❌ Staff only.", ephemeral=True)
        
        await self.cast_vote(interaction, "approve")
    
    @discord.ui.button(label="❌ Deny", style=discord.ButtonStyle.danger, custom_id="app_vote_deny")
    async def deny(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not is_staff(interaction.user):
            return await interaction.response.send_message("❌ Staff only.", ephemeral=True)
        
        await self.cast_vote(interaction, "deny")
    
    @discord.ui.button(label="📝 Add Note", style=discord.ButtonStyle.secondary, custom_id="app_add_note")
    async def add_note(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not is_staff(interaction.user):
            return await interaction.response.send_message("❌ Staff only.", ephemeral=True)
        await interaction.response.send_modal(ApplicationNoteModal(self.applicant.id if self.applicant else None))
    
    @discord.ui.button(label="🎤 Schedule Interview", style=discord.ButtonStyle.primary, custom_id="app_interview")
    async def interview(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not is_staff(interaction.user):
            return await interaction.response.send_message("❌ Staff only.", ephemeral=True)
        
        if self.applicant and self.applicant.id in applications_data:
            applications_data[self.applicant.id]["status"] = "interview"
            
            # Update embed
            await self.update_status_embed(interaction, "🟣 Interview Scheduled")
            
            # DM applicant
            try:
                dm_embed = discord.Embed(
                    title="🎤 Interview Scheduled!",
                    description=f"Congratulations! Your application has moved to the interview stage.\n\n"
                               f"A staff member will contact you soon to schedule a voice interview.",
                    color=0x9b59b6
                )
                await self.applicant.send(embed=dm_embed)
            except:
                pass
            
            await interaction.response.send_message("✅ Interview scheduled! Applicant has been notified.", ephemeral=True)
        else:
            await interaction.response.send_message("❌ Application data not found.", ephemeral=True)
    
    async def cast_vote(self, interaction: discord.Interaction, vote_type: str):
        if not self.applicant or self.applicant.id not in applications_data:
            return await interaction.response.send_message("❌ Application data not found.", ephemeral=True)
        
        app_data = applications_data[self.applicant.id]
        config = APPLICATION_TYPES.get(app_data["type"], APPLICATION_TYPES["tryout_host"])
        
        # Check if already voted
        if interaction.user.id in app_data["votes"]["approve"] or interaction.user.id in app_data["votes"]["deny"]:
            return await interaction.response.send_message("❌ You already voted on this application!", ephemeral=True)
        
        # Add vote
        app_data["votes"][vote_type].append(interaction.user.id)
        
        approve_count = len(app_data["votes"]["approve"])
        deny_count = len(app_data["votes"]["deny"])
        
        # Check if decision reached
        if approve_count >= config["votes_required"]:
            await self.finalize_application(interaction, "accepted")
        elif deny_count >= config["votes_required"]:
            await self.finalize_application(interaction, "denied")
        else:
            # Update status
            app_data["status"] = "under_review"
            await self.update_status_embed(interaction, f"🟠 Under Review (✅ {approve_count} | ❌ {deny_count})")
            await interaction.response.send_message(f"✅ Vote recorded! (✅ {approve_count} | ❌ {deny_count} / {config['votes_required']} needed)", ephemeral=True)
    
    async def update_status_embed(self, interaction: discord.Interaction, status_text: str):
        """Update the application embed with new status"""
        if interaction.message and interaction.message.embeds:
            embed = interaction.message.embeds[0]
            
            # Update status field
            for i, field in enumerate(embed.fields):
                if field.name == "📊 Status":
                    app_data = applications_data.get(self.applicant.id, {})
                    config = APPLICATION_TYPES.get(app_data.get("type", "tryout_host"), APPLICATION_TYPES["tryout_host"])
                    approve_count = len(app_data.get("votes", {}).get("approve", []))
                    deny_count = len(app_data.get("votes", {}).get("deny", []))
                    
                    embed.set_field_at(i,
                        name="📊 Status",
                        value=f"**Status:** {status_text}\n"
                              f"**Votes:** ✅ {approve_count} | ❌ {deny_count}\n"
                              f"**Required:** {config['votes_required']} votes",
                        inline=True
                    )
                    break
            
            try:
                await interaction.message.edit(embed=embed)
            except:
                pass
    
    async def finalize_application(self, interaction: discord.Interaction, result: str):
        """Finalize the application (accept or deny)"""
        app_data = applications_data[self.applicant.id]
        config = APPLICATION_TYPES.get(app_data["type"], APPLICATION_TYPES["tryout_host"])
        app_data["status"] = result
        
        if result == "accepted":
            # Give role
            role = discord.utils.get(interaction.guild.roles, name=config["role"])
            if role and self.applicant:
                try:
                    await self.applicant.add_roles(role)
                except:
                    pass
            
            # Update embed
            await self.update_status_embed(interaction, "🟢 ACCEPTED")
            
            # DM applicant
            try:
                dm_embed = discord.Embed(
                    title="🎉 Application Accepted!",
                    description=f"Congratulations! Your **{config['name']}** application has been accepted!\n\n"
                               f"You have been granted the **{config['role']}** role.\n\n"
                               f"Welcome to the team!",
                    color=0x2ecc71
                )
                await self.applicant.send(embed=dm_embed)
            except:
                pass
            
            await log_action(interaction.guild, "✅ Application Accepted", 
                           f"{self.applicant.mention}'s {config['name']} application was accepted", 0x2ecc71)
            
            await interaction.response.send_message("✅ Application ACCEPTED! Closing channel in 10 seconds...", ephemeral=False)
            
        else:  # denied
            # Update embed
            await self.update_status_embed(interaction, "🔴 DENIED")
            
            # DM applicant
            try:
                dm_embed = discord.Embed(
                    title="❌ Application Denied",
                    description=f"Unfortunately, your **{config['name']}** application was not accepted at this time.\n\n"
                               f"You may reapply in **{config['cooldown_days']} days**.\n\n"
                               f"Keep working on improving and try again!",
                    color=0xe74c3c
                )
                await self.applicant.send(embed=dm_embed)
            except:
                pass
            
            await log_action(interaction.guild, "❌ Application Denied",
                           f"{self.applicant.mention}'s {config['name']} application was denied", 0xe74c3c)
            
            await interaction.response.send_message("❌ Application DENIED. Closing channel in 10 seconds...", ephemeral=False)
        
        # Disable buttons
        for child in self.children:
            child.disabled = True
        
        try:
            await interaction.message.edit(view=self)
        except:
            pass
        
        # Close channel after delay
        await asyncio.sleep(10)
        try:
            await interaction.channel.delete()
        except:
            pass

class ApplicationNoteModal(discord.ui.Modal, title="📝 Add Staff Note"):
    note = discord.ui.TextInput(
        label="Note",
        placeholder="Add a note about this application...",
        style=discord.TextStyle.paragraph,
        required=True
    )
    
    def __init__(self, applicant_id):
        super().__init__()
        self.applicant_id = applicant_id
    
    async def on_submit(self, interaction: discord.Interaction):
        if self.applicant_id and self.applicant_id in applications_data:
            applications_data[self.applicant_id]["notes"].append({
                "by": interaction.user.id,
                "note": self.note.value,
                "time": datetime.datetime.now(datetime.timezone.utc).isoformat()
            })
            
            note_embed = discord.Embed(
                title="📝 Staff Note Added",
                description=self.note.value,
                color=0x3498db,
                timestamp=datetime.datetime.now(datetime.timezone.utc)
            )
            note_embed.set_author(name=interaction.user.display_name, icon_url=interaction.user.display_avatar.url)
            
            await interaction.response.send_message(embed=note_embed)
        else:
            await interaction.response.send_message("❌ Could not add note.", ephemeral=True)

class ApplicationStartView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)
        self.add_item(ApplicationTypeSelect())

class ApplicationPanelView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)
    
    @discord.ui.button(label="📋 Apply Now", style=discord.ButtonStyle.success, custom_id="apply_panel_btn")
    async def apply_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        embed = discord.Embed(
            title="📋 Select Application Type",
            description="Choose the position you want to apply for:\n\n"
                       "🎯 **Tryout Host** - Host tryouts for new members\n"
                       "🛡️ **Moderator** - Help moderate the server\n"
                       "📚 **Training Host** - Host training sessions",
            color=0x3498db
        )
        await interaction.response.send_message(embed=embed, view=ApplicationStartView(), ephemeral=True)

class TournamentTypeView(discord.ui.View):
    def __init__(self, title, author): 
        super().__init__(timeout=60)
        self.title = title
        self.author = author
    
    async def interaction_check(self, interaction: discord.Interaction): 
        return interaction.user == self.author
    
    @discord.ui.button(label="🏆 Ranked", style=discord.ButtonStyle.success)
    async def ranked(self, interaction: discord.Interaction, button: discord.ui.Button): 
        await self.setup_tournament(interaction, True)
    
    @discord.ui.button(label="🎉 Unranked", style=discord.ButtonStyle.secondary)
    async def unranked(self, interaction: discord.Interaction, button: discord.ui.Button): 
        await self.setup_tournament(interaction, False)
    
    async def setup_tournament(self, interaction: discord.Interaction, ranked: bool):
        tournament_state["active"] = True
        tournament_state["title"] = self.title
        tournament_state["ranked_mode"] = ranked
        tournament_state["players"] = []
        tournament_state["next_round"] = []
        tournament_state["losers_stack"] = []
        tournament_state["match_count"] = 0
        tournament_state["finished_matches"] = 0
        
        mode_text = "RANKED" if ranked else "UNRANKED"
        desc = f"**{self.title}**\nMode: {mode_text}\n\nRequirement: `{REQUIRED_ROLE_NAME}` role\n\nClick **Join** to participate!"
        embed = discord.Embed(title="🏆 Tournament Signups Open!", description=desc, color=0xFFD700)
        await interaction.response.edit_message(content=None, embed=embed, view=TournamentJoinView())

class MatchButtonView(discord.ui.View):
    def __init__(self, p1, p2): 
        super().__init__(timeout=None)
        self.p1 = p1
        self.p2 = p2
    
    @discord.ui.button(label="P1 Won", style=discord.ButtonStyle.primary)
    async def w1(self, interaction: discord.Interaction, button: discord.ui.Button): 
        await self.record_win(interaction, self.p1, self.p2)
    
    @discord.ui.button(label="P2 Won", style=discord.ButtonStyle.secondary)
    async def w2(self, interaction: discord.Interaction, button: discord.ui.Button): 
        await self.record_win(interaction, self.p2, self.p1)
    
    async def interaction_check(self, interaction: discord.Interaction): 
        return is_staff(interaction.user)
    
    async def record_win(self, interaction: discord.Interaction, winner, loser):
        tournament_state["next_round"].append(winner)
        tournament_state["losers_stack"].append(loser)
        tournament_state["finished_matches"] += 1
        
        for child in self.children: 
            child.disabled = True
        
        await interaction.response.edit_message(content=f"✅ Winner: <@{winner}>", view=self)
        
        if tournament_state["finished_matches"] >= tournament_state["match_count"]: 
            await advance_round(interaction)

class TournamentJoinView(discord.ui.View):
    def __init__(self): 
        super().__init__(timeout=None)
    
    @discord.ui.button(label="🎮 Join Tournament", style=discord.ButtonStyle.success, custom_id="tj_j")
    async def join(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not tournament_state["active"]: 
            return await interaction.response.send_message("❌ No active tournament.", ephemeral=True)
        
        if REQUIRED_ROLE_NAME not in [r.name for r in interaction.user.roles]: 
            return await interaction.response.send_message(f"🔒 You need the `{REQUIRED_ROLE_NAME}` role to join.", ephemeral=True)
        
        if interaction.user.id in tournament_state["players"]: 
            return await interaction.response.send_message("⚠️ You're already registered!", ephemeral=True)
        
        tournament_state["players"].append(interaction.user.id)
        await interaction.response.send_message(f"✅ You joined the tournament! ({len(tournament_state['players'])} players registered)", ephemeral=True)
    
    @discord.ui.button(label="⚙️ Manage (Staff)", style=discord.ButtonStyle.secondary, custom_id="tj_m")
    async def manage(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not is_staff(interaction.user):
            return await interaction.response.send_message("❌ Staff only.", ephemeral=True)
        
        player_list = "\n".join([f"<@{pid}>" for pid in tournament_state["players"][:10]]) or "No players yet"
        if len(tournament_state["players"]) > 10:
            player_list += f"\n... and {len(tournament_state['players']) - 10} more"
        
        embed = discord.Embed(
            title="Tournament Management",
            description=f"**Players Registered:** {len(tournament_state['players'])}\n\n{player_list}",
            color=0x3498db
        )
        await interaction.response.send_message(embed=embed, view=TournamentManageView(), ephemeral=True)

class TournamentManageView(discord.ui.View):
    def __init__(self): 
        super().__init__(timeout=None)
    
    @discord.ui.button(label="🚀 Start Tournament", style=discord.ButtonStyle.success, custom_id="tm_start")
    async def start(self, interaction: discord.Interaction, button: discord.ui.Button):
        if len(tournament_state["players"]) < 2: 
            return await interaction.response.send_message("❌ Need at least 2 players to start.", ephemeral=True)
        
        tournament_state["losers_stack"] = []
        tournament_state["next_round"] = []
        tournament_state["finished_matches"] = 0
        
        await interaction.response.send_message("🚀 Starting tournament...", ephemeral=True)
        await generate_matchups(interaction.channel)
    
    @discord.ui.button(label="🛑 Cancel Tournament", style=discord.ButtonStyle.danger, custom_id="tm_cancel")
    async def cancel(self, interaction: discord.Interaction, button: discord.ui.Button):
        tournament_state["active"] = False
        tournament_state["players"] = []
        tournament_state["next_round"] = []
        tournament_state["losers_stack"] = []
        
        await interaction.response.send_message("🛑 Tournament cancelled.", ephemeral=True)
        await interaction.channel.send(embed=discord.Embed(
            title="🛑 Tournament Cancelled",
            description="The tournament has been cancelled by staff.",
            color=0xe74c3c
        ))

class ChallengeRequestView(discord.ui.View):
    def __init__(self): 
        super().__init__(timeout=None)
    
    @discord.ui.button(label="Request Challenge", style=discord.ButtonStyle.danger, emoji="⚔️", custom_id="c_c")
    async def challenge(self, interaction: discord.Interaction, button: discord.ui.Button): 
        await interaction.response.send_modal(ChallengeModal())

class LeaderboardView(discord.ui.View):
    def __init__(self): 
        super().__init__(timeout=None)
    
    @discord.ui.button(label="✏️ Edit Roster", style=discord.ButtonStyle.secondary, custom_id="lb_edit")
    async def edit_roster(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not is_high_staff(interaction.user):
            return await interaction.response.send_message("❌ High Staff only.", ephemeral=True)
        
        roster = load_leaderboard()
        roster_text = "\n".join([str(u) if u else "VACANT" for u in roster])
        await interaction.response.send_modal(EditLeaderboardModal(roster_text))
    
    @discord.ui.button(label="🎨 Edit Design", style=discord.ButtonStyle.secondary, custom_id="lb_des")
    async def edit_design(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not is_high_staff(interaction.user):
            return await interaction.response.send_message("❌ High Staff only.", ephemeral=True)
        
        await interaction.response.send_modal(EditDesignModal(load_data()["theme"]))

class EditLeaderboardModal(discord.ui.Modal, title="Edit Roster"):
    roster_data = discord.ui.TextInput(
        label="User IDs (one per line)", 
        style=discord.TextStyle.paragraph,
        placeholder="Enter user IDs, one per line. Use 'VACANT' for empty slots."
    )
    
    def __init__(self, current_roster): 
        super().__init__()
        self.roster_data.default = current_roster
    
    async def on_submit(self, interaction: discord.Interaction):
        new_roster = []
        for line in self.roster_data.value.split('\n'):
            line = line.strip()
            if line.upper() == "VACANT" or not line:
                new_roster.append(None)
            else:
                user_id = ''.join(filter(str.isdigit, line))
                new_roster.append(int(user_id) if user_id else None)
        
        new_roster = (new_roster + [None] * 10)[:10]
        save_leaderboard(new_roster)
        
        await interaction.response.edit_message(embed=create_leaderboard_embed(interaction.guild))
        await log_action(interaction.guild, "📝 Roster Updated", f"Updated by {interaction.user.mention}", 0xF1C40F)

class EditDesignModal(discord.ui.Modal, title="Edit Theme"):
    title_input = discord.ui.TextInput(label="Title", max_length=100)
    desc_input = discord.ui.TextInput(label="Description", style=discord.TextStyle.paragraph)
    image_input = discord.ui.TextInput(label="Image URL", required=False)
    color_input = discord.ui.TextInput(label="Color (Hex)", placeholder="#FF0000")
    
    def __init__(self, theme): 
        super().__init__()
        self.title_input.default = theme.get("title", "")
        self.desc_input.default = theme.get("description", "")
        self.image_input.default = theme.get("image", "")
        self.color_input.default = str(hex(theme.get("color", 0))).replace("0x", "#")
    
    async def on_submit(self, interaction: discord.Interaction):
        try: 
            color = int(self.color_input.value.replace("#", ""), 16)
        except: 
            color = 0x2b2d31
        
        save_theme({
            "title": self.title_input.value,
            "description": self.desc_input.value,
            "image": self.image_input.value,
            "color": color
        })
        
        await interaction.response.edit_message(embed=create_leaderboard_embed(interaction.guild))
        await log_action(interaction.guild, "🎨 Theme Updated", f"Updated by {interaction.user.mention}", 0xF1C40F)

# --- DATA WIPE CONFIRMATION VIEW ---
# ==========================================
# VERIFICATION SYSTEM (Secure - Roblox API)
# ==========================================

import aiohttp
import hashlib

# Store pending verifications: {user_id: {"username": str, "code": str, "roblox_id": int}}
pending_verifications = {}

def generate_verify_code(user_id: int) -> str:
    """Generate a unique verification code for a user"""
    hash_input = f"{user_id}-{datetime.datetime.now().timestamp()}-fallen"
    return "FALLEN-" + hashlib.md5(hash_input.encode()).hexdigest()[:8].upper()

async def get_roblox_user_by_username(username: str) -> dict:
    """Get Roblox user info by username"""
    try:
        async with aiohttp.ClientSession() as session:
            # First, get user ID from username
            async with session.post(
                "https://users.roblox.com/v1/usernames/users",
                json={"usernames": [username], "excludeBannedUsers": True}
            ) as resp:
                if resp.status != 200:
                    return None
                data = await resp.json()
                if not data.get("data"):
                    return None
                user_data = data["data"][0]
                return {
                    "id": user_data["id"],
                    "name": user_data["name"],
                    "display_name": user_data.get("displayName", user_data["name"])
                }
    except Exception as e:
        print(f"Roblox API error: {e}")
        return None

async def get_roblox_user_description(roblox_id: int) -> str:
    """Get a Roblox user's profile description"""
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(f"https://users.roblox.com/v1/users/{roblox_id}") as resp:
                if resp.status != 200:
                    return ""
                data = await resp.json()
                return data.get("description", "")
    except Exception as e:
        print(f"Roblox API error: {e}")
        return ""

async def verify_roblox_code(roblox_id: int, code: str) -> bool:
    """Check if the verification code is in the user's Roblox description"""
    description = await get_roblox_user_description(roblox_id)
    return code in description

class VerifyUsernameModal(discord.ui.Modal, title="🔗 Step 1: Enter Roblox Username"):
    roblox_username = discord.ui.TextInput(
        label="Roblox Username",
        placeholder="Enter your exact Roblox username",
        min_length=3,
        max_length=20,
        required=True
    )
    
    async def on_submit(self, interaction: discord.Interaction):
        username = self.roblox_username.value.strip()
        
        await interaction.response.defer(ephemeral=True)
        
        # Look up the Roblox user
        roblox_user = await get_roblox_user_by_username(username)
        
        if not roblox_user:
            return await interaction.followup.send(
                f"❌ Could not find Roblox user **{username}**. Please check the spelling and try again.",
                ephemeral=True
            )
        
        # Generate verification code
        code = generate_verify_code(interaction.user.id)
        
        # Store pending verification
        pending_verifications[interaction.user.id] = {
            "username": roblox_user["name"],
            "display_name": roblox_user["display_name"],
            "code": code,
            "roblox_id": roblox_user["id"]
        }
        
        embed = discord.Embed(
            title="🔗 Step 2: Verify Your Account",
            description=(
                f"We found the Roblox account **{roblox_user['name']}**!\n\n"
                f"To prove this is your account, please add this code to your **Roblox profile description**:\n\n"
                f"```{code}```\n\n"
                f"**How to add the code:**\n"
                f"1. Go to [roblox.com](https://www.roblox.com/users/{roblox_user['id']}/profile)\n"
                f"2. Click the **pencil icon** to edit your profile\n"
                f"3. Paste the code anywhere in your **About/Description**\n"
                f"4. **Save** your profile\n"
                f"5. Click the **Verify** button below\n\n"
                f"*You can remove the code after verification!*"
            ),
            color=0xF1C40F
        )
        embed.set_footer(text="The code expires in 10 minutes")
        
        await interaction.followup.send(embed=embed, view=VerifyCodeCheckView(), ephemeral=True)

class VerifyCodeCheckView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=600)  # 10 minute timeout
    
    @discord.ui.button(label="✅ I Added the Code - Verify Me!", style=discord.ButtonStyle.success)
    async def verify_code(self, interaction: discord.Interaction, button: discord.ui.Button):
        # Check if user has pending verification
        if interaction.user.id not in pending_verifications:
            return await interaction.response.send_message(
                "❌ No pending verification found. Please start over with `/verify`.",
                ephemeral=True
            )
        
        await interaction.response.defer(ephemeral=True)
        
        pending = pending_verifications[interaction.user.id]
        
        # Check if code is in their Roblox description
        verified = await verify_roblox_code(pending["roblox_id"], pending["code"])
        
        if not verified:
            return await interaction.followup.send(
                f"❌ Could not find the code `{pending['code']}` in your Roblox profile description.\n\n"
                f"**Make sure you:**\n"
                f"• Added the code to your **About/Description** section\n"
                f"• **Saved** your profile after adding it\n"
                f"• Wait a few seconds and try again\n\n"
                f"[Click here to edit your profile](https://www.roblox.com/my/account#!/info)",
                ephemeral=True
            )
        
        # Verification successful!
        username = pending["username"]
        roblox_id = pending["roblox_id"]
        
        # Remove from pending
        del pending_verifications[interaction.user.id]
        
        # Store the verified Roblox info
        update_user_data(interaction.user.id, "roblox_username", username)
        update_user_data(interaction.user.id, "roblox_id", roblox_id)
        update_user_data(interaction.user.id, "verified", True)
        
        # Get roles
        unv = discord.utils.get(interaction.guild.roles, name=UNVERIFIED_ROLE_NAME)
        ver = discord.utils.get(interaction.guild.roles, name=VERIFIED_ROLE_NAME)
        mem = discord.utils.get(interaction.guild.roles, name=MEMBER_ROLE_NAME)
        
        if not ver or not mem:
            return await interaction.followup.send("❌ Server roles not configured. Please contact an admin.", ephemeral=True)
        
        try:
            # Remove unverified role if present
            if unv and unv in interaction.user.roles:
                await interaction.user.remove_roles(unv)
            
            # Add verified and member roles
            await interaction.user.add_roles(ver, mem)
            
            # Update nickname
            try:
                await interaction.user.edit(nick=username)
                nickname_msg = f"Your nickname has been set to **{username}**."
            except discord.Forbidden:
                nickname_msg = "*(Could not update nickname - missing permissions)*"
            
            embed = discord.Embed(
                title="✅ Verification Successful!",
                description=f"Welcome to **The Fallen**, **{username}**!\n\n{nickname_msg}\n\n*You can now remove the code from your Roblox profile!*",
                color=0x2ecc71
            )
            embed.add_field(name="🎮 Roblox Account", value=f"[{username}](https://www.roblox.com/users/{roblox_id}/profile)", inline=True)
            embed.add_field(name="🆔 Roblox ID", value=str(roblox_id), inline=True)
            embed.add_field(name="🎭 Roles Given", value="✅ Verified\n✅ Abyssbound", inline=True)
            embed.set_footer(text="Use /daily to claim your first reward!")
            
            await interaction.followup.send(embed=embed, ephemeral=True)
            await log_action(interaction.guild, "✅ Member Verified", f"{interaction.user.mention} verified as **{username}** (ID: {roblox_id})", 0x2ecc71)
            
        except discord.Forbidden:
            await interaction.followup.send("❌ I don't have permission to manage roles. Please contact an admin.", ephemeral=True)
        except Exception as e:
            print(f"Verify error: {e}")
            await interaction.followup.send("❌ An error occurred. Please try again.", ephemeral=True)
    
    @discord.ui.button(label="🔄 Use Different Account", style=discord.ButtonStyle.secondary)
    async def different_account(self, interaction: discord.Interaction, button: discord.ui.Button):
        # Remove pending verification
        if interaction.user.id in pending_verifications:
            del pending_verifications[interaction.user.id]
        await interaction.response.send_modal(VerifyUsernameModal())
    
    @discord.ui.button(label="❌ Cancel", style=discord.ButtonStyle.danger)
    async def cancel(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user.id in pending_verifications:
            del pending_verifications[interaction.user.id]
        await interaction.response.edit_message(content="❌ Verification cancelled.", embed=None, view=None)

class VerifyView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)
    
    @discord.ui.button(label="🔗 Verify with Roblox", style=discord.ButtonStyle.success, custom_id="verify_roblox_btn")
    async def verify_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        # Check if already verified
        mem = discord.utils.get(interaction.guild.roles, name=MEMBER_ROLE_NAME)
        if mem and mem in interaction.user.roles:
            user_data = get_user_data(interaction.user.id)
            current = user_data.get('roblox_username', 'Unknown')
            roblox_id = user_data.get('roblox_id', 'Unknown')
            
            embed = discord.Embed(
                title="🔄 Already Verified",
                description=f"You're already verified as **{current}**!\n\nWant to link a different Roblox account?",
                color=0x3498db
            )
            embed.add_field(name="Current Account", value=f"[{current}](https://www.roblox.com/users/{roblox_id}/profile)" if roblox_id != 'Unknown' else current)
            
            view = discord.ui.View(timeout=60)
            reverify_btn = discord.ui.Button(label="🔄 Reverify", style=discord.ButtonStyle.primary)
            
            async def reverify_callback(inter: discord.Interaction):
                await inter.response.send_modal(VerifyUsernameModal())
            
            reverify_btn.callback = reverify_callback
            view.add_item(reverify_btn)
            
            await interaction.response.send_message(embed=embed, view=view, ephemeral=True)
        else:
            await interaction.response.send_modal(VerifyUsernameModal())

class UpdateNicknameModal(discord.ui.Modal, title="🔄 Update Roblox Username"):
    roblox_username = discord.ui.TextInput(
        label="New Roblox Username",
        placeholder="Enter your new Roblox username",
        min_length=3,
        max_length=20,
        required=True
    )
    
    async def on_submit(self, interaction: discord.Interaction):
        username = self.roblox_username.value.strip()
        
        await interaction.response.defer(ephemeral=True)
        
        # Look up the Roblox user
        roblox_user = await get_roblox_user_by_username(username)
        
        if not roblox_user:
            return await interaction.followup.send(
                f"❌ Could not find Roblox user **{username}**.",
                ephemeral=True
            )
        
        # Generate verification code
        code = generate_verify_code(interaction.user.id)
        
        # Store pending verification
        pending_verifications[interaction.user.id] = {
            "username": roblox_user["name"],
            "display_name": roblox_user["display_name"],
            "code": code,
            "roblox_id": roblox_user["id"],
            "is_update": True
        }
        
        embed = discord.Embed(
            title="🔄 Verify New Account",
            description=(
                f"To update to **{roblox_user['name']}**, add this code to your Roblox description:\n\n"
                f"```{code}```\n\n"
                f"[Edit your profile](https://www.roblox.com/my/account#!/info), then click Verify below."
            ),
            color=0xF1C40F
        )
        
        await interaction.followup.send(embed=embed, view=UpdateVerifyCheckView(), ephemeral=True)

class UpdateVerifyCheckView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=600)
    
    @discord.ui.button(label="✅ Verify", style=discord.ButtonStyle.success)
    async def verify(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user.id not in pending_verifications:
            return await interaction.response.send_message("❌ No pending verification.", ephemeral=True)
        
        await interaction.response.defer(ephemeral=True)
        
        pending = pending_verifications[interaction.user.id]
        verified = await verify_roblox_code(pending["roblox_id"], pending["code"])
        
        if not verified:
            return await interaction.followup.send(
                f"❌ Code `{pending['code']}` not found in your Roblox description. Try again!",
                ephemeral=True
            )
        
        username = pending["username"]
        roblox_id = pending["roblox_id"]
        del pending_verifications[interaction.user.id]
        
        update_user_data(interaction.user.id, "roblox_username", username)
        update_user_data(interaction.user.id, "roblox_id", roblox_id)
        
        try:
            await interaction.user.edit(nick=username)
            await interaction.followup.send(f"✅ Updated to **{username}**! You can remove the code now.", ephemeral=True)
        except discord.Forbidden:
            await interaction.followup.send(f"✅ Linked to **{username}**! (Couldn't update nickname)", ephemeral=True)
        
        await log_action(interaction.guild, "🔄 Roblox Updated", f"{interaction.user.mention} → **{username}**", 0x3498db)

class DataWipeConfirmView(discord.ui.View):
    def __init__(self, author):
        super().__init__(timeout=30)
        self.author = author
        self.confirmed = False
    
    async def interaction_check(self, interaction: discord.Interaction):
        return interaction.user == self.author
    
    @discord.ui.button(label="⚠️ YES, WIPE ALL DATA", style=discord.ButtonStyle.danger)
    async def confirm(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.confirmed = True
        reset_all_data()
        
        embed = discord.Embed(
            title="🗑️ DATA WIPED",
            description="All user data, XP, coins, levels, and roster have been reset.",
            color=0xe74c3c
        )
        await interaction.response.edit_message(embed=embed, view=None)
        await log_action(interaction.guild, "🗑️ DATA WIPE", f"All data wiped by {interaction.user.mention}", 0xe74c3c)
    
    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.secondary)
    async def cancel(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.edit_message(content="❌ Data wipe cancelled.", embed=None, view=None)

# --- BOT SETUP ---
class PersistentBot(commands.Bot):
    def __init__(self): 
        super().__init__(
            command_prefix="!", 
            intents=discord.Intents.all(), 
            help_command=None
        )
    
    async def setup_hook(self):
        # Register persistent views (only views with custom_id buttons that persist after restart)
        self.add_view(LeaderboardView())
        self.add_view(TournamentJoinView())
        self.add_view(TournamentManageView())
        self.add_view(ChallengeRequestView())
        self.add_view(StaffApprovalView())
        self.add_view(MatchAnnouncementView())
        self.add_view(ShopView())
        self.add_view(VerifyView())
        self.add_view(ApplicationPanelView())
        self.add_view(SupportTicketPanelView())
        self.add_view(ClanRosterView())
        self.add_view(TicketControlView("tryout"))
        self.add_view(TicketControlView("role"))
        
        # Start background task
        self.bg_voice_xp.start()
        print("Bot setup complete!")

    @tasks.loop(minutes=1)
    async def bg_voice_xp(self):
        for guild in self.guilds:
            for member in guild.members:
                if member.voice and not member.voice.self_deaf and not member.bot:
                    xp = random.randint(*XP_VOICE_RANGE)
                    add_xp_to_user(member.id, xp)
                    await check_level_up(member.id, guild)

    @bg_voice_xp.before_loop
    async def before_voice_xp(self):
        await self.wait_until_ready()

bot = PersistentBot()

@bot.event
async def on_ready():
    print("=" * 50)
    print(f"✅ Logged in as {bot.user} (ID: {bot.user.id})")
    print(f"✅ Connected to {len(bot.guilds)} guild(s)")
    print(f"✅ PIL Available: {PIL_AVAILABLE}")
    print("=" * 50)
    
    # Check database health
    print("Checking database health...")
    data = load_data()
    fixed_count = 0
    for uid in list(data["users"].keys()):
        if "weekly_xp" not in data["users"][uid] or "monthly_xp" not in data["users"][uid]:
            ensure_user_structure(data, uid)
            fixed_count += 1
    if fixed_count > 0:
        save_data(data)
        print(f"✅ Repaired {fixed_count} user profiles.")
    
    # Auto-sync slash commands on startup
    try:
        synced = await bot.tree.sync()
        print(f"✅ Synced {len(synced)} slash commands globally!")
    except Exception as e:
        print(f"❌ Failed to sync commands: {e}")
    
    print("=" * 50)
    print("🚀 Bot is ready!")
    print("=" * 50)

@bot.event
async def on_member_join(member):
    # Give Unverified role
    unv_role = discord.utils.get(member.guild.roles, name=UNVERIFIED_ROLE_NAME)
    if unv_role:
        try:
            await member.add_roles(unv_role)
        except Exception as e:
            print(f"Could not add unverified role: {e}")
    
    # Try to DM them with verification instructions
    try:
        embed = discord.Embed(
            title=f"👋 Welcome to The Fallen!",
            description=(
                f"Hey **{member.name}**, welcome to the server!\n\n"
                f"**🔒 You currently have the Unverified role.**\n\n"
                f"To access the server and all its features, you need to verify your Roblox account.\n\n"
                f"**How to verify:**\n"
                f"1️⃣ Go to the verification channel\n"
                f"2️⃣ Click the **Verify with Roblox** button\n"
                f"3️⃣ Enter your Roblox username\n"
                f"4️⃣ Add the code to your Roblox profile\n"
                f"5️⃣ Click verify!\n\n"
                f"**After verifying, you'll receive:**\n"
                f"• ✅ Verified role\n"
                f"• ✅ Abyssbound role (full server access)\n"
                f"• ✅ Your nickname set to your Roblox name\n\n"
                f"See you inside! ⚔️"
            ),
            color=0x2ecc71
        )
        embed.set_thumbnail(url=member.guild.icon.url if member.guild.icon else None)
        await member.send(embed=embed)
    except:
        pass  # Can't DM user
    
    # Log the join
    await log_action(member.guild, "👋 Member Joined", f"{member.mention} joined the server\nAccount created: <t:{int(member.created_at.timestamp())}:R>", 0x3498db)

@bot.event
async def on_message(message):
    if not message.author.bot and message.guild:
        xp = random.randint(*XP_TEXT_RANGE)
        add_xp_to_user(message.author.id, xp)
        await check_level_up(message.author.id, message.guild)
    await bot.process_commands(message)

@bot.event
async def on_reaction_add(reaction, user):
    if not user.bot and reaction.message.guild:
        xp = random.randint(*XP_REACTION_RANGE)
        add_xp_to_user(user.id, xp)
        await check_level_up(user.id, reaction.message.guild)

# ============================================
# COMMANDS - All work with both ! and /
# ============================================

@bot.command(name="sync")
@commands.has_permissions(administrator=True)
async def sync_cmd(ctx):
    """Sync slash commands to this server"""
    msg = await ctx.send("🔄 Syncing slash commands to this server...")
    try:
        synced = await bot.tree.sync(guild=ctx.guild)
        await msg.edit(content=f"✅ Synced {len(synced)} slash commands to this server!")
    except Exception as e:
        await msg.edit(content=f"❌ Sync failed: {e}")

# --- MEMBER COMMANDS ---

@bot.hybrid_command(name="verify", description="Verify with your Roblox account")
async def verify(ctx):
    """Verify and link your Roblox account (secure verification)"""
    mem = discord.utils.get(ctx.guild.roles, name=MEMBER_ROLE_NAME)
    
    if mem and mem in ctx.author.roles:
        # Already verified - show update option
        user_data = get_user_data(ctx.author.id)
        current = user_data.get('roblox_username', 'Not set')
        roblox_id = user_data.get('roblox_id', None)
        
        embed = discord.Embed(
            title="🔄 Already Verified",
            description="You're already verified! Use the button below to link a different Roblox account.",
            color=0x3498db
        )
        if roblox_id:
            embed.add_field(name="Current Roblox", value=f"[{current}](https://www.roblox.com/users/{roblox_id}/profile)", inline=True)
        else:
            embed.add_field(name="Current Roblox", value=current, inline=True)
        
        view = discord.ui.View(timeout=60)
        update_btn = discord.ui.Button(label="🔄 Update Roblox", style=discord.ButtonStyle.primary)
        
        async def update_callback(interaction: discord.Interaction):
            if interaction.user.id != ctx.author.id:
                return await interaction.response.send_message("❌ This isn't your verification!", ephemeral=True)
            await interaction.response.send_modal(VerifyUsernameModal())
        
        update_btn.callback = update_callback
        view.add_item(update_btn)
        
        await ctx.send(embed=embed, view=view, ephemeral=True)
    else:
        # Not verified - show verification embed
        embed = discord.Embed(
            title="🔗 Secure Roblox Verification",
            description=(
                "Welcome! To access the server, you need to verify your Roblox account.\n\n"
                "**How it works:**\n"
                "1️⃣ Enter your Roblox username\n"
                "2️⃣ Add a unique code to your Roblox profile\n"
                "3️⃣ Click verify - we'll check your profile!\n\n"
                "**What happens when you verify:**\n"
                "• ✅ Your nickname will be set to your Roblox username\n"
                "• ✅ You'll get access to all server channels\n"
                "• ✅ Your account is securely linked\n\n"
                "Click the button below to begin!"
            ),
            color=0x2ecc71
        )
        embed.set_footer(text="This prevents impersonation - only you can verify your account!")
        
        view = discord.ui.View(timeout=120)
        verify_btn = discord.ui.Button(label="🔗 Start Verification", style=discord.ButtonStyle.success)
        
        async def verify_callback(interaction: discord.Interaction):
            if interaction.user.id != ctx.author.id:
                return await interaction.response.send_message("❌ This isn't your verification!", ephemeral=True)
            await interaction.response.send_modal(VerifyUsernameModal())
        
        verify_btn.callback = verify_callback
        view.add_item(verify_btn)
        
        await ctx.send(embed=embed, view=view, ephemeral=True)

@bot.hybrid_command(name="level", description="Check your level and XP")
async def level(ctx, member: discord.Member = None):
    """Display your Fallen level card"""
    target = member or ctx.author
    user_data = get_user_data(target.id)
    rank = get_level_rank(target.id)
    
    # Try to create image card if PIL is available
    if PIL_AVAILABLE:
        try:
            card_image = await create_level_card_image(target, user_data, rank)
            if card_image:
                file = discord.File(card_image, filename="level_card.png")
                await ctx.send(file=file)
                return
        except Exception as e:
            print(f"Level card image error: {e}")
    
    # Fallback to embed if PIL fails
    embed = create_arcane_level_embed(target, user_data, rank)
    await ctx.send(embed=embed)

@bot.hybrid_command(name="setlevelbackground", description="Admin: Set the level card banner image")
@commands.has_permissions(administrator=True)
async def setlevelbackground(ctx, url: str = None):
    """Set the level card banner image URL (shown at bottom of card)"""
    global LEVEL_CARD_BACKGROUND
    
    if url is None or url.lower() == "none":
        LEVEL_CARD_BACKGROUND = None
        await ctx.send("✅ Level card banner removed.", ephemeral=True)
    else:
        LEVEL_CARD_BACKGROUND = url
        await ctx.send(f"✅ Level card banner set! Test it with `/level`", ephemeral=True)

@bot.hybrid_command(name="levelcard_debug", description="Admin: Debug level card system")
@commands.has_permissions(administrator=True)
async def levelcard_debug(ctx):
    """Debug the level card system"""
    debug_info = []
    
    # Check PIL
    debug_info.append(f"**PIL Available:** {PIL_AVAILABLE}")
    
    # Check background URL
    debug_info.append(f"**Background URL Set:** {LEVEL_CARD_BACKGROUND is not None}")
    if LEVEL_CARD_BACKGROUND:
        debug_info.append(f"**URL:** {LEVEL_CARD_BACKGROUND[:50]}...")
    
    # Check local files
    debug_info.append(f"\n**Checking local paths:**")
    for path in LEVEL_CARD_PATHS:
        exists = os.path.exists(path)
        debug_info.append(f"• `{path}`: {'✅ Found' if exists else '❌ Not found'}")
    
    # Try to load from URL
    if PIL_AVAILABLE and LEVEL_CARD_BACKGROUND:
        debug_info.append(f"\n**Testing URL download:**")
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(LEVEL_CARD_BACKGROUND) as resp:
                    debug_info.append(f"• Status: {resp.status}")
                    if resp.status == 200:
                        img_data = await resp.read()
                        debug_info.append(f"• Downloaded: {len(img_data)} bytes")
                        try:
                            from PIL import Image
                            img = Image.open(BytesIO(img_data))
                            debug_info.append(f"• Image size: {img.size}")
                            debug_info.append(f"• ✅ Image loaded successfully!")
                        except Exception as e:
                            debug_info.append(f"• ❌ Image load error: {e}")
                    else:
                        debug_info.append(f"• ❌ Failed to download")
        except Exception as e:
            debug_info.append(f"• ❌ URL Error: {e}")
    
    embed = discord.Embed(
        title="🔧 Level Card Debug",
        description="\n".join(debug_info),
        color=0x3498db
    )
    await ctx.send(embed=embed)

@bot.hybrid_command(name="leaderboard", aliases=["lb"], description="View the XP leaderboard")
async def leaderboard(ctx):
    """Display the XP leaderboard with Fallen background"""
    users = load_data()["users"]
    
    # Try to create image leaderboard
    if PIL_AVAILABLE:
        try:
            lb_image = await create_leaderboard_image(ctx.guild, users, "xp", "Overall XP")
            if lb_image:
                file = discord.File(lb_image, filename="leaderboard.png")
                await ctx.send(file=file, view=LeaderboardViewUI())
                return
        except Exception as e:
            print(f"Leaderboard image error: {e}")
    
    # Fallback to embed
    embed = create_arcane_leaderboard_embed(ctx.guild, users)
    await ctx.send(embed=embed, view=LeaderboardViewUI())

@bot.hybrid_command(name="fcoins", description="Check your Fallen Coins balance")
async def fcoins(ctx):
    """Display your coin balance"""
    coins = get_user_data(ctx.author.id)['coins']
    embed = discord.Embed(
        description=f"💰 **{ctx.author.display_name}** has **{coins:,}** Fallen Coins",
        color=0xF1C40F
    )
    await ctx.send(embed=embed)

@bot.hybrid_command(name="help", description="Get help with bot commands")
async def help_cmd(ctx):
    """Display help information"""
    embed = discord.Embed(
        title="🤖 Fallen Bot Help",
        description="Select a category below to view commands:\n\n**All commands work with both `!` and `/`**",
        color=0x3498db
    )
    embed.add_field(name="👤 Member", value="Basic commands", inline=True)
    embed.add_field(name="💰 Economy", value="Coins & rewards", inline=True)
    embed.add_field(name="🏴‍☠️ Raids & Wars", value="Clan battles", inline=True)
    embed.add_field(name="📚 Training", value="Events & tryouts", inline=True)
    embed.add_field(name="🛡️ Staff", value="Moderation", inline=True)
    embed.add_field(name="⚙️ Admin", value="Setup & config", inline=True)
    await ctx.send(embed=embed, view=HelpView())

# --- STAFF COMMANDS ---

@bot.hybrid_command(name="checklevel", description="Staff: Check another user's level")
async def checklevel(ctx, member: discord.Member):
    """Staff command to check another user's stats"""
    if not is_staff(ctx.author):
        return await ctx.send("❌ Staff only.", ephemeral=True)
    
    user_data = get_user_data(member.id)
    lvl = user_data['level']
    xp = user_data['xp']
    coins = user_data['coins']
    req = calculate_next_level_xp(lvl)
    rank = get_level_rank(member.id)
    
    embed = discord.Embed(color=0x2F3136, title="👤 User Stats (Staff View)")
    embed.set_author(name=f"@{member.name}", icon_url=member.display_avatar.url)
    embed.add_field(name="📊 Level", value=str(lvl), inline=True)
    embed.add_field(name="✨ XP", value=f"{format_number(xp)} / {format_number(req)}", inline=True)
    embed.add_field(name="🏆 Rank", value=f"#{rank}", inline=True)
    embed.add_field(name="💰 Coins", value=f"{coins:,}", inline=True)
    embed.add_field(name="📅 Weekly XP", value=f"{format_number(user_data.get('weekly_xp', 0))}", inline=True)
    embed.add_field(name="📆 Monthly XP", value=f"{format_number(user_data.get('monthly_xp', 0))}", inline=True)
    embed.set_thumbnail(url=member.display_avatar.url)
    
    await ctx.send(embed=embed)

@bot.hybrid_command(name="addxp", description="Staff: Add XP to a user")
async def addxp(ctx, member: discord.Member, amount: int):
    """Add XP to a user"""
    if not is_staff(ctx.author):
        return await ctx.send("❌ Staff only.", ephemeral=True)
    
    new_xp = add_xp_to_user(member.id, amount)
    await ctx.send(f"✅ Added **{amount:,} XP** to {member.mention}. Total: **{new_xp:,} XP**")
    await log_action(ctx.guild, "✨ XP Added", f"{member.mention} received +{amount:,} XP from {ctx.author.mention}", 0xF1C40F)
    await check_level_up(member.id, ctx.guild)

@bot.hybrid_command(name="removexp", description="Staff: Remove XP from a user")
async def removexp(ctx, member: discord.Member, amount: int):
    """Remove XP from a user"""
    if not is_staff(ctx.author):
        return await ctx.send("❌ Staff only.", ephemeral=True)
    
    new_xp = add_xp_to_user(member.id, -amount)
    await ctx.send(f"✅ Removed **{amount:,} XP** from {member.mention}. Total: **{new_xp:,} XP**")
    await log_action(ctx.guild, "✨ XP Removed", f"{member.mention} lost -{amount:,} XP by {ctx.author.mention}", 0xe74c3c)

@bot.hybrid_command(name="addfcoins", description="Staff: Add coins to a user")
async def addfcoins(ctx, member: discord.Member, amount: int):
    """Add coins to a user"""
    if not is_staff(ctx.author):
        return await ctx.send("❌ Staff only.", ephemeral=True)
    
    new_coins = add_user_stat(member.id, "coins", amount)
    await ctx.send(f"✅ Added **{amount:,} coins** to {member.mention}. Total: **{new_coins:,}**")
    await log_action(ctx.guild, "💰 Coins Added", f"{member.mention} received +{amount:,} coins from {ctx.author.mention}", 0xF1C40F)

@bot.hybrid_command(name="removefcoins", description="Staff: Remove coins from a user")
async def removefcoins(ctx, member: discord.Member, amount: int):
    """Remove coins from a user"""
    if not is_staff(ctx.author):
        return await ctx.send("❌ Staff only.", ephemeral=True)
    
    new_coins = add_user_stat(member.id, "coins", -amount)
    await ctx.send(f"✅ Removed **{amount:,} coins** from {member.mention}. Total: **{new_coins:,}**")
    await log_action(ctx.guild, "💰 Coins Removed", f"{member.mention} lost -{amount:,} coins by {ctx.author.mention}", 0xe74c3c)

@bot.hybrid_command(name="levelchange", description="Staff: Set a user's level")
async def levelchange(ctx, member: discord.Member, level: int):
    """Set a user's level directly"""
    if not is_staff(ctx.author):
        return await ctx.send("❌ Staff only.", ephemeral=True)
    
    update_user_data(member.id, "level", level)
    await ctx.send(f"✅ Set {member.mention}'s level to **{level}**")
    await log_action(ctx.guild, "📊 Level Changed", f"{member.mention}'s level set to {level} by {ctx.author.mention}", 0xF1C40F)

@bot.hybrid_command(name="report_set", description="Staff: Report a set result")
async def report_set(ctx, winner: discord.Member, loser: discord.Member):
    """Report match results and update rankings"""
    if not is_staff(ctx.author):
        return await ctx.send("❌ Staff only.", ephemeral=True)
    
    changed = process_rank_update(winner.id, loser.id)
    desc = f"🏆 **Winner:** {winner.mention}\n💀 **Loser:** {loser.mention}"
    if changed:
        desc += "\n\n🚨 **RANK SWAP!**"
    
    await post_result(ctx.guild, SET_RESULTS_CHANNEL_NAME, "⚔️ Set Result", desc)
    
    embed = discord.Embed(title="✅ Set Reported", description=desc, color=0xF1C40F)
    await ctx.send(embed=embed)
    await log_action(ctx.guild, "⚔️ Set Reported", f"Reported by {ctx.author.mention}\n{desc}", 0xF1C40F)

@bot.hybrid_command(name="tstart", description="Staff: Start a tournament")
async def tstart(ctx, *, title: str = "Tournament"):
    """Start a new tournament"""
    if not is_staff(ctx.author):
        return await ctx.send("❌ Staff only.", ephemeral=True)
    
    if tournament_state["active"]:
        return await ctx.send("❌ A tournament is already active. Cancel it first.", ephemeral=True)
    
    await ctx.send(f"⚙️ Setting up **{title}**...\nSelect tournament type:", view=TournamentTypeView(title, ctx.author))

# --- ADMIN COMMANDS ---

@bot.hybrid_command(name="wipedata", description="Owner: Complete data wipe")
async def wipedata(ctx):
    """Complete data wipe - Owner only"""
    if not (ctx.author.id == ctx.guild.owner_id or is_high_staff(ctx.author)):
        return await ctx.send("❌ **Owner/High Staff only.** This command wipes ALL data.", ephemeral=True)
    
    embed = discord.Embed(
        title="⚠️ DATA WIPE CONFIRMATION",
        description="**This will permanently delete:**\n"
                    "• All user XP and levels\n"
                    "• All user coins\n"
                    "• All roster rankings\n"
                    "• Weekly/Monthly XP data\n\n"
                    "**This action CANNOT be undone!**",
        color=0xe74c3c
    )
    
    await ctx.send(embed=embed, view=DataWipeConfirmView(ctx.author), ephemeral=True)

@bot.hybrid_command(name="setup_verify", description="Admin: Set up the verification panel")
@commands.has_permissions(administrator=True)
async def setup_verify(ctx, channel: discord.TextChannel = None):
    """Create a verification panel in the specified channel"""
    target_channel = channel or ctx.channel
    
    embed = discord.Embed(
        title="🔗 Secure Roblox Verification",
        description=(
            "Welcome to **The Fallen**!\n\n"
            "**🔒 You currently have the Unverified role.**\n"
            "To access the server, you must verify your Roblox account.\n\n"
            "**📝 How Verification Works:**\n"
            "1️⃣ Click the button and enter your Roblox username\n"
            "2️⃣ Add a unique code to your Roblox profile description\n"
            "3️⃣ Click verify - we'll check your profile automatically!\n\n"
            "**✅ After verifying, you'll receive:**\n"
            "• 🏷️ **Verified** role\n"
            "• 🏷️ **Abyssbound** role (full server access)\n"
            "• 📝 Nickname set to your Roblox username\n"
            "• 🎮 Access to all channels & features\n\n"
            "**Click the button below to start!**"
        ),
        color=0x2ecc71
    )
    embed.set_footer(text="🛡️ Secure verification prevents impersonation")
    
    await target_channel.send(embed=embed, view=VerifyView())
    await ctx.send(f"✅ Verification panel posted in {target_channel.mention}", ephemeral=True)
    await log_action(ctx.guild, "📋 Verify Panel", f"Posted in {target_channel.mention} by {ctx.author.mention}", 0x2ecc71)

@bot.hybrid_command(name="setup_shop", description="Admin: Set up the shop panel")
@commands.has_permissions(administrator=True)
async def setup_shop(ctx):
    """Create the shop panel in the shop channel"""
    ch = discord.utils.get(ctx.guild.text_channels, name=SHOP_CHANNEL_NAME)
    if not ch:
        return await ctx.send(f"❌ Channel `{SHOP_CHANNEL_NAME}` not found. Create it first!", ephemeral=True)
    
    embed = discord.Embed(
        title="🛒 The Fallen Shop",
        description="Spend your hard-earned Fallen Coins here!",
        color=0xDC143C
    )
    for item in SHOP_ITEMS:
        embed.add_field(name=f"{item['name']} — {item['price']:,} 💰", value=item['desc'], inline=False)
    embed.set_footer(text="Click a button below to purchase")
    
    await ch.send(embed=embed, view=ShopView())
    await ctx.send(f"✅ Shop panel posted in {ch.mention}", ephemeral=True)

@bot.hybrid_command(name="top10_setup", description="Admin: Set up the Top 10 ranked leaderboard panel")
@commands.has_permissions(administrator=True)
async def top10_setup(ctx):
    """Create the Top 10 ranked leaderboard panel"""
    embed = create_leaderboard_embed(ctx.guild)
    await ctx.send(embed=embed, view=LeaderboardView())

# ==========================================
# CLAN ROSTER SYSTEM (EU Roster Style)
# ==========================================

# Store clan roster: [{"roblox": str, "discord_id": int, "position": int}, ...]
CLAN_ROSTER_FILE = "clan_roster.json"

def load_clan_roster():
    """Load clan roster from file"""
    try:
        with open(CLAN_ROSTER_FILE, "r") as f:
            return json.load(f)
    except:
        return {"members": [], "title": "✝ FALLEN ✝ - The Fallen Saints", "description": "Through shattered skies and broken crowns,\nThe descent carves its mark.\nFallen endures — not erased, but remade.\nIn ruin lies the seed of power.", "role_name": "Fallen", "image_url": None}

def save_clan_roster(data):
    """Save clan roster to file"""
    with open(CLAN_ROSTER_FILE, "w") as f:
        json.dump(data, f, indent=2)

def create_clan_roster_embed(guild):
    """Create the clan roster embed like the EU Roster image"""
    roster_data = load_clan_roster()
    
    embed = discord.Embed(
        title=roster_data.get("title", "✝ FALLEN ✝ - The Fallen Saints"),
        description=roster_data.get("description", ""),
        color=0x2b2d31
    )
    
    # Add image if set
    if roster_data.get("image_url"):
        embed.set_image(url=roster_data["image_url"])
    
    # Add role mention
    role_name = roster_data.get("role_name", "Fallen")
    role = discord.utils.get(guild.roles, name=role_name)
    if role:
        embed.add_field(name="Role:", value=role.mention, inline=False)
    
    # Add members
    members = roster_data.get("members", [])
    if members:
        # Sort by position
        members_sorted = sorted(members, key=lambda x: x.get("position", 999))
        
        roster_text = ""
        for m in members_sorted:
            roblox_name = m.get("roblox", "Unknown")
            discord_id = m.get("discord_id")
            
            if discord_id:
                member = guild.get_member(discord_id)
                discord_mention = f"@{member.name}" if member else f"<@{discord_id}>"
            else:
                discord_mention = "Unknown"
            
            roster_text += f"✝ **{roblox_name}** 🦇🦇\n| {discord_mention} |\n\n"
        
        embed.add_field(name="\u200b", value=roster_text, inline=False)
    else:
        embed.add_field(name="Members", value="No members added yet. Use `/roster_add` to add members.", inline=False)
    
    return embed

class ClanRosterView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)
    
    @discord.ui.button(label="🔄 Refresh", style=discord.ButtonStyle.secondary, custom_id="clan_roster_refresh")
    async def refresh(self, interaction: discord.Interaction, button: discord.ui.Button):
        embed = create_clan_roster_embed(interaction.guild)
        await interaction.response.edit_message(embed=embed)

@bot.hybrid_command(name="setup_roster", description="Admin: Set up the clan roster panel (EU Roster style)")
@commands.has_permissions(administrator=True)
async def setup_roster(ctx, channel: discord.TextChannel = None):
    """Create the clan roster panel"""
    target_channel = channel or ctx.channel
    
    embed = create_clan_roster_embed(ctx.guild)
    await target_channel.send(embed=embed, view=ClanRosterView())
    await ctx.send(f"✅ Clan roster posted in {target_channel.mention}", ephemeral=True)

@bot.hybrid_command(name="roster_add", description="Admin: Add member to clan roster")
@commands.has_permissions(administrator=True)
async def roster_add_clan(ctx, member: discord.Member, roblox_name: str, position: int = None):
    """Add a member to the clan roster"""
    roster_data = load_clan_roster()
    
    # Check if already exists
    for m in roster_data["members"]:
        if m.get("discord_id") == member.id:
            # Update existing
            m["roblox"] = roblox_name
            if position:
                m["position"] = position
            save_clan_roster(roster_data)
            return await ctx.send(f"✅ Updated **{roblox_name}** ({member.mention}) on the roster!", ephemeral=True)
    
    # Add new
    new_position = position if position else len(roster_data["members"]) + 1
    roster_data["members"].append({
        "roblox": roblox_name,
        "discord_id": member.id,
        "position": new_position
    })
    save_clan_roster(roster_data)
    
    await ctx.send(f"✅ Added **{roblox_name}** ({member.mention}) to the roster at position {new_position}!", ephemeral=True)
    await log_action(ctx.guild, "📝 Roster Added", f"**{roblox_name}** ({member.mention}) added by {ctx.author.mention}", 0x2ecc71)

@bot.hybrid_command(name="roster_remove", description="Admin: Remove member from clan roster")
@commands.has_permissions(administrator=True)
async def roster_remove_clan(ctx, member: discord.Member):
    """Remove a member from the clan roster"""
    roster_data = load_clan_roster()
    
    for i, m in enumerate(roster_data["members"]):
        if m.get("discord_id") == member.id:
            removed = roster_data["members"].pop(i)
            save_clan_roster(roster_data)
            await ctx.send(f"✅ Removed **{removed['roblox']}** ({member.mention}) from the roster!", ephemeral=True)
            await log_action(ctx.guild, "📝 Roster Removed", f"**{removed['roblox']}** ({member.mention}) removed by {ctx.author.mention}", 0xe74c3c)
            return
    
    await ctx.send(f"❌ {member.mention} is not on the roster.", ephemeral=True)

@bot.hybrid_command(name="roster_set_title", description="Admin: Set roster title")
@commands.has_permissions(administrator=True)
async def roster_set_title(ctx, *, title: str):
    """Set the clan roster title"""
    roster_data = load_clan_roster()
    roster_data["title"] = title
    save_clan_roster(roster_data)
    await ctx.send(f"✅ Roster title set to: **{title}**", ephemeral=True)

@bot.hybrid_command(name="roster_set_description", description="Admin: Set roster description")
@commands.has_permissions(administrator=True)
async def roster_set_description(ctx, *, description: str):
    """Set the clan roster description (use \\n for new lines)"""
    roster_data = load_clan_roster()
    roster_data["description"] = description.replace("\\n", "\n")
    save_clan_roster(roster_data)
    await ctx.send(f"✅ Roster description updated!", ephemeral=True)

@bot.hybrid_command(name="roster_set_image", description="Admin: Set roster banner image URL")
@commands.has_permissions(administrator=True)
async def roster_set_image(ctx, url: str):
    """Set the clan roster banner image"""
    roster_data = load_clan_roster()
    roster_data["image_url"] = url
    save_clan_roster(roster_data)
    await ctx.send(f"✅ Roster image set!", ephemeral=True)

@bot.hybrid_command(name="roster_set_role", description="Admin: Set the roster role name")
@commands.has_permissions(administrator=True)
async def roster_set_role(ctx, role_name: str):
    """Set the role shown on the roster"""
    roster_data = load_clan_roster()
    roster_data["role_name"] = role_name
    save_clan_roster(roster_data)
    await ctx.send(f"✅ Roster role set to: **{role_name}**", ephemeral=True)

@bot.hybrid_command(name="roster_list", description="View all roster members")
async def roster_list(ctx):
    """View the current clan roster"""
    embed = create_clan_roster_embed(ctx.guild)
    await ctx.send(embed=embed)

@bot.hybrid_command(name="ticket_panel", description="Admin: Set up the challenge ticket panel")
@commands.has_permissions(administrator=True)
async def ticket_panel(ctx):
    """Create the challenge request panel"""
    embed = discord.Embed(
        title="⚔️ Challenge Ticket",
        description=(
            "• Want to challenge someone for their rank?\n"
            "• Click the button below to submit a challenge request."
        ),
        color=0x2b2d31  # Dark theme color
    )
    await ctx.send(embed=embed, view=ChallengeRequestView())

@bot.hybrid_command(name="apply_panel", description="Admin: Set up the application panel")
@commands.has_permissions(administrator=True)
async def apply_panel(ctx, channel: discord.TextChannel = None):
    """Create the comprehensive application panel"""
    target_channel = channel or ctx.channel
    
    embed = discord.Embed(
        title="📋 Staff Applications",
        description=(
            "Want to join our team? Apply for one of these positions!\n\n"
            "**Available Positions:**\n"
            "🎯 **Tryout Host** - Host tryouts for new members\n"
            "🛡️ **Moderator** - Help moderate the server\n"
            "📚 **Training Host** - Host training sessions\n\n"
            "**Application Process:**\n"
            "1️⃣ Check if you meet the requirements\n"
            "2️⃣ Fill out the application form\n"
            "3️⃣ Wait for staff to review & vote\n"
            "4️⃣ Interview (if required)\n"
            "5️⃣ Get accepted or feedback!\n\n"
            "**Click below to start your application!**"
        ),
        color=0x2ecc71
    )
    embed.set_footer(text="📊 Requirements are checked automatically")
    
    await target_channel.send(embed=embed, view=ApplicationPanelView())
    await ctx.send(f"✅ Application panel posted in {target_channel.mention}", ephemeral=True)

@bot.hybrid_command(name="app_status", description="Check your application status")
async def app_status(ctx):
    """Check your current application status"""
    if ctx.author.id not in applications_data:
        return await ctx.send("❌ You don't have any pending applications.", ephemeral=True)
    
    app_data = applications_data[ctx.author.id]
    config = APPLICATION_TYPES.get(app_data["type"], APPLICATION_TYPES["tryout_host"])
    
    status_emoji = {
        "pending": "🟡",
        "under_review": "🟠",
        "interview": "🟣",
        "accepted": "🟢",
        "denied": "🔴"
    }
    
    embed = discord.Embed(
        title=f"{config['emoji']} Your {config['name']} Application",
        color=config["color"]
    )
    
    status = app_data.get("status", "pending")
    embed.add_field(name="Status", value=f"{status_emoji.get(status, '⚪')} {status.replace('_', ' ').title()}", inline=True)
    
    votes = app_data.get("votes", {"approve": [], "deny": []})
    embed.add_field(name="Votes", value=f"✅ {len(votes['approve'])} | ❌ {len(votes['deny'])} / {config['votes_required']} needed", inline=True)
    
    created = app_data.get("created")
    if created:
        try:
            created_dt = datetime.datetime.fromisoformat(created)
            embed.add_field(name="Submitted", value=f"<t:{int(created_dt.timestamp())}:R>", inline=True)
        except:
            pass
    
    # Show notes if any
    notes = app_data.get("notes", [])
    if notes:
        notes_text = "\n".join([f"• {n['note'][:50]}..." for n in notes[-3:]])
        embed.add_field(name="Staff Notes", value=notes_text, inline=False)
    
    await ctx.send(embed=embed, ephemeral=True)

# ==========================================
# NEW FEATURES - PROFILE, DAILY, STATS
# ==========================================

@bot.hybrid_command(name="profile", description="View your or another user's full profile")
async def profile(ctx, member: discord.Member = None):
    """Display detailed profile"""
    target = member or ctx.author
    user_data = get_user_data(target.id)
    rank = get_level_rank(target.id)
    
    embed = discord.Embed(title=f"⚔️ {target.display_name}'s Profile", color=0x2F3136)
    embed.set_thumbnail(url=target.display_avatar.url)
    embed.add_field(name="🎮 Roblox", value=user_data.get('roblox_username') or "Not linked", inline=True)
    embed.add_field(name="🏆 Rank", value=f"#{rank}", inline=True)
    embed.add_field(name="📊 Level", value=str(user_data.get('level', 0)), inline=True)
    embed.add_field(name="✨ Total XP", value=format_number(user_data.get('xp', 0)), inline=True)
    embed.add_field(name="💰 Coins", value=f"{user_data.get('coins', 0):,}", inline=True)
    embed.add_field(name="🔥 Streak", value=f"{user_data.get('daily_streak', 0)} days", inline=True)
    
    w, l = user_data.get('wins', 0), user_data.get('losses', 0)
    total = w + l
    wr = round((w / total) * 100, 1) if total > 0 else 0
    embed.add_field(name="⚔️ W/L Record", value=f"{w}W - {l}L ({wr}%)", inline=True)
    
    rw, rl, rp = user_data.get('raid_wins', 0), user_data.get('raid_losses', 0), user_data.get('raid_participation', 0)
    embed.add_field(name="🏴‍☠️ Raids", value=f"{rw}W - {rl}L ({rp} total)", inline=True)
    embed.add_field(name="📚 Training", value=f"{user_data.get('training_attendance', 0)} attended", inline=True)
    embed.add_field(name="⚠️ Warnings", value=f"{len(user_data.get('warnings', []))}/3", inline=True)
    
    await ctx.send(embed=embed)

@bot.hybrid_command(name="stats", description="View combat stats")
async def stats(ctx, member: discord.Member = None):
    """Display W/L stats"""
    target = member or ctx.author
    user_data = get_user_data(target.id)
    
    w, l = user_data.get('wins', 0), user_data.get('losses', 0)
    total = w + l
    wr = round((w / total) * 100, 1) if total > 0 else 0
    
    embed = discord.Embed(title=f"⚔️ {target.display_name}'s Combat Stats", color=0xFF4500)
    embed.set_thumbnail(url=target.display_avatar.url)
    embed.add_field(name="🏆 Wins", value=str(w), inline=True)
    embed.add_field(name="💀 Losses", value=str(l), inline=True)
    embed.add_field(name="📊 Win Rate", value=f"{wr}%", inline=True)
    
    rw, rl = user_data.get('raid_wins', 0), user_data.get('raid_losses', 0)
    rtotal = rw + rl
    rwr = round((rw / rtotal) * 100, 1) if rtotal > 0 else 0
    embed.add_field(name="🏴‍☠️ Raid W/L", value=f"{rw}W - {rl}L ({rwr}%)", inline=False)
    
    await ctx.send(embed=embed)

@bot.hybrid_command(name="link_roblox", description="Update your linked Roblox account (secure)")
async def link_roblox(ctx):
    """Start secure Roblox re-verification process"""
    embed = discord.Embed(
        title="🔄 Update Roblox Account",
        description="To update your linked Roblox account, you'll need to verify ownership again.\n\nClick the button below to start!",
        color=0x3498db
    )
    
    user_data = get_user_data(ctx.author.id)
    current = user_data.get('roblox_username', 'Not set')
    embed.add_field(name="Current Account", value=current, inline=True)
    
    view = discord.ui.View(timeout=60)
    btn = discord.ui.Button(label="🔄 Update Account", style=discord.ButtonStyle.primary)
    
    async def callback(interaction: discord.Interaction):
        if interaction.user.id != ctx.author.id:
            return await interaction.response.send_message("❌ Not your request!", ephemeral=True)
        await interaction.response.send_modal(VerifyUsernameModal())
    
    btn.callback = callback
    view.add_item(btn)
    
    await ctx.send(embed=embed, view=view, ephemeral=True)

@bot.hybrid_command(name="update_roblox", description="Update your Roblox username (secure verification)")
async def update_roblox(ctx):
    """Update Roblox username via secure modal"""
    await ctx.interaction.response.send_modal(VerifyUsernameModal())

@bot.hybrid_command(name="daily", description="Claim your daily reward")
async def daily(ctx):
    """Claim daily coins and XP"""
    user_data = get_user_data(ctx.author.id)
    now = datetime.datetime.now(datetime.timezone.utc)
    last = user_data.get('last_daily')
    
    if last:
        try:
            last_dt = datetime.datetime.fromisoformat(last)
            if last_dt.tzinfo is None:
                last_dt = last_dt.replace(tzinfo=datetime.timezone.utc)
            diff = (now - last_dt).total_seconds()
            if diff < 86400:
                h, m = int((86400 - diff) // 3600), int(((86400 - diff) % 3600) // 60)
                return await ctx.send(f"⏰ You can claim your daily in **{h}h {m}m**", ephemeral=True)
            streak = user_data.get('daily_streak', 0) + 1 if diff < 172800 else 1
        except:
            streak = 1
    else:
        streak = 1
    
    base_coins, base_xp = 100, 50
    bonus = min(streak, 30)
    coins = base_coins + (25 * bonus)
    xp = base_xp + (10 * bonus)
    
    add_user_stat(ctx.author.id, "coins", coins)
    add_xp_to_user(ctx.author.id, xp)
    update_user_data(ctx.author.id, "last_daily", now.isoformat())
    update_user_data(ctx.author.id, "daily_streak", streak)
    
    embed = discord.Embed(title="🎁 Daily Reward Claimed!", description=f"**+{coins}** 💰 Fallen Coins\n**+{xp}** ✨ XP", color=0x2ecc71)
    embed.add_field(name="🔥 Streak", value=f"{streak} days", inline=True)
    if streak > 1:
        embed.set_footer(text=f"Streak bonus: +{25 * bonus} coins, +{10 * bonus} XP")
    
    await ctx.send(embed=embed)
    await check_level_up(ctx.author.id, ctx.guild)

@bot.hybrid_command(name="schedule", description="View upcoming events")
async def schedule(ctx):
    """View scheduled trainings, tryouts, and raids"""
    data = load_data()
    embed = discord.Embed(title="📅 Upcoming Events", color=0x3498db)
    
    raids = data.get("raids", [])[-5:]
    raid_text = "\n".join([f"• **{r.get('target', 'Unknown')}** - {r.get('time', 'TBD')}" for r in raids]) or "None scheduled"
    embed.add_field(name="🏴‍☠️ Raids", value=raid_text, inline=False)
    
    trainings = data.get("trainings", [])[-5:]
    training_text = "\n".join([f"• **{t.get('type', 'Training')}** - {t.get('time', 'TBD')}" for t in trainings]) or "None scheduled"
    embed.add_field(name="📚 Trainings", value=training_text, inline=False)
    
    tryouts = data.get("tryouts", [])[-5:]
    tryout_text = "\n".join([f"• **{t.get('type', 'Tryout')}** - {t.get('time', 'TBD')}" for t in tryouts]) or "None scheduled"
    embed.add_field(name="🎯 Tryouts", value=tryout_text, inline=False)
    
    await ctx.send(embed=embed)

# ==========================================
# RAID & WAR COMMANDS
# ==========================================

@bot.hybrid_command(name="raid_lb", description="View raid leaderboard")
async def raid_lb(ctx):
    """Display raid leaderboard"""
    users = load_data()["users"]
    sorted_users = sorted(users.items(), key=lambda x: x[1].get('raid_wins', 0), reverse=True)[:10]
    
    embed = discord.Embed(title="🏴‍☠️ Raid Leaderboard", color=0xFF4500)
    lines = []
    for i, (uid, stats) in enumerate(sorted_users, 1):
        m = ctx.guild.get_member(int(uid))
        name = f"@{m.name}" if m else f"@user_{uid[:8]}"
        rw, rl = stats.get('raid_wins', 0), stats.get('raid_losses', 0)
        total = rw + rl
        wr = round((rw / total) * 100, 1) if total > 0 else 0
        rank = "🥇" if i == 1 else "🥈" if i == 2 else "🥉" if i == 3 else f"**#{i}**"
        lines.append(f"{rank} • {name} • {rw}W-{rl}L ({wr}%)")
    
    embed.description = "\n".join(lines) or "No raid data yet."
    await ctx.send(embed=embed)

@bot.hybrid_command(name="wars", description="View active and past wars")
async def wars(ctx):
    """Display war information"""
    wars_data = load_data().get("wars", {})
    embed = discord.Embed(title="⚔️ Clan Wars", color=0xFF4500)
    
    if not wars_data:
        embed.description = "No war records yet."
    else:
        lines = []
        for clan, record in list(wars_data.items())[:10]:
            status = "🔴 Active" if record.get('active') else "⚪ Ended"
            lines.append(f"**vs {clan}** - {record['wins']}W/{record['losses']}L {status}")
        embed.description = "\n".join(lines)
    
    await ctx.send(embed=embed)

@bot.hybrid_command(name="war_record", description="View record against a specific clan")
async def war_record(ctx, clan_name: str):
    """View war record against specific clan"""
    wars = load_data().get("wars", {})
    record = None
    for c, r in wars.items():
        if c.lower() == clan_name.lower():
            record = r
            clan_name = c
            break
    
    if not record:
        return await ctx.send(f"❌ No war record found against **{clan_name}**", ephemeral=True)
    
    total = record['wins'] + record['losses']
    wr = round((record['wins'] / total) * 100, 1) if total > 0 else 0
    
    embed = discord.Embed(title=f"⚔️ War Record vs {clan_name}", color=0xFF4500)
    embed.add_field(name="🏆 Wins", value=str(record['wins']), inline=True)
    embed.add_field(name="💀 Losses", value=str(record['losses']), inline=True)
    embed.add_field(name="📊 Win Rate", value=f"{wr}%", inline=True)
    
    await ctx.send(embed=embed)

@bot.hybrid_command(name="raid_call", description="Staff: Call a raid")
async def raid_call(ctx, target: str, time: str, *, requirements: str = "None"):
    """Call a raid"""
    if not is_staff(ctx.author):
        return await ctx.send("❌ Staff only.", ephemeral=True)
    
    data = load_data()
    raid_id = f"raid_{int(datetime.datetime.now().timestamp())}"
    
    if "raids" not in data:
        data["raids"] = []
    
    data["raids"].append({
        "id": raid_id, "target": target, "time": time,
        "requirements": requirements, "host": ctx.author.id, "participants": []
    })
    save_data(data)
    
    embed = discord.Embed(title="🏴‍☠️ RAID CALL", description=f"**Target:** {target}\n**Time:** {time}\n**Requirements:** {requirements}", color=0xFF4500)
    embed.add_field(name="Host", value=ctx.author.mention, inline=True)
    embed.set_footer(text="React ⚔️ to join!")
    
    await ctx.send(embed=embed)
    await log_action(ctx.guild, "🏴‍☠️ Raid Called", f"Target: {target}\nTime: {time}\nHost: {ctx.author.mention}", 0xFF4500)

@bot.hybrid_command(name="raid_log", description="Staff: Log raid results")
async def raid_log(ctx, result: str):
    """Log raid results (win/loss) - mention participants"""
    if not is_staff(ctx.author):
        return await ctx.send("❌ Staff only.", ephemeral=True)
    
    if result.lower() not in ['win', 'w', 'loss', 'l']:
        return await ctx.send("❌ Result must be `win` or `loss`", ephemeral=True)
    
    is_win = result.lower() in ['win', 'w']
    mentioned = ctx.message.mentions if hasattr(ctx, 'message') else []
    
    for m in mentioned:
        add_user_stat(m.id, "raid_participation", 1)
        add_user_stat(m.id, "raid_wins" if is_win else "raid_losses", 1)
    
    result_text = "🏆 WIN" if is_win else "💀 LOSS"
    embed = discord.Embed(title=f"🏴‍☠️ Raid Result: {result_text}", description=f"**Participants:** {len(mentioned)} raiders", color=0x2ecc71 if is_win else 0xe74c3c)
    await ctx.send(embed=embed)
    await log_action(ctx.guild, f"🏴‍☠️ Raid {result_text}", f"Participants: {len(mentioned)}", 0xFF4500)

@bot.hybrid_command(name="war_declare", description="Staff: Declare war on a clan")
async def war_declare(ctx, clan_name: str):
    """Declare war on another clan"""
    if not is_high_staff(ctx.author):
        return await ctx.send("❌ High Staff only.", ephemeral=True)
    
    data = load_data()
    if "wars" not in data:
        data["wars"] = {}
    
    if clan_name in data["wars"]:
        data["wars"][clan_name]["active"] = True
    else:
        data["wars"][clan_name] = {"wins": 0, "losses": 0, "active": True}
    save_data(data)
    
    embed = discord.Embed(title="⚔️ WAR DECLARED", description=f"**The Fallen** has declared war on **{clan_name}**!", color=0xFF0000)
    await ctx.send(embed=embed)
    await log_action(ctx.guild, "⚔️ War Declared", f"vs {clan_name} by {ctx.author.mention}", 0xFF0000)

@bot.hybrid_command(name="war_result", description="Staff: Log a war result")
async def war_result(ctx, clan_name: str, result: str):
    """Log war result"""
    if not is_staff(ctx.author):
        return await ctx.send("❌ Staff only.", ephemeral=True)
    
    if result.lower() not in ['win', 'w', 'loss', 'l']:
        return await ctx.send("❌ Result must be `win` or `loss`", ephemeral=True)
    
    data = load_data()
    if "wars" not in data:
        data["wars"] = {}
    if clan_name not in data["wars"]:
        data["wars"][clan_name] = {"wins": 0, "losses": 0, "active": False}
    
    is_win = result.lower() in ['win', 'w']
    data["wars"][clan_name]["wins" if is_win else "losses"] += 1
    save_data(data)
    
    r = data["wars"][clan_name]
    result_text = "🏆 WIN" if is_win else "💀 LOSS"
    embed = discord.Embed(title=f"⚔️ vs {clan_name}: {result_text}", description=f"**Record:** {r['wins']}W - {r['losses']}L", color=0x2ecc71 if is_win else 0xe74c3c)
    await ctx.send(embed=embed)

@bot.hybrid_command(name="scrim", description="Staff: Schedule a scrim")
async def scrim(ctx, opponent: str, time: str, *, details: str = ""):
    """Schedule a scrim"""
    if not is_staff(ctx.author):
        return await ctx.send("❌ Staff only.", ephemeral=True)
    
    embed = discord.Embed(title="🎮 SCRIM SCHEDULED", description=f"**Opponent:** {opponent}\n**Time:** {time}", color=0x9b59b6)
    if details:
        embed.add_field(name="Details", value=details, inline=False)
    embed.add_field(name="Scheduled by", value=ctx.author.mention, inline=True)
    await ctx.send(embed=embed)

# ==========================================
# TRAINING & TRYOUT COMMANDS
# ==========================================

@bot.hybrid_command(name="schedule_training", description="Staff: Schedule a training session")
async def schedule_training(ctx, training_type: str, time: str):
    """Schedule a training session"""
    if not is_staff(ctx.author):
        return await ctx.send("❌ Staff only.", ephemeral=True)
    
    data = load_data()
    if "trainings" not in data:
        data["trainings"] = []
    
    data["trainings"].append({
        "type": training_type, "time": time,
        "host": ctx.author.display_name, "participants": []
    })
    save_data(data)
    
    embed = discord.Embed(title="📚 TRAINING SCHEDULED", description=f"**Type:** {training_type}\n**Time:** {time}\n**Host:** {ctx.author.mention}", color=0x3498db)
    await ctx.send(embed=embed)
    await log_action(ctx.guild, "📚 Training Scheduled", f"Type: {training_type}\nTime: {time}", 0x3498db)

@bot.hybrid_command(name="training_log", description="Staff: Log training attendance")
async def training_log(ctx):
    """Log training attendance - mention attendees"""
    if not is_staff(ctx.author):
        return await ctx.send("❌ Staff only.", ephemeral=True)
    
    mentioned = ctx.message.mentions if hasattr(ctx, 'message') else []
    if not mentioned:
        return await ctx.send("❌ Please mention the attendees", ephemeral=True)
    
    for m in mentioned:
        add_user_stat(m.id, "training_attendance", 1)
        add_xp_to_user(m.id, 50)
        await check_level_up(m.id, ctx.guild)
    
    embed = discord.Embed(title="📚 Training Logged", description=f"**Attendees:** {len(mentioned)}\n**XP Awarded:** 50 each", color=0x2ecc71)
    await ctx.send(embed=embed)

@bot.hybrid_command(name="schedule_tryout", description="Staff: Schedule a tryout")
async def schedule_tryout(ctx, tryout_type: str, time: str):
    """Schedule a tryout"""
    if not is_staff(ctx.author):
        return await ctx.send("❌ Staff only.", ephemeral=True)
    
    data = load_data()
    if "tryouts" not in data:
        data["tryouts"] = []
    
    data["tryouts"].append({
        "type": tryout_type, "time": time,
        "host": ctx.author.display_name, "participants": []
    })
    save_data(data)
    
    embed = discord.Embed(title="🎯 TRYOUT SCHEDULED", description=f"**Type:** {tryout_type}\n**Time:** {time}\n**Host:** {ctx.author.mention}", color=0xF1C40F)
    await ctx.send(embed=embed)

@bot.hybrid_command(name="tryout_result", description="Staff: Log tryout result")
async def tryout_result(ctx, member: discord.Member, result: str):
    """Log tryout result (pass/fail)"""
    if not is_staff(ctx.author):
        return await ctx.send("❌ Staff only.", ephemeral=True)
    
    if result.lower() not in ['pass', 'p', 'fail', 'f']:
        return await ctx.send("❌ Result must be `pass` or `fail`", ephemeral=True)
    
    passed = result.lower() in ['pass', 'p']
    add_user_stat(member.id, "tryout_passes" if passed else "tryout_fails", 1)
    
    result_text = "✅ PASSED" if passed else "❌ FAILED"
    embed = discord.Embed(title=f"🎯 Tryout Result: {result_text}", description=member.mention, color=0x2ecc71 if passed else 0xe74c3c)
    await ctx.send(embed=embed)
    await log_action(ctx.guild, f"🎯 Tryout Result", f"{member.mention}: {result_text}", 0x2ecc71 if passed else 0xe74c3c)

# ==========================================
# WARNING & MODERATION COMMANDS
# ==========================================

@bot.hybrid_command(name="warn", description="Staff: Warn a user")
async def warn(ctx, member: discord.Member, *, reason: str = "No reason provided"):
    """Warn a user"""
    if not is_staff(ctx.author):
        return await ctx.send("❌ Staff only.", ephemeral=True)
    
    data = load_data()
    uid = str(member.id)
    data = ensure_user_structure(data, uid)
    
    warning = {"reason": reason, "by": ctx.author.id, "date": datetime.datetime.now(datetime.timezone.utc).isoformat()}
    data["users"][uid]["warnings"].append(warning)
    save_data(data)
    
    count = len(data["users"][uid]["warnings"])
    embed = discord.Embed(title="⚠️ Warning Issued", description=f"{member.mention} has been warned.", color=0xFFA500)
    embed.add_field(name="Reason", value=reason, inline=False)
    embed.add_field(name="Total Warnings", value=f"{count}/3", inline=True)
    
    await ctx.send(embed=embed)
    await log_action(ctx.guild, "⚠️ Warning", f"{member.mention} by {ctx.author.mention}\nReason: {reason}", 0xFFA500)

@bot.hybrid_command(name="warnings", description="Staff: View a user's warnings")
async def warnings(ctx, member: discord.Member):
    """View user's warnings"""
    if not is_staff(ctx.author):
        return await ctx.send("❌ Staff only.", ephemeral=True)
    
    user_warnings = get_user_data(member.id).get("warnings", [])
    embed = discord.Embed(title=f"⚠️ {member.display_name}'s Warnings", color=0xFFA500)
    
    if not user_warnings:
        embed.description = "No warnings on record."
    else:
        for i, w in enumerate(user_warnings, 1):
            embed.add_field(name=f"Warning #{i}", value=f"**Reason:** {w['reason']}\n**By:** <@{w['by']}>", inline=False)
    
    embed.set_footer(text=f"Total: {len(user_warnings)}/3")
    await ctx.send(embed=embed)

@bot.hybrid_command(name="clearwarnings", description="Staff: Clear a user's warnings")
async def clearwarnings(ctx, member: discord.Member):
    """Clear all warnings"""
    if not is_high_staff(ctx.author):
        return await ctx.send("❌ High Staff only.", ephemeral=True)
    
    update_user_data(member.id, "warnings", [])
    await ctx.send(f"✅ Cleared all warnings for {member.mention}")
    await log_action(ctx.guild, "⚠️ Warnings Cleared", f"{member.mention} by {ctx.author.mention}", 0x2ecc71)

@bot.hybrid_command(name="promote", description="Staff: Promote a user")
async def promote(ctx, member: discord.Member, role: discord.Role):
    """Promote a user"""
    if not is_high_staff(ctx.author):
        return await ctx.send("❌ High Staff only.", ephemeral=True)
    
    try:
        await member.add_roles(role)
        await ctx.send(f"✅ {member.mention} has been promoted to {role.mention}!")
        await log_action(ctx.guild, "📈 Promotion", f"{member.mention} → {role.mention} by {ctx.author.mention}", 0x2ecc71)
    except discord.Forbidden:
        await ctx.send("❌ Cannot assign that role.", ephemeral=True)

@bot.hybrid_command(name="demote", description="Staff: Demote a user")
async def demote(ctx, member: discord.Member, role: discord.Role):
    """Demote a user"""
    if not is_high_staff(ctx.author):
        return await ctx.send("❌ High Staff only.", ephemeral=True)
    
    try:
        await member.remove_roles(role)
        await ctx.send(f"✅ {member.mention} has been demoted from {role.mention}!")
        await log_action(ctx.guild, "📉 Demotion", f"{member.mention} lost {role.mention} by {ctx.author.mention}", 0xe74c3c)
    except discord.Forbidden:
        await ctx.send("❌ Cannot remove that role.", ephemeral=True)

@bot.hybrid_command(name="announce", description="Staff: Send an announcement")
async def announce(ctx, channel: discord.TextChannel, *, message: str):
    """Send formatted announcement"""
    if not is_staff(ctx.author):
        return await ctx.send("❌ Staff only.", ephemeral=True)
    
    embed = discord.Embed(title="📢 Announcement", description=message, color=0xF1C40F, timestamp=datetime.datetime.now(datetime.timezone.utc))
    embed.set_footer(text=f"By {ctx.author.display_name}", icon_url=ctx.author.display_avatar.url)
    await channel.send(embed=embed)
    await ctx.send(f"✅ Sent to {channel.mention}", ephemeral=True)

@bot.hybrid_command(name="top10_add", description="Admin: Add to Top 10 leaderboard")
@commands.has_permissions(administrator=True)
async def top10_add(ctx, member: discord.Member, position: int):
    """Add user to Top 10 roster at position (1-10)"""
    if position < 1 or position > 10:
        return await ctx.send("❌ Position must be 1-10", ephemeral=True)
    
    roster = load_leaderboard()
    roster[position - 1] = member.id
    save_leaderboard(roster)
    
    await ctx.send(f"✅ Added {member.mention} to Top 10 at position **{position}**")
    await log_action(ctx.guild, "📝 Top 10 Updated", f"{member.mention} at #{position} by {ctx.author.mention}", 0xF1C40F)

@bot.hybrid_command(name="top10_remove", description="Admin: Remove from Top 10 leaderboard")
@commands.has_permissions(administrator=True)
async def top10_remove(ctx, position: int):
    """Remove user from Top 10 position (1-10)"""
    if position < 1 or position > 10:
        return await ctx.send("❌ Position must be 1-10", ephemeral=True)
    
    roster = load_leaderboard()
    roster[position - 1] = None
    save_leaderboard(roster)
    
    await ctx.send(f"✅ Top 10 position **{position}** cleared")
    await log_action(ctx.guild, "📝 Top 10 Updated", f"Position {position} cleared by {ctx.author.mention}", 0xF1C40F)

@bot.hybrid_command(name="inactive_check", description="Admin: Check inactive members")
@commands.has_permissions(administrator=True)
async def inactive_check(ctx, days: int = 7):
    """List members inactive for X days"""
    data = load_data()
    now = datetime.datetime.now(datetime.timezone.utc)
    inactive = []
    
    for uid, ud in data["users"].items():
        last = ud.get("last_active")
        if last:
            try:
                last_dt = datetime.datetime.fromisoformat(last)
                if last_dt.tzinfo is None:
                    last_dt = last_dt.replace(tzinfo=datetime.timezone.utc)
                diff = (now - last_dt).days
                if diff >= days:
                    m = ctx.guild.get_member(int(uid))
                    if m:
                        inactive.append((m, diff))
            except:
                pass
    
    if not inactive:
        return await ctx.send(f"✅ No members inactive for {days}+ days!")
    
    inactive.sort(key=lambda x: -x[1])
    embed = discord.Embed(title=f"😴 Inactive Members ({days}+ days)", color=0xFFA500)
    embed.description = "\n".join([f"• {m.mention} - {d} days" for m, d in inactive[:20]])
    if len(inactive) > 20:
        embed.set_footer(text=f"...and {len(inactive) - 20} more")
    await ctx.send(embed=embed)

@bot.hybrid_command(name="reset_weekly", description="Admin: Reset weekly XP")
@commands.has_permissions(administrator=True)
async def reset_weekly(ctx):
    """Reset weekly XP for all users"""
    data = load_data()
    for uid in data["users"]:
        data["users"][uid]["weekly_xp"] = 0
    save_data(data)
    await ctx.send("✅ Weekly XP reset!")
    await log_action(ctx.guild, "🔄 Weekly Reset", f"By {ctx.author.mention}", 0x3498db)

@bot.hybrid_command(name="reset_monthly", description="Admin: Reset monthly XP")
@commands.has_permissions(administrator=True)
async def reset_monthly(ctx):
    """Reset monthly XP for all users"""
    data = load_data()
    for uid in data["users"]:
        data["users"][uid]["monthly_xp"] = 0
    save_data(data)
    await ctx.send("✅ Monthly XP reset!")
    await log_action(ctx.guild, "🔄 Monthly Reset", f"By {ctx.author.mention}", 0x3498db)

# ==========================================
# SUPPORT TICKET COMMANDS
# ==========================================

@bot.hybrid_command(name="setup_tickets", description="Admin: Setup the support ticket panel")
@commands.has_permissions(administrator=True)
async def setup_tickets(ctx, channel: discord.TextChannel = None):
    """Create the support ticket panel"""
    target_channel = channel or ctx.channel
    
    embed = discord.Embed(
        title="🎫 Support Tickets",
        description=(
            "Need help? Click a button below to create a ticket!\n\n"
            "**🎫 Support** - General questions & help\n"
            "**🚨 Report** - Report a rule breaker\n"
            "**💡 Suggestion** - Submit an idea\n\n"
            "A staff member will assist you shortly after creating a ticket."
        ),
        color=0x3498db
    )
    embed.set_footer(text="Please don't spam tickets • One ticket at a time")
    
    await target_channel.send(embed=embed, view=SupportTicketPanelView())
    await ctx.send(f"✅ Ticket panel posted in {target_channel.mention}", ephemeral=True)
    await log_action(ctx.guild, "🎫 Ticket Panel", f"Posted in {target_channel.mention} by {ctx.author.mention}", 0x3498db)

@bot.hybrid_command(name="ticket_stats", description="Staff: View ticket statistics")
async def ticket_stats(ctx):
    """View ticket statistics"""
    if not is_staff(ctx.author):
        return await ctx.send("❌ Staff only.", ephemeral=True)
    
    total = len(support_tickets)
    open_tickets = sum(1 for t in support_tickets.values() if t["status"] == "open")
    closed_tickets = sum(1 for t in support_tickets.values() if t["status"] == "closed")
    
    by_type = {}
    for t in support_tickets.values():
        t_type = t.get("type", "unknown")
        by_type[t_type] = by_type.get(t_type, 0) + 1
    
    embed = discord.Embed(title="🎫 Ticket Statistics", color=0x3498db)
    embed.add_field(name="📊 Total Tickets", value=str(total), inline=True)
    embed.add_field(name="🟢 Open", value=str(open_tickets), inline=True)
    embed.add_field(name="🔴 Closed", value=str(closed_tickets), inline=True)
    
    type_text = "\n".join([f"{SUPPORT_TICKET_TYPES.get(k, {}).get('emoji', '📁')} {k.title()}: {v}" for k, v in by_type.items()])
    embed.add_field(name="📁 By Type", value=type_text or "No tickets yet", inline=False)
    
    await ctx.send(embed=embed)

@bot.hybrid_command(name="close_ticket", description="Close the current ticket")
async def close_ticket(ctx):
    """Close the current ticket channel"""
    if ctx.channel.id not in support_tickets:
        return await ctx.send("❌ This is not a ticket channel.", ephemeral=True)
    
    ticket = support_tickets[ctx.channel.id]
    
    # Only staff or ticket creator can close
    if not is_staff(ctx.author) and ctx.author.id != ticket["user_id"]:
        return await ctx.send("❌ You can't close this ticket.", ephemeral=True)
    
    config = SUPPORT_TICKET_TYPES.get(ticket["type"], SUPPORT_TICKET_TYPES["support"])
    
    # Generate transcript
    transcript = []
    async for msg in ctx.channel.history(limit=100, oldest_first=True):
        if not msg.author.bot or msg.embeds:
            transcript.append(f"[{msg.created_at.strftime('%Y-%m-%d %H:%M')}] {msg.author.name}: {msg.content or '[Embed/Attachment]'}")
    
    ticket["status"] = "closed"
    ticket["transcript"] = transcript
    
    # DM creator
    creator = ctx.guild.get_member(ticket["user_id"])
    if creator:
        try:
            dm_embed = discord.Embed(
                title=f"{config['emoji']} Ticket Closed",
                description=f"Your **{config['name']}** ticket has been closed.\n\nThank you for contacting us!",
                color=config["color"]
            )
            await creator.send(embed=dm_embed)
        except:
            pass
    
    await log_action(ctx.guild, f"{config['emoji']} Ticket Closed", f"**Type:** {config['name']}\n**Closed By:** {ctx.author.mention}", 0xe74c3c)
    await ctx.send("🔒 Closing ticket in 5 seconds...")
    await asyncio.sleep(5)
    await ctx.channel.delete()

# ==========================================
# ADVANCED STATS COMMANDS
# ==========================================

@bot.hybrid_command(name="serverstats", description="View server statistics")
async def serverstats(ctx):
    """Display comprehensive server statistics"""
    stats = get_server_stats(ctx.guild)
    
    embed = discord.Embed(
        title=f"📊 {ctx.guild.name} Statistics",
        color=0x3498db,
        timestamp=datetime.datetime.now(datetime.timezone.utc)
    )
    
    if ctx.guild.icon:
        embed.set_thumbnail(url=ctx.guild.icon.url)
    
    # Member stats
    embed.add_field(
        name="👥 Members",
        value=f"**Total:** {stats['total_members']:,}\n"
              f"**Online:** {stats['online_members']:,}\n"
              f"**Bots:** {stats['bot_count']:,}",
        inline=True
    )
    
    # Activity stats
    embed.add_field(
        name="📈 Activity",
        value=f"**Today:** {stats['active_today']:,}\n"
              f"**This Week:** {stats['active_week']:,}\n"
              f"**This Month:** {stats['active_month']:,}",
        inline=True
    )
    
    # Economy stats
    embed.add_field(
        name="💰 Economy",
        value=f"**Total XP:** {stats['total_xp']:,}\n"
              f"**Total Coins:** {stats['total_coins']:,}\n"
              f"**Avg Level:** {stats['avg_level']}",
        inline=True
    )
    
    # Combat stats
    total_matches = stats['total_wins'] + stats['total_losses']
    embed.add_field(
        name="⚔️ Combat",
        value=f"**Total Matches:** {total_matches:,}\n"
              f"**Total Raids:** {stats['total_raids']:,}\n"
              f"**Trainings:** {stats['total_trainings']:,}",
        inline=True
    )
    
    # Current status
    activity = get_activity_by_hour(ctx.guild)
    embed.add_field(
        name="🟢 Current Status",
        value=f"🟢 {activity['online']} | 🟡 {activity['idle']} | 🔴 {activity['dnd']} | ⚫ {activity['offline']}",
        inline=True
    )
    
    # Top level
    embed.add_field(
        name="🏆 Highest Level",
        value=f"Level **{stats['top_level']}**",
        inline=True
    )
    
    embed.set_footer(text=f"Server ID: {ctx.guild.id}")
    await ctx.send(embed=embed)

@bot.hybrid_command(name="topactive", description="View most active members this week")
async def topactive(ctx, days: int = 7):
    """Show most active members"""
    if days not in [7, 30]:
        days = 7
    
    top_users = get_top_active_users(ctx.guild, days=days, limit=10)
    
    period = "This Week" if days == 7 else "This Month"
    xp_type = "Weekly" if days == 7 else "Monthly"
    
    embed = discord.Embed(
        title=f"🔥 Most Active Members - {period}",
        color=0xFF4500
    )
    
    if not top_users:
        embed.description = "No activity data yet!"
    else:
        lines = []
        for i, (member, xp) in enumerate(top_users, 1):
            medal = "🥇" if i == 1 else "🥈" if i == 2 else "🥉" if i == 3 else f"**#{i}**"
            lines.append(f"{medal} {member.mention} • **{xp:,}** {xp_type} XP")
        embed.description = "\n".join(lines)
    
    embed.set_footer(text=f"Based on {xp_type.lower()} XP gains")
    await ctx.send(embed=embed)

@bot.hybrid_command(name="mystats", description="View your detailed activity stats")
async def mystats(ctx, member: discord.Member = None):
    """Display detailed personal statistics"""
    target = member or ctx.author
    stats = get_user_activity_stats(target.id)
    
    embed = discord.Embed(
        title=f"📊 {target.display_name}'s Statistics",
        color=0x3498db
    )
    embed.set_thumbnail(url=target.display_avatar.url)
    
    # Level & XP
    embed.add_field(
        name="📈 Progress",
        value=f"**Level:** {stats['level']}\n"
              f"**Total XP:** {stats['xp']:,}\n"
              f"**Weekly XP:** {stats['weekly_xp']:,}\n"
              f"**Monthly XP:** {stats['monthly_xp']:,}",
        inline=True
    )
    
    # Economy
    embed.add_field(
        name="💰 Economy",
        value=f"**Coins:** {stats['coins']:,}\n"
              f"**Daily Streak:** {stats['daily_streak']} days\n"
              f"**Roblox:** {stats['roblox']}",
        inline=True
    )
    
    # Combat
    total = stats['wins'] + stats['losses']
    winrate = round((stats['wins'] / total) * 100, 1) if total > 0 else 0
    embed.add_field(
        name="⚔️ Combat",
        value=f"**W/L:** {stats['wins']}W - {stats['losses']}L\n"
              f"**Win Rate:** {winrate}%\n"
              f"**Total Matches:** {total}",
        inline=True
    )
    
    # Raids
    raid_total = stats['raid_wins'] + stats['raid_losses']
    raid_wr = round((stats['raid_wins'] / raid_total) * 100, 1) if raid_total > 0 else 0
    embed.add_field(
        name="🏴‍☠️ Raids",
        value=f"**W/L:** {stats['raid_wins']}W - {stats['raid_losses']}L\n"
              f"**Win Rate:** {raid_wr}%\n"
              f"**Participated:** {stats['raid_participation']}",
        inline=True
    )
    
    # Training & Moderation
    embed.add_field(
        name="📚 Activity",
        value=f"**Trainings:** {stats['training_attendance']}\n"
              f"**Warnings:** {stats['warnings']}/3",
        inline=True
    )
    
    # Last Active
    if stats['last_active']:
        try:
            last_dt = datetime.datetime.fromisoformat(stats['last_active'])
            embed.add_field(
                name="🕐 Last Active",
                value=f"<t:{int(last_dt.timestamp())}:R>",
                inline=True
            )
        except:
            pass
    
    # Rank
    rank = get_level_rank(target.id)
    embed.add_field(name="🏆 Server Rank", value=f"#{rank}", inline=True)
    
    await ctx.send(embed=embed)

@bot.hybrid_command(name="compare", description="Compare stats with another member")
async def compare(ctx, member: discord.Member):
    """Compare your stats with another member"""
    if member.id == ctx.author.id:
        return await ctx.send("❌ You can't compare with yourself!", ephemeral=True)
    
    your_stats = get_user_activity_stats(ctx.author.id)
    their_stats = get_user_activity_stats(member.id)
    
    embed = discord.Embed(
        title=f"⚔️ {ctx.author.display_name} vs {member.display_name}",
        color=0xFF4500
    )
    
    def compare_stat(yours, theirs, label, emoji):
        if yours > theirs:
            return f"{emoji} **{label}:** {yours:,} ✅ vs {theirs:,}"
        elif theirs > yours:
            return f"{emoji} **{label}:** {yours:,} vs {theirs:,} ✅"
        else:
            return f"{emoji} **{label}:** {yours:,} 🤝 {theirs:,}"
    
    comparisons = [
        compare_stat(your_stats['level'], their_stats['level'], "Level", "📊"),
        compare_stat(your_stats['xp'], their_stats['xp'], "Total XP", "✨"),
        compare_stat(your_stats['coins'], their_stats['coins'], "Coins", "💰"),
        compare_stat(your_stats['wins'], their_stats['wins'], "Wins", "🏆"),
        compare_stat(your_stats['raid_wins'], their_stats['raid_wins'], "Raid Wins", "🏴‍☠️"),
        compare_stat(your_stats['training_attendance'], their_stats['training_attendance'], "Trainings", "📚"),
        compare_stat(your_stats['daily_streak'], their_stats['daily_streak'], "Daily Streak", "🔥"),
    ]
    
    embed.description = "\n".join(comparisons)
    
    # Count who won more categories
    your_wins = sum([
        your_stats['level'] > their_stats['level'],
        your_stats['xp'] > their_stats['xp'],
        your_stats['coins'] > their_stats['coins'],
        your_stats['wins'] > their_stats['wins'],
        your_stats['raid_wins'] > their_stats['raid_wins'],
    ])
    their_wins = sum([
        their_stats['level'] > your_stats['level'],
        their_stats['xp'] > your_stats['xp'],
        their_stats['coins'] > your_stats['coins'],
        their_stats['wins'] > your_stats['wins'],
        their_stats['raid_wins'] > your_stats['raid_wins'],
    ])
    
    if your_wins > their_wins:
        winner = f"🏆 **{ctx.author.display_name}** wins {your_wins}-{their_wins}!"
    elif their_wins > your_wins:
        winner = f"🏆 **{member.display_name}** wins {their_wins}-{your_wins}!"
    else:
        winner = "🤝 It's a tie!"
    
    embed.add_field(name="Result", value=winner, inline=False)
    
    await ctx.send(embed=embed)

@bot.hybrid_command(name="leaderboards", description="View various leaderboards")
async def leaderboards(ctx):
    """Show all available leaderboards"""
    embed = discord.Embed(
        title="📊 Leaderboards",
        description="Select a leaderboard to view:",
        color=0x3498db
    )
    embed.add_field(name="📈 XP Leaderboards", value="`/leaderboard` - Overall, Weekly, Monthly XP", inline=False)
    embed.add_field(name="🔥 Activity", value="`/topactive` - Most active this week", inline=False)
    embed.add_field(name="🏴‍☠️ Raids", value="`/raid_lb` - Top raiders", inline=False)
    embed.add_field(name="⚔️ Combat", value="Coming soon!", inline=False)
    
    await ctx.send(embed=embed)

# Run the bot
if __name__ == "__main__":
    bot.run(TOKEN)
