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
import aiohttp

# Try to import PIL for level cards (optional)
try:
    from PIL import Image, ImageDraw, ImageFont, ImageFilter
    PIL_AVAILABLE = True
except ImportError:
    PIL_AVAILABLE = False
    print("PIL not installed - using embed-based level cards. Install with: pip install Pillow")

# Try to import PostgreSQL (optional - falls back to JSON)
try:
    import asyncpg
    POSTGRES_AVAILABLE = True
except ImportError:
    POSTGRES_AVAILABLE = False
    print("asyncpg not installed - using JSON storage. Install with: pip install asyncpg")

# Database connection pool
db_pool = None

# --- CONFIGURATION ---
# Get token from environment variable (set in Render dashboard)
TOKEN = os.getenv("DISCORD_TOKEN")

# PostgreSQL Database URL (set in Render dashboard)
DATABASE_URL = os.getenv("DATABASE_URL") 

# --- ROLE SETTINGS ---
REQUIRED_ROLE_NAME = "Mainer"         # Legacy - keeping for backwards compatibility         
STAFF_ROLE_NAME = "Staff"             
UNVERIFIED_ROLE_NAME = "Unverified"
VERIFIED_ROLE_NAME = "Verified"
MEMBER_ROLE_NAME = "Abyssbound"
BLOXLINK_VERIFIED_ROLE = "Bloxlink Verified"  # The role Bloxlink gives when verified - change if different
FALLEN_VERIFIED_ROLE = "Fallen Verified"  # The role Fallen bot gives - create this role or change name

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
    # Milestone levels with role and coin rewards
    # XP is calculated dynamically: Level 5 = ~500 XP, Level 10 = ~1,625 XP, etc.
    5: {"role": "Faint Emberling", "coins": 50},
    10: {"role": "Initiate of Shadows", "coins": 100},
    20: {"role": "Abysswalk Student", "coins": 200},
    30: {"role": "Twilight Disciple", "coins": 400},
    40: {"role": "Duskforged Aspirant", "coins": 600},
    50: {"role": "Bearer of Abyssal Echo", "coins": 1000},
    60: {"role": "Nightwoven Adept", "coins": 1500},
    70: {"role": "Veilmarked Veteran", "coins": 2000},
    80: {"role": "Shadowborn Ascendant", "coins": 2500},
    100: {"role": "Abyssforged Warden", "coins": 5000},
    120: {"role": "Eclipsed Oathbearer", "coins": 7500},
    140: {"role": "Harbinger of Dusk", "coins": 10000},
    160: {"role": "Ascended Dreadkeeper", "coins": 15000},
    200: {"role": "Eternal Shadow Sovereign", "coins": 50000},
}

XP_TEXT_RANGE = (5, 15)      # XP per message
XP_VOICE_RANGE = (15, 30)    # XP per 2 minutes in voice
XP_REACTION_RANGE = (2, 8)   # XP per reaction

# XP Cooldowns (in seconds) - prevents spam
XP_MESSAGE_COOLDOWN = 60     # 1 minute between message XP
XP_REACTION_COOLDOWN = 30    # 30 seconds between reaction XP

# ==========================================
# RATE LIMIT PROTECTION
# ==========================================
# Discord API limits: 50 requests per second globally
# We add delays and queuing to stay well under this

API_CALL_DELAY = 0.5  # Seconds between API calls in bulk operations
BULK_OPERATION_DELAY = 1.0  # Delay between bulk operations (role adds, kicks, etc.)
MAX_BULK_ACTIONS_PER_MINUTE = 30  # Max bulk actions per minute

# Track API calls for rate limiting
api_call_tracker = {
    "last_call": 0,
    "calls_this_minute": 0,
    "minute_start": 0
}

async def rate_limited_action(coro, delay=API_CALL_DELAY):
    """Execute an action with rate limit protection"""
    global api_call_tracker
    
    now = datetime.datetime.now().timestamp()
    
    # Reset counter every minute
    if now - api_call_tracker["minute_start"] > 60:
        api_call_tracker["calls_this_minute"] = 0
        api_call_tracker["minute_start"] = now
    
    # Check if we're over the limit
    if api_call_tracker["calls_this_minute"] >= MAX_BULK_ACTIONS_PER_MINUTE:
        wait_time = 60 - (now - api_call_tracker["minute_start"])
        if wait_time > 0:
            await asyncio.sleep(wait_time)
        api_call_tracker["calls_this_minute"] = 0
        api_call_tracker["minute_start"] = datetime.datetime.now().timestamp()
    
    # Add delay since last call
    time_since_last = now - api_call_tracker["last_call"]
    if time_since_last < delay:
        await asyncio.sleep(delay - time_since_last)
    
    # Execute the action
    api_call_tracker["last_call"] = datetime.datetime.now().timestamp()
    api_call_tracker["calls_this_minute"] += 1
    
    return await coro

async def safe_add_role(member, role):
    """Safely add a role with rate limit protection"""
    try:
        await rate_limited_action(member.add_roles(role))
        return True
    except discord.HTTPException as e:
        if e.status == 429:  # Rate limited
            retry_after = e.retry_after if hasattr(e, 'retry_after') else 5
            print(f"Rate limited! Waiting {retry_after}s...")
            await asyncio.sleep(retry_after)
            try:
                await member.add_roles(role)
                return True
            except:
                return False
        return False
    except:
        return False

async def safe_remove_role(member, role):
    """Safely remove a role with rate limit protection"""
    try:
        await rate_limited_action(member.remove_roles(role))
        return True
    except discord.HTTPException as e:
        if e.status == 429:
            retry_after = e.retry_after if hasattr(e, 'retry_after') else 5
            print(f"Rate limited! Waiting {retry_after}s...")
            await asyncio.sleep(retry_after)
            try:
                await member.remove_roles(role)
                return True
            except:
                return False
        return False
    except:
        return False

async def safe_send_message(channel, content=None, embed=None, view=None):
    """Safely send a message with rate limit protection"""
    try:
        return await rate_limited_action(
            channel.send(content=content, embed=embed, view=view),
            delay=0.3
        )
    except discord.HTTPException as e:
        if e.status == 429:
            retry_after = e.retry_after if hasattr(e, 'retry_after') else 5
            await asyncio.sleep(retry_after)
            try:
                return await channel.send(content=content, embed=embed, view=view)
            except:
                return None
        return None
    except:
        return None

async def safe_kick(member, reason=None):
    """Safely kick a member with rate limit protection"""
    try:
        await rate_limited_action(member.kick(reason=reason), delay=BULK_OPERATION_DELAY)
        return True
    except discord.HTTPException as e:
        if e.status == 429:
            retry_after = e.retry_after if hasattr(e, 'retry_after') else 5
            await asyncio.sleep(retry_after)
            try:
                await member.kick(reason=reason)
                return True
            except:
                return False
        return False
    except:
        return False

async def safe_create_channel(guild, name, category=None, overwrites=None):
    """Safely create a channel with rate limit protection"""
    try:
        return await rate_limited_action(
            guild.create_text_channel(name, category=category, overwrites=overwrites),
            delay=BULK_OPERATION_DELAY
        )
    except discord.HTTPException as e:
        if e.status == 429:
            retry_after = e.retry_after if hasattr(e, 'retry_after') else 5
            await asyncio.sleep(retry_after)
            try:
                return await guild.create_text_channel(name, category=category, overwrites=overwrites)
            except:
                return None
        return None
    except:
        return None

async def safe_delete_channel(channel):
    """Safely delete a channel with rate limit protection"""
    try:
        await rate_limited_action(channel.delete(), delay=BULK_OPERATION_DELAY)
        return True
    except:
        return False

# Store cooldowns in memory (user_id: last_xp_time)
xp_cooldowns = {
    "message": {},
    "reaction": {}
}

def check_xp_cooldown(user_id, cooldown_type):
    """Check if user is on XP cooldown. Returns True if they can earn XP."""
    now = datetime.datetime.now(datetime.timezone.utc).timestamp()
    user_id = str(user_id)
    
    cooldown_time = XP_MESSAGE_COOLDOWN if cooldown_type == "message" else XP_REACTION_COOLDOWN
    
    if user_id in xp_cooldowns[cooldown_type]:
        last_time = xp_cooldowns[cooldown_type][user_id]
        if now - last_time < cooldown_time:
            return False  # Still on cooldown
    
    # Update last XP time
    xp_cooldowns[cooldown_type][user_id] = now
    return True  # Can earn XP 
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

# --- DATABASE MANAGEMENT ---

async def init_database():
    """Initialize PostgreSQL database connection and tables"""
    global db_pool
    
    if not POSTGRES_AVAILABLE or not DATABASE_URL:
        print("📁 Using JSON file storage (PostgreSQL not configured)")
        return False
    
    try:
        db_pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=10)
        
        # Create tables if they don't exist
        async with db_pool.acquire() as conn:
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS users (
                    user_id BIGINT PRIMARY KEY,
                    xp INTEGER DEFAULT 0,
                    level INTEGER DEFAULT 0,
                    coins INTEGER DEFAULT 0,
                    wins INTEGER DEFAULT 0,
                    losses INTEGER DEFAULT 0,
                    raid_wins INTEGER DEFAULT 0,
                    raid_losses INTEGER DEFAULT 0,
                    raid_participation INTEGER DEFAULT 0,
                    daily_streak INTEGER DEFAULT 0,
                    last_daily TIMESTAMP,
                    weekly_xp INTEGER DEFAULT 0,
                    monthly_xp INTEGER DEFAULT 0,
                    messages INTEGER DEFAULT 0,
                    warnings INTEGER DEFAULT 0,
                    verified BOOLEAN DEFAULT FALSE,
                    roblox_username TEXT,
                    roblox_id BIGINT,
                    achievements TEXT[] DEFAULT ARRAY[]::TEXT[],
                    activity_log JSONB DEFAULT '[]'::JSONB,
                    created_at TIMESTAMP DEFAULT NOW()
                )
            ''')
            
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS raids (
                    id SERIAL PRIMARY KEY,
                    target TEXT NOT NULL,
                    result TEXT NOT NULL,
                    participants BIGINT[] DEFAULT ARRAY[]::BIGINT[],
                    xp_gained INTEGER DEFAULT 0,
                    created_at TIMESTAMP DEFAULT NOW()
                )
            ''')
            
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS wars (
                    id SERIAL PRIMARY KEY,
                    clan_name TEXT NOT NULL,
                    wins INTEGER DEFAULT 0,
                    losses INTEGER DEFAULT 0,
                    status TEXT DEFAULT 'active',
                    created_at TIMESTAMP DEFAULT NOW()
                )
            ''')
            
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS tournaments (
                    id SERIAL PRIMARY KEY,
                    name TEXT NOT NULL,
                    status TEXT DEFAULT 'signup',
                    participants BIGINT[] DEFAULT ARRAY[]::BIGINT[],
                    bracket JSONB DEFAULT '[]'::JSONB,
                    winner_id BIGINT,
                    created_at TIMESTAMP DEFAULT NOW()
                )
            ''')
            
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS roster (
                    position INTEGER PRIMARY KEY,
                    user_id BIGINT,
                    roblox_name TEXT
                )
            ''')
            
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS settings (
                    key TEXT PRIMARY KEY,
                    value JSONB
                )
            ''')
        
        print("✅ PostgreSQL database connected and initialized!")
        return True
        
    except Exception as e:
        print(f"❌ PostgreSQL connection failed: {e}")
        print("📁 Falling back to JSON file storage")
        db_pool = None
        return False

async def db_get_user(user_id: int):
    """Get user data from database"""
    if db_pool:
        async with db_pool.acquire() as conn:
            row = await conn.fetchrow('SELECT * FROM users WHERE user_id = $1', user_id)
            if row:
                return dict(row)
    return None

async def db_save_user(user_id: int, data: dict):
    """Save user data to database"""
    if db_pool:
        async with db_pool.acquire() as conn:
            await conn.execute('''
                INSERT INTO users (user_id, xp, level, coins, wins, losses, raid_wins, raid_losses, 
                    raid_participation, daily_streak, weekly_xp, monthly_xp, messages, warnings,
                    verified, roblox_username, roblox_id, achievements)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18)
                ON CONFLICT (user_id) DO UPDATE SET
                    xp = $2, level = $3, coins = $4, wins = $5, losses = $6, raid_wins = $7,
                    raid_losses = $8, raid_participation = $9, daily_streak = $10, weekly_xp = $11,
                    monthly_xp = $12, messages = $13, warnings = $14, verified = $15,
                    roblox_username = $16, roblox_id = $17, achievements = $18
            ''', user_id, 
                data.get('xp', 0), data.get('level', 0), data.get('coins', 0),
                data.get('wins', 0), data.get('losses', 0), data.get('raid_wins', 0),
                data.get('raid_losses', 0), data.get('raid_participation', 0),
                data.get('daily_streak', 0), data.get('weekly_xp', 0), data.get('monthly_xp', 0),
                data.get('messages', 0), data.get('warnings', 0), data.get('verified', False),
                data.get('roblox_username'), data.get('roblox_id'),
                data.get('achievements', [])
            )

async def db_get_all_users():
    """Get all users from database"""
    if db_pool:
        async with db_pool.acquire() as conn:
            rows = await conn.fetch('SELECT * FROM users ORDER BY xp DESC')
            return {str(row['user_id']): dict(row) for row in rows}
    return {}

async def db_log_raid(target: str, result: str, participants: list, xp_gained: int):
    """Log raid to database"""
    if db_pool:
        async with db_pool.acquire() as conn:
            await conn.execute('''
                INSERT INTO raids (target, result, participants, xp_gained)
                VALUES ($1, $2, $3, $4)
            ''', target, result, participants, xp_gained)

async def db_get_raid_history(limit: int = 10):
    """Get raid history from database"""
    if db_pool:
        async with db_pool.acquire() as conn:
            rows = await conn.fetch('''
                SELECT * FROM raids ORDER BY created_at DESC LIMIT $1
            ''', limit)
            return [dict(row) for row in rows]
    return []

# --- JSON DATA MANAGEMENT (Fallback) ---
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
    """Calculate XP needed for the next level (continuous leveling like Arcane)"""
    # Lower XP requirements - base 50, increases by 25 per level
    # Level 1: 50 XP, Level 2: 75 XP, Level 3: 100 XP, etc.
    base_xp = 50
    increment = 25
    return base_xp + (level * increment)

def get_total_xp_for_level(level):
    """Calculate total XP needed to reach a specific level"""
    total = 0
    for lvl in range(level):
        total += calculate_next_level_xp(lvl)
    return total

def get_level_from_xp(total_xp):
    """Calculate level from total XP"""
    level = 0
    xp_remaining = total_xp
    while True:
        xp_needed = calculate_next_level_xp(level)
        if xp_remaining < xp_needed:
            break
        xp_remaining -= xp_needed
        level += 1
    return level, xp_remaining  # Returns level and XP progress into current level

def get_milestone_reward(level):
    """Get role and coin reward for milestone levels"""
    return LEVEL_CONFIG.get(level)

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
    total_xp = user_data['xp']
    
    # Calculate level from total XP (continuous leveling)
    lvl, xp_into_level = get_level_from_xp(total_xp)
    xp_needed = calculate_next_level_xp(lvl)
    
    coins = user_data.get('coins', 0)
    roblox = user_data.get('roblox_username', None)
    
    progress_percent = min(100, int((xp_into_level / xp_needed) * 100)) if xp_needed > 0 else 0
    
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
        value=f"{progress_bar}\n**{format_number(xp_into_level)}** / **{format_number(xp_needed)}**",
        inline=False
    )
    
    # Next milestone
    next_milestone = None
    for mlvl in sorted(LEVEL_CONFIG.keys()):
        if mlvl > lvl:
            next_milestone = mlvl
            break
    if next_milestone:
        milestone_data = LEVEL_CONFIG[next_milestone]
        embed.add_field(
            name="🎯 Next Milestone",
            value=f"Level **{next_milestone}** → {milestone_data['role']}",
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
    
    # Footer with total XP
    embed.set_footer(text=f"✝ The Fallen ✝ • Total XP: {total_xp:,}")
    
    return embed

async def create_level_card_image(member, user_data, rank):
    """Create a custom image-based level card with background and rank borders"""
    if not PIL_AVAILABLE:
        print("PIL not available for level card")
        return None
    
    total_xp = user_data['xp']
    # Calculate level from total XP (continuous leveling)
    lvl, xp_into_level = get_level_from_xp(total_xp)
    xp_needed = calculate_next_level_xp(lvl)
    progress = min(1.0, xp_into_level / xp_needed) if xp_needed > 0 else 0
    
    # Get rank border style
    border_style = get_rank_border(rank)
    
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
        font_title = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf", 14)
    except:
        font_large = ImageFont.load_default()
        font_medium = ImageFont.load_default()
        font_small = ImageFont.load_default()
        font_title = ImageFont.load_default()
    
    # Avatar position
    avatar_size = 180
    avatar_x, avatar_y = 50, 51
    
    # Draw custom rank border
    draw_rank_border(draw, card, avatar_x, avatar_y, avatar_size, border_style)
    
    # Download and add avatar
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(str(member.display_avatar.url)) as resp:
                if resp.status == 200:
                    avatar_data = await resp.read()
                    avatar = Image.open(BytesIO(avatar_data)).convert("RGBA")
                    avatar = avatar.resize((avatar_size, avatar_size), Image.Resampling.LANCZOS)
                    
                    # Create circular mask
                    mask = Image.new("L", (avatar_size, avatar_size), 0)
                    mask_draw = ImageDraw.Draw(mask)
                    mask_draw.ellipse((0, 0, avatar_size, avatar_size), fill=255)
                    
                    # Paste avatar
                    card.paste(avatar, (avatar_x, avatar_y), mask)
                    draw = ImageDraw.Draw(card)
    except:
        pass
    
    # Rank title badge (below avatar)
    rank_title = border_style.get("title", "Member")
    title_color = border_style["color"]
    draw.text((avatar_x + 40, avatar_y + avatar_size + 5), rank_title, font=font_title, fill=title_color)
    
    # Username
    draw.text((260, 60), member.display_name, font=font_large, fill=(255, 255, 255))
    draw.text((260, 110), f"@{member.name}", font=font_small, fill=(180, 180, 180))
    
    # Stats with rank color
    draw.text((260, 160), f"LEVEL", font=font_small, fill=(150, 150, 150))
    draw.text((260, 185), str(lvl), font=font_large, fill=(255, 255, 255))
    
    draw.text((400, 160), f"RANK", font=font_small, fill=(150, 150, 150))
    draw.text((400, 185), f"#{rank}", font=font_large, fill=title_color)  # Rank colored
    
    draw.text((550, 160), f"XP", font=font_small, fill=(150, 150, 150))
    draw.text((550, 185), f"{format_number(xp_into_level)} / {format_number(xp_needed)}", font=font_medium, fill=(255, 255, 255))
    
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

# ==========================================
# WELCOME CARD IMAGE GENERATOR
# ==========================================

async def create_welcome_card(member):
    """Create a beautiful welcome card image for new members"""
    if not PIL_AVAILABLE:
        return None
    
    width, height = 900, 350
    
    # Load background
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
    overlay = Image.new("RGBA", (width, height), (0, 0, 0, 180))
    card = Image.alpha_composite(card, overlay)
    
    draw = ImageDraw.Draw(card)
    
    # Load fonts
    try:
        font_title = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf", 42)
        font_name = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf", 32)
        font_text = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf", 20)
        font_small = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf", 16)
    except:
        font_title = font_name = font_text = font_small = ImageFont.load_default()
    
    # Top decorative line
    draw.rectangle([(20, 15), (width - 20, 20)], fill=(139, 0, 0))
    
    # Welcome text
    welcome_text = "WELCOME TO"
    w_bbox = draw.textbbox((0, 0), welcome_text, font=font_text)
    w_width = w_bbox[2] - w_bbox[0]
    draw.text(((width - w_width) // 2, 40), welcome_text, font=font_text, fill=(200, 200, 200))
    
    # Server name
    server_text = "THE FALLEN"
    s_bbox = draw.textbbox((0, 0), server_text, font=font_title)
    s_width = s_bbox[2] - s_bbox[0]
    draw.text(((width - s_width) // 2, 70), server_text, font=font_title, fill=(255, 255, 255))
    
    # Avatar
    avatar_size = 120
    avatar_x = (width - avatar_size) // 2
    avatar_y = 130
    
    # Download avatar
    try:
        async with aiohttp.ClientSession() as session:
            avatar_url = member.display_avatar.with_format('png').with_size(256).url
            async with session.get(avatar_url) as resp:
                if resp.status == 200:
                    avatar_data = await resp.read()
                    avatar_img = Image.open(BytesIO(avatar_data)).convert("RGBA")
                    avatar_img = avatar_img.resize((avatar_size, avatar_size), Image.Resampling.LANCZOS)
                    
                    # Create circular mask
                    mask = Image.new("L", (avatar_size, avatar_size), 0)
                    mask_draw = ImageDraw.Draw(mask)
                    mask_draw.ellipse((0, 0, avatar_size, avatar_size), fill=255)
                    
                    # Red border
                    draw.ellipse(
                        [avatar_x - 5, avatar_y - 5, avatar_x + avatar_size + 5, avatar_y + avatar_size + 5],
                        fill=(139, 0, 0)
                    )
                    
                    card.paste(avatar_img, (avatar_x, avatar_y), mask)
                    draw = ImageDraw.Draw(card)
    except:
        draw.ellipse(
            [avatar_x, avatar_y, avatar_x + avatar_size, avatar_y + avatar_size],
            fill=(60, 60, 70)
        )
    
    # Username
    name_text = member.display_name
    n_bbox = draw.textbbox((0, 0), name_text, font=font_name)
    n_width = n_bbox[2] - n_bbox[0]
    draw.text(((width - n_width) // 2, 265), name_text, font=font_name, fill=(255, 255, 255))
    
    # Member count
    member_num = f"Member #{member.guild.member_count}"
    m_bbox = draw.textbbox((0, 0), member_num, font=font_text)
    m_width = m_bbox[2] - m_bbox[0]
    draw.text(((width - m_width) // 2, 305), member_num, font=font_text, fill=(139, 0, 0))
    
    # Bottom decorative line
    draw.rectangle([(20, height - 20), (width - 20, height - 15)], fill=(139, 0, 0))
    
    # Save
    output = BytesIO()
    card.save(output, format="PNG")
    output.seek(0)
    return output

# ==========================================
# PROFILE CARD IMAGE GENERATOR
# ==========================================

async def create_profile_card(member, user_data, rank, achievements):
    """Create a detailed profile card image"""
    if not PIL_AVAILABLE:
        return None
    
    width, height = 900, 500
    
    # Load background
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
    overlay = Image.new("RGBA", (width, height), (0, 0, 0, 200))
    card = Image.alpha_composite(card, overlay)
    
    draw = ImageDraw.Draw(card)
    
    # Load fonts
    try:
        font_title = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf", 28)
        font_name = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf", 24)
        font_stats = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf", 18)
        font_label = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf", 14)
        font_small = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf", 12)
    except:
        font_title = font_name = font_stats = font_label = font_small = ImageFont.load_default()
    
    # Get user stats
    lvl = user_data.get('level', 0)
    xp = user_data.get('xp', 0)
    coins = user_data.get('coins', 0)
    wins = user_data.get('wins', 0)
    losses = user_data.get('losses', 0)
    raid_wins = user_data.get('raid_wins', 0)
    raid_losses = user_data.get('raid_losses', 0)
    daily_streak = user_data.get('daily_streak', 0)
    roblox = user_data.get('roblox_username', None)
    req = calculate_next_level_xp(lvl)
    progress = min(1.0, xp / req) if req > 0 else 0
    
    # Rank border color
    if rank == 1:
        border_color = (255, 215, 0)  # Gold
    elif rank == 2:
        border_color = (192, 192, 192)  # Silver
    elif rank == 3:
        border_color = (205, 127, 50)  # Bronze
    elif rank <= 10:
        border_color = (139, 0, 0)  # Red
    else:
        border_color = (100, 100, 100)  # Gray
    
    # Top border
    draw.rectangle([(0, 0), (width, 8)], fill=border_color)
    
    # Header
    draw.text((30, 20), "PLAYER PROFILE", font=font_title, fill=(200, 200, 200))
    
    # Avatar section (left side)
    avatar_size = 150
    avatar_x, avatar_y = 40, 70
    
    try:
        async with aiohttp.ClientSession() as session:
            avatar_url = member.display_avatar.with_format('png').with_size(256).url
            async with session.get(avatar_url) as resp:
                if resp.status == 200:
                    avatar_data = await resp.read()
                    avatar_img = Image.open(BytesIO(avatar_data)).convert("RGBA")
                    avatar_img = avatar_img.resize((avatar_size, avatar_size), Image.Resampling.LANCZOS)
                    
                    mask = Image.new("L", (avatar_size, avatar_size), 0)
                    mask_draw = ImageDraw.Draw(mask)
                    mask_draw.ellipse((0, 0, avatar_size, avatar_size), fill=255)
                    
                    # Border
                    draw.ellipse(
                        [avatar_x - 5, avatar_y - 5, avatar_x + avatar_size + 5, avatar_y + avatar_size + 5],
                        fill=border_color
                    )
                    
                    card.paste(avatar_img, (avatar_x, avatar_y), mask)
                    draw = ImageDraw.Draw(card)
    except:
        draw.ellipse([avatar_x, avatar_y, avatar_x + avatar_size, avatar_y + avatar_size], fill=(60, 60, 70))
    
    # Name and rank below avatar
    draw.text((avatar_x, avatar_y + avatar_size + 15), member.display_name[:15], font=font_name, fill=(255, 255, 255))
    draw.text((avatar_x, avatar_y + avatar_size + 45), f"Rank #{rank}", font=font_stats, fill=border_color)
    
    if roblox:
        draw.text((avatar_x, avatar_y + avatar_size + 70), f"Roblox: {roblox}", font=font_small, fill=(150, 150, 150))
    
    # Stats section (right side)
    stats_x = 250
    stats_y = 70
    box_width = 190
    box_height = 70
    gap = 15
    
    stats_boxes = [
        ("LEVEL", str(lvl), (100, 200, 255)),
        ("XP", format_number(xp), (255, 200, 100)),
        ("COINS", format_number(coins), (255, 215, 0)),
        ("WINS", str(wins), (100, 255, 100)),
        ("LOSSES", str(losses), (255, 100, 100)),
        ("STREAK", f"{daily_streak} days", (255, 150, 50)),
    ]
    
    for i, (label, value, color) in enumerate(stats_boxes):
        col = i % 3
        row = i // 3
        x = stats_x + col * (box_width + gap)
        y = stats_y + row * (box_height + gap)
        
        # Box background
        box_bg = Image.new("RGBA", (box_width, box_height), (40, 40, 50, 200))
        card.paste(box_bg, (x, y), box_bg)
        draw = ImageDraw.Draw(card)
        
        # Top accent
        draw.rectangle([(x, y), (x + box_width, y + 4)], fill=color)
        
        # Label and value
        draw.text((x + 10, y + 15), label, font=font_label, fill=(150, 150, 150))
        draw.text((x + 10, y + 35), value, font=font_stats, fill=color)
    
    # Progress bar
    bar_y = 250
    draw.text((stats_x, bar_y), f"Progress to Level {lvl + 1}", font=font_label, fill=(150, 150, 150))
    
    bar_x, bar_width, bar_height = stats_x, 600, 25
    draw.rounded_rectangle([(bar_x, bar_y + 25), (bar_x + bar_width, bar_y + 25 + bar_height)], radius=12, fill=(40, 40, 50))
    
    if progress > 0:
        fill_width = int(bar_width * progress)
        if fill_width > 24:
            draw.rounded_rectangle([(bar_x, bar_y + 25), (bar_x + fill_width, bar_y + 25 + bar_height)], radius=12, fill=(139, 0, 0))
    
    progress_text = f"{int(progress * 100)}% ({format_number(xp)} / {format_number(req)})"
    draw.text((bar_x + bar_width - 200, bar_y + 28), progress_text, font=font_small, fill=(255, 255, 255))
    
    # Combat stats
    combat_y = 320
    draw.text((stats_x, combat_y), "COMBAT STATS", font=font_stats, fill=(200, 200, 200))
    
    total_matches = wins + losses
    winrate = round((wins / total_matches) * 100, 1) if total_matches > 0 else 0
    raid_total = raid_wins + raid_losses
    raid_wr = round((raid_wins / raid_total) * 100, 1) if raid_total > 0 else 0
    
    draw.text((stats_x, combat_y + 30), f"Matches: {total_matches} ({winrate}% WR)", font=font_label, fill=(150, 150, 150))
    draw.text((stats_x + 250, combat_y + 30), f"Raids: {raid_total} ({raid_wr}% WR)", font=font_label, fill=(150, 150, 150))
    
    # Achievements section
    ach_y = 380
    draw.text((stats_x, ach_y), "ACHIEVEMENTS", font=font_stats, fill=(200, 200, 200))
    
    # Draw achievement badges
    badge_x = stats_x
    badge_size = 40
    unlocked = [a for a in achievements if a.get('unlocked', False)][:10]
    
    for i, ach in enumerate(unlocked):
        x = badge_x + i * (badge_size + 10)
        # Badge circle
        draw.ellipse([x, ach_y + 30, x + badge_size, ach_y + 30 + badge_size], fill=(139, 0, 0))
        # Badge icon (first letter)
        icon = ach.get('icon', '?')[0] if ach.get('icon') else '?'
        draw.text((x + 12, ach_y + 38), icon, font=font_stats, fill=(255, 255, 255))
    
    if not unlocked:
        draw.text((stats_x, ach_y + 35), "No achievements yet", font=font_small, fill=(100, 100, 100))
    
    # Footer
    draw.rectangle([(0, height - 8), (width, height)], fill=border_color)
    draw.text((30, height - 30), "The Fallen", font=font_small, fill=(150, 150, 150))
    
    output = BytesIO()
    card.save(output, format="PNG")
    output.seek(0)
    return output

# ==========================================
# ACHIEVEMENT SYSTEM
# ==========================================

ACHIEVEMENTS = {
    "first_message": {"name": "First Words", "desc": "Send your first message", "icon": "💬", "requirement": 1, "stat": "messages"},
    "chatterbox": {"name": "Chatterbox", "desc": "Send 100 messages", "icon": "🗣️", "requirement": 100, "stat": "messages"},
    "social_butterfly": {"name": "Social Butterfly", "desc": "Send 1000 messages", "icon": "🦋", "requirement": 1000, "stat": "messages"},
    "level_5": {"name": "Rising Star", "desc": "Reach level 5", "icon": "⭐", "requirement": 5, "stat": "level"},
    "level_10": {"name": "Dedicated", "desc": "Reach level 10", "icon": "🌟", "requirement": 10, "stat": "level"},
    "level_25": {"name": "Veteran", "desc": "Reach level 25", "icon": "💫", "requirement": 25, "stat": "level"},
    "level_50": {"name": "Elite", "desc": "Reach level 50", "icon": "🏆", "requirement": 50, "stat": "level"},
    "level_100": {"name": "Legendary", "desc": "Reach level 100", "icon": "👑", "requirement": 100, "stat": "level"},
    "first_win": {"name": "Victory!", "desc": "Win your first match", "icon": "⚔️", "requirement": 1, "stat": "wins"},
    "fighter": {"name": "Fighter", "desc": "Win 10 matches", "icon": "🥊", "requirement": 10, "stat": "wins"},
    "champion": {"name": "Champion", "desc": "Win 50 matches", "icon": "🏅", "requirement": 50, "stat": "wins"},
    "raider": {"name": "Raider", "desc": "Participate in 5 raids", "icon": "🏴‍☠️", "requirement": 5, "stat": "raid_participation"},
    "raid_master": {"name": "Raid Master", "desc": "Win 10 raids", "icon": "⚡", "requirement": 10, "stat": "raid_wins"},
    "rich": {"name": "Getting Rich", "desc": "Earn 10,000 coins", "icon": "💰", "requirement": 10000, "stat": "coins"},
    "wealthy": {"name": "Wealthy", "desc": "Earn 100,000 coins", "icon": "💎", "requirement": 100000, "stat": "coins"},
    "streak_7": {"name": "Week Warrior", "desc": "7 day daily streak", "icon": "🔥", "requirement": 7, "stat": "daily_streak"},
    "streak_30": {"name": "Monthly Master", "desc": "30 day daily streak", "icon": "🌙", "requirement": 30, "stat": "daily_streak"},
    "verified": {"name": "Verified", "desc": "Link your Roblox account", "icon": "✅", "requirement": 1, "stat": "verified"},
}

def check_achievements(user_data):
    """Check which achievements the user has unlocked"""
    unlocked = []
    user_achievements = user_data.get('achievements', [])
    
    for ach_id, ach_data in ACHIEVEMENTS.items():
        stat = ach_data['stat']
        requirement = ach_data['requirement']
        
        # Get the stat value
        if stat == 'verified':
            value = 1 if user_data.get('verified', False) else 0
        else:
            value = user_data.get(stat, 0)
        
        is_unlocked = value >= requirement
        
        unlocked.append({
            'id': ach_id,
            'name': ach_data['name'],
            'desc': ach_data['desc'],
            'icon': ach_data['icon'],
            'unlocked': is_unlocked,
            'progress': min(value, requirement),
            'requirement': requirement
        })
    
    return unlocked

async def check_new_achievements(user_id, guild):
    """Check if user unlocked any new achievements and announce them"""
    data = load_data()
    uid = str(user_id)
    user_data = data["users"].get(uid, {})
    
    old_achievements = set(user_data.get('achievements', []))
    current_achievements = check_achievements(user_data)
    
    new_unlocks = []
    for ach in current_achievements:
        if ach['unlocked'] and ach['id'] not in old_achievements:
            new_unlocks.append(ach)
            old_achievements.add(ach['id'])
    
    if new_unlocks:
        data["users"][uid]['achievements'] = list(old_achievements)
        save_data(data)
        
        # Announce new achievements
        member = guild.get_member(user_id)
        if member:
            for ach in new_unlocks:
                channel = discord.utils.get(guild.text_channels, name=LEVEL_UP_CHANNEL_NAME)
                if channel:
                    embed = discord.Embed(
                        title=f"🏆 Achievement Unlocked!",
                        description=f"{member.mention} unlocked **{ach['name']}**!\n\n{ach['icon']} *{ach['desc']}*",
                        color=0xFFD700
                    )
                    try:
                        await channel.send(embed=embed)
                    except:
                        pass
    
    return new_unlocks

# ==========================================
# ACTIVITY GRAPH GENERATOR
# ==========================================

async def create_activity_graph(member, user_data):
    """Create an activity graph showing XP over time"""
    if not PIL_AVAILABLE:
        return None
    
    width, height = 800, 400
    
    # Create dark background
    card = Image.new("RGBA", (width, height), (20, 20, 30, 255))
    draw = ImageDraw.Draw(card)
    
    try:
        font_title = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf", 24)
        font_label = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf", 12)
        font_small = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf", 10)
    except:
        font_title = font_label = font_small = ImageFont.load_default()
    
    # Title
    draw.text((30, 20), f"{member.display_name}'s Activity", font=font_title, fill=(255, 255, 255))
    
    # Graph area
    graph_x, graph_y = 70, 70
    graph_width, graph_height = 680, 280
    
    # Draw grid
    draw.rectangle([(graph_x, graph_y), (graph_x + graph_width, graph_y + graph_height)], outline=(50, 50, 60))
    
    # Get activity data (simulated - would need to track daily XP)
    activity_log = user_data.get('activity_log', [])
    
    # If no data, show message
    if len(activity_log) < 2:
        draw.text((graph_x + 200, graph_y + 120), "Not enough data yet", font=font_title, fill=(100, 100, 100))
        draw.text((graph_x + 180, graph_y + 160), "Activity tracking starts now!", font=font_label, fill=(80, 80, 80))
    else:
        # Draw line graph
        max_xp = max(d.get('xp', 0) for d in activity_log) or 1
        points = []
        
        for i, day_data in enumerate(activity_log[-30:]):  # Last 30 days
            x = graph_x + int((i / min(29, len(activity_log) - 1)) * graph_width)
            y = graph_y + graph_height - int((day_data.get('xp', 0) / max_xp) * graph_height)
            points.append((x, y))
        
        # Draw line
        if len(points) > 1:
            for i in range(len(points) - 1):
                draw.line([points[i], points[i + 1]], fill=(139, 0, 0), width=3)
            
            # Draw points
            for point in points:
                draw.ellipse([point[0] - 4, point[1] - 4, point[0] + 4, point[1] + 4], fill=(255, 100, 100))
    
    # Grid lines
    for i in range(5):
        y = graph_y + int(i * graph_height / 4)
        draw.line([(graph_x, y), (graph_x + graph_width, y)], fill=(40, 40, 50))
    
    # Y-axis labels
    draw.text((10, graph_y), "High", font=font_small, fill=(100, 100, 100))
    draw.text((10, graph_y + graph_height - 10), "Low", font=font_small, fill=(100, 100, 100))
    
    # X-axis label
    draw.text((graph_x + graph_width // 2 - 30, graph_y + graph_height + 10), "Last 30 Days", font=font_label, fill=(100, 100, 100))
    
    output = BytesIO()
    card.save(output, format="PNG")
    output.seek(0)
    return output

# ==========================================
# RAID HISTORY TRACKER
# ==========================================

RAID_HISTORY_FILE = "raid_history.json"

def load_raid_history():
    try:
        with open(RAID_HISTORY_FILE, "r") as f:
            return json.load(f)
    except:
        return {"raids": []}

def save_raid_history(data):
    with open(RAID_HISTORY_FILE, "w") as f:
        json.dump(data, f, indent=2)

def log_raid(target, result, participants, xp_gained):
    """Log a raid to history"""
    history = load_raid_history()
    history["raids"].append({
        "target": target,
        "result": result,  # "win" or "loss"
        "participants": participants,
        "xp_gained": xp_gained,
        "date": datetime.datetime.now(datetime.timezone.utc).isoformat()
    })
    # Keep last 100 raids
    history["raids"] = history["raids"][-100:]
    save_raid_history(history)

# ==========================================
# TOURNAMENT BRACKET SYSTEM
# ==========================================

TOURNAMENT_FILE = "tournaments.json"

def load_tournaments():
    try:
        with open(TOURNAMENT_FILE, "r") as f:
            return json.load(f)
    except:
        return {"active": None, "history": []}

def save_tournaments(data):
    with open(TOURNAMENT_FILE, "w") as f:
        json.dump(data, f, indent=2)

def create_bracket(participants):
    """Create a tournament bracket from participants"""
    import math
    
    # Pad to power of 2
    n = len(participants)
    size = 2 ** math.ceil(math.log2(max(n, 2)))
    
    # Shuffle participants
    random.shuffle(participants)
    
    # Pad with BYEs
    while len(participants) < size:
        participants.append({"id": None, "name": "BYE"})
    
    # Create bracket structure
    bracket = {
        "rounds": [],
        "current_round": 0,
        "size": size
    }
    
    # First round matchups
    round1 = []
    for i in range(0, size, 2):
        match = {
            "id": len(round1),
            "player1": participants[i],
            "player2": participants[i + 1],
            "winner": None,
            "score": ""
        }
        # Auto-advance BYE matches
        if participants[i]["id"] is None:
            match["winner"] = participants[i + 1]
        elif participants[i + 1]["id"] is None:
            match["winner"] = participants[i]
        round1.append(match)
    
    bracket["rounds"].append(round1)
    
    # Create empty subsequent rounds
    current_matches = len(round1)
    while current_matches > 1:
        current_matches //= 2
        bracket["rounds"].append([{"id": i, "player1": None, "player2": None, "winner": None, "score": ""} for i in range(current_matches)])
    
    return bracket

async def create_bracket_image(tournament_name, bracket):
    """Generate a visual tournament bracket image"""
    if not PIL_AVAILABLE:
        return None
    
    rounds = bracket.get("rounds", [])
    if not rounds:
        return None
    
    num_rounds = len(rounds)
    first_round_matches = len(rounds[0])
    
    # Calculate dimensions
    match_width = 200
    match_height = 60
    round_spacing = 250
    match_spacing = 20
    
    width = num_rounds * round_spacing + 100
    height = max(400, first_round_matches * (match_height + match_spacing) + 200)
    
    # Load background or create dark one
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
        card = Image.new("RGBA", (width, height), (20, 20, 30, 255))
    else:
        background = background.resize((width, height), Image.Resampling.LANCZOS)
        card = background.copy()
        overlay = Image.new("RGBA", (width, height), (0, 0, 0, 220))
        card = Image.alpha_composite(card, overlay)
    
    draw = ImageDraw.Draw(card)
    
    # Load fonts
    try:
        font_title = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf", 28)
        font_round = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf", 16)
        font_name = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf", 14)
        font_small = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf", 11)
    except:
        font_title = font_round = font_name = font_small = ImageFont.load_default()
    
    # Title
    draw.rectangle([(0, 0), (width, 8)], fill=(139, 0, 0))
    title_text = tournament_name.upper()
    title_bbox = draw.textbbox((0, 0), title_text, font=font_title)
    title_width = title_bbox[2] - title_bbox[0]
    draw.text(((width - title_width) // 2, 20), title_text, font=font_title, fill=(255, 255, 255))
    
    # Round names
    round_names = ["Round 1", "Quarter Finals", "Semi Finals", "Finals", "Champion"]
    
    # Draw each round
    for round_idx, round_matches in enumerate(rounds):
        x = 50 + round_idx * round_spacing
        num_matches = len(round_matches)
        
        # Calculate vertical spacing for this round
        total_height = height - 150
        if num_matches > 0:
            spacing = total_height / num_matches
        else:
            spacing = total_height
        
        # Round label
        round_name = round_names[min(round_idx, len(round_names) - 1)] if round_idx < len(rounds) - 1 else "Champion"
        draw.text((x, 60), round_name, font=font_round, fill=(200, 200, 200))
        
        for match_idx, match in enumerate(round_matches):
            y = 100 + match_idx * spacing + (spacing - match_height) / 2
            
            # Match box
            box_color = (60, 20, 20, 200) if match.get("winner") else (40, 40, 50, 200)
            match_bg = Image.new("RGBA", (match_width, match_height), box_color)
            card.paste(match_bg, (int(x), int(y)), match_bg)
            draw = ImageDraw.Draw(card)
            
            # Player names
            p1 = match.get("player1") or {}
            p2 = match.get("player2") or {}
            winner = match.get("winner") or {}
            
            p1_name = p1.get("name", "TBD")[:18] if p1 else "TBD"
            p2_name = p2.get("name", "TBD")[:18] if p2 else "TBD"
            
            # Highlight winner
            p1_color = (100, 255, 100) if winner.get("id") == p1.get("id") and p1.get("id") else (255, 255, 255)
            p2_color = (100, 255, 100) if winner.get("id") == p2.get("id") and p2.get("id") else (255, 255, 255)
            
            if p1_name == "BYE":
                p1_color = (100, 100, 100)
            if p2_name == "BYE":
                p2_color = (100, 100, 100)
            
            draw.text((x + 10, y + 8), p1_name, font=font_name, fill=p1_color)
            draw.line([(x + 5, y + match_height // 2), (x + match_width - 5, y + match_height // 2)], fill=(80, 80, 80))
            draw.text((x + 10, y + match_height // 2 + 5), p2_name, font=font_name, fill=p2_color)
            
            # Score if available
            score = match.get("score", "")
            if score:
                draw.text((x + match_width - 40, y + match_height // 2 - 8), score, font=font_small, fill=(200, 200, 200))
            
            # Draw connector lines to next round
            if round_idx < len(rounds) - 1:
                next_x = x + round_spacing
                # Line from this match to next round
                mid_y = y + match_height // 2
                draw.line([(x + match_width, mid_y), (x + match_width + 20, mid_y)], fill=(139, 0, 0), width=2)
    
    # Footer
    draw.rectangle([(0, height - 8), (width, height)], fill=(139, 0, 0))
    draw.text((20, height - 30), "The Fallen Tournament System", font=font_small, fill=(150, 150, 150))
    
    output = BytesIO()
    card.save(output, format="PNG")
    output.seek(0)
    return output

# ==========================================
# CUSTOM RANK BORDERS
# ==========================================

RANK_BORDERS = {
    # Rank range: (border_color, border_style, title)
    1: {"color": (255, 215, 0), "style": "legendary", "title": "Champion", "glow": True},
    2: {"color": (192, 192, 192), "style": "elite", "title": "Elite", "glow": True},
    3: {"color": (205, 127, 50), "style": "elite", "title": "Veteran", "glow": True},
    10: {"color": (139, 0, 0), "style": "rare", "title": "Top 10", "glow": False},
    25: {"color": (100, 100, 200), "style": "uncommon", "title": "Rising", "glow": False},
    50: {"color": (100, 200, 100), "style": "common", "title": "Active", "glow": False},
    100: {"color": (150, 150, 150), "style": "common", "title": "Member", "glow": False},
}

def get_rank_border(rank):
    """Get the appropriate border style for a rank"""
    for threshold, style in sorted(RANK_BORDERS.items()):
        if rank <= threshold:
            return style
    return {"color": (100, 100, 100), "style": "common", "title": "Newcomer", "glow": False}

def draw_rank_border(draw, card, x, y, size, border_style):
    """Draw a custom border around avatar based on rank"""
    color = border_style["color"]
    style = border_style["style"]
    glow = border_style.get("glow", False)
    
    if style == "legendary":
        # Double border with glow effect
        if glow and PIL_AVAILABLE:
            # Outer glow
            for i in range(3, 0, -1):
                alpha = 50 * (4 - i)
                glow_color = (*color, alpha)
                draw.ellipse(
                    [x - 8 - i*2, y - 8 - i*2, x + size + 8 + i*2, y + size + 8 + i*2],
                    outline=color, width=2
                )
        # Main borders
        draw.ellipse([x - 8, y - 8, x + size + 8, y + size + 8], outline=color, width=4)
        draw.ellipse([x - 3, y - 3, x + size + 3, y + size + 3], outline=(255, 255, 255), width=2)
        
    elif style == "elite":
        # Thick colored border
        draw.ellipse([x - 6, y - 6, x + size + 6, y + size + 6], outline=color, width=5)
        
    elif style == "rare":
        # Double thin border
        draw.ellipse([x - 5, y - 5, x + size + 5, y + size + 5], outline=color, width=3)
        draw.ellipse([x - 2, y - 2, x + size + 2, y + size + 2], outline=(60, 60, 70), width=1)
        
    elif style == "uncommon":
        # Single colored border
        draw.ellipse([x - 4, y - 4, x + size + 4, y + size + 4], outline=color, width=3)
        
    else:  # common
        # Simple border
        draw.ellipse([x - 3, y - 3, x + size + 3, y + size + 3], outline=color, width=2)

# ==========================================
# ==========================================
# SERVER STATS IMAGE GENERATOR
# ==========================================

async def create_server_stats_image(guild):
    """Create a beautiful server statistics image"""
    if not PIL_AVAILABLE:
        return None
    
    width, height = 900, 550
    
    # Load background
    background = None
    for path in LEVEL_CARD_PATHS:
        if os.path.exists(path):
            try:
                background = Image.open(path).convert("RGBA")
                break
            except:
                pass
    
    if background is None:
        background = Image.new("RGBA", (width, height), (20, 20, 30, 255))
    else:
        background = background.resize((width, height), Image.Resampling.LANCZOS)
    
    card = background.copy()
    overlay = Image.new("RGBA", (width, height), (0, 0, 0, 200))
    card = Image.alpha_composite(card, overlay)
    draw = ImageDraw.Draw(card)
    
    try:
        font_title = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf", 36)
        font_stat = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf", 28)
        font_label = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf", 16)
        font_small = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf", 12)
    except:
        font_title = font_stat = font_label = font_small = ImageFont.load_default()
    
    # Top border
    draw.rectangle([(0, 0), (width, 8)], fill=(139, 0, 0))
    
    # Title
    title = "THE FALLEN STATISTICS"
    t_bbox = draw.textbbox((0, 0), title, font=font_title)
    t_width = t_bbox[2] - t_bbox[0]
    draw.text(((width - t_width) // 2, 25), title, font=font_title, fill=(255, 255, 255))
    
    # Load data
    data = load_data()
    users = data.get("users", {})
    
    total_xp = sum(u.get('xp', 0) for u in users.values())
    total_coins = sum(u.get('coins', 0) for u in users.values())
    total_voice = sum(u.get('voice_time', 0) for u in users.values())
    total_wins = sum(u.get('wins', 0) for u in users.values())
    total_raids = sum(u.get('raid_participation', 0) for u in users.values())
    verified_count = sum(1 for u in users.values() if u.get('verified', False))
    
    # Stats boxes
    stats = [
        ("👥 MEMBERS", str(guild.member_count), (100, 200, 255)),
        ("✅ VERIFIED", str(verified_count), (100, 255, 100)),
        ("✨ TOTAL XP", format_number(total_xp), (255, 200, 100)),
        ("💰 TOTAL COINS", format_number(total_coins), (255, 215, 0)),
        ("🎙️ VOICE HOURS", f"{total_voice // 60}h", (200, 100, 255)),
        ("⚔️ MATCHES", format_number(total_wins * 2), (255, 100, 100)),
    ]
    
    box_width = 250
    box_height = 100
    start_x = 75
    start_y = 90
    gap = 30
    
    for i, (label, value, color) in enumerate(stats):
        col = i % 3
        row = i // 3
        x = start_x + col * (box_width + gap)
        y = start_y + row * (box_height + gap)
        
        # Box background
        box_bg = Image.new("RGBA", (box_width, box_height), (40, 40, 50, 200))
        card.paste(box_bg, (x, y), box_bg)
        draw = ImageDraw.Draw(card)
        
        # Top accent
        draw.rectangle([(x, y), (x + box_width, y + 5)], fill=color)
        
        # Label
        draw.text((x + 15, y + 20), label, font=font_label, fill=(150, 150, 150))
        
        # Value
        draw.text((x + 15, y + 50), value, font=font_stat, fill=color)
    
    # Top members section
    section_y = 320
    draw.text((75, section_y), "🏆 TOP MEMBERS", font=font_label, fill=(200, 200, 200))
    
    sorted_users = sorted(users.items(), key=lambda x: x[1].get('xp', 0), reverse=True)[:5]
    
    for i, (uid, udata) in enumerate(sorted_users):
        member = guild.get_member(int(uid))
        name = member.display_name[:15] if member else f"User {uid[:8]}"
        xp = format_number(udata.get('xp', 0))
        
        y = section_y + 30 + i * 25
        rank_color = [(255, 215, 0), (192, 192, 192), (205, 127, 50), (150, 150, 150), (150, 150, 150)][i]
        
        draw.text((75, y), f"#{i+1}", font=font_label, fill=rank_color)
        draw.text((110, y), name, font=font_label, fill=(255, 255, 255))
        draw.text((280, y), xp, font=font_label, fill=(255, 200, 100))
    
    # Recent activity
    draw.text((450, section_y), "📊 SERVER INFO", font=font_label, fill=(200, 200, 200))
    
    online = sum(1 for m in guild.members if m.status != discord.Status.offline)
    text_channels = len(guild.text_channels)
    voice_channels = len(guild.voice_channels)
    roles = len(guild.roles)
    
    info_items = [
        f"🟢 Online: {online}",
        f"💬 Text Channels: {text_channels}",
        f"🔊 Voice Channels: {voice_channels}",
        f"🏷️ Roles: {roles}",
        f"📅 Created: {guild.created_at.strftime('%b %d, %Y')}"
    ]
    
    for i, item in enumerate(info_items):
        draw.text((450, section_y + 30 + i * 25), item, font=font_label, fill=(180, 180, 180))
    
    # Footer
    draw.rectangle([(0, height - 8), (width, height)], fill=(139, 0, 0))
    draw.text((30, height - 35), f"Generated: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M')}", font=font_small, fill=(100, 100, 100))
    
    output = BytesIO()
    card.save(output, format="PNG")
    output.seek(0)
    return output

# ==========================================
# SHOP IMAGE GENERATOR
# ==========================================

async def create_shop_image():
    """Create a visual shop image"""
    if not PIL_AVAILABLE:
        return None
    
    width, height = 900, 600
    
    # Load background
    background = None
    for path in LEVEL_CARD_PATHS:
        if os.path.exists(path):
            try:
                background = Image.open(path).convert("RGBA")
                break
            except:
                pass
    
    if background is None:
        background = Image.new("RGBA", (width, height), (20, 20, 30, 255))
    else:
        background = background.resize((width, height), Image.Resampling.LANCZOS)
    
    card = background.copy()
    overlay = Image.new("RGBA", (width, height), (0, 0, 0, 210))
    card = Image.alpha_composite(card, overlay)
    draw = ImageDraw.Draw(card)
    
    try:
        font_title = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf", 36)
        font_item = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf", 20)
        font_desc = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf", 14)
        font_price = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf", 18)
    except:
        font_title = font_item = font_desc = font_price = ImageFont.load_default()
    
    # Top border
    draw.rectangle([(0, 0), (width, 8)], fill=(139, 0, 0))
    
    # Title
    title = "🛒 THE FALLEN SHOP"
    t_bbox = draw.textbbox((0, 0), title, font=font_title)
    t_width = t_bbox[2] - t_bbox[0]
    draw.text(((width - t_width) // 2, 25), title, font=font_title, fill=(255, 255, 255))
    
    draw.text((width // 2 - 100, 70), "Spend your Fallen Coins!", font=font_desc, fill=(180, 180, 180))
    
    # Shop items
    item_height = 90
    start_y = 110
    
    for i, item in enumerate(SHOP_ITEMS):
        y = start_y + i * (item_height + 10)
        
        # Item background
        item_bg = Image.new("RGBA", (width - 80, item_height), (50, 30, 30, 200))
        card.paste(item_bg, (40, y), item_bg)
        draw = ImageDraw.Draw(card)
        
        # Left accent bar
        draw.rectangle([(40, y), (48, y + item_height)], fill=(139, 0, 0))
        
        # Item name
        draw.text((70, y + 15), item['name'], font=font_item, fill=(255, 255, 255))
        
        # Description
        draw.text((70, y + 45), item['desc'], font=font_desc, fill=(180, 180, 180))
        
        # Price tag
        price_text = f"💰 {item['price']:,}"
        p_bbox = draw.textbbox((0, 0), price_text, font=font_price)
        p_width = p_bbox[2] - p_bbox[0]
        
        # Price background
        draw.rounded_rectangle([(width - 180, y + 25), (width - 60, y + 60)], radius=8, fill=(139, 0, 0))
        draw.text((width - 170, y + 30), price_text, font=font_price, fill=(255, 215, 0))
    
    # Footer
    draw.rectangle([(0, height - 8), (width, height)], fill=(139, 0, 0))
    draw.text((40, height - 35), "Click the buttons below to purchase!", font=font_desc, fill=(150, 150, 150))
    
    output = BytesIO()
    card.save(output, format="PNG")
    output.seek(0)
    return output

# ==========================================
# MEMBER MILESTONES SYSTEM
# ==========================================

MEMBER_MILESTONES = [50, 100, 150, 200, 250, 300, 400, 500, 750, 1000]

async def check_member_milestone(guild):
    """Check if guild hit a member milestone and announce it"""
    member_count = guild.member_count
    
    for milestone in MEMBER_MILESTONES:
        if member_count == milestone:
            # Find announcement channel
            channel = discord.utils.get(guild.text_channels, name="general") or \
                      discord.utils.get(guild.text_channels, name="welcome") or \
                      guild.text_channels[0] if guild.text_channels else None
            
            if channel:
                # Create milestone image
                milestone_image = await create_milestone_image(guild, milestone)
                
                if milestone_image:
                    file = discord.File(milestone_image, filename="milestone.png")
                    embed = discord.Embed(
                        title="🎉 MEMBER MILESTONE! 🎉",
                        description=f"**The Fallen** has reached **{milestone} members**!\n\nThank you all for being part of our community! ⚔️",
                        color=0xFFD700
                    )
                    embed.set_image(url="attachment://milestone.png")
                    await channel.send(embed=embed, file=file)
                else:
                    embed = discord.Embed(
                        title="🎉 MEMBER MILESTONE! 🎉",
                        description=f"**The Fallen** has reached **{milestone} members**!\n\nThank you all for being part of our community! ⚔️",
                        color=0xFFD700
                    )
                    await channel.send(embed=embed)
            break

async def create_milestone_image(guild, milestone):
    """Create a celebration image for member milestones"""
    if not PIL_AVAILABLE:
        return None
    
    width, height = 800, 400
    
    # Load background
    background = None
    for path in LEVEL_CARD_PATHS:
        if os.path.exists(path):
            try:
                background = Image.open(path).convert("RGBA")
                break
            except:
                pass
    
    if background is None:
        background = Image.new("RGBA", (width, height), (20, 20, 30, 255))
    else:
        background = background.resize((width, height), Image.Resampling.LANCZOS)
    
    card = background.copy()
    overlay = Image.new("RGBA", (width, height), (0, 0, 0, 180))
    card = Image.alpha_composite(card, overlay)
    draw = ImageDraw.Draw(card)
    
    try:
        font_big = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf", 72)
        font_title = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf", 32)
        font_sub = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf", 20)
    except:
        font_big = font_title = font_sub = ImageFont.load_default()
    
    # Top border
    draw.rectangle([(0, 0), (width, 10)], fill=(255, 215, 0))
    
    # Celebration text
    text1 = "🎉 MILESTONE REACHED! 🎉"
    t1_bbox = draw.textbbox((0, 0), text1, font=font_title)
    t1_width = t1_bbox[2] - t1_bbox[0]
    draw.text(((width - t1_width) // 2, 50), text1, font=font_title, fill=(255, 215, 0))
    
    # Big number
    number = str(milestone)
    n_bbox = draw.textbbox((0, 0), number, font=font_big)
    n_width = n_bbox[2] - n_bbox[0]
    draw.text(((width - n_width) // 2, 120), number, font=font_big, fill=(255, 255, 255))
    
    # Members text
    text2 = "MEMBERS"
    t2_bbox = draw.textbbox((0, 0), text2, font=font_title)
    t2_width = t2_bbox[2] - t2_bbox[0]
    draw.text(((width - t2_width) // 2, 210), text2, font=font_title, fill=(200, 200, 200))
    
    # Thank you message
    text3 = "Thank you for being part of The Fallen!"
    t3_bbox = draw.textbbox((0, 0), text3, font=font_sub)
    t3_width = t3_bbox[2] - t3_bbox[0]
    draw.text(((width - t3_width) // 2, 280), text3, font=font_sub, fill=(180, 180, 180))
    
    # Server name
    text4 = "✝ THE FALLEN ✝"
    t4_bbox = draw.textbbox((0, 0), text4, font=font_title)
    t4_width = t4_bbox[2] - t4_bbox[0]
    draw.text(((width - t4_width) // 2, 330), text4, font=font_title, fill=(139, 0, 0))
    
    # Bottom border
    draw.rectangle([(0, height - 10), (width, height)], fill=(255, 215, 0))
    
    output = BytesIO()
    card.save(output, format="PNG")
    output.seek(0)
    return output

# ==========================================
# VOICE LEADERBOARD IMAGE GENERATOR
# ==========================================

async def create_voice_leaderboard_image(guild):
    """Create a voice time leaderboard image"""
    if not PIL_AVAILABLE:
        return None
    
    data = load_data()
    users = data.get("users", {})
    
    # Sort by voice time
    sorted_users = sorted(users.items(), key=lambda x: x[1].get('voice_time', 0), reverse=True)[:10]
    
    if not sorted_users:
        return None
    
    num_users = len(sorted_users)
    row_height = 60
    header_height = 140
    footer_height = 50
    width = 900
    height = header_height + (num_users * row_height) + footer_height
    
    # Load background
    background = None
    for path in LEVEL_CARD_PATHS:
        if os.path.exists(path):
            try:
                background = Image.open(path).convert("RGBA")
                break
            except:
                pass
    
    if background is None:
        background = Image.new("RGBA", (width, height), (20, 20, 30, 255))
    else:
        background = background.resize((width, height), Image.Resampling.LANCZOS)
    
    card = background.copy()
    overlay = Image.new("RGBA", (width, height), (0, 0, 0, 200))
    card = Image.alpha_composite(card, overlay)
    draw = ImageDraw.Draw(card)
    
    try:
        font_title = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf", 32)
        font_sub = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf", 16)
        font_name = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf", 18)
        font_stat = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf", 16)
    except:
        font_title = font_sub = font_name = font_stat = ImageFont.load_default()
    
    # Header
    draw.rectangle([(0, 0), (width, 8)], fill=(139, 0, 0))
    
    title = "🎙️ VOICE LEADERBOARD"
    t_bbox = draw.textbbox((0, 0), title, font=font_title)
    t_width = t_bbox[2] - t_bbox[0]
    draw.text(((width - t_width) // 2, 30), title, font=font_title, fill=(255, 255, 255))
    
    draw.text((width // 2 - 80, 75), "Top Voice Activity", font=font_sub, fill=(180, 180, 180))
    
    # Column headers
    draw.rectangle([(30, 105), (width - 30, 135)], fill=(60, 20, 20))
    draw.text((50, 110), "RANK", font=font_sub, fill=(200, 200, 200))
    draw.text((150, 110), "USER", font=font_sub, fill=(200, 200, 200))
    draw.text((500, 110), "VOICE TIME", font=font_sub, fill=(200, 200, 200))
    draw.text((700, 110), "XP FROM VOICE", font=font_sub, fill=(200, 200, 200))
    
    # Rows
    rank_colors = {1: (255, 215, 0), 2: (192, 192, 192), 3: (205, 127, 50)}
    
    for i, (uid, udata) in enumerate(sorted_users):
        y = header_height + i * row_height
        rank = i + 1
        
        # Row background
        row_color = (80, 20, 20, 150) if i % 2 == 0 else (60, 15, 15, 150)
        row_bg = Image.new("RGBA", (width - 60, row_height - 5), row_color)
        card.paste(row_bg, (30, y), row_bg)
        draw = ImageDraw.Draw(card)
        
        # Rank
        rank_color = rank_colors.get(rank, (180, 180, 180))
        draw.text((60, y + 18), f"#{rank}", font=font_name, fill=rank_color)
        
        # Username
        member = guild.get_member(int(uid))
        name = member.display_name[:20] if member else f"User {uid[:8]}"
        draw.text((150, y + 18), name, font=font_name, fill=(255, 255, 255))
        
        # Voice time
        voice_mins = udata.get('voice_time', 0)
        hours = voice_mins // 60
        mins = voice_mins % 60
        time_text = f"{hours}h {mins}m"
        draw.text((500, y + 18), time_text, font=font_stat, fill=(200, 100, 255))
        
        # Estimated XP from voice
        voice_xp = voice_mins * 10  # Rough estimate
        draw.text((700, y + 18), format_number(voice_xp), font=font_stat, fill=(255, 200, 100))
    
    # Footer
    draw.rectangle([(0, height - footer_height), (width, height - footer_height + 3)], fill=(139, 0, 0))
    draw.text((30, height - 35), f"The Fallen | {guild.member_count} Members", font=font_stat, fill=(150, 150, 150))
    
    output = BytesIO()
    card.save(output, format="PNG")
    output.seek(0)
    return output

# ==========================================
# ELO DUEL SYSTEM
# ==========================================

DUELS_FILE = "duels_data.json"
DEFAULT_ELO = 1000
MIN_ELO = 100
K_FACTOR = 32  # How much ELO changes per match

# ELO Rank Tiers
ELO_TIERS = [
    (2000, "🏆 Grandmaster", (255, 215, 0)),
    (1800, "💎 Diamond", (185, 242, 255)),
    (1600, "🥇 Platinum", (229, 228, 226)),
    (1400, "🥈 Gold", (255, 215, 0)),
    (1200, "🥉 Silver", (192, 192, 192)),
    (1000, "⚔️ Bronze", (205, 127, 50)),
    (0, "🗡️ Unranked", (128, 128, 128)),
]

def load_duels_data():
    try:
        with open(DUELS_FILE, "r") as f:
            return json.load(f)
    except:
        return {"elo": {}, "pending_duels": {}, "duel_history": [], "active_duels": {}}

def save_duels_data(data):
    with open(DUELS_FILE, "w") as f:
        json.dump(data, f, indent=2)

def get_elo(user_id):
    """Get a user's ELO rating"""
    data = load_duels_data()
    return data["elo"].get(str(user_id), DEFAULT_ELO)

def set_elo(user_id, elo):
    """Set a user's ELO rating"""
    data = load_duels_data()
    data["elo"][str(user_id)] = max(MIN_ELO, elo)  # Can't go below MIN_ELO
    save_duels_data(data)

def get_elo_tier(elo):
    """Get the tier name and color for an ELO rating"""
    for threshold, name, color in ELO_TIERS:
        if elo >= threshold:
            return name, color
    return "🗡️ Unranked", (128, 128, 128)

def calculate_elo_change(winner_elo, loser_elo):
    """Calculate ELO change using standard ELO formula"""
    expected_winner = 1 / (1 + 10 ** ((loser_elo - winner_elo) / 400))
    expected_loser = 1 / (1 + 10 ** ((winner_elo - loser_elo) / 400))
    
    winner_change = round(K_FACTOR * (1 - expected_winner))
    loser_change = round(K_FACTOR * (0 - expected_loser))
    
    return winner_change, loser_change

def create_pending_duel(challenger_id, opponent_id, ps_link):
    """Create a pending duel request"""
    data = load_duels_data()
    duel_id = f"duel_{int(datetime.datetime.now().timestamp())}"
    
    data["pending_duels"][duel_id] = {
        "challenger": str(challenger_id),
        "opponent": str(opponent_id),
        "ps_link": ps_link,
        "created_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "status": "pending"
    }
    save_duels_data(data)
    return duel_id

def get_pending_duel(duel_id):
    """Get a pending duel by ID"""
    data = load_duels_data()
    return data["pending_duels"].get(duel_id)

def accept_duel(duel_id, channel_id):
    """Accept a duel and move it to active"""
    data = load_duels_data()
    if duel_id in data["pending_duels"]:
        duel = data["pending_duels"].pop(duel_id)
        duel["status"] = "active"
        duel["channel_id"] = str(channel_id)
        duel["accepted_at"] = datetime.datetime.now(datetime.timezone.utc).isoformat()
        data["active_duels"][duel_id] = duel
        save_duels_data(data)
        return duel
    return None

def decline_duel(duel_id):
    """Decline/cancel a duel"""
    data = load_duels_data()
    if duel_id in data["pending_duels"]:
        del data["pending_duels"][duel_id]
        save_duels_data(data)
        return True
    if duel_id in data["active_duels"]:
        del data["active_duels"][duel_id]
        save_duels_data(data)
        return True
    return False

def complete_duel(duel_id, winner_id, loser_id):
    """Complete a duel and update ELO"""
    data = load_duels_data()
    
    if duel_id not in data["active_duels"]:
        return None
    
    duel = data["active_duels"].pop(duel_id)
    
    # Get current ELO
    winner_elo = data["elo"].get(str(winner_id), DEFAULT_ELO)
    loser_elo = data["elo"].get(str(loser_id), DEFAULT_ELO)
    
    # Calculate changes
    winner_change, loser_change = calculate_elo_change(winner_elo, loser_elo)
    
    # Apply changes
    new_winner_elo = winner_elo + winner_change
    new_loser_elo = max(MIN_ELO, loser_elo + loser_change)
    
    data["elo"][str(winner_id)] = new_winner_elo
    data["elo"][str(loser_id)] = new_loser_elo
    
    # Record history
    history_entry = {
        "duel_id": duel_id,
        "winner": str(winner_id),
        "loser": str(loser_id),
        "winner_elo_before": winner_elo,
        "winner_elo_after": new_winner_elo,
        "loser_elo_before": loser_elo,
        "loser_elo_after": new_loser_elo,
        "completed_at": datetime.datetime.now(datetime.timezone.utc).isoformat()
    }
    data["duel_history"].append(history_entry)
    
    # Keep only last 500 duels in history
    if len(data["duel_history"]) > 500:
        data["duel_history"] = data["duel_history"][-500:]
    
    save_duels_data(data)
    
    return {
        "winner_change": winner_change,
        "loser_change": loser_change,
        "winner_new_elo": new_winner_elo,
        "loser_new_elo": new_loser_elo
    }

def get_duel_history(user_id, limit=10):
    """Get duel history for a user"""
    data = load_duels_data()
    uid = str(user_id)
    
    user_duels = [d for d in data["duel_history"] if d["winner"] == uid or d["loser"] == uid]
    return user_duels[-limit:][::-1]  # Most recent first

def get_elo_leaderboard(limit=10):
    """Get top ELO players"""
    data = load_duels_data()
    sorted_elo = sorted(data["elo"].items(), key=lambda x: x[1], reverse=True)
    return sorted_elo[:limit]

# Duel Request View
class DuelRequestView(discord.ui.View):
    def __init__(self, duel_id, challenger, opponent, ps_link):
        super().__init__(timeout=300)  # 5 minute timeout
        self.duel_id = duel_id
        self.challenger = challenger
        self.opponent = opponent
        self.ps_link = ps_link
    
    @discord.ui.button(label="Accept Duel", style=discord.ButtonStyle.success, emoji="⚔️")
    async def accept(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            if interaction.user.id != self.opponent.id:
                return await interaction.response.send_message("❌ Only the challenged player can accept!", ephemeral=True)
            
            # Defer immediately to prevent timeout
            await interaction.response.defer()
            
            guild = interaction.guild
            
            # Create duel ticket channel
            category = discord.utils.get(guild.categories, name="DUELS") or discord.utils.get(guild.categories, name="TICKETS")
            
            if not category:
                try:
                    category = await guild.create_category("DUELS")
                except:
                    category = None
            
            # Create the channel with truncated names
            c_name = self.challenger.display_name[:8].replace(" ", "-")
            o_name = self.opponent.display_name[:8].replace(" ", "-")
            channel_name = f"duel-{c_name}-vs-{o_name}"
            
            overwrites = {
                guild.default_role: discord.PermissionOverwrite(view_channel=False),
                self.challenger: discord.PermissionOverwrite(view_channel=True, send_messages=True),
                self.opponent: discord.PermissionOverwrite(view_channel=True, send_messages=True),
                guild.me: discord.PermissionOverwrite(view_channel=True, send_messages=True, manage_channels=True),
            }
            
            # Add staff roles
            for role_name in HIGH_STAFF_ROLES + [STAFF_ROLE_NAME]:
                role = discord.utils.get(guild.roles, name=role_name)
                if role:
                    overwrites[role] = discord.PermissionOverwrite(view_channel=True, send_messages=True)
            
            duel_channel = await guild.create_text_channel(
                channel_name, 
                category=category, 
                overwrites=overwrites
            )
            
            # Accept the duel
            accept_duel(self.duel_id, duel_channel.id)
            
            # Get ELO info
            challenger_elo = get_elo(self.challenger.id)
            opponent_elo = get_elo(self.opponent.id)
            challenger_tier, _ = get_elo_tier(challenger_elo)
            opponent_tier, _ = get_elo_tier(opponent_elo)
            
            # Send duel embed to channel
            embed = discord.Embed(
                title="⚔️ DUEL MATCH ⚔️",
                description=(
                    f"**{self.challenger.display_name}** vs **{self.opponent.display_name}**\n\n"
                    f"🎮 **Private Server Link:**\n{self.ps_link}\n\n"
                    f"**ELO Ratings:**\n"
                    f"• {self.challenger.mention}: **{challenger_elo}** {challenger_tier}\n"
                    f"• {self.opponent.mention}: **{opponent_elo}** {opponent_tier}\n\n"
                    f"*Join the private server and fight! Staff will report the winner.*"
                ),
                color=0xe74c3c
            )
            embed.set_footer(text=f"Duel ID: {self.duel_id}")
            
            await duel_channel.send(
                f"{self.challenger.mention} {self.opponent.mention}",
                embed=embed,
                view=DuelStaffControlView(self.duel_id, self.challenger, self.opponent)
            )
            
            # Update original message
            await interaction.followup.edit_message(
                message_id=interaction.message.id,
                content=f"✅ Duel accepted! Go to {duel_channel.mention}",
                embed=None,
                view=None
            )
        except Exception as e:
            print(f"Duel accept error: {e}")
            try:
                await interaction.followup.send(f"❌ Error creating duel channel. Please try again.", ephemeral=True)
            except:
                pass
    
    @discord.ui.button(label="Decline", style=discord.ButtonStyle.danger, emoji="❌")
    async def decline(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            if interaction.user.id != self.opponent.id:
                return await interaction.response.send_message("❌ Only the challenged player can decline!", ephemeral=True)
            
            decline_duel(self.duel_id)
            await interaction.response.edit_message(
                content=f"❌ {self.opponent.display_name} declined the duel.",
                embed=None,
                view=None
            )
        except Exception as e:
            print(f"Duel decline error: {e}")
    
    async def on_timeout(self):
        """Handle view timeout"""
        decline_duel(self.duel_id)

# Staff Control View for Duel Channel
class DuelStaffControlView(discord.ui.View):
    def __init__(self, duel_id, player1, player2):
        super().__init__(timeout=None)
        self.duel_id = duel_id
        self.player1 = player1
        self.player2 = player2
        
        # Add custom_id for persistence
        self.player1_btn = discord.ui.Button(
            label=f"{player1.display_name} Wins",
            style=discord.ButtonStyle.success,
            custom_id=f"duel_win_{duel_id}_p1",
            row=0
        )
        self.player1_btn.callback = self.player1_wins
        self.add_item(self.player1_btn)
        
        self.player2_btn = discord.ui.Button(
            label=f"{player2.display_name} Wins",
            style=discord.ButtonStyle.success,
            custom_id=f"duel_win_{duel_id}_p2",
            row=0
        )
        self.player2_btn.callback = self.player2_wins
        self.add_item(self.player2_btn)
    
    async def player1_wins(self, interaction: discord.Interaction):
        await self.report_winner(interaction, self.player1, self.player2)
    
    async def player2_wins(self, interaction: discord.Interaction):
        await self.report_winner(interaction, self.player2, self.player1)
    
    async def report_winner(self, interaction: discord.Interaction, winner, loser):
        # Check if staff
        if not is_staff(interaction.user):
            return await interaction.response.send_message("❌ Only staff can report duel results!", ephemeral=True)
        
        await interaction.response.defer()
        
        # Complete the duel
        result = complete_duel(self.duel_id, winner.id, loser.id)
        
        if not result:
            return await interaction.followup.send("❌ This duel has already been completed or doesn't exist.")
        
        winner_tier, _ = get_elo_tier(result["winner_new_elo"])
        loser_tier, _ = get_elo_tier(result["loser_new_elo"])
        
        # Result embed
        embed = discord.Embed(
            title="🏆 DUEL COMPLETE 🏆",
            description=f"**Winner:** {winner.mention}",
            color=0x2ecc71
        )
        embed.add_field(
            name=f"⬆️ {winner.display_name}",
            value=f"**{result['winner_new_elo']}** (+{result['winner_change']})\n{winner_tier}",
            inline=True
        )
        embed.add_field(
            name=f"⬇️ {loser.display_name}",
            value=f"**{result['loser_new_elo']}** ({result['loser_change']})\n{loser_tier}",
            inline=True
        )
        embed.set_footer(text=f"Reported by {interaction.user.display_name}")
        
        await interaction.message.edit(embed=embed, view=None)
        await interaction.followup.send(f"✅ Duel result recorded! This channel will be deleted in 30 seconds.")
        
        # Log to dashboard
        await log_to_dashboard(
            interaction.guild, "⚔️ DUEL", "Duel Completed",
            f"**Winner:** {winner.mention} (+{result['winner_change']} → {result['winner_new_elo']})\n"
            f"**Loser:** {loser.mention} ({result['loser_change']} → {result['loser_new_elo']})",
            color=0x2ecc71,
            fields={"Reported By": interaction.user.mention}
        )
        
        # Delete channel after delay
        await asyncio.sleep(30)
        try:
            await interaction.channel.delete()
        except:
            pass
    
    @discord.ui.button(label="Cancel Duel", style=discord.ButtonStyle.danger, emoji="🚫", row=1)
    async def cancel_duel(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not is_staff(interaction.user):
            return await interaction.response.send_message("❌ Only staff can cancel duels!", ephemeral=True)
        
        decline_duel(self.duel_id)
        
        embed = discord.Embed(
            title="🚫 DUEL CANCELLED",
            description=f"This duel has been cancelled by {interaction.user.mention}.\n\nNo ELO changes.",
            color=0xe74c3c
        )
        
        await interaction.response.edit_message(embed=embed, view=None)
        await interaction.followup.send("Channel will be deleted in 10 seconds.")
        
        await asyncio.sleep(10)
        try:
            await interaction.channel.delete()
        except:
            pass

# ==========================================
# UPDATED TOURNAMENT SYSTEM
# ==========================================

TOURNAMENTS_FILE = "tournaments.json"

def load_tournaments():
    try:
        with open(TOURNAMENTS_FILE, "r") as f:
            return json.load(f)
    except:
        return {"active": None, "history": []}

def save_tournaments(data):
    with open(TOURNAMENTS_FILE, "w") as f:
        json.dump(data, f, indent=2)

def create_tournament(name, creator_id):
    """Create a new tournament"""
    data = load_tournaments()
    
    if data["active"]:
        return None  # Tournament already active
    
    tournament = {
        "id": f"tourney_{int(datetime.datetime.now().timestamp())}",
        "name": name,
        "creator": str(creator_id),
        "participants": [],
        "bracket": None,
        "status": "signup",  # signup, active, complete
        "created_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "winner": None
    }
    
    data["active"] = tournament
    save_tournaments(data)
    return tournament

def join_tournament(user_id):
    """Join the active tournament"""
    data = load_tournaments()
    
    if not data["active"] or data["active"]["status"] != "signup":
        return False
    
    uid = str(user_id)
    if uid not in data["active"]["participants"]:
        data["active"]["participants"].append(uid)
        save_tournaments(data)
        return True
    return False

def leave_tournament(user_id):
    """Leave the active tournament"""
    data = load_tournaments()
    
    if not data["active"] or data["active"]["status"] != "signup":
        return False
    
    uid = str(user_id)
    if uid in data["active"]["participants"]:
        data["active"]["participants"].remove(uid)
        save_tournaments(data)
        return True
    return False

def start_tournament():
    """Start the tournament and generate bracket"""
    data = load_tournaments()
    
    if not data["active"] or data["active"]["status"] != "signup":
        return None
    
    if len(data["active"]["participants"]) < 2:
        return None
    
    # Generate bracket
    participants = data["active"]["participants"].copy()
    random.shuffle(participants)
    
    # Pad to power of 2
    import math
    size = 2 ** math.ceil(math.log2(len(participants)))
    while len(participants) < size:
        participants.append(None)  # BYE
    
    # Create bracket structure
    bracket = {
        "rounds": [],
        "current_round": 0
    }
    
    # First round
    round1 = []
    for i in range(0, len(participants), 2):
        match = {
            "id": f"m{len(round1)+1}",
            "player1": participants[i],
            "player2": participants[i+1],
            "winner": None
        }
        # Auto-advance BYEs
        if match["player1"] is None:
            match["winner"] = match["player2"]
        elif match["player2"] is None:
            match["winner"] = match["player1"]
        round1.append(match)
    
    bracket["rounds"].append(round1)
    
    # Create empty subsequent rounds
    num_rounds = int(math.log2(size))
    matches_in_round = len(round1) // 2
    
    for r in range(1, num_rounds):
        round_matches = []
        for i in range(matches_in_round):
            round_matches.append({
                "id": f"r{r+1}m{i+1}",
                "player1": None,
                "player2": None,
                "winner": None
            })
        bracket["rounds"].append(round_matches)
        matches_in_round = max(1, matches_in_round // 2)
    
    data["active"]["bracket"] = bracket
    data["active"]["status"] = "active"
    save_tournaments(data)
    
    return data["active"]

def report_tournament_match(match_id, winner_id):
    """Report a tournament match result"""
    data = load_tournaments()
    
    if not data["active"] or data["active"]["status"] != "active":
        return None
    
    bracket = data["active"]["bracket"]
    winner_uid = str(winner_id)
    
    # Find the match
    for round_idx, round_matches in enumerate(bracket["rounds"]):
        for match in round_matches:
            if match["id"] == match_id:
                if winner_uid not in [match["player1"], match["player2"]]:
                    return None
                
                match["winner"] = winner_uid
                
                # Advance winner to next round
                if round_idx < len(bracket["rounds"]) - 1:
                    next_round = bracket["rounds"][round_idx + 1]
                    match_idx = round_matches.index(match)
                    next_match_idx = match_idx // 2
                    
                    if match_idx % 2 == 0:
                        next_round[next_match_idx]["player1"] = winner_uid
                    else:
                        next_round[next_match_idx]["player2"] = winner_uid
                    
                    # Check for auto-advance (BYE)
                    next_match = next_round[next_match_idx]
                    if next_match["player1"] and next_match["player2"] is None:
                        next_match["winner"] = next_match["player1"]
                    elif next_match["player2"] and next_match["player1"] is None:
                        next_match["winner"] = next_match["player2"]
                else:
                    # Final match - tournament complete
                    data["active"]["winner"] = winner_uid
                    data["active"]["status"] = "complete"
                
                save_tournaments(data)
                return match
    
    return None

def end_tournament():
    """End the tournament and archive it"""
    data = load_tournaments()
    
    if data["active"]:
        data["active"]["ended_at"] = datetime.datetime.now(datetime.timezone.utc).isoformat()
        data["history"].append(data["active"])
        data["active"] = None
        
        # Keep only last 20 tournaments
        if len(data["history"]) > 20:
            data["history"] = data["history"][-20:]
        
        save_tournaments(data)
        return True
    return False

def get_active_tournament():
    """Get the active tournament"""
    data = load_tournaments()
    return data["active"]

# Tournament Panel View
class TournamentPanelView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)
    
    @discord.ui.button(label="Join Tournament", style=discord.ButtonStyle.success, emoji="⚔️", custom_id="tournament_join")
    async def join(self, interaction: discord.Interaction, button: discord.ui.Button):
        tournament = get_active_tournament()
        
        if not tournament:
            return await interaction.response.send_message("❌ No active tournament!", ephemeral=True)
        
        if tournament["status"] != "signup":
            return await interaction.response.send_message("❌ Signups are closed!", ephemeral=True)
        
        if str(interaction.user.id) in tournament["participants"]:
            return await interaction.response.send_message("❌ You're already signed up!", ephemeral=True)
        
        if join_tournament(interaction.user.id):
            await interaction.response.send_message(f"✅ You joined **{tournament['name']}**!", ephemeral=True)
            
            # Update the panel
            await self.update_panel(interaction.message)
        else:
            await interaction.response.send_message("❌ Failed to join!", ephemeral=True)
    
    @discord.ui.button(label="Leave", style=discord.ButtonStyle.secondary, emoji="🚪", custom_id="tournament_leave")
    async def leave(self, interaction: discord.Interaction, button: discord.ui.Button):
        tournament = get_active_tournament()
        
        if not tournament or tournament["status"] != "signup":
            return await interaction.response.send_message("❌ Cannot leave at this time!", ephemeral=True)
        
        if leave_tournament(interaction.user.id):
            await interaction.response.send_message("✅ You left the tournament.", ephemeral=True)
            await self.update_panel(interaction.message)
        else:
            await interaction.response.send_message("❌ You're not in the tournament!", ephemeral=True)
    
    @discord.ui.button(label="View Bracket", style=discord.ButtonStyle.primary, emoji="📊", custom_id="tournament_bracket")
    async def bracket(self, interaction: discord.Interaction, button: discord.ui.Button):
        tournament = get_active_tournament()
        
        if not tournament:
            return await interaction.response.send_message("❌ No active tournament!", ephemeral=True)
        
        if tournament["status"] == "signup":
            # Show participants
            participants = tournament["participants"]
            if not participants:
                desc = "No participants yet!"
            else:
                names = []
                for uid in participants[:20]:
                    member = interaction.guild.get_member(int(uid))
                    names.append(member.display_name if member else f"User {uid[:8]}")
                desc = "\n".join([f"• {name}" for name in names])
                if len(participants) > 20:
                    desc += f"\n...and {len(participants) - 20} more"
            
            embed = discord.Embed(
                title=f"📋 {tournament['name']} - Participants ({len(participants)})",
                description=desc,
                color=0x3498db
            )
            await interaction.response.send_message(embed=embed, ephemeral=True)
        else:
            # Show bracket
            embed = await self.create_bracket_embed(tournament, interaction.guild)
            await interaction.response.send_message(embed=embed, ephemeral=True)
    
    @discord.ui.button(label="Staff: Manage", style=discord.ButtonStyle.danger, emoji="⚙️", custom_id="tournament_manage", row=1)
    async def manage(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not is_staff(interaction.user):
            return await interaction.response.send_message("❌ Staff only!", ephemeral=True)
        
        tournament = get_active_tournament()
        if not tournament:
            return await interaction.response.send_message("❌ No active tournament!", ephemeral=True)
        
        await interaction.response.send_message(
            "**Tournament Management**",
            view=TournamentManageStaffView(interaction.message),
            ephemeral=True
        )
    
    async def update_panel(self, message):
        """Update the tournament panel embed"""
        tournament = get_active_tournament()
        
        if not tournament:
            return
        
        participant_count = len(tournament["participants"])
        status_text = {
            "signup": "🟢 Signups Open",
            "active": "🔴 In Progress",
            "complete": "🏆 Complete"
        }.get(tournament["status"], "Unknown")
        
        embed = discord.Embed(
            title=f"🏆 {tournament['name']}",
            description=(
                f"**Status:** {status_text}\n"
                f"**Participants:** {participant_count}\n\n"
                f"Click **Join Tournament** to participate!"
            ),
            color=0xFFD700
        )
        embed.set_footer(text="✝ The Fallen ✝ • Tournament System")
        
        try:
            await message.edit(embed=embed)
        except:
            pass
    
    async def create_bracket_embed(self, tournament, guild):
        """Create a bracket embed"""
        bracket = tournament.get("bracket")
        if not bracket:
            return discord.Embed(title="No bracket yet", color=0x3498db)
        
        embed = discord.Embed(
            title=f"📊 {tournament['name']} - Bracket",
            color=0xFFD700
        )
        
        for round_idx, round_matches in enumerate(bracket["rounds"]):
            round_name = ["Round 1", "Quarter Finals", "Semi Finals", "Finals"][min(round_idx, 3)]
            if round_idx >= len(bracket["rounds"]) - 1:
                round_name = "Finals"
            
            matches_text = ""
            for match in round_matches:
                p1 = guild.get_member(int(match["player1"])).display_name if match["player1"] else "BYE"
                p2 = guild.get_member(int(match["player2"])).display_name if match["player2"] else "BYE"
                
                if match["winner"]:
                    winner = guild.get_member(int(match["winner"]))
                    winner_name = winner.display_name if winner else "Unknown"
                    matches_text += f"~~{p1}~~ vs ~~{p2}~~ → **{winner_name}**\n"
                else:
                    matches_text += f"{p1} vs {p2}\n"
            
            embed.add_field(name=round_name, value=matches_text or "TBD", inline=False)
        
        if tournament["winner"]:
            winner = guild.get_member(int(tournament["winner"]))
            embed.add_field(name="🏆 CHAMPION", value=winner.mention if winner else "Unknown", inline=False)
        
        return embed

# Staff Management View
class TournamentManageStaffView(discord.ui.View):
    def __init__(self, panel_message):
        super().__init__(timeout=300)
        self.panel_message = panel_message
    
    @discord.ui.button(label="Start Tournament", style=discord.ButtonStyle.success, emoji="▶️")
    async def start(self, interaction: discord.Interaction, button: discord.ui.Button):
        tournament = get_active_tournament()
        
        if not tournament:
            return await interaction.response.send_message("❌ No tournament!", ephemeral=True)
        
        if tournament["status"] != "signup":
            return await interaction.response.send_message("❌ Already started!", ephemeral=True)
        
        if len(tournament["participants"]) < 2:
            return await interaction.response.send_message("❌ Need at least 2 participants!", ephemeral=True)
        
        result = start_tournament()
        if result:
            await interaction.response.send_message("✅ Tournament started! Bracket generated.", ephemeral=True)
            
            # Update panel
            tournament = get_active_tournament()
            embed = discord.Embed(
                title=f"🏆 {tournament['name']}",
                description=f"**Status:** 🔴 In Progress\n**Participants:** {len(tournament['participants'])}",
                color=0xFFD700
            )
            embed.set_footer(text="✝ The Fallen ✝ • Tournament System")
            
            try:
                await self.panel_message.edit(embed=embed)
            except:
                pass
        else:
            await interaction.response.send_message("❌ Failed to start!", ephemeral=True)
    
    @discord.ui.button(label="Report Match", style=discord.ButtonStyle.primary, emoji="📝")
    async def report(self, interaction: discord.Interaction, button: discord.ui.Button):
        tournament = get_active_tournament()
        
        if not tournament or tournament["status"] != "active":
            return await interaction.response.send_message("❌ No active tournament!", ephemeral=True)
        
        # Show match selection modal
        await interaction.response.send_modal(TournamentReportModal())
    
    @discord.ui.button(label="End Tournament", style=discord.ButtonStyle.danger, emoji="🛑")
    async def end(self, interaction: discord.Interaction, button: discord.ui.Button):
        if end_tournament():
            await interaction.response.send_message("✅ Tournament ended!", ephemeral=True)
            
            try:
                embed = discord.Embed(
                    title="🏆 Tournament Ended",
                    description="No active tournament. Stay tuned for the next one!",
                    color=0x95a5a6
                )
                await self.panel_message.edit(embed=embed, view=None)
            except:
                pass
        else:
            await interaction.response.send_message("❌ No tournament to end!", ephemeral=True)

class TournamentReportModal(discord.ui.Modal, title="Report Tournament Match"):
    match_id = discord.ui.TextInput(
        label="Match ID",
        placeholder="e.g., m1, r2m1",
        required=True,
        max_length=20
    )
    winner_id = discord.ui.TextInput(
        label="Winner User ID",
        placeholder="Right-click user > Copy ID",
        required=True,
        max_length=30
    )
    
    async def on_submit(self, interaction: discord.Interaction):
        try:
            winner = int(self.winner_id.value)
            result = report_tournament_match(self.match_id.value, winner)
            
            if result:
                member = interaction.guild.get_member(winner)
                await interaction.response.send_message(
                    f"✅ Match **{self.match_id.value}** reported!\n"
                    f"Winner: {member.mention if member else winner}",
                    ephemeral=True
                )
            else:
                await interaction.response.send_message("❌ Invalid match or winner!", ephemeral=True)
        except:
            await interaction.response.send_message("❌ Invalid input!", ephemeral=True)

# ==========================================
# EVENT SCHEDULING SYSTEM (Trainings, Tryouts)
# ==========================================

EVENTS_FILE = "events_data.json"
TRAINING_PING_ROLE = "Training Ping"
TRYOUT_PING_ROLE = "Tryout Ping"

# Attendance Rewards
ATTENDANCE_REWARDS = {
    "training": {"coins": 100, "xp": 50},
    "tryout": {"coins": 150, "xp": 75},
    "host": {"coins": 300, "xp": 100}
}

# Streak Bonuses (attendance streak -> bonus coins)
STREAK_BONUSES = {
    3: 50,    # 3 events in a row = +50 bonus
    5: 100,   # 5 events = +100 bonus
    7: 200,   # 7 events = +200 bonus
    10: 500,  # 10 events = +500 bonus
}

# Attendance Role Rewards (total attendance -> role name)
# These roles should be created in Discord with desired colors
ATTENDANCE_ROLE_REWARDS = {
    5: "Fallen Initiate",          # 5 total trainings
    15: "Fallen Disciple",         # 15 total trainings
    30: "Fallen Warrior",          # 30 total trainings
    50: "Fallen Slayer",           # 50 total trainings
    100: "Fallen Immortal",        # 100 total trainings
}

# Streak Role Rewards (current streak -> role name)
# These are for maintaining consistent attendance
STREAK_ROLE_REWARDS = {
    5: "♰ Rising Shadow",          # 5 streak
    10: "♰ Relentless",            # 10 streak
    20: "♰ Undying",               # 20 streak
    50: "♰ Eternal Fallen",        # 50 streak
}

async def check_attendance_roles(member, guild):
    """Check and award attendance milestone roles"""
    user_data = get_user_data(member.id)
    total_trainings = user_data.get("training_attendance", 0) + user_data.get("tryout_attendance", 0)
    
    roles_to_add = []
    highest_earned = None
    
    # Check total attendance roles
    for threshold, role_name in sorted(ATTENDANCE_ROLE_REWARDS.items()):
        if total_trainings >= threshold:
            highest_earned = role_name
    
    if highest_earned:
        # Remove lower attendance roles, keep only highest
        for threshold, role_name in ATTENDANCE_ROLE_REWARDS.items():
            role = discord.utils.get(guild.roles, name=role_name)
            if role:
                if role_name == highest_earned:
                    if role not in member.roles:
                        await safe_add_role(member, role)
                        roles_to_add.append(role_name)
                else:
                    if role in member.roles:
                        await safe_remove_role(member, role)
    
    return roles_to_add

async def check_streak_roles(member, guild, current_streak):
    """Check and award streak milestone roles"""
    roles_to_add = []
    highest_earned = None
    
    # Find highest earned streak role
    for threshold, role_name in sorted(STREAK_ROLE_REWARDS.items()):
        if current_streak >= threshold:
            highest_earned = role_name
    
    if highest_earned:
        # Remove lower streak roles, keep only highest
        for threshold, role_name in STREAK_ROLE_REWARDS.items():
            role = discord.utils.get(guild.roles, name=role_name)
            if role:
                if role_name == highest_earned:
                    if role not in member.roles:
                        await safe_add_role(member, role)
                        roles_to_add.append(role_name)
                else:
                    if role in member.roles:
                        await safe_remove_role(member, role)
    else:
        # No streak role earned, remove all streak roles
        for threshold, role_name in STREAK_ROLE_REWARDS.items():
            role = discord.utils.get(guild.roles, name=role_name)
            if role and role in member.roles:
                await safe_remove_role(member, role)
    
    return roles_to_add

async def remove_streak_roles(member, guild):
    """Remove all streak roles when streak breaks"""
    for threshold, role_name in STREAK_ROLE_REWARDS.items():
        role = discord.utils.get(guild.roles, name=role_name)
        if role and role in member.roles:
            await safe_remove_role(member, role)

def load_events_data():
    try:
        with open(EVENTS_FILE, "r") as f:
            return json.load(f)
    except:
        return {"scheduled_events": [], "attendance_streaks": {}, "attendance_history": {}}

def save_events_data(data):
    with open(EVENTS_FILE, "w") as f:
        json.dump(data, f, indent=2)

def create_event(event_type, title, scheduled_time, host_id, ping_role=None, channel_id=None):
    """Create a new scheduled event"""
    data = load_events_data()
    
    event_id = f"event_{int(datetime.datetime.now().timestamp())}"
    
    event = {
        "id": event_id,
        "type": event_type,  # "training" or "tryout"
        "title": title,
        "scheduled_time": scheduled_time,  # ISO format datetime
        "host_id": str(host_id),
        "ping_role": ping_role,
        "channel_id": str(channel_id) if channel_id else None,
        "message_id": None,  # Will store the announcement message ID
        "rsvp_yes": [],
        "rsvp_maybe": [],
        "attendees": [],  # Logged after event
        "status": "scheduled",  # scheduled, active, completed, cancelled
        "created_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "reminder_30_sent": False,
        "reminder_5_sent": False
    }
    
    data["scheduled_events"].append(event)
    save_events_data(data)
    return event

def get_event(event_id):
    """Get an event by ID"""
    data = load_events_data()
    for event in data["scheduled_events"]:
        if event["id"] == event_id:
            return event
    return None

def update_event(event_id, updates):
    """Update an event"""
    data = load_events_data()
    for i, event in enumerate(data["scheduled_events"]):
        if event["id"] == event_id:
            data["scheduled_events"][i].update(updates)
            save_events_data(data)
            return True
    return False

def add_rsvp(event_id, user_id, response):
    """Add RSVP response (yes/maybe)"""
    data = load_events_data()
    uid = str(user_id)
    
    for event in data["scheduled_events"]:
        if event["id"] == event_id:
            # Remove from other list if exists
            if uid in event["rsvp_yes"]:
                event["rsvp_yes"].remove(uid)
            if uid in event["rsvp_maybe"]:
                event["rsvp_maybe"].remove(uid)
            
            # Add to appropriate list
            if response == "yes":
                event["rsvp_yes"].append(uid)
            elif response == "maybe":
                event["rsvp_maybe"].append(uid)
            
            save_events_data(data)
            return True
    return False

def remove_rsvp(event_id, user_id):
    """Remove RSVP"""
    data = load_events_data()
    uid = str(user_id)
    
    for event in data["scheduled_events"]:
        if event["id"] == event_id:
            if uid in event["rsvp_yes"]:
                event["rsvp_yes"].remove(uid)
            if uid in event["rsvp_maybe"]:
                event["rsvp_maybe"].remove(uid)
            save_events_data(data)
            return True
    return False

def log_attendance(event_id, attendee_ids, host_id):
    """Log attendance for an event and award rewards"""
    data = load_events_data()
    
    event = None
    for e in data["scheduled_events"]:
        if e["id"] == event_id:
            event = e
            break
    
    if not event:
        return None
    
    event_type = event["type"]
    rewards_given = []
    
    # Award attendees
    for uid in attendee_ids:
        uid = str(uid)
        rewards = ATTENDANCE_REWARDS.get(event_type, {"coins": 50, "xp": 25})
        
        # Add base rewards
        add_user_stat(int(uid), "coins", rewards["coins"])
        add_xp_to_user(int(uid), rewards["xp"])
        add_user_stat(int(uid), f"{event_type}_attendance", 1)
        
        # Reset activity timestamp - prevents inactivity strikes
        reset_member_activity(int(uid))
        
        # Update streak
        streak = update_attendance_streak(uid)
        streak_bonus = get_streak_bonus(streak)
        
        if streak_bonus > 0:
            add_user_stat(int(uid), "coins", streak_bonus)
        
        rewards_given.append({
            "user_id": uid,
            "coins": rewards["coins"] + streak_bonus,
            "xp": rewards["xp"],
            "streak": streak,
            "streak_bonus": streak_bonus
        })
    
    # Award host
    host_rewards = ATTENDANCE_REWARDS.get("host", {"coins": 300, "xp": 100})
    add_user_stat(int(host_id), "coins", host_rewards["coins"])
    add_xp_to_user(int(host_id), host_rewards["xp"])
    add_user_stat(int(host_id), "events_hosted", 1)
    reset_member_activity(int(host_id))  # Reset host activity too
    
    # Update event status
    event["attendees"] = [str(uid) for uid in attendee_ids]
    event["status"] = "completed"
    save_events_data(data)
    
    return {
        "event": event,
        "attendees": rewards_given,
        "host_rewards": host_rewards
    }

def update_attendance_streak(user_id):
    """Update user's attendance streak"""
    data = load_events_data()
    uid = str(user_id)
    
    if "attendance_streaks" not in data:
        data["attendance_streaks"] = {}
    
    if uid not in data["attendance_streaks"]:
        data["attendance_streaks"][uid] = {"current": 0, "best": 0, "last_event": None}
    
    streak_data = data["attendance_streaks"][uid]
    streak_data["current"] += 1
    
    if streak_data["current"] > streak_data["best"]:
        streak_data["best"] = streak_data["current"]
    
    streak_data["last_event"] = datetime.datetime.now(datetime.timezone.utc).isoformat()
    save_events_data(data)
    
    return streak_data["current"]

def break_attendance_streak(user_id):
    """Break user's attendance streak (missed event)"""
    data = load_events_data()
    uid = str(user_id)
    
    if "attendance_streaks" not in data:
        data["attendance_streaks"] = {}
    
    if uid in data["attendance_streaks"]:
        data["attendance_streaks"][uid]["current"] = 0
        save_events_data(data)

def get_attendance_streak(user_id):
    """Get user's current attendance streak"""
    data = load_events_data()
    uid = str(user_id)
    
    if "attendance_streaks" not in data:
        return {"current": 0, "best": 0}
    
    return data["attendance_streaks"].get(uid, {"current": 0, "best": 0})

def get_streak_bonus(streak):
    """Get bonus coins for streak milestone"""
    bonus = 0
    for milestone, coins in STREAK_BONUSES.items():
        if streak == milestone:
            bonus = coins
            break
    return bonus

def get_upcoming_events(limit=10):
    """Get upcoming scheduled events"""
    data = load_events_data()
    now = datetime.datetime.now(datetime.timezone.utc)
    
    upcoming = []
    for event in data["scheduled_events"]:
        if event["status"] == "scheduled":
            try:
                event_time = datetime.datetime.fromisoformat(event["scheduled_time"])
                if event_time.tzinfo is None:
                    event_time = event_time.replace(tzinfo=datetime.timezone.utc)
                if event_time > now:
                    upcoming.append(event)
            except:
                pass
    
    # Sort by time
    upcoming.sort(key=lambda x: x["scheduled_time"])
    return upcoming[:limit]

def get_events_needing_reminder():
    """Get events that need reminders sent"""
    data = load_events_data()
    now = datetime.datetime.now(datetime.timezone.utc)
    
    needs_30 = []
    needs_5 = []
    
    for event in data["scheduled_events"]:
        if event["status"] != "scheduled":
            continue
        
        try:
            event_time = datetime.datetime.fromisoformat(event["scheduled_time"])
            if event_time.tzinfo is None:
                event_time = event_time.replace(tzinfo=datetime.timezone.utc)
            
            time_until = (event_time - now).total_seconds() / 60  # minutes
            
            if 25 <= time_until <= 35 and not event.get("reminder_30_sent"):
                needs_30.append(event)
            elif 3 <= time_until <= 7 and not event.get("reminder_5_sent"):
                needs_5.append(event)
        except:
            pass
    
    return needs_30, needs_5

def cancel_event(event_id):
    """Cancel an event"""
    data = load_events_data()
    for event in data["scheduled_events"]:
        if event["id"] == event_id:
            event["status"] = "cancelled"
            save_events_data(data)
            return event
    return None

# Event RSVP View
class EventRSVPView(discord.ui.View):
    def __init__(self, event_id):
        super().__init__(timeout=None)
        self.event_id = event_id
    
    @discord.ui.button(label="✅ Attending", style=discord.ButtonStyle.success, custom_id="event_rsvp_yes")
    async def rsvp_yes(self, interaction: discord.Interaction, button: discord.ui.Button):
        add_rsvp(self.event_id, interaction.user.id, "yes")
        await interaction.response.send_message("✅ You're marked as **attending**!", ephemeral=True)
        await self.update_embed(interaction)
    
    @discord.ui.button(label="❓ Maybe", style=discord.ButtonStyle.secondary, custom_id="event_rsvp_maybe")
    async def rsvp_maybe(self, interaction: discord.Interaction, button: discord.ui.Button):
        add_rsvp(self.event_id, interaction.user.id, "maybe")
        await interaction.response.send_message("❓ You're marked as **maybe**.", ephemeral=True)
        await self.update_embed(interaction)
    
    @discord.ui.button(label="❌ Can't Attend", style=discord.ButtonStyle.danger, custom_id="event_rsvp_no")
    async def rsvp_no(self, interaction: discord.Interaction, button: discord.ui.Button):
        remove_rsvp(self.event_id, interaction.user.id)
        await interaction.response.send_message("❌ Your RSVP has been removed.", ephemeral=True)
        await self.update_embed(interaction)
    
    async def update_embed(self, interaction):
        """Update the event embed with current RSVPs"""
        event = get_event(self.event_id)
        if not event:
            return
        
        embed = await create_event_embed(event, interaction.guild)
        try:
            await interaction.message.edit(embed=embed)
        except:
            pass

async def create_event_embed(event, guild):
    """Create embed for an event"""
    event_type = event["type"]
    emoji = "📚" if event_type == "training" else "🎯"
    color = 0x3498db if event_type == "training" else 0xF1C40F
    
    # Parse time
    try:
        event_time = datetime.datetime.fromisoformat(event["scheduled_time"])
        time_str = f"<t:{int(event_time.timestamp())}:F>"  # Discord timestamp
        relative_str = f"<t:{int(event_time.timestamp())}:R>"  # Relative time
    except:
        time_str = event["scheduled_time"]
        relative_str = ""
    
    # Get host
    host = guild.get_member(int(event["host_id"]))
    host_str = host.mention if host else "Unknown"
    
    # Get rewards
    rewards = ATTENDANCE_REWARDS.get(event_type, {"coins": 50, "xp": 25})
    
    embed = discord.Embed(
        title=f"{emoji} {event['title'].upper()}",
        description=(
            f"**📅 When:** {time_str}\n"
            f"**⏰ Starts:** {relative_str}\n"
            f"**👑 Host:** {host_str}\n\n"
            f"**💰 Rewards:**\n"
            f"• {rewards['coins']} Fallen Coins\n"
            f"• {rewards['xp']} XP\n"
            f"• Attendance streak bonus!"
        ),
        color=color
    )
    
    # RSVP counts
    yes_count = len(event.get("rsvp_yes", []))
    maybe_count = len(event.get("rsvp_maybe", []))
    
    rsvp_text = f"✅ **Attending:** {yes_count}\n❓ **Maybe:** {maybe_count}"
    
    # Show names if not too many
    if yes_count > 0 and yes_count <= 10:
        names = []
        for uid in event["rsvp_yes"][:10]:
            member = guild.get_member(int(uid))
            if member:
                names.append(member.display_name)
        if names:
            rsvp_text += f"\n\n**Confirmed:** {', '.join(names)}"
    
    embed.add_field(name="📋 RSVPs", value=rsvp_text, inline=False)
    embed.set_footer(text=f"Event ID: {event['id']} • Click a button to RSVP!")
    
    return embed

# Background task for event reminders
async def check_event_reminders():
    """Check and send event reminders"""
    await bot.wait_until_ready()
    await asyncio.sleep(60)  # Wait 1 minute after startup
    
    while not bot.is_closed():
        try:
            needs_30, needs_5 = get_events_needing_reminder()
            
            for event in needs_30:
                await send_event_reminder(event, 30)
                update_event(event["id"], {"reminder_30_sent": True})
                await asyncio.sleep(1)
            
            for event in needs_5:
                await send_event_reminder(event, 5)
                update_event(event["id"], {"reminder_5_sent": True})
                await asyncio.sleep(1)
        except Exception as e:
            print(f"Event reminder error: {e}")
        
        await asyncio.sleep(60)  # Check every minute

async def send_event_reminder(event, minutes):
    """Send a reminder for an event"""
    for guild in bot.guilds:
        if event.get("channel_id"):
            channel = guild.get_channel(int(event["channel_id"]))
        else:
            channel = discord.utils.get(guild.text_channels, name="trainings") or \
                     discord.utils.get(guild.text_channels, name="events") or \
                     discord.utils.get(guild.text_channels, name="general")
        
        if not channel:
            continue
        
        event_type = event["type"]
        emoji = "📚" if event_type == "training" else "🎯"
        ping_role_name = TRAINING_PING_ROLE if event_type == "training" else TRYOUT_PING_ROLE
        ping_role = discord.utils.get(guild.roles, name=ping_role_name)
        
        if minutes == 30:
            title = f"⏰ {emoji} {event['title']} - 30 MINUTES!"
            desc = f"**{event['title']}** starts in **30 minutes**!\n\nMake sure you're ready!"
        else:
            title = f"🚨 {emoji} {event['title']} - STARTING NOW!"
            desc = f"**{event['title']}** is starting **NOW**!\n\n**Join the voice channel!**"
        
        embed = discord.Embed(title=title, description=desc, color=0xff6b6b)
        
        # Show who RSVPd
        if event.get("rsvp_yes"):
            mentions = " ".join([f"<@{uid}>" for uid in event["rsvp_yes"][:15]])
            embed.add_field(name="📋 Expected Attendees", value=mentions, inline=False)
        
        ping_text = ping_role.mention if ping_role else ""
        await channel.send(content=ping_text, embed=embed)
        break  # Only send to first guild found

# ==========================================
# INACTIVITY STRIKE SYSTEM
# ==========================================

INACTIVITY_FILE = "inactivity_data.json"
INACTIVITY_CHECK_DAYS = 3  # Days of inactivity before strike
MAX_INACTIVITY_STRIKES = 5  # Strikes before kick
INACTIVITY_IMMUNITY_ROLE = "Inactivity Immunity"  # Role that bypasses inactivity checks

# Rank demotion order (highest to lowest) - Stage 0 is highest
RANK_DEMOTION_ORDER = [
    "Stage 0〢FALLEN DEITY",
    "Stage 1〢FALLEN APEX", 
    "Stage 2〢FALLEN ASCENDANT",
    "Stage 3〢FORSAKEN WARRIOR",
    "Stage 4〢ABYSS-TOUCHED",
    "Stage 5〢BROKEN INITIATE",
]

# All member ranks that should be checked for inactivity
MEMBER_RANKS = RANK_DEMOTION_ORDER.copy()

def has_inactivity_immunity(member):
    """Check if member has immunity role"""
    immunity_role = discord.utils.get(member.roles, name=INACTIVITY_IMMUNITY_ROLE)
    return immunity_role is not None

def reset_member_activity(user_id):
    """Reset a member's last_active timestamp to now (prevents inactivity strike)"""
    update_user_data(user_id, "last_active", datetime.datetime.now(datetime.timezone.utc).isoformat())

def get_member_rank(member):
    """Get the member's current rank from the rank order"""
    for rank in RANK_DEMOTION_ORDER:
        role = discord.utils.get(member.roles, name=rank)
        if role:
            return rank
    return None

def get_next_demotion_rank(current_rank):
    """Get the rank below the current one"""
    try:
        current_index = RANK_DEMOTION_ORDER.index(current_rank)
        if current_index < len(RANK_DEMOTION_ORDER) - 1:
            return RANK_DEMOTION_ORDER[current_index + 1]
    except ValueError:
        pass
    return None  # Already at lowest or not found

def load_inactivity_data():
    try:
        with open(INACTIVITY_FILE, "r") as f:
            return json.load(f)
    except:
        return {"strikes": {}, "last_check": None}

def save_inactivity_data(data):
    with open(INACTIVITY_FILE, "w") as f:
        json.dump(data, f, indent=2)

def get_inactivity_strikes(user_id):
    """Get inactivity strikes for a user"""
    data = load_inactivity_data()
    return data["strikes"].get(str(user_id), {
        "count": 0,
        "history": [],
        "demoted": False
    })

def add_inactivity_strike(user_id, reason="Inactivity"):
    """Add an inactivity strike to a user"""
    data = load_inactivity_data()
    uid = str(user_id)
    
    if uid not in data["strikes"]:
        data["strikes"][uid] = {
            "count": 0,
            "history": [],
            "demoted": False
        }
    
    data["strikes"][uid]["count"] += 1
    data["strikes"][uid]["history"].append({
        "date": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "reason": reason
    })
    
    save_inactivity_data(data)
    return data["strikes"][uid]["count"]

def clear_inactivity_strikes(user_id):
    """Clear all inactivity strikes for a user"""
    data = load_inactivity_data()
    uid = str(user_id)
    
    if uid in data["strikes"]:
        data["strikes"][uid] = {
            "count": 0,
            "history": [],
            "demoted": False
        }
        save_inactivity_data(data)

def remove_inactivity_strike(user_id):
    """Remove one inactivity strike from a user"""
    data = load_inactivity_data()
    uid = str(user_id)
    
    if uid in data["strikes"] and data["strikes"][uid]["count"] > 0:
        data["strikes"][uid]["count"] -= 1
        save_inactivity_data(data)
        return data["strikes"][uid]["count"]
    return 0

def mark_user_demoted(user_id):
    """Mark that user has been demoted due to inactivity"""
    data = load_inactivity_data()
    uid = str(user_id)
    
    if uid in data["strikes"]:
        data["strikes"][uid]["demoted"] = True
        save_inactivity_data(data)

async def send_inactivity_strike_dm(member, strike_count, demoted=False, kicked=False, old_rank=None, new_rank=None):
    """Send DM to user about inactivity strike"""
    try:
        if kicked:
            embed = discord.Embed(
                title="⛔ Removed from The Fallen",
                description=(
                    f"You have been **kicked** from **The Fallen** due to reaching "
                    f"**{MAX_INACTIVITY_STRIKES} inactivity strikes**.\n\n"
                    f"We understand life gets busy, but consistent activity is required "
                    f"to maintain membership.\n\n"
                    f"**You may rejoin and reapply if you can commit to being active.**"
                ),
                color=0xe74c3c
            )
        elif demoted:
            old_rank_display = old_rank or "Previous Rank"
            new_rank_display = new_rank or "Lower Rank"
            embed = discord.Embed(
                title="📉 Rank Demotion - Inactivity",
                description=(
                    f"You have received **Strike {strike_count}/{MAX_INACTIVITY_STRIKES}** for inactivity.\n\n"
                    f"Due to continuous inactivity, you have been **demoted**:\n"
                    f"```\n{old_rank_display}\n       ↓\n{new_rank_display}```\n\n"
                    f"**What this means:**\n"
                    f"• Further inactivity will result in more demotions\n"
                    f"• At the lowest rank with max strikes = removal from clan\n"
                    f"• You can regain rank through activity and tryouts\n\n"
                    f"**To avoid further demotions:**\n"
                    f"• Participate in raids and trainings\n"
                    f"• Be active in chat\n"
                    f"• Attend clan events"
                ),
                color=0xe67e22
            )
        else:
            embed = discord.Embed(
                title="⚠️ Inactivity Strike Received",
                description=(
                    f"You have received **Strike {strike_count}/{MAX_INACTIVITY_STRIKES}** for inactivity.\n\n"
                    f"**What happens next:**\n"
                    f"• **Strike 3+:** Rank demotion (one rank per strike)\n"
                    f"• **Strike 5 at lowest rank:** Removal from The Fallen\n\n"
                    f"**Rank Order (highest to lowest):**\n"
                    f"```\n"
                    f"Stage 0〢FALLEN DEITY\n"
                    f"Stage 1〢FALLEN APEX\n"
                    f"Stage 2〢FALLEN ASCENDANT\n"
                    f"Stage 3〢FORSAKEN WARRIOR\n"
                    f"Stage 4〢ABYSS-TOUCHED\n"
                    f"Stage 5〢BROKEN INITIATE\n"
                    f"```\n\n"
                    f"**To avoid further strikes:**\n"
                    f"• Participate in raids and trainings\n"
                    f"• Be active in chat\n"
                    f"• Attend clan events\n\n"
                    f"*If you're going to be away, let staff know in advance!*"
                ),
                color=0xf1c40f
            )
        
        embed.set_footer(text="✝ The Fallen ✝ • Inactivity System")
        embed.timestamp = datetime.datetime.now(datetime.timezone.utc)
        await member.send(embed=embed)
        return True
    except:
        return False  # Can't DM user

async def check_member_inactivity(member, guild):
    """Check if a member is inactive and apply strikes if needed"""
    # Check if member has immunity role - skip if they do
    if has_inactivity_immunity(member):
        return None  # Has immunity, skip
    
    # Check if member has any of the ranked roles
    current_rank = get_member_rank(member)
    
    if not current_rank:
        return None  # Not a ranked member, skip
    
    # Get user data
    user_data = get_user_data(member.id)
    last_active = user_data.get("last_active")
    
    if not last_active:
        return None
    
    # Parse last active date
    try:
        last_active_date = datetime.datetime.fromisoformat(last_active.replace('Z', '+00:00'))
    except:
        return None
    
    # Check if inactive for more than threshold
    now = datetime.datetime.now(datetime.timezone.utc)
    days_inactive = (now - last_active_date).days
    
    if days_inactive < INACTIVITY_CHECK_DAYS:
        return None  # Not inactive long enough
    
    # Get current strikes
    strike_info = get_inactivity_strikes(member.id)
    current_strikes = strike_info["count"]
    
    # Add a strike
    new_strike_count = add_inactivity_strike(member.id, f"Inactive for {days_inactive} days")
    
    result = {
        "member": member,
        "strikes": new_strike_count,
        "days_inactive": days_inactive,
        "action": "strike",
        "current_rank": current_rank,
        "new_rank": None
    }
    
    # Check for demotion (Strike 3+) - demote one rank per strike after 3
    if new_strike_count >= 3:
        next_rank = get_next_demotion_rank(current_rank)
        
        if next_rank:
            # Demote to next rank using safe rate-limited functions
            try:
                current_role = discord.utils.get(guild.roles, name=current_rank)
                next_role = discord.utils.get(guild.roles, name=next_rank)
                
                if current_role and next_role:
                    await safe_remove_role(member, current_role)
                    await safe_add_role(member, next_role)
                    result["action"] = "demoted"
                    result["new_rank"] = next_rank
                    await send_inactivity_strike_dm(member, new_strike_count, demoted=True, old_rank=current_rank, new_rank=next_rank)
            except Exception as e:
                print(f"Failed to demote {member}: {e}")
        
        # Already at lowest rank (Stage 5) and hit max strikes = kick
        elif new_strike_count >= MAX_INACTIVITY_STRIKES:
            result["action"] = "kicked"
            await send_inactivity_strike_dm(member, new_strike_count, kicked=True)
            kicked = await safe_kick(member, reason=f"Inactivity: {MAX_INACTIVITY_STRIKES} strikes at lowest rank")
            if not kicked:
                print(f"Failed to kick {member}")
                result["action"] = "kick_failed"
    
    else:
        # Just a regular strike (1-2)
        await send_inactivity_strike_dm(member, new_strike_count)
    
    return result

async def run_inactivity_check(guild):
    """Run inactivity check on all ranked members with rate limit protection"""
    results = {
        "checked": 0,
        "strikes_given": 0,
        "demotions": 0,
        "kicks": 0,
        "details": []
    }
    
    # Get all rank roles
    rank_roles = []
    for rank_name in MEMBER_RANKS:
        role = discord.utils.get(guild.roles, name=rank_name)
        if role:
            rank_roles.append(role)
    
    if not rank_roles:
        return results
    
    for member in guild.members:
        if member.bot:
            continue
        
        # Check if member has any ranked role
        has_rank = any(role in member.roles for role in rank_roles)
        if not has_rank:
            continue
        
        results["checked"] += 1
        
        try:
            result = await check_member_inactivity(member, guild)
            if result:
                results["strikes_given"] += 1
                results["details"].append(result)
                
                if result["action"] == "demoted":
                    results["demotions"] += 1
                elif result["action"] == "kicked":
                    results["kicks"] += 1
                
                # Longer delay after actions that modify roles/kick
                await asyncio.sleep(2.0)
            else:
                # Small delay even when no action taken
                await asyncio.sleep(0.5)
        except Exception as e:
            print(f"Error checking {member}: {e}")
            await asyncio.sleep(1.0)  # Delay on error too
            continue
    
    # Update last check time
    data = load_inactivity_data()
    data["last_check"] = datetime.datetime.now(datetime.timezone.utc).isoformat()
    save_inactivity_data(data)
    
    return results

# ==========================================
# LOGGING DASHBOARD
# ==========================================

LOG_CHANNEL_NAME = "fallen-logs"

async def log_to_dashboard(guild, log_type, title, description, color=0x3498db, fields=None):
    """Send a formatted log to the logging channel"""
    channel = discord.utils.get(guild.text_channels, name=LOG_CHANNEL_NAME)
    if not channel:
        return
    
    embed = discord.Embed(
        title=f"{log_type} | {title}",
        description=description,
        color=color,
        timestamp=datetime.datetime.now(datetime.timezone.utc)
    )
    
    if fields:
        for name, value in fields.items():
            embed.add_field(name=name, value=value, inline=True)
    
    embed.set_footer(text="Fallen Logging System")
    
    try:
        await channel.send(embed=embed)
    except:
        pass

# --- LEVELING CHECKER ---
async def check_level_up(user_id, guild):
    """Check if user leveled up - continuous leveling with milestone rewards"""
    data = load_data()
    uid = str(user_id)
    data = ensure_user_structure(data, uid) 
    user_data = data["users"][uid]
    
    total_xp = user_data["xp"]
    current_level = user_data["level"]
    
    # Calculate what level they should be based on total XP
    new_level, xp_into_level = get_level_from_xp(total_xp)
    
    # Check if they leveled up
    if new_level > current_level:
        # Update their level
        update_user_data(user_id, "level", new_level)
        
        member = guild.get_member(user_id)
        channel = discord.utils.get(guild.text_channels, name=LEVEL_UP_CHANNEL_NAME)
        
        # Check each level they passed for milestone rewards
        levels_gained = []
        for lvl in range(current_level + 1, new_level + 1):
            levels_gained.append(lvl)
            
            # Check if this is a milestone level
            milestone = get_milestone_reward(lvl)
            if milestone:
                coins = milestone["coins"]
                role_name = milestone["role"]
                add_user_stat(user_id, "coins", coins)
                
                role_msg = ""
                if member and role_name:
                    role = discord.utils.get(guild.roles, name=role_name)
                    if role:
                        try: 
                            await member.add_roles(role)
                            role_msg = f"\n🎭 **Role Unlocked:** {role.mention}"
                        except Exception as e:
                            print(f"Role assign error: {e}")
                            role_msg = "\n❌ Role assign failed (Hierarchy)."
                
                # Send milestone announcement
                if channel:
                    embed = discord.Embed(
                        title="🌟 MILESTONE REACHED! 🌟", 
                        description=f"<@{user_id}> has reached **Level {lvl}**!", 
                        color=0xFFD700
                    )
                    embed.add_field(name="🎁 Rewards", value=f"💰 +{coins} Fallen Coins{role_msg}")
                    if member: 
                        embed.set_thumbnail(url=member.display_avatar.url)
                    await channel.send(embed=embed)
                    await asyncio.sleep(0.5)  # Small delay to avoid rate limits
        
        # Send regular level up for non-milestone levels (only if didn't hit milestone)
        if channel and new_level not in LEVEL_CONFIG:
            embed = discord.Embed(
                title="✨ LEVEL UP!", 
                description=f"<@{user_id}> is now **Level {new_level}**!", 
                color=0xDC143C
            )
            xp_needed = calculate_next_level_xp(new_level)
            embed.add_field(name="Next Level", value=f"{xp_into_level}/{xp_needed} XP")
            if member: 
                embed.set_thumbnail(url=member.display_avatar.url)
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
            discord.SelectOption(label="Weekly XP", emoji="📆", value="weekly_xp", default=(current_selection == "weekly_xp")),
            discord.SelectOption(label="Voice Time", emoji="🎙️", value="voice_time", default=(current_selection == "voice_time"))
        ]
        super().__init__(placeholder="Overall XP", min_values=1, max_values=1, options=options)

    async def callback(self, interaction: discord.Interaction):
        sort_key = self.values[0]
        users = load_data()["users"]
        title_map = {"xp": "Overall XP", "monthly_xp": "Monthly XP", "weekly_xp": "Weekly XP", "voice_time": "Voice Time"}
        title = title_map[sort_key]
        
        # Try to create image leaderboard
        if PIL_AVAILABLE:
            try:
                if sort_key == "voice_time":
                    lb_image = await create_voice_leaderboard_image(interaction.guild)
                else:
                    lb_image = await create_leaderboard_image(interaction.guild, users, sort_key, title)
                if lb_image:
                    file = discord.File(lb_image, filename="leaderboard.png")
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
            discord.SelectOption(label="Profile & Stats", emoji="📊", description="Profile, rank, stats"),
            discord.SelectOption(label="Events", emoji="📅", description="Trainings & tryouts"),
            discord.SelectOption(label="Duels & ELO", emoji="⚔️", description="1v1 duels & rankings"),
            discord.SelectOption(label="Tournaments", emoji="🏆", description="Tournament system"),
            discord.SelectOption(label="Economy", emoji="💰", description="Coins & rewards"),
            discord.SelectOption(label="Raids & Wars", emoji="🏴‍☠️", description="Clan battles"),
            discord.SelectOption(label="Staff", emoji="🛡️", description="Staff commands"),
            discord.SelectOption(label="Admin", emoji="⚙️", description="Setup & management"),
        ]
        super().__init__(placeholder="Select a category...", min_values=1, max_values=1, options=options)
    
    async def callback(self, interaction: discord.Interaction):
        e = discord.Embed(color=0x8B0000)
        
        if self.values[0] == "Member": 
            e.title="👤 Member Commands"
            e.description=(
                "**🔗 Verification**\n"
                "`/verify` - Verify with Roblox\n\n"
                "**📊 Quick Stats**\n"
                "`/level` - Check your level card\n"
                "`/rank` - View your rank card\n"
                "`/fcoins` - Check coin balance\n\n"
                "**🎁 Daily**\n"
                "`/daily` - Claim daily reward\n"
                "`/schedule` - View upcoming events\n"
                "`/attendance_streak` - View your streak"
            )
            
        elif self.values[0] == "Profile & Stats":
            e.title="📊 Profile & Statistics"
            e.description=(
                "**🖼️ Visual Cards**\n"
                "`/profile` - Full profile card with all stats\n"
                "`/rank` - Rank card with XP bar\n"
                "`/level` - Level card\n\n"
                "**📈 Statistics**\n"
                "`/mystats` - Detailed stats breakdown\n"
                "`/leaderboard` - XP leaderboard\n"
                "`/voicetop` - Voice time leaderboard\n"
                "`/serverstats` - Server statistics"
            )
        
        elif self.values[0] == "Duels & ELO":
            e.title="⚔️ Duels & ELO System"
            e.description=(
                "**⚔️ Duel Commands**\n"
                "`/duel @user` - Challenge to 1v1 duel\n"
                "`/elo` - Check your ELO rating\n"
                "`/elo @user` - Check someone's ELO\n"
                "`/elo_leaderboard` - Top ranked players\n"
                "`/duel_history` - View your duel history\n\n"
                "**🏅 ELO Ranks**\n"
                "🏆 Grandmaster (2000+)\n"
                "💎 Diamond (1800+)\n"
                "🥇 Platinum (1600+)\n"
                "🥈 Gold (1400+)\n"
                "🥉 Silver (1200+)\n"
                "⚔️ Bronze (1000+)\n\n"
                "*Win duels to climb the ranks!*"
            )
        
        elif self.values[0] == "Tournaments":
            e.title="🏆 Tournament System"
            e.description=(
                "**👤 Member Commands**\n"
                "• Click **Join Tournament** on panel\n"
                "• Click **View Bracket** to see matches\n"
                "• Click **Leave** to withdraw\n\n"
                "**🛡️ Staff Commands**\n"
                "• Click **Staff: Manage** on panel\n"
                "• **Start Tournament** - Begin bracket\n"
                "• **Report Match** - Log winner\n"
                "• **End Tournament** - Cancel/finish\n\n"
                "**⚙️ Admin Commands**\n"
                "`/tournament_create <name>` - Create new\n"
                "`/tournament_panel` - Post panel\n"
                "`/tournament_end confirm` - End tournament"
            )
            
        elif self.values[0] == "Achievements":
            e.title="🎖️ Achievements"
            e.description=(
                "**📜 Commands**\n"
                "`/achievements` - View all achievements\n\n"
                "**🎯 How to Earn**\n"
                "• Level up to unlock level badges\n"
                "• Win duels for combat achievements\n"
                "• Participate in raids for raider badges\n"
                "• Maintain daily streaks\n"
                "• Earn coins for wealth achievements\n"
                "• Verify your Roblox account"
            )
            
        elif self.values[0] == "Events":
            e.title="📅 Events (Trainings & Tryouts)"
            e.description=(
                "**👤 Member Commands**\n"
                "`/schedule` - View upcoming events\n"
                "`/event_list` - See all scheduled events\n"
                "`/attendance_streak` - Check your streak\n\n"
                "**💰 Attendance Rewards**\n"
                "• Training: 100 coins + 50 XP\n"
                "• Tryout: 150 coins + 75 XP\n"
                "• Streak bonuses at 3, 5, 7, 10!\n\n"
                "**🛡️ Staff Commands**\n"
                "`/event_create <type> <title> <time>`\n"
                "`/quick_training` `/quick_tryout`\n"
                "`/log_training` `/log_tryout` @users"
            )
            
        elif self.values[0] == "Economy": 
            e.title="💰 Economy"
            e.description=(
                "**💵 Earning Coins**\n"
                "• Chat and be active\n"
                "• Attend trainings/tryouts\n"
                "• Join voice channels\n"
                "• Claim daily rewards\n"
                "• Win raids & duels\n\n"
                "**📜 Commands**\n"
                "`/fcoins` - Check balance\n"
                "`/daily` - Claim daily (streak bonus!)\n\n"
                "**🛒 Shop**\n"
                "Visit the shop channel to spend coins!"
            )
            
        elif self.values[0] == "Raids & Wars":
            e.title="🏴‍☠️ Raids & Wars"
            e.description=(
                "**👤 Member Commands**\n"
                "`/raid_lb` - Raid leaderboard\n"
                "`/raid_history` - View raid history\n"
                "`/wars` - View all clan wars\n\n"
                "**🛡️ Staff Commands**\n"
                "`/raid_call <target> <time>` - Call raid\n"
                "`/raid_log <win/loss> @users` - Log result\n"
                "`/war_declare <clan>` - Declare war\n"
                "`/war_result <clan> <win/loss>` - Log war"
            )
            
        elif self.values[0] == "Staff": 
            e.title="🛡️ Staff Commands"
            e.description=(
                "**📅 Events**\n"
                "`/event_create <type> <title> <time>`\n"
                "`/log_training` `/log_tryout` @users\n\n"
                "**📊 Levels & Economy**\n"
                "`/setlevel @user <lvl>` - Set level\n"
                "`/importlevel @user <lvl>` - Import\n"
                "`/addxp` `/addfcoins` @user amt\n\n"
                "**🛡️ Inactivity & Immunity**\n"
                "`/inactivity_check` - Run check\n"
                "`/immunity_add @user [reason]`\n"
                "`/immunity_remove @user`\n"
                "`/immunity_list` - View immune\n\n"
                "**🔨 Moderation**\n"
                "`/warn @user [reason]`\n"
                "`/warnings @user`"
            )
            
        elif self.values[0] == "Admin":
            e.title="⚙️ Admin Commands"
            e.description=(
                "**📋 Setup Panels**\n"
                "`/setup_verify` - Verification\n"
                "`/setup_tickets` - Tickets\n"
                "`/setup_shop` - Shop\n"
                "`/tournament_create <n>`\n"
                "`/setup_logs` - Logging\n\n"
                "**📊 Level Management**\n"
                "`/bulkimport` - Import guide\n"
                "`/setlevel` `/setxp`\n\n"
                "**🔄 Resets & Admin**\n"
                "`/reset_weekly` `/reset_monthly`\n"
                "`/elo_reset confirm`\n"
                "`!sync` - Sync commands"
            )
        
        e.set_footer(text="The Fallen Bot • Use / for slash commands")
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
    
    @discord.ui.button(label="✅ Verify with Fallen", style=discord.ButtonStyle.success, custom_id="verify_fallen_btn")
    async def verify_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Quick verify if user has Bloxlink verified role"""
        member = interaction.user
        guild = interaction.guild
        
        # Check if already has Abyssbound (full access)
        abyssbound = discord.utils.get(guild.roles, name=MEMBER_ROLE_NAME)
        if abyssbound and abyssbound in member.roles:
            return await interaction.response.send_message(
                "✅ You're already verified with The Fallen!",
                ephemeral=True
            )
        
        # Check if user has Bloxlink verified role
        bloxlink_role = discord.utils.get(guild.roles, name=BLOXLINK_VERIFIED_ROLE)
        
        if not bloxlink_role or bloxlink_role not in member.roles:
            embed = discord.Embed(
                title="❌ Bloxlink Verification Required",
                description=(
                    "You need to verify with **Bloxlink** first!\n\n"
                    "**Steps:**\n"
                    "1️⃣ Use `/verify` or go to the Bloxlink verification channel\n"
                    "2️⃣ Complete Bloxlink verification\n"
                    "3️⃣ Come back here and click this button again\n\n"
                    "*Once you have the Bloxlink verified role, you can get full access to The Fallen!*"
                ),
                color=0xe74c3c
            )
            return await interaction.response.send_message(embed=embed, ephemeral=True)
        
        # User has Bloxlink role - try to get their Roblox info from nickname
        # Bloxlink usually sets nickname to Roblox username
        roblox_username = member.display_name
        
        # Remove any clan tags or extra stuff (common format: "Username | Tag" or "[Tag] Username")
        if " | " in roblox_username:
            roblox_username = roblox_username.split(" | ")[0]
        if "] " in roblox_username:
            roblox_username = roblox_username.split("] ")[-1]
        
        await interaction.response.defer(ephemeral=True)
        
        # Try to look up the Roblox user to get their ID
        roblox_user = await get_roblox_user_by_username(roblox_username)
        
        roblox_id = None
        if roblox_user:
            roblox_username = roblox_user["name"]  # Use correct capitalization
            roblox_id = roblox_user["id"]
        
        # Give roles
        roles_given = []
        roles_removed = []
        
        # Remove Unverified
        unverified = discord.utils.get(guild.roles, name=UNVERIFIED_ROLE_NAME)
        if unverified and unverified in member.roles:
            try:
                await member.remove_roles(unverified)
                roles_removed.append(unverified.name)
            except:
                pass
        
        # Add Verified (Fallen's own verified role, can be different from Bloxlink's)
        fallen_verified = discord.utils.get(guild.roles, name=FALLEN_VERIFIED_ROLE)
        if fallen_verified and fallen_verified not in member.roles:
            try:
                await member.add_roles(fallen_verified)
                roles_given.append(fallen_verified.name)
            except:
                pass
        
        # Add Abyssbound (member role)
        if abyssbound and abyssbound not in member.roles:
            try:
                await member.add_roles(abyssbound)
                roles_given.append(abyssbound.name)
            except:
                pass
        
        # Save to database
        data = load_data()
        uid = str(member.id)
        data = ensure_user_structure(data, uid)
        data["users"][uid]["roblox_username"] = roblox_username
        data["users"][uid]["verified"] = True
        if roblox_id:
            data["users"][uid]["roblox_id"] = roblox_id
        save_data(data)
        
        # Check achievements
        await check_new_achievements(member.id, guild)
        
        # Success message
        embed = discord.Embed(
            title="✅ Welcome to The Fallen!",
            description=(
                f"You've been verified as **{roblox_username}**!\n\n"
                f"**Roles Given:** {', '.join(roles_given) if roles_given else 'None needed'}\n\n"
                "You now have full access to the server. Enjoy! ⚔️"
            ),
            color=0x2ecc71
        )
        if roblox_id:
            embed.add_field(
                name="🎮 Roblox Profile",
                value=f"[View Profile](https://www.roblox.com/users/{roblox_id}/profile)"
            )
        embed.set_thumbnail(url=member.display_avatar.url)
        
        await interaction.followup.send(embed=embed, ephemeral=True)
        
        # Log to dashboard
        await log_to_dashboard(
            guild, "✅ VERIFY", "Member Verified",
            f"{member.mention} verified as **{roblox_username}**",
            color=0x2ecc71,
            fields={"Method": "Bloxlink Quick Verify", "Roblox": roblox_username}
        )

class ManualVerifyModal(discord.ui.Modal, title="🔗 Link Roblox Account"):
    roblox_username = discord.ui.TextInput(
        label="Roblox Username",
        placeholder="Enter your Roblox username",
        min_length=3,
        max_length=20,
        required=True
    )
    
    async def on_submit(self, interaction: discord.Interaction):
        username = self.roblox_username.value.strip()
        member = interaction.user
        guild = interaction.guild
        
        await interaction.response.defer(ephemeral=True)
        
        # Look up the Roblox user
        roblox_user = await get_roblox_user_by_username(username)
        
        if not roblox_user:
            return await interaction.followup.send(
                f"❌ Could not find Roblox user **{username}**. Please check the spelling.",
                ephemeral=True
            )
        
        roblox_username = roblox_user["name"]
        roblox_id = roblox_user["id"]
        
        # Give roles
        roles_given = []
        
        # Remove Unverified
        unverified = discord.utils.get(guild.roles, name=UNVERIFIED_ROLE_NAME)
        if unverified and unverified in member.roles:
            try:
                await member.remove_roles(unverified)
            except:
                pass
        
        # Add Fallen Verified
        fallen_verified = discord.utils.get(guild.roles, name=FALLEN_VERIFIED_ROLE)
        if fallen_verified and fallen_verified not in member.roles:
            try:
                await member.add_roles(fallen_verified)
                roles_given.append(fallen_verified.name)
            except:
                pass
        
        # Add Abyssbound
        abyssbound = discord.utils.get(guild.roles, name=MEMBER_ROLE_NAME)
        if abyssbound and abyssbound not in member.roles:
            try:
                await member.add_roles(abyssbound)
                roles_given.append(abyssbound.name)
            except:
                pass
        
        # Try to set nickname
        try:
            await member.edit(nick=roblox_username)
        except:
            pass
        
        # Save to database
        data = load_data()
        uid = str(member.id)
        data = ensure_user_structure(data, uid)
        data["users"][uid]["roblox_username"] = roblox_username
        data["users"][uid]["roblox_id"] = roblox_id
        data["users"][uid]["verified"] = True
        save_data(data)
        
        # Check achievements
        await check_new_achievements(member.id, guild)
        
        embed = discord.Embed(
            title="✅ Account Linked!",
            description=(
                f"Successfully linked to **{roblox_username}**!\n\n"
                f"[View Roblox Profile](https://www.roblox.com/users/{roblox_id}/profile)"
            ),
            color=0x2ecc71
        )
        embed.set_thumbnail(url=member.display_avatar.url)
        
        await interaction.followup.send(embed=embed, ephemeral=True)
        
        # Log
        await log_to_dashboard(
            guild, "🔗 LINK", "Account Linked",
            f"{member.mention} linked to **{roblox_username}**",
            color=0x3498db,
            fields={"Roblox ID": str(roblox_id)}
        )

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
        intents = discord.Intents.all()
        
        super().__init__(
            command_prefix="!", 
            intents=intents, 
            help_command=None,
            # Rate limit settings
            max_messages=1000,  # Reduce message cache to save memory
            heartbeat_timeout=120.0,  # Longer timeout for stability
            guild_ready_timeout=10.0,  # Faster guild ready
            assume_unsync_clock=True,  # Better for cloud hosting
        )
        
        # Track rate limits
        self.rate_limit_hits = 0
        self.last_rate_limit = None
    
    async def on_error(self, event_method, *args, **kwargs):
        """Handle errors gracefully"""
        import traceback
        print(f"Error in {event_method}: {traceback.format_exc()}")
    
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

    @tasks.loop(minutes=2)  # Changed from 1 to 2 minutes to reduce API calls
    async def bg_voice_xp(self):
        try:
            for guild in self.guilds:
                for member in guild.members:
                    if member.voice and not member.voice.self_deaf and not member.bot:
                        xp = random.randint(*XP_VOICE_RANGE)
                        add_xp_to_user(member.id, xp)
                        # Track voice time (in minutes)
                        add_user_stat(member.id, 'voice_time', 2)  # 2 minutes now
                        # Update last_active for inactivity tracking
                        update_user_data(member.id, "last_active", datetime.datetime.now(datetime.timezone.utc).isoformat())
                        await check_level_up(member.id, guild)
                        await asyncio.sleep(0.1)  # Small delay between users
        except Exception as e:
            print(f"Voice XP error: {e}")

    @bg_voice_xp.before_loop
    async def before_voice_xp(self):
        await self.wait_until_ready()
        await asyncio.sleep(30)  # Wait 30 seconds after ready before starting

bot = PersistentBot()

# ==========================================
# SUBCOMMAND GROUPS (Saves command slots)
# ==========================================

class EloCommands(commands.GroupCog, name="elo"):
    """ELO and Duel commands"""
    
    def __init__(self, bot):
        self.bot = bot
        super().__init__()
    
    @app_commands.command(name="view", description="Check ELO rating")
    @app_commands.describe(member="Member to check (leave empty for yourself)")
    async def elo_view(self, interaction: discord.Interaction, member: discord.Member = None):
        """Check your or another player's ELO rating"""
        target = member or interaction.user
        
        elo = get_elo(target.id)
        tier, color = get_elo_tier(elo)
        
        history = get_duel_history(target.id, 100)
        wins = sum(1 for d in history if d["winner"] == str(target.id))
        losses = len(history) - wins
        
        embed = discord.Embed(
            title=f"⚔️ {target.display_name}'s ELO",
            color=discord.Color.from_rgb(*color)
        )
        embed.add_field(name="Rating", value=f"**{elo}**", inline=True)
        embed.add_field(name="Rank", value=tier, inline=True)
        embed.add_field(name="Record", value=f"{wins}W - {losses}L", inline=True)
        embed.set_thumbnail(url=target.display_avatar.url)
        
        await interaction.response.send_message(embed=embed)
    
    @app_commands.command(name="leaderboard", description="View the ELO leaderboard")
    async def elo_leaderboard(self, interaction: discord.Interaction):
        """View the top ELO players"""
        top_players = get_elo_leaderboard(10)
        
        if not top_players:
            return await interaction.response.send_message("❌ No ELO data yet! Challenge someone with `/duel`")
        
        embed = discord.Embed(title="🏆 ELO Leaderboard", color=0xFFD700)
        
        desc = ""
        for i, (uid, elo) in enumerate(top_players):
            member = interaction.guild.get_member(int(uid))
            name = member.display_name if member else f"User {uid[:8]}"
            tier, _ = get_elo_tier(elo)
            
            medal = ["🥇", "🥈", "🥉"][i] if i < 3 else f"#{i+1}"
            desc += f"{medal} **{name}** - {elo} {tier}\n"
        
        embed.description = desc
        await interaction.response.send_message(embed=embed)
    
    @app_commands.command(name="history", description="View duel history")
    @app_commands.describe(member="Member to check (leave empty for yourself)")
    async def elo_history(self, interaction: discord.Interaction, member: discord.Member = None):
        """View your duel history"""
        target = member or interaction.user
        history = get_duel_history(target.id, 10)
        
        if not history:
            return await interaction.response.send_message(f"❌ {target.display_name} has no duel history yet!")
        
        embed = discord.Embed(
            title=f"📜 {target.display_name}'s Duel History",
            color=0x3498db
        )
        
        desc = ""
        for duel in history:
            is_winner = duel["winner"] == str(target.id)
            result = "🏆 WIN" if is_winner else "💀 LOSS"
            
            opponent_id = duel["loser"] if is_winner else duel["winner"]
            opponent = interaction.guild.get_member(int(opponent_id))
            opponent_name = opponent.display_name if opponent else f"User"
            
            if is_winner:
                elo_change = f"+{duel['winner_elo_after'] - duel['winner_elo_before']}"
            else:
                elo_change = f"{duel['loser_elo_after'] - duel['loser_elo_before']}"
            
            desc += f"{result} vs **{opponent_name}** ({elo_change})\n"
        
        embed.description = desc
        embed.set_thumbnail(url=target.display_avatar.url)
        await interaction.response.send_message(embed=embed)
    
    @app_commands.command(name="reset", description="Admin: Reset all ELO ratings")
    @app_commands.describe(confirm="Type 'confirm' to reset all ELO")
    @app_commands.checks.has_permissions(administrator=True)
    async def elo_reset(self, interaction: discord.Interaction, confirm: str = None):
        """Reset all ELO ratings - Admin only"""
        if confirm != "confirm":
            return await interaction.response.send_message(
                "⚠️ This will reset ALL ELO ratings!\nUse `/elo reset confirm` to confirm.",
                ephemeral=True
            )
        
        data = load_duels_data()
        data["elo"] = {}
        data["duel_history"] = []
        save_duels_data(data)
        
        await interaction.response.send_message("✅ All ELO ratings have been reset!")


class InactivityCommands(commands.GroupCog, name="inactivity"):
    """Inactivity management commands"""
    
    def __init__(self, bot):
        self.bot = bot
        super().__init__()
    
    @app_commands.command(name="check", description="Staff: Run inactivity check on all ranked members")
    async def inactivity_check(self, interaction: discord.Interaction):
        """Run inactivity check on all ranked members"""
        if not is_staff(interaction.user):
            return await interaction.response.send_message("❌ Staff only.", ephemeral=True)
        
        await interaction.response.defer()
        
        results = await run_inactivity_check(interaction.guild)
        
        embed = discord.Embed(
            title="📋 Inactivity Check Complete",
            color=0xe74c3c if results["strikes_given"] > 0 else 0x2ecc71
        )
        embed.add_field(name="👥 Checked", value=str(results["checked"]), inline=True)
        embed.add_field(name="⚠️ Strikes", value=str(results["strikes_given"]), inline=True)
        embed.add_field(name="⬇️ Demotions", value=str(results["demotions"]), inline=True)
        embed.add_field(name="🚪 Kicks", value=str(results["kicks"]), inline=True)
        
        await interaction.followup.send(embed=embed)
    
    @app_commands.command(name="strikes", description="Check inactivity strikes for a user")
    @app_commands.describe(member="Member to check (leave empty for yourself)")
    async def inactivity_strikes(self, interaction: discord.Interaction, member: discord.Member = None):
        """Check inactivity strikes for a user"""
        target = member or interaction.user
        
        strike_info = get_inactivity_strikes(target.id)
        current_rank = get_member_rank(target)
        
        embed = discord.Embed(
            title=f"⚠️ Inactivity Strikes - {target.display_name}",
            color=0xe74c3c if strike_info["count"] > 0 else 0x2ecc71
        )
        
        strike_bar = "🔴" * strike_info["count"] + "⚪" * (MAX_INACTIVITY_STRIKES - strike_info["count"])
        embed.add_field(name="Strikes", value=f"{strike_bar}\n**{strike_info['count']}/{MAX_INACTIVITY_STRIKES}**", inline=False)
        
        if current_rank:
            embed.add_field(name="Current Rank", value=current_rank, inline=True)
        
        if strike_info["history"]:
            recent = strike_info["history"][-3:]
            history_text = "\n".join([f"• {s['reason']} ({s['date'][:10]})" for s in recent])
            embed.add_field(name="Recent Strikes", value=history_text, inline=False)
        
        embed.set_thumbnail(url=target.display_avatar.url)
        await interaction.response.send_message(embed=embed)
    
    @app_commands.command(name="add", description="Staff: Add an inactivity strike to a user")
    @app_commands.describe(member="Member to strike", reason="Reason for strike")
    async def inactivity_add(self, interaction: discord.Interaction, member: discord.Member, reason: str = "Manual strike by staff"):
        """Add an inactivity strike to a user"""
        if not is_staff(interaction.user):
            return await interaction.response.send_message("❌ Staff only.", ephemeral=True)
        
        new_count = add_inactivity_strike(member.id, reason)
        
        embed = discord.Embed(
            title="⚠️ Inactivity Strike Added",
            description=f"{member.mention} now has **{new_count}** strikes.",
            color=0xe74c3c
        )
        embed.add_field(name="Reason", value=reason, inline=False)
        embed.add_field(name="Added by", value=interaction.user.mention, inline=True)
        
        await interaction.response.send_message(embed=embed)
        await send_inactivity_strike_dm(member, new_count)
    
    @app_commands.command(name="remove", description="Staff: Remove an inactivity strike from a user")
    @app_commands.describe(member="Member to remove strike from")
    async def inactivity_remove(self, interaction: discord.Interaction, member: discord.Member):
        """Remove an inactivity strike from a user"""
        if not is_staff(interaction.user):
            return await interaction.response.send_message("❌ Staff only.", ephemeral=True)
        
        new_count = remove_inactivity_strike(member.id)
        
        embed = discord.Embed(
            title="✅ Inactivity Strike Removed",
            description=f"{member.mention} now has **{new_count}** strikes.",
            color=0x2ecc71
        )
        embed.add_field(name="Removed by", value=interaction.user.mention, inline=True)
        
        await interaction.response.send_message(embed=embed)
    
    @app_commands.command(name="clear", description="Admin: Clear all inactivity strikes from a user")
    @app_commands.describe(member="Member to clear strikes from")
    @app_commands.checks.has_permissions(administrator=True)
    async def inactivity_clear(self, interaction: discord.Interaction, member: discord.Member):
        """Clear all inactivity strikes from a user"""
        clear_inactivity_strikes(member.id)
        
        embed = discord.Embed(
            title="✅ Strikes Cleared",
            description=f"All inactivity strikes cleared for {member.mention}.",
            color=0x2ecc71
        )
        embed.add_field(name="Cleared by", value=interaction.user.mention, inline=True)
        
        await interaction.response.send_message(embed=embed)
    
    @app_commands.command(name="list", description="Staff: Show all members with inactivity strikes")
    async def inactivity_list(self, interaction: discord.Interaction):
        """Show all members with inactivity strikes"""
        if not is_staff(interaction.user):
            return await interaction.response.send_message("❌ Staff only.", ephemeral=True)
        
        data = load_inactivity_data()
        striked_members = []
        
        for uid, info in data.get("strikes", {}).items():
            if info.get("count", 0) > 0:
                member = interaction.guild.get_member(int(uid))
                if member:
                    striked_members.append((member, info["count"]))
        
        striked_members.sort(key=lambda x: x[1], reverse=True)
        
        if not striked_members:
            return await interaction.response.send_message("✅ No members currently have inactivity strikes!")
        
        embed = discord.Embed(
            title="⚠️ Members with Inactivity Strikes",
            color=0xe74c3c
        )
        
        desc = ""
        for member, count in striked_members[:20]:
            strike_bar = "🔴" * count + "⚪" * (MAX_INACTIVITY_STRIKES - count)
            desc += f"{member.mention} - {strike_bar} ({count})\n"
        
        embed.description = desc
        embed.set_footer(text=f"Total: {len(striked_members)} members with strikes")
        
        await interaction.response.send_message(embed=embed)
    
    @app_commands.command(name="setdays", description="Admin: Set days of inactivity before strike")
    @app_commands.describe(days="Number of days (1-90)")
    @app_commands.checks.has_permissions(administrator=True)
    async def inactivity_setdays(self, interaction: discord.Interaction, days: int):
        """Set the number of days before inactivity strike"""
        global INACTIVITY_CHECK_DAYS
        
        if days < 1 or days > 90:
            return await interaction.response.send_message("❌ Days must be between 1 and 90.", ephemeral=True)
        
        INACTIVITY_CHECK_DAYS = days
        await interaction.response.send_message(f"✅ Inactivity threshold set to **{days} days**.")


class ImmunityCommands(commands.GroupCog, name="immunity"):
    """Inactivity immunity commands"""
    
    def __init__(self, bot):
        self.bot = bot
        super().__init__()
    
    @app_commands.command(name="add", description="Staff: Give inactivity immunity to a member")
    @app_commands.describe(member="Member to give immunity", reason="Reason for immunity")
    async def immunity_add(self, interaction: discord.Interaction, member: discord.Member, reason: str = "No reason provided"):
        """Give a member immunity from inactivity checks"""
        if not is_staff(interaction.user):
            return await interaction.response.send_message("❌ Staff only.", ephemeral=True)
        
        immunity_role = discord.utils.get(interaction.guild.roles, name=INACTIVITY_IMMUNITY_ROLE)
        
        if not immunity_role:
            return await interaction.response.send_message(
                f"❌ Role **{INACTIVITY_IMMUNITY_ROLE}** not found!\nPlease create it first.",
                ephemeral=True
            )
        
        if immunity_role in member.roles:
            return await interaction.response.send_message(f"❌ {member.mention} already has immunity!", ephemeral=True)
        
        await safe_add_role(member, immunity_role)
        
        embed = discord.Embed(
            title="🛡️ Inactivity Immunity Granted",
            description=f"{member.mention} is now **immune** to inactivity checks.",
            color=0x2ecc71
        )
        embed.add_field(name="📝 Reason", value=reason, inline=False)
        embed.add_field(name="👤 Granted by", value=interaction.user.mention, inline=True)
        
        await interaction.response.send_message(embed=embed)
        
        try:
            dm_embed = discord.Embed(
                title="🛡️ Inactivity Immunity Granted",
                description=f"You've been given **inactivity immunity** in **{interaction.guild.name}**.\n\n**Reason:** {reason}",
                color=0x2ecc71
            )
            await member.send(embed=dm_embed)
        except:
            pass
    
    @app_commands.command(name="remove", description="Staff: Remove inactivity immunity from a member")
    @app_commands.describe(member="Member to remove immunity from")
    async def immunity_remove(self, interaction: discord.Interaction, member: discord.Member):
        """Remove immunity from a member"""
        if not is_staff(interaction.user):
            return await interaction.response.send_message("❌ Staff only.", ephemeral=True)
        
        immunity_role = discord.utils.get(interaction.guild.roles, name=INACTIVITY_IMMUNITY_ROLE)
        
        if not immunity_role:
            return await interaction.response.send_message(f"❌ Role **{INACTIVITY_IMMUNITY_ROLE}** not found!", ephemeral=True)
        
        if immunity_role not in member.roles:
            return await interaction.response.send_message(f"❌ {member.mention} doesn't have immunity!", ephemeral=True)
        
        await safe_remove_role(member, immunity_role)
        reset_member_activity(member.id)
        
        embed = discord.Embed(
            title="🛡️ Inactivity Immunity Removed",
            description=f"{member.mention}'s immunity has been **removed**.\nActivity timer reset.",
            color=0xe74c3c
        )
        embed.add_field(name="👤 Removed by", value=interaction.user.mention, inline=True)
        
        await interaction.response.send_message(embed=embed)
    
    @app_commands.command(name="list", description="Staff: View all members with inactivity immunity")
    async def immunity_list(self, interaction: discord.Interaction):
        """View all members with immunity"""
        if not is_staff(interaction.user):
            return await interaction.response.send_message("❌ Staff only.", ephemeral=True)
        
        immunity_role = discord.utils.get(interaction.guild.roles, name=INACTIVITY_IMMUNITY_ROLE)
        
        if not immunity_role:
            return await interaction.response.send_message(f"❌ Role **{INACTIVITY_IMMUNITY_ROLE}** not found!", ephemeral=True)
        
        members_with_immunity = [m for m in interaction.guild.members if immunity_role in m.roles]
        
        if not members_with_immunity:
            embed = discord.Embed(
                title="🛡️ Immunity List",
                description="No members currently have inactivity immunity.",
                color=0x95a5a6
            )
        else:
            member_list = "\n".join([f"• {m.mention}" for m in members_with_immunity[:20]])
            embed = discord.Embed(
                title="🛡️ Immunity List",
                description=f"**{len(members_with_immunity)} members** have immunity:\n\n{member_list}",
                color=0x3498db
            )
        
        await interaction.response.send_message(embed=embed)


class EventCommands(commands.GroupCog, name="event"):
    """Event management commands"""
    
    def __init__(self, bot):
        self.bot = bot
        super().__init__()
    
    @app_commands.command(name="create", description="Staff: Create a training or tryout event")
    @app_commands.describe(
        event_type="Type of event",
        title="Event title", 
        minutes_from_now="Minutes from now (e.g., 30 = starts in 30 mins)"
    )
    @app_commands.choices(event_type=[
        app_commands.Choice(name="Training", value="training"),
        app_commands.Choice(name="Tryout", value="tryout"),
    ])
    async def event_create(self, interaction: discord.Interaction, event_type: str, title: str, minutes_from_now: int):
        """Create a scheduled event with RSVP and reminders"""
        if not is_staff(interaction.user):
            return await interaction.response.send_message("❌ Staff only.", ephemeral=True)
        
        if minutes_from_now < 1:
            return await interaction.response.send_message("❌ Time must be at least 1 minute from now.", ephemeral=True)
        
        if minutes_from_now > 10080:  # 7 days max
            return await interaction.response.send_message("❌ Time must be within 7 days.", ephemeral=True)
        
        # Calculate the actual scheduled time
        scheduled_time = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(minutes=minutes_from_now)
        
        event = create_event(
            event_type=event_type.lower(),
            title=title,
            scheduled_time=scheduled_time.isoformat(),
            host_id=interaction.user.id,
            channel_id=interaction.channel.id
        )
        
        ping_role_name = TRAINING_PING_ROLE if event_type.lower() == "training" else TRYOUT_PING_ROLE
        ping_role = discord.utils.get(interaction.guild.roles, name=ping_role_name)
        
        embed = await create_event_embed(event, interaction.guild)
        
        ping_text = ping_role.mention if ping_role else ""
        await interaction.response.send_message(content=ping_text, embed=embed, view=EventRSVPView(event["id"]))
    
    @app_commands.command(name="schedule", description="Staff: Schedule event at specific time")
    @app_commands.describe(
        event_type="Type of event",
        title="Event title",
        hour="Hour (0-23, in UTC)",
        minute="Minute (0-59)"
    )
    @app_commands.choices(event_type=[
        app_commands.Choice(name="Training", value="training"),
        app_commands.Choice(name="Tryout", value="tryout"),
    ])
    async def event_schedule(self, interaction: discord.Interaction, event_type: str, title: str, hour: int, minute: int = 0):
        """Schedule event at a specific UTC time today or tomorrow"""
        if not is_staff(interaction.user):
            return await interaction.response.send_message("❌ Staff only.", ephemeral=True)
        
        if hour < 0 or hour > 23:
            return await interaction.response.send_message("❌ Hour must be 0-23.", ephemeral=True)
        if minute < 0 or minute > 59:
            return await interaction.response.send_message("❌ Minute must be 0-59.", ephemeral=True)
        
        # Calculate scheduled time
        now = datetime.datetime.now(datetime.timezone.utc)
        scheduled_time = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
        
        # If time has passed today, schedule for tomorrow
        if scheduled_time <= now:
            scheduled_time += datetime.timedelta(days=1)
        
        event = create_event(
            event_type=event_type.lower(),
            title=title,
            scheduled_time=scheduled_time.isoformat(),
            host_id=interaction.user.id,
            channel_id=interaction.channel.id
        )
        
        ping_role_name = TRAINING_PING_ROLE if event_type.lower() == "training" else TRYOUT_PING_ROLE
        ping_role = discord.utils.get(interaction.guild.roles, name=ping_role_name)
        
        embed = await create_event_embed(event, interaction.guild)
        
        ping_text = ping_role.mention if ping_role else ""
        await interaction.response.send_message(content=ping_text, embed=embed, view=EventRSVPView(event["id"]))
    
    @app_commands.command(name="list", description="View upcoming events")
    async def event_list(self, interaction: discord.Interaction):
        """View all upcoming scheduled events"""
        events = get_upcoming_events(10)
        
        if not events:
            embed = discord.Embed(
                title="📅 Upcoming Events",
                description="No events scheduled!",
                color=0x95a5a6
            )
            return await interaction.response.send_message(embed=embed)
        
        embed = discord.Embed(title="📅 Upcoming Events", color=0x3498db)
        
        for event in events:
            emoji = "📚" if event["type"] == "training" else "🎯"
            host = interaction.guild.get_member(int(event["host_id"]))
            host_name = host.display_name if host else "Unknown"
            rsvp_count = len(event.get("rsvp_yes", []))
            
            embed.add_field(
                name=f"{emoji} {event['title']}",
                value=f"**Host:** {host_name}\n**RSVPs:** {rsvp_count}",
                inline=False
            )
        
        await interaction.response.send_message(embed=embed)
    
    @app_commands.command(name="cancel", description="Staff: Cancel a scheduled event")
    @app_commands.describe(event_id="Event ID to cancel")
    async def event_cancel(self, interaction: discord.Interaction, event_id: str):
        """Cancel a scheduled event"""
        if not is_staff(interaction.user):
            return await interaction.response.send_message("❌ Staff only.", ephemeral=True)
        
        event = cancel_event(event_id)
        
        if not event:
            return await interaction.response.send_message("❌ Event not found!", ephemeral=True)
        
        await interaction.response.send_message(f"✅ Event **{event['title']}** has been cancelled.")


class RosterCommands(commands.GroupCog, name="roster"):
    """Clan roster commands"""
    
    def __init__(self, bot):
        self.bot = bot
        super().__init__()
    
    @app_commands.command(name="setup", description="Admin: Set up the clan roster panel")
    @app_commands.checks.has_permissions(administrator=True)
    async def roster_setup(self, interaction: discord.Interaction):
        """Set up the clan roster panel"""
        await update_roster_panel(interaction.guild, interaction.channel)
        await interaction.response.send_message("✅ Roster panel created/updated!", ephemeral=True)
    
    @app_commands.command(name="add", description="Admin: Add member to clan roster")
    @app_commands.describe(member="Member to add", rank="Their rank/role")
    @app_commands.checks.has_permissions(administrator=True)
    async def roster_add(self, interaction: discord.Interaction, member: discord.Member, rank: str):
        """Add a member to the roster"""
        data = load_data()
        if "roster" not in data:
            data["roster"] = {"members": [], "title": "THE FALLEN", "description": "Official Clan Roster"}
        
        for m in data["roster"]["members"]:
            if m["id"] == str(member.id):
                return await interaction.response.send_message("❌ Member already in roster!", ephemeral=True)
        
        data["roster"]["members"].append({
            "id": str(member.id),
            "name": member.display_name,
            "rank": rank,
            "added": datetime.datetime.now().isoformat()
        })
        save_data(data)
        
        await interaction.response.send_message(f"✅ Added **{member.display_name}** to roster as **{rank}**")
    
    @app_commands.command(name="remove", description="Admin: Remove member from clan roster")
    @app_commands.describe(member="Member to remove")
    @app_commands.checks.has_permissions(administrator=True)
    async def roster_remove(self, interaction: discord.Interaction, member: discord.Member):
        """Remove a member from the roster"""
        data = load_data()
        if "roster" not in data:
            return await interaction.response.send_message("❌ No roster exists!", ephemeral=True)
        
        data["roster"]["members"] = [m for m in data["roster"]["members"] if m["id"] != str(member.id)]
        save_data(data)
        
        await interaction.response.send_message(f"✅ Removed **{member.display_name}** from roster")
    
    @app_commands.command(name="list", description="View all roster members")
    async def roster_list(self, interaction: discord.Interaction):
        """View all roster members"""
        data = load_data()
        roster = data.get("roster", {}).get("members", [])
        
        if not roster:
            return await interaction.response.send_message("❌ Roster is empty!")
        
        embed = discord.Embed(
            title=data.get("roster", {}).get("title", "Clan Roster"),
            color=0x8B0000
        )
        
        desc = ""
        for m in roster[:25]:
            member = interaction.guild.get_member(int(m["id"]))
            name = member.display_name if member else m.get("name", "Unknown")
            desc += f"• **{name}** - {m.get('rank', 'Member')}\n"
        
        embed.description = desc
        embed.set_footer(text=f"Total: {len(roster)} members")
        
        await interaction.response.send_message(embed=embed)


class TournamentCommands(commands.GroupCog, name="tournament"):
    """Tournament management commands"""
    
    def __init__(self, bot):
        self.bot = bot
        super().__init__()
    
    @app_commands.command(name="create", description="Admin: Create a new tournament")
    @app_commands.describe(name="Tournament name", channel="Channel to post panel (optional)")
    @app_commands.checks.has_permissions(administrator=True)
    async def tournament_create(self, interaction: discord.Interaction, name: str, channel: discord.TextChannel = None):
        """Create a new tournament with panel"""
        target_channel = channel or interaction.channel
        
        data = load_tournaments_data()
        
        if data.get("active") and data["active"].get("status") == "active":
            return await interaction.response.send_message("❌ A tournament is already active!", ephemeral=True)
        
        tournament_id = f"tournament_{int(datetime.datetime.now().timestamp())}"
        
        data["active"] = {
            "id": tournament_id,
            "name": name,
            "participants": [],
            "bracket": None,
            "status": "registration",
            "created_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            "winner": None
        }
        save_tournaments_data(data)
        
        embed = discord.Embed(
            title=f"🏆 {name.upper()}",
            description="**Registration is OPEN!**\n\nClick the buttons below to join or leave.\nStaff can manage the tournament with the Staff button.",
            color=0xFFD700
        )
        embed.add_field(name="📊 Status", value="Registration Open", inline=True)
        embed.add_field(name="👥 Participants", value="0", inline=True)
        embed.set_footer(text=f"Tournament ID: {tournament_id}")
        
        await target_channel.send(embed=embed, view=TournamentPanelView())
        await interaction.response.send_message(f"✅ Tournament **{name}** created in {target_channel.mention}!", ephemeral=True)
    
    @app_commands.command(name="panel", description="Admin: Re-post the tournament panel")
    @app_commands.checks.has_permissions(administrator=True)
    async def tournament_panel(self, interaction: discord.Interaction):
        """Re-post the tournament panel"""
        data = load_tournaments_data()
        
        if not data.get("active"):
            return await interaction.response.send_message("❌ No active tournament!", ephemeral=True)
        
        tournament = data["active"]
        
        embed = discord.Embed(
            title=f"🏆 {tournament['name'].upper()}",
            description="Click the buttons below to interact with the tournament.",
            color=0xFFD700
        )
        embed.add_field(name="📊 Status", value=tournament["status"].title(), inline=True)
        embed.add_field(name="👥 Participants", value=str(len(tournament["participants"])), inline=True)
        
        await interaction.response.send_message(embed=embed, view=TournamentPanelView())
    
    @app_commands.command(name="end", description="Admin: End the current tournament")
    @app_commands.describe(confirm="Type 'confirm' to end")
    @app_commands.checks.has_permissions(administrator=True)
    async def tournament_end(self, interaction: discord.Interaction, confirm: str = None):
        """End the current tournament"""
        if confirm != "confirm":
            return await interaction.response.send_message(
                "⚠️ This will end the tournament!\nUse `/tournament end confirm` to confirm.",
                ephemeral=True
            )
        
        data = load_tournaments_data()
        
        if not data.get("active"):
            return await interaction.response.send_message("❌ No active tournament!", ephemeral=True)
        
        # Archive
        if "history" not in data:
            data["history"] = []
        data["history"].append(data["active"])
        data["active"] = None
        save_tournaments_data(data)
        
        await interaction.response.send_message("✅ Tournament ended and archived!")


class LevelCommands(commands.GroupCog, name="lvl"):
    """Level management commands for staff"""
    
    def __init__(self, bot):
        self.bot = bot
        super().__init__()
    
    @app_commands.command(name="set", description="Staff: Set a user's level directly")
    @app_commands.describe(member="Member to set level for", level="Level to set (0-500)")
    async def level_set(self, interaction: discord.Interaction, member: discord.Member, level: int):
        """Set a user's level directly"""
        if not is_staff(interaction.user):
            return await interaction.response.send_message("❌ Staff only.", ephemeral=True)
        
        if level < 0 or level > 500:
            return await interaction.response.send_message("❌ Level must be between 0 and 500.", ephemeral=True)
        
        total_xp = get_total_xp_for_level(level)
        
        data = load_data()
        uid = str(member.id)
        data = ensure_user_structure(data, uid)
        
        old_level = data["users"][uid]["level"]
        
        data["users"][uid]["xp"] = total_xp
        data["users"][uid]["level"] = level
        save_data(data)
        
        embed = discord.Embed(
            title="📊 Level Set",
            description=f"{member.mention}: Level {old_level} → Level **{level}**",
            color=0x2ecc71
        )
        
        await interaction.response.send_message(embed=embed)
    
    @app_commands.command(name="setxp", description="Staff: Set a user's total XP directly")
    @app_commands.describe(member="Member to set XP for", total_xp="Total XP to set")
    async def level_setxp(self, interaction: discord.Interaction, member: discord.Member, total_xp: int):
        """Set a user's total XP directly"""
        if not is_staff(interaction.user):
            return await interaction.response.send_message("❌ Staff only.", ephemeral=True)
        
        if total_xp < 0:
            return await interaction.response.send_message("❌ XP cannot be negative.", ephemeral=True)
        
        new_level, _ = get_level_from_xp(total_xp)
        
        data = load_data()
        uid = str(member.id)
        data = ensure_user_structure(data, uid)
        
        old_level = data["users"][uid]["level"]
        
        data["users"][uid]["xp"] = total_xp
        data["users"][uid]["level"] = new_level
        save_data(data)
        
        embed = discord.Embed(
            title="✨ XP Set",
            description=f"{member.mention}: {total_xp:,} XP → Level **{new_level}**",
            color=0x2ecc71
        )
        
        await interaction.response.send_message(embed=embed)
    
    @app_commands.command(name="import", description="Staff: Import level from Arcane bot")
    @app_commands.describe(member="Member to import", arcane_level="Their Arcane level", arcane_xp="XP into current level (optional)")
    async def level_import(self, interaction: discord.Interaction, member: discord.Member, arcane_level: int, arcane_xp: int = 0):
        """Import a user's level from Arcane bot"""
        if not is_staff(interaction.user):
            return await interaction.response.send_message("❌ Staff only.", ephemeral=True)
        
        total_xp = get_total_xp_for_level(arcane_level) + arcane_xp
        new_level, _ = get_level_from_xp(total_xp)
        
        data = load_data()
        uid = str(member.id)
        data = ensure_user_structure(data, uid)
        
        data["users"][uid]["xp"] = total_xp
        data["users"][uid]["level"] = new_level
        save_data(data)
        
        embed = discord.Embed(
            title="📥 Arcane Import Complete",
            description=f"{member.mention}: Arcane Level {arcane_level} → Fallen Level **{new_level}**",
            color=0x9b59b6
        )
        
        await interaction.response.send_message(embed=embed)
    
    @app_commands.command(name="add", description="Staff: Add XP to a user")
    @app_commands.describe(member="Member to add XP to", amount="Amount of XP to add")
    async def level_addxp(self, interaction: discord.Interaction, member: discord.Member, amount: int):
        """Add XP to a user"""
        if not is_staff(interaction.user):
            return await interaction.response.send_message("❌ Staff only.", ephemeral=True)
        
        new_xp = add_xp_to_user(member.id, amount)
        await check_level_up(member.id, interaction.guild)
        
        await interaction.response.send_message(f"✅ Added **{amount:,} XP** to {member.mention}. Total: **{new_xp:,} XP**")


# Add cogs to bot
async def setup_cogs():
    await bot.add_cog(EloCommands(bot))
    await bot.add_cog(InactivityCommands(bot))
    await bot.add_cog(ImmunityCommands(bot))
    await bot.add_cog(EventCommands(bot))
    await bot.add_cog(RosterCommands(bot))
    await bot.add_cog(TournamentCommands(bot))
    await bot.add_cog(LevelCommands(bot))

@bot.event
async def on_ready():
    print("=" * 50)
    print(f"✅ Logged in as {bot.user} (ID: {bot.user.id})")
    print(f"✅ Connected to {len(bot.guilds)} guild(s)")
    print(f"✅ PIL Available: {PIL_AVAILABLE}")
    print(f"✅ PostgreSQL Available: {POSTGRES_AVAILABLE}")
    print("=" * 50)
    
    # Initialize PostgreSQL database
    if POSTGRES_AVAILABLE and DATABASE_URL:
        print("Connecting to PostgreSQL database...")
        await init_database()
    else:
        print("📁 Using JSON file storage")
    
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
    
    # Setup subcommand cogs
    if not hasattr(bot, 'cogs_loaded'):
        await setup_cogs()
        bot.cogs_loaded = True
        print("✅ Subcommand groups loaded!")
    
    # Auto-sync slash commands on startup
    try:
        synced = await bot.tree.sync()
        print(f"✅ Synced {len(synced)} slash commands globally!")
    except Exception as e:
        print(f"❌ Failed to sync commands: {e}")
    
    # Start event reminder background task
    if not hasattr(bot, 'event_reminder_task_started'):
        bot.loop.create_task(check_event_reminders())
        bot.event_reminder_task_started = True
        print("✅ Event reminder task started!")
    
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
    
    # Send welcome card to welcome channel
    welcome_channel = discord.utils.get(member.guild.text_channels, name="welcome") or \
                      discord.utils.get(member.guild.text_channels, name="welcomes") or \
                      discord.utils.get(member.guild.text_channels, name="general")
    
    if welcome_channel:
        try:
            # Generate welcome card image
            welcome_card = await create_welcome_card(member)
            if welcome_card:
                file = discord.File(welcome_card, filename="welcome.png")
                embed = discord.Embed(
                    description=f"Welcome {member.mention} to **The Fallen**!\n\nVerify with Bloxlink then click the verify button to gain full access.",
                    color=0x8B0000
                )
                embed.set_image(url="attachment://welcome.png")
                await welcome_channel.send(file=file, embed=embed)
            else:
                # Fallback to text welcome
                embed = discord.Embed(
                    title="👋 Welcome to The Fallen!",
                    description=f"Welcome {member.mention}!\n\nMember #{member.guild.member_count}",
                    color=0x8B0000
                )
                embed.set_thumbnail(url=member.display_avatar.url)
                await welcome_channel.send(embed=embed)
        except Exception as e:
            print(f"Welcome card error: {e}")
    
    # Try to DM them with verification instructions
    try:
        embed = discord.Embed(
            title=f"✝ Welcome to The Fallen! ✝",
            description=(
                f"Hey **{member.name}**, welcome to the server!\n\n"
                f"**🔒 You currently have the Unverified role.**\n\n"
                f"To access the server, you need to verify your Roblox account.\n\n"
                f"**How to verify:**\n"
                f"1️⃣ Use `/verify` to verify with **Bloxlink**\n"
                f"2️⃣ Go to the verification channel\n"
                f"3️⃣ Click the **Verify with Fallen** button\n"
                f"4️⃣ Done! You'll get full access instantly.\n\n"
                f"**After verifying, you'll receive:**\n"
                f"• ✅ Fallen Verified role\n"
                f"• ✅ Abyssbound role (full server access)\n\n"
                f"See you inside! ⚔️"
            ),
            color=0x8B0000
        )
        embed.set_thumbnail(url=member.guild.icon.url if member.guild.icon else None)
        embed.set_footer(text="✝ The Fallen ✝")
        await member.send(embed=embed)
    except:
        pass  # Can't DM user
    
    # Log to dashboard
    await log_to_dashboard(
        member.guild, "👋 JOIN", "Member Joined",
        f"{member.mention} joined the server",
        color=0x2ecc71,
        fields={"Account Age": f"<t:{int(member.created_at.timestamp())}:R>", "Member #": str(member.guild.member_count)}
    )
    
    # Log the join
    await log_action(member.guild, "👋 Member Joined", f"{member.mention} joined the server\nAccount created: <t:{int(member.created_at.timestamp())}:R>", 0x3498db)
    
    # Check for member milestones
    await check_member_milestone(member.guild)

@bot.event
async def on_message(message):
    if not message.author.bot and message.guild:
        # Check cooldown before giving XP
        if check_xp_cooldown(message.author.id, "message"):
            xp = random.randint(*XP_TEXT_RANGE)
            add_xp_to_user(message.author.id, xp)
            await check_level_up(message.author.id, message.guild)
        
        # Always update last_active timestamp for inactivity tracking
        update_user_data(message.author.id, "last_active", datetime.datetime.now(datetime.timezone.utc).isoformat())
        
    await bot.process_commands(message)

@bot.event
async def on_reaction_add(reaction, user):
    if not user.bot and reaction.message.guild:
        # Check cooldown before giving XP
        if check_xp_cooldown(user.id, "reaction"):
            xp = random.randint(*XP_REACTION_RANGE)
            add_xp_to_user(user.id, xp)
            await check_level_up(user.id, reaction.message.guild)
        
        # Always update last_active timestamp for inactivity tracking
        update_user_data(user.id, "last_active", datetime.datetime.now(datetime.timezone.utc).isoformat())

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
@commands.cooldown(1, 10, commands.BucketType.user)  # 1 use per 10 seconds per user
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

@bot.hybrid_command(name="leaderboard", aliases=["lb"], description="View the XP leaderboard")
@commands.cooldown(1, 15, commands.BucketType.user)  # 1 use per 15 seconds per user
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
        title="✝ THE FALLEN ✝",
        description="**Welcome to the Fallen Bot!**\n\nSelect a category below to explore commands.",
        color=0x8B0000
    )
    
    # Categories with emojis
    embed.add_field(
        name="━━━━━ User Commands ━━━━━",
        value=(
            "👤 **Member** - Verification & basics\n"
            "📊 **Profile & Stats** - Cards & statistics\n"
            "🏆 **Achievements** - Badges & progress\n"
            "💰 **Economy** - Coins & shop"
        ),
        inline=False
    )
    
    embed.add_field(
        name="━━━━━ Activities ━━━━━",
        value=(
            "🎫 **Tickets** - Support system\n"
            "🏴‍☠️ **Raids & Wars** - Combat & battles\n"
            "📚 **Training** - Events & tryouts\n"
            "📋 **Applications** - Staff applications"
        ),
        inline=False
    )
    
    embed.add_field(
        name="━━━━━ Staff Only ━━━━━",
        value=(
            "🛡️ **Staff** - Moderation tools\n"
            "⚙️ **Admin** - Server management"
        ),
        inline=False
    )
    
    embed.set_thumbnail(url=ctx.guild.icon.url if ctx.guild.icon else None)
    embed.set_footer(text="Use the dropdown below to view commands • / or ! prefix")
    
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

@bot.hybrid_command(name="setlevel", description="Staff: Set a user's level directly")
@commands.has_any_role(*HIGH_STAFF_ROLES)
async def setlevel(ctx, member: discord.Member, level: int):
    """Set a user's level directly (calculates required XP)"""
    if level < 0 or level > 500:
        return await ctx.send("❌ Level must be between 0 and 500.", ephemeral=True)
    
    # Calculate total XP needed for this level
    total_xp = get_total_xp_for_level(level)
    
    # Set the user's XP and level
    data = load_data()
    uid = str(member.id)
    data = ensure_user_structure(data, uid)
    
    old_level = data["users"][uid]["level"]
    old_xp = data["users"][uid]["xp"]
    
    data["users"][uid]["xp"] = total_xp
    data["users"][uid]["level"] = level
    save_data(data)
    
    # Award milestone roles if applicable
    milestone = get_milestone_reward(level)
    role_msg = ""
    if milestone:
        role = discord.utils.get(ctx.guild.roles, name=milestone["role"])
        if role:
            try:
                await member.add_roles(role)
                role_msg = f"\n🎭 Role: {role.mention}"
            except:
                pass
    
    embed = discord.Embed(
        title="📊 Level Set",
        description=f"{member.mention}'s level has been updated!",
        color=0x2ecc71
    )
    embed.add_field(name="Before", value=f"Level {old_level}\n{old_xp:,} XP", inline=True)
    embed.add_field(name="After", value=f"Level {level}\n{total_xp:,} XP{role_msg}", inline=True)
    
    await ctx.send(embed=embed)
    await log_action(ctx.guild, "📊 Level Set", f"{member.mention} set to Level {level} by {ctx.author.mention}", 0x2ecc71)

@bot.hybrid_command(name="setxp", description="Staff: Set a user's total XP directly")
@commands.has_any_role(*HIGH_STAFF_ROLES)
async def setxp(ctx, member: discord.Member, total_xp: int):
    """Set a user's total XP directly (level auto-calculates)"""
    if total_xp < 0:
        return await ctx.send("❌ XP cannot be negative.", ephemeral=True)
    
    # Calculate level from XP
    new_level, xp_into_level = get_level_from_xp(total_xp)
    
    # Set the user's XP and level
    data = load_data()
    uid = str(member.id)
    data = ensure_user_structure(data, uid)
    
    old_level = data["users"][uid]["level"]
    old_xp = data["users"][uid]["xp"]
    
    data["users"][uid]["xp"] = total_xp
    data["users"][uid]["level"] = new_level
    save_data(data)
    
    # Award milestone roles if applicable
    milestone = get_milestone_reward(new_level)
    role_msg = ""
    if milestone:
        role = discord.utils.get(ctx.guild.roles, name=milestone["role"])
        if role:
            try:
                await member.add_roles(role)
                role_msg = f"\n🎭 Role: {role.mention}"
            except:
                pass
    
    embed = discord.Embed(
        title="✨ XP Set",
        description=f"{member.mention}'s XP has been updated!",
        color=0x2ecc71
    )
    embed.add_field(name="Before", value=f"Level {old_level}\n{old_xp:,} XP", inline=True)
    embed.add_field(name="After", value=f"Level {new_level}\n{total_xp:,} XP{role_msg}", inline=True)
    
    await ctx.send(embed=embed)
    await log_action(ctx.guild, "✨ XP Set", f"{member.mention} set to {total_xp:,} XP (Level {new_level}) by {ctx.author.mention}", 0x2ecc71)

@bot.hybrid_command(name="importlevel", description="Staff: Import level from Arcane bot")
@commands.has_any_role(*HIGH_STAFF_ROLES)
async def importlevel(ctx, member: discord.Member, arcane_level: int, arcane_xp: int = 0):
    """
    Import a user's level from Arcane bot.
    
    Usage: /importlevel @user <arcane_level> [arcane_xp]
    Example: /importlevel @User 25 1500
    
    The arcane_xp is their XP progress into the current level (shown as X/Y in Arcane)
    """
    if arcane_level < 0 or arcane_level > 500:
        return await ctx.send("❌ Level must be between 0 and 500.", ephemeral=True)
    
    # Calculate our equivalent total XP for their Arcane level
    # Get base XP for reaching that level
    total_xp = get_total_xp_for_level(arcane_level)
    # Add their progress into current level
    total_xp += arcane_xp
    
    # Calculate what level they'll be in our system
    new_level, xp_into_level = get_level_from_xp(total_xp)
    
    # Set the user's data
    data = load_data()
    uid = str(member.id)
    data = ensure_user_structure(data, uid)
    
    old_level = data["users"][uid]["level"]
    old_xp = data["users"][uid]["xp"]
    
    data["users"][uid]["xp"] = total_xp
    data["users"][uid]["level"] = new_level
    save_data(data)
    
    # Award all milestone roles up to their level
    roles_given = []
    for milestone_level in sorted(LEVEL_CONFIG.keys()):
        if milestone_level <= new_level:
            milestone = LEVEL_CONFIG[milestone_level]
            role = discord.utils.get(ctx.guild.roles, name=milestone["role"])
            if role and role not in member.roles:
                try:
                    await member.add_roles(role)
                    roles_given.append(role.mention)
                    await asyncio.sleep(0.5)  # Rate limit protection
                except:
                    pass
    
    embed = discord.Embed(
        title="📥 Arcane Import Complete",
        description=f"{member.mention}'s level has been imported!",
        color=0x9b59b6
    )
    embed.add_field(name="Arcane Stats", value=f"Level {arcane_level}\n+{arcane_xp} XP progress", inline=True)
    embed.add_field(name="Fallen Stats", value=f"Level {new_level}\n{total_xp:,} Total XP", inline=True)
    
    if roles_given:
        embed.add_field(name="🎭 Roles Given", value="\n".join(roles_given[:5]) + (f"\n+{len(roles_given)-5} more" if len(roles_given) > 5 else ""), inline=False)
    
    embed.set_footer(text=f"Imported by {ctx.author.display_name}")
    
    await ctx.send(embed=embed)
    await log_action(ctx.guild, "📥 Arcane Import", f"{member.mention} imported from Arcane Level {arcane_level} → Fallen Level {new_level} by {ctx.author.mention}", 0x9b59b6)

@bot.hybrid_command(name="bulkimport", description="Admin: Show bulk import instructions")
@commands.has_permissions(administrator=True)
async def bulkimport(ctx):
    """Show instructions for bulk importing from Arcane"""
    embed = discord.Embed(
        title="📥 Bulk Import from Arcane",
        description="How to migrate your members from Arcane to The Fallen bot:",
        color=0x9b59b6
    )
    
    embed.add_field(
        name="Step 1: Get Arcane Data",
        value=(
            "Use Arcane's `/leaderboard` or `/rank @user` to see each member's:\n"
            "• **Level** (e.g., Level 25)\n"
            "• **XP Progress** (e.g., 1,500/3,000 XP)"
        ),
        inline=False
    )
    
    embed.add_field(
        name="Step 2: Import Each User",
        value=(
            "Use this command for each member:\n"
            "```/importlevel @user <level> <xp_progress>```\n"
            "**Example:** `/importlevel @John 25 1500`"
        ),
        inline=False
    )
    
    embed.add_field(
        name="Step 3: Verify",
        value="Use `/level @user` to confirm their stats were imported correctly.",
        inline=False
    )
    
    embed.add_field(
        name="📊 Level Conversion",
        value=(
            "Our system uses similar XP scaling to Arcane.\n"
            "Members will be at approximately the same level.\n\n"
            "**Commands Available:**\n"
            "`/importlevel @user <level> [xp]` - Import from Arcane\n"
            "`/setlevel @user <level>` - Set level directly\n"
            "`/setxp @user <total_xp>` - Set total XP directly"
        ),
        inline=False
    )
    
    await ctx.send(embed=embed)

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
        title="✝ The Fallen Verification ✝",
        description=(
            "Welcome to **The Fallen**!\n\n"
            "**🔒 You need to verify to access the server.**\n\n"
            "**📝 How to Verify:**\n"
            "1️⃣ First, verify with **Bloxlink** (`/verify`)\n"
            "2️⃣ Once you have the Bloxlink verified role, click the button below\n"
            "3️⃣ You'll automatically get access to The Fallen!\n\n"
            "**✅ After verifying, you'll receive:**\n"
            "• 🏷️ **Fallen Verified** role\n"
            "• 🏷️ **Abyssbound** role (full server access)\n"
            "• 🎮 Access to all channels & features\n\n"
            "**Already verified with Bloxlink?**\n"
            "Click the button below to get your Fallen roles!"
        ),
        color=0x8B0000
    )
    embed.set_footer(text="✝ The Fallen ✝ • Powered by Bloxlink")
    
    await target_channel.send(embed=embed, view=VerifyView())
    await ctx.send(f"✅ Verification panel posted in {target_channel.mention}", ephemeral=True)
    await log_action(ctx.guild, "📋 Verify Panel", f"Posted in {target_channel.mention} by {ctx.author.mention}", 0x2ecc71)

@bot.hybrid_command(name="set_bloxlink_role", description="Admin: Set the Bloxlink verified role name")
@commands.has_permissions(administrator=True)
async def set_bloxlink_role(ctx, role: discord.Role):
    """Set which role Bloxlink gives when users verify"""
    global BLOXLINK_VERIFIED_ROLE
    BLOXLINK_VERIFIED_ROLE = role.name
    await ctx.send(f"✅ Bloxlink verified role set to **{role.name}**\n\nUsers with this role can now click the verify button to get Fallen access.", ephemeral=True)

@bot.hybrid_command(name="setup_shop", description="Admin: Set up the shop panel")
@commands.has_permissions(administrator=True)
async def setup_shop(ctx):
    """Create the shop panel in the shop channel with image"""
    ch = discord.utils.get(ctx.guild.text_channels, name=SHOP_CHANNEL_NAME)
    if not ch:
        return await ctx.send(f"❌ Channel `{SHOP_CHANNEL_NAME}` not found. Create it first!", ephemeral=True)
    
    # Try to create shop image
    if PIL_AVAILABLE:
        try:
            shop_image = await create_shop_image()
            if shop_image:
                file = discord.File(shop_image, filename="shop.png")
                embed = discord.Embed(color=0x8B0000)
                embed.set_image(url="attachment://shop.png")
                await ch.send(file=file, embed=embed, view=ShopView())
                await ctx.send(f"✅ Shop panel posted in {ch.mention}", ephemeral=True)
                return
        except Exception as e:
            print(f"Shop image error: {e}")
    
    # Fallback to embed
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

@bot.hybrid_command(name="voicetop", description="View voice time leaderboard")
async def voicetop(ctx):
    """Display voice time leaderboard"""
    if PIL_AVAILABLE:
        try:
            lb_image = await create_voice_leaderboard_image(ctx.guild)
            if lb_image:
                file = discord.File(lb_image, filename="voice_lb.png")
                await ctx.send(file=file)
                return
        except Exception as e:
            print(f"Voice leaderboard error: {e}")
    
    # Fallback to embed
    data = load_data()
    users = data.get("users", {})
    sorted_users = sorted(users.items(), key=lambda x: x[1].get('voice_time', 0), reverse=True)[:10]
    
    embed = discord.Embed(title="🎙️ Voice Time Leaderboard", color=0x9b59b6)
    
    desc = ""
    for i, (uid, udata) in enumerate(sorted_users):
        member = ctx.guild.get_member(int(uid))
        name = member.display_name if member else f"User {uid[:8]}"
        mins = udata.get('voice_time', 0)
        hours = mins // 60
        desc += f"**#{i+1}** {name} - {hours}h {mins % 60}m\n"
    
    embed.description = desc or "No voice data yet!"
    await ctx.send(embed=embed)

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
    """View scheduled events (alias for /event_list)"""
    await event_list(ctx)

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

@bot.hybrid_command(name="event_create", description="Staff: Create a training or tryout event")
@commands.has_any_role(*HIGH_STAFF_ROLES, STAFF_ROLE_NAME)
async def event_create(ctx, event_type: str, title: str, minutes_from_now: int):
    """
    Create a scheduled event with RSVP and reminders.
    
    event_type: 'training' or 'tryout'
    title: Name of the event
    minutes_from_now: How many minutes until the event starts (e.g., 30 = in 30 mins)
    """
    if event_type.lower() not in ["training", "tryout"]:
        return await ctx.send("❌ Event type must be `training` or `tryout`", ephemeral=True)
    
    if minutes_from_now < 1:
        return await ctx.send("❌ Time must be at least 1 minute from now.", ephemeral=True)
    
    if minutes_from_now > 10080:  # 7 days max
        return await ctx.send("❌ Time must be within 7 days.", ephemeral=True)
    
    # Calculate the actual scheduled time
    scheduled_time = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(minutes=minutes_from_now)
    
    # Create the event
    event = create_event(
        event_type=event_type.lower(),
        title=title,
        scheduled_time=scheduled_time.isoformat(),
        host_id=ctx.author.id,
        channel_id=ctx.channel.id
    )
    
    # Get ping role
    ping_role_name = TRAINING_PING_ROLE if event_type.lower() == "training" else TRYOUT_PING_ROLE
    ping_role = discord.utils.get(ctx.guild.roles, name=ping_role_name)
    
    # Create embed
    embed = await create_event_embed(event, ctx.guild)
    
    # Send announcement
    ping_text = ping_role.mention if ping_role else ""
    msg = await ctx.send(content=ping_text, embed=embed, view=EventRSVPView(event["id"]))
    
    # Store message ID for later updates
    update_event(event["id"], {"message_id": str(msg.id)})
    
    await log_action(ctx.guild, f"📅 Event Created", f"**{title}**\nType: {event_type}\nStarts: <t:{int(scheduled_time.timestamp())}:R>\nHost: {ctx.author.mention}", 0x3498db)

@bot.hybrid_command(name="event_schedule", description="Staff: Schedule event at specific UTC time")
@commands.has_any_role(*HIGH_STAFF_ROLES, STAFF_ROLE_NAME)
async def event_schedule_cmd(ctx, event_type: str, title: str, hour: int, minute: int = 0):
    """
    Schedule event at a specific UTC time today or tomorrow.
    
    event_type: 'training' or 'tryout'
    title: Name of the event
    hour: Hour in UTC (0-23)
    minute: Minute (0-59)
    """
    if event_type.lower() not in ["training", "tryout"]:
        return await ctx.send("❌ Event type must be `training` or `tryout`", ephemeral=True)
    
    if hour < 0 or hour > 23:
        return await ctx.send("❌ Hour must be 0-23.", ephemeral=True)
    if minute < 0 or minute > 59:
        return await ctx.send("❌ Minute must be 0-59.", ephemeral=True)
    
    # Calculate scheduled time
    now = datetime.datetime.now(datetime.timezone.utc)
    scheduled_time = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
    
    # If time has passed today, schedule for tomorrow
    if scheduled_time <= now:
        scheduled_time += datetime.timedelta(days=1)
    
    # Create the event
    event = create_event(
        event_type=event_type.lower(),
        title=title,
        scheduled_time=scheduled_time.isoformat(),
        host_id=ctx.author.id,
        channel_id=ctx.channel.id
    )
    
    # Get ping role
    ping_role_name = TRAINING_PING_ROLE if event_type.lower() == "training" else TRYOUT_PING_ROLE
    ping_role = discord.utils.get(ctx.guild.roles, name=ping_role_name)
    
    # Create embed
    embed = await create_event_embed(event, ctx.guild)
    
    # Send announcement
    ping_text = ping_role.mention if ping_role else ""
    msg = await ctx.send(content=ping_text, embed=embed, view=EventRSVPView(event["id"]))
    
    # Store message ID
    update_event(event["id"], {"message_id": str(msg.id)})
    
    await log_action(ctx.guild, f"📅 Event Scheduled", f"**{title}**\nType: {event_type}\nTime: <t:{int(scheduled_time.timestamp())}:F>\nHost: {ctx.author.mention}", 0x3498db)

@bot.hybrid_command(name="event_list", description="View upcoming events")
async def event_list(ctx):
    """View all upcoming scheduled events"""
    events = get_upcoming_events(10)
    
    if not events:
        embed = discord.Embed(
            title="📅 Upcoming Events",
            description="No events scheduled!\n\nStaff can create events with `/event_create`",
            color=0x95a5a6
        )
        return await ctx.send(embed=embed)
    
    embed = discord.Embed(title="📅 Upcoming Events", color=0x3498db)
    
    for event in events:
        emoji = "📚" if event["type"] == "training" else "🎯"
        
        try:
            event_time = datetime.datetime.fromisoformat(event["scheduled_time"])
            time_str = f"<t:{int(event_time.timestamp())}:F>"
        except:
            time_str = event["scheduled_time"]
        
        host = ctx.guild.get_member(int(event["host_id"]))
        host_name = host.display_name if host else "Unknown"
        
        rsvp_count = len(event.get("rsvp_yes", []))
        
        embed.add_field(
            name=f"{emoji} {event['title']}",
            value=f"**Time:** {time_str}\n**Host:** {host_name}\n**RSVPs:** {rsvp_count} attending",
            inline=False
        )
    
    embed.set_footer(text="React to event announcements to RSVP!")
    await ctx.send(embed=embed)

@bot.hybrid_command(name="event_cancel", description="Staff: Cancel a scheduled event")
@commands.has_any_role(*HIGH_STAFF_ROLES, STAFF_ROLE_NAME)
async def event_cancel(ctx, event_id: str):
    """Cancel a scheduled event"""
    event = cancel_event(event_id)
    
    if not event:
        return await ctx.send("❌ Event not found!", ephemeral=True)
    
    await ctx.send(f"✅ Event **{event['title']}** has been cancelled.")
    await log_action(ctx.guild, "❌ Event Cancelled", f"**{event['title']}** cancelled by {ctx.author.mention}", 0xe74c3c)

@bot.hybrid_command(name="event_log", description="Staff: Log attendance and give rewards")
@commands.has_any_role(*HIGH_STAFF_ROLES, STAFF_ROLE_NAME)
async def event_log(ctx, event_id: str):
    """
    Log attendance for an event. Mention all attendees.
    
    Usage: /event_log <event_id> @user1 @user2 @user3
    """
    # Get mentioned users
    mentioned = ctx.message.mentions if hasattr(ctx, 'message') and ctx.message else []
    
    if not mentioned:
        return await ctx.send(
            "❌ Please mention the attendees!\n"
            "Usage: `/event_log <event_id>` then mention users\n"
            "Example: `/event_log event_123456` @user1 @user2",
            ephemeral=True
        )
    
    # Log attendance
    attendee_ids = [m.id for m in mentioned]
    result = log_attendance(event_id, attendee_ids, ctx.author.id)
    
    if not result:
        return await ctx.send("❌ Event not found!", ephemeral=True)
    
    event = result["event"]
    
    # Build results embed
    embed = discord.Embed(
        title=f"✅ Attendance Logged - {event['title']}",
        description=f"**{len(attendee_ids)} attendees** rewarded!",
        color=0x2ecc71
    )
    
    # Show rewards
    rewards = ATTENDANCE_REWARDS.get(event["type"], {"coins": 50, "xp": 25})
    embed.add_field(
        name="💰 Rewards Given",
        value=f"• {rewards['coins']} coins each\n• {rewards['xp']} XP each",
        inline=True
    )
    
    # Show host rewards
    host_rewards = result["host_rewards"]
    embed.add_field(
        name="👑 Host Rewards",
        value=f"• {host_rewards['coins']} coins\n• {host_rewards['xp']} XP",
        inline=True
    )
    
    # Show streaks
    streak_info = []
    for r in result["attendees"]:
        if r["streak_bonus"] > 0:
            member = ctx.guild.get_member(int(r["user_id"]))
            name = member.display_name if member else f"User {r['user_id'][:8]}"
            streak_info.append(f"🔥 **{name}**: {r['streak']} streak! (+{r['streak_bonus']} bonus)")
    
    if streak_info:
        embed.add_field(name="🔥 Streak Bonuses", value="\n".join(streak_info[:5]), inline=False)
    
    # Show attendees
    attendee_names = [m.display_name for m in mentioned[:10]]
    embed.add_field(
        name=f"📋 Attendees ({len(mentioned)})",
        value=", ".join(attendee_names) + ("..." if len(mentioned) > 10 else ""),
        inline=False
    )
    
    await ctx.send(embed=embed)
    
    # Level up checks
    for uid in attendee_ids:
        await check_level_up(uid, ctx.guild)
    await check_level_up(ctx.author.id, ctx.guild)
    
    await log_action(
        ctx.guild, "📋 Attendance Logged",
        f"**{event['title']}**\nAttendees: {len(attendee_ids)}\nHost: {ctx.author.mention}",
        0x2ecc71
    )

@bot.hybrid_command(name="attendance_streak", description="Check your attendance streak")
async def attendance_streak(ctx, member: discord.Member = None):
    """Check attendance streak"""
    target = member or ctx.author
    
    streak = get_attendance_streak(target.id)
    user_data = get_user_data(target.id)
    
    training_count = user_data.get("training_attendance", 0)
    tryout_count = user_data.get("tryout_attendance", 0)
    events_hosted = user_data.get("events_hosted", 0)
    
    embed = discord.Embed(
        title=f"🔥 {target.display_name}'s Attendance",
        color=0xff6b6b
    )
    
    # Current streak
    current = streak.get("current", 0)
    best = streak.get("best", 0)
    
    streak_bar = "🔥" * min(current, 10) + "⬜" * max(0, 10 - current)
    
    embed.add_field(
        name="📊 Current Streak",
        value=f"{streak_bar}\n**{current}** events in a row\n(Best: {best})",
        inline=False
    )
    
    # Next bonus
    next_bonus = None
    for milestone in sorted(STREAK_BONUSES.keys()):
        if current < milestone:
            next_bonus = (milestone, STREAK_BONUSES[milestone])
            break
    
    if next_bonus:
        embed.add_field(
            name="🎁 Next Bonus",
            value=f"{next_bonus[1]} coins at {next_bonus[0]} streak ({next_bonus[0] - current} more!)",
            inline=True
        )
    
    # Total attendance
    embed.add_field(
        name="📋 Total Attendance",
        value=f"📚 Trainings: **{training_count}**\n🎯 Tryouts: **{tryout_count}**\n👑 Hosted: **{events_hosted}**",
        inline=True
    )
    
    embed.set_thumbnail(url=target.display_avatar.url)
    await ctx.send(embed=embed)

@bot.hybrid_command(name="quick_training", description="Staff: Quick announce training (no RSVP)")
@commands.has_any_role(*HIGH_STAFF_ROLES, STAFF_ROLE_NAME)
async def quick_training(ctx, time: str, *, description: str = ""):
    """Quick training announcement without full event system"""
    ping_role = discord.utils.get(ctx.guild.roles, name=TRAINING_PING_ROLE)
    
    embed = discord.Embed(
        title="📚 TRAINING ANNOUNCEMENT",
        description=(
            f"**⏰ Time:** {time}\n"
            f"**👑 Host:** {ctx.author.mention}\n\n"
            f"{description if description else 'Join voice when ready!'}\n\n"
            f"**💰 Rewards:** 100 coins + 50 XP"
        ),
        color=0x3498db
    )
    embed.set_footer(text="Be there or break your streak!")
    
    ping_text = ping_role.mention if ping_role else ""
    await ctx.send(content=ping_text, embed=embed)

@bot.hybrid_command(name="quick_tryout", description="Staff: Quick announce tryout (no RSVP)")
@commands.has_any_role(*HIGH_STAFF_ROLES, STAFF_ROLE_NAME)
async def quick_tryout(ctx, time: str, *, description: str = ""):
    """Quick tryout announcement without full event system"""
    ping_role = discord.utils.get(ctx.guild.roles, name=TRYOUT_PING_ROLE)
    
    embed = discord.Embed(
        title="🎯 TRYOUT ANNOUNCEMENT",
        description=(
            f"**⏰ Time:** {time}\n"
            f"**👑 Host:** {ctx.author.mention}\n\n"
            f"{description if description else 'DM to sign up!'}\n\n"
            f"**💰 Rewards:** 150 coins + 75 XP"
        ),
        color=0xF1C40F
    )
    embed.set_footer(text="Good luck to all participants!")
    
    ping_text = ping_role.mention if ping_role else ""
    await ctx.send(content=ping_text, embed=embed)

@bot.hybrid_command(name="log_training", description="Staff: Quick log training attendance")
@commands.has_any_role(*HIGH_STAFF_ROLES, STAFF_ROLE_NAME)
async def log_training(ctx):
    """Quick log training attendance - mention attendees"""
    mentioned = ctx.message.mentions if hasattr(ctx, 'message') and ctx.message else []
    
    if not mentioned:
        return await ctx.send("❌ Please mention the attendees!", ephemeral=True)
    
    rewards = ATTENDANCE_REWARDS["training"]
    streak_bonuses = []
    role_rewards = []
    
    for m in mentioned:
        add_user_stat(m.id, "coins", rewards["coins"])
        add_xp_to_user(m.id, rewards["xp"])
        add_user_stat(m.id, "training_attendance", 1)
        
        # Reset activity timestamp - prevents inactivity strikes
        reset_member_activity(m.id)
        
        streak = update_attendance_streak(m.id)
        bonus = get_streak_bonus(streak)
        if bonus > 0:
            add_user_stat(m.id, "coins", bonus)
            streak_bonuses.append(f"🔥 {m.display_name}: {streak} streak (+{bonus})")
        
        # Check for attendance role rewards
        new_roles = await check_attendance_roles(m, ctx.guild)
        if new_roles:
            role_rewards.append(f"🎖️ {m.display_name}: **{new_roles[0]}**")
        
        # Check for streak role rewards
        streak_roles = await check_streak_roles(m, ctx.guild, streak)
        if streak_roles:
            role_rewards.append(f"🔥 {m.display_name}: **{streak_roles[0]}**")
        
        await check_level_up(m.id, ctx.guild)
        await asyncio.sleep(0.5)
    
    # Host rewards
    host_rewards = ATTENDANCE_REWARDS["host"]
    add_user_stat(ctx.author.id, "coins", host_rewards["coins"])
    add_xp_to_user(ctx.author.id, host_rewards["xp"])
    add_user_stat(ctx.author.id, "events_hosted", 1)
    reset_member_activity(ctx.author.id)  # Reset host activity too
    
    embed = discord.Embed(
        title="📚 Training Attendance Logged",
        description=f"**{len(mentioned)} attendees** rewarded!",
        color=0x2ecc71
    )
    embed.add_field(name="💰 Each Received", value=f"{rewards['coins']} coins + {rewards['xp']} XP", inline=True)
    embed.add_field(name="👑 Host Received", value=f"{host_rewards['coins']} coins + {host_rewards['xp']} XP", inline=True)
    
    if streak_bonuses:
        embed.add_field(name="🔥 Streak Bonuses", value="\n".join(streak_bonuses[:5]), inline=False)
    
    if role_rewards:
        embed.add_field(name="🎉 Role Rewards Earned!", value="\n".join(role_rewards[:5]), inline=False)
    
    await ctx.send(embed=embed)

@bot.hybrid_command(name="log_tryout", description="Staff: Quick log tryout attendance")
@commands.has_any_role(*HIGH_STAFF_ROLES, STAFF_ROLE_NAME)
async def log_tryout(ctx):
    """Quick log tryout attendance - mention attendees"""
    mentioned = ctx.message.mentions if hasattr(ctx, 'message') and ctx.message else []
    
    if not mentioned:
        return await ctx.send("❌ Please mention the attendees!", ephemeral=True)
    
    rewards = ATTENDANCE_REWARDS["tryout"]
    streak_bonuses = []
    role_rewards = []
    
    for m in mentioned:
        add_user_stat(m.id, "coins", rewards["coins"])
        add_xp_to_user(m.id, rewards["xp"])
        add_user_stat(m.id, "tryout_attendance", 1)
        
        # Reset activity timestamp - prevents inactivity strikes
        reset_member_activity(m.id)
        
        streak = update_attendance_streak(m.id)
        bonus = get_streak_bonus(streak)
        if bonus > 0:
            add_user_stat(m.id, "coins", bonus)
            streak_bonuses.append(f"🔥 {m.display_name}: {streak} streak (+{bonus})")
        
        # Check for attendance role rewards
        new_roles = await check_attendance_roles(m, ctx.guild)
        if new_roles:
            role_rewards.append(f"🎖️ {m.display_name}: **{new_roles[0]}**")
        
        # Check for streak role rewards
        streak_roles = await check_streak_roles(m, ctx.guild, streak)
        if streak_roles:
            role_rewards.append(f"🔥 {m.display_name}: **{streak_roles[0]}**")
        
        await check_level_up(m.id, ctx.guild)
        await asyncio.sleep(0.5)
    
    # Host rewards
    host_rewards = ATTENDANCE_REWARDS["host"]
    add_user_stat(ctx.author.id, "coins", host_rewards["coins"])
    add_xp_to_user(ctx.author.id, host_rewards["xp"])
    add_user_stat(ctx.author.id, "events_hosted", 1)
    reset_member_activity(ctx.author.id)  # Reset host activity too
    
    embed = discord.Embed(
        title="🎯 Tryout Attendance Logged",
        description=f"**{len(mentioned)} attendees** rewarded!",
        color=0x2ecc71
    )
    embed.add_field(name="💰 Each Received", value=f"{rewards['coins']} coins + {rewards['xp']} XP", inline=True)
    embed.add_field(name="👑 Host Received", value=f"{host_rewards['coins']} coins + {host_rewards['xp']} XP", inline=True)
    
    if streak_bonuses:
        embed.add_field(name="🔥 Streak Bonuses", value="\n".join(streak_bonuses[:5]), inline=False)
    
    if role_rewards:
        embed.add_field(name="🎉 Role Rewards Earned!", value="\n".join(role_rewards[:5]), inline=False)
    
    await ctx.send(embed=embed)

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
@commands.cooldown(1, 30, commands.BucketType.guild)  # Once per 30 seconds per server
async def serverstats(ctx):
    """Display comprehensive server statistics with image"""
    # Try to generate image first
    if PIL_AVAILABLE:
        try:
            stats_image = await create_server_stats_image(ctx.guild)
            if stats_image:
                file = discord.File(stats_image, filename="serverstats.png")
                await ctx.send(file=file)
                return
        except Exception as e:
            print(f"Server stats image error: {e}")
    
    # Fallback to embed
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
@commands.cooldown(1, 10, commands.BucketType.user)  # Once per 10 seconds per user
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

# ==========================================
# NEW VISUAL COMMANDS
# ==========================================

@bot.hybrid_command(name="profile", description="View your detailed profile card")
@commands.cooldown(1, 15, commands.BucketType.user)  # 1 use per 15 seconds per user
async def profile(ctx, member: discord.Member = None):
    """Display a beautiful profile card with all stats"""
    target = member or ctx.author
    user_data = get_user_data(target.id)
    rank = get_level_rank(target.id)
    achievements = check_achievements(user_data)
    
    if PIL_AVAILABLE:
        try:
            profile_card = await create_profile_card(target, user_data, rank, achievements)
            if profile_card:
                file = discord.File(profile_card, filename="profile.png")
                await ctx.send(file=file)
                return
        except Exception as e:
            print(f"Profile card error: {e}")
    
    # Fallback to embed
    embed = discord.Embed(title=f"{target.display_name}'s Profile", color=0x8B0000)
    embed.set_thumbnail(url=target.display_avatar.url)
    embed.add_field(name="Level", value=user_data.get('level', 0), inline=True)
    embed.add_field(name="XP", value=format_number(user_data.get('xp', 0)), inline=True)
    embed.add_field(name="Rank", value=f"#{rank}", inline=True)
    embed.add_field(name="Coins", value=format_number(user_data.get('coins', 0)), inline=True)
    embed.add_field(name="Wins", value=user_data.get('wins', 0), inline=True)
    embed.add_field(name="Losses", value=user_data.get('losses', 0), inline=True)
    await ctx.send(embed=embed)

@bot.hybrid_command(name="rank", description="View your rank card")
@commands.cooldown(1, 10, commands.BucketType.user)  # Once per 10 seconds per user
async def rank_cmd(ctx, member: discord.Member = None):
    """Display your rank card (same as level command)"""
    target = member or ctx.author
    user_data = get_user_data(target.id)
    rank = get_level_rank(target.id)
    
    if PIL_AVAILABLE:
        try:
            card_image = await create_level_card_image(target, user_data, rank)
            if card_image:
                file = discord.File(card_image, filename="rank_card.png")
                await ctx.send(file=file)
                return
        except Exception as e:
            print(f"Rank card error: {e}")
    
    embed = create_arcane_level_embed(target, user_data, rank)
    await ctx.send(embed=embed)

@bot.hybrid_command(name="achievements", description="View your achievements")
@commands.cooldown(1, 15, commands.BucketType.user)  # Once per 15 seconds per user
async def achievements_cmd(ctx, member: discord.Member = None):
    """Display all achievements and progress"""
    target = member or ctx.author
    user_data = get_user_data(target.id)
    achievements = check_achievements(user_data)
    
    unlocked = [a for a in achievements if a['unlocked']]
    locked = [a for a in achievements if not a['unlocked']]
    
    embed = discord.Embed(
        title=f"🏆 {target.display_name}'s Achievements",
        description=f"**{len(unlocked)}/{len(achievements)}** achievements unlocked",
        color=0xFFD700
    )
    embed.set_thumbnail(url=target.display_avatar.url)
    
    # Unlocked achievements
    if unlocked:
        unlocked_text = "\n".join([f"{a['icon']} **{a['name']}** - {a['desc']}" for a in unlocked[:10]])
        embed.add_field(name="✅ Unlocked", value=unlocked_text or "None", inline=False)
    
    # Locked achievements (show progress)
    if locked:
        locked_text = "\n".join([f"🔒 **{a['name']}** - {a['progress']}/{a['requirement']}" for a in locked[:5]])
        embed.add_field(name="🔒 In Progress", value=locked_text or "All unlocked!", inline=False)
    
    await ctx.send(embed=embed)

@bot.hybrid_command(name="activity", description="View your activity graph")
async def activity_cmd(ctx, member: discord.Member = None):
    """Display an activity graph"""
    target = member or ctx.author
    user_data = get_user_data(target.id)
    
    if PIL_AVAILABLE:
        try:
            graph = await create_activity_graph(target, user_data)
            if graph:
                file = discord.File(graph, filename="activity.png")
                await ctx.send(file=file)
                return
        except Exception as e:
            print(f"Activity graph error: {e}")
    
    # Fallback
    embed = discord.Embed(
        title=f"📊 {target.display_name}'s Activity",
        description="Activity tracking requires image generation.\nYour recent stats:",
        color=0x3498db
    )
    embed.add_field(name="Weekly XP", value=format_number(user_data.get('weekly_xp', 0)), inline=True)
    embed.add_field(name="Monthly XP", value=format_number(user_data.get('monthly_xp', 0)), inline=True)
    await ctx.send(embed=embed)

@bot.hybrid_command(name="raid_history", description="View raid history")
async def raid_history_cmd(ctx):
    """Display recent raid history"""
    history = load_raid_history()
    raids = history.get("raids", [])[-10:]  # Last 10 raids
    
    if not raids:
        return await ctx.send("📜 No raid history yet!")
    
    embed = discord.Embed(
        title="🏴‍☠️ Raid History",
        description="Last 10 raids:",
        color=0x8B0000
    )
    
    for raid in reversed(raids):
        result_emoji = "✅" if raid['result'] == "win" else "❌"
        date = raid.get('date', 'Unknown')[:10]
        participants = len(raid.get('participants', []))
        xp = raid.get('xp_gained', 0)
        
        embed.add_field(
            name=f"{result_emoji} vs {raid['target']}",
            value=f"📅 {date} | 👥 {participants} | +{xp} XP",
            inline=False
        )
    
    # Stats summary
    total_raids = len(history.get("raids", []))
    wins = sum(1 for r in history.get("raids", []) if r['result'] == "win")
    winrate = round((wins / total_raids) * 100, 1) if total_raids > 0 else 0
    
    embed.set_footer(text=f"Total: {total_raids} raids | Win Rate: {winrate}%")
    await ctx.send(embed=embed)

# ==========================================
# TOURNAMENT BRACKET COMMANDS
# ==========================================
# DUEL & ELO COMMANDS
# ==========================================

class DuelModal(discord.ui.Modal, title="Challenge to Duel"):
    ps_link = discord.ui.TextInput(
        label="Roblox Private Server Link",
        placeholder="https://www.roblox.com/games/...",
        required=True,
        style=discord.TextStyle.short
    )
    
    def __init__(self, opponent):
        super().__init__()
        self.opponent = opponent
    
    async def on_submit(self, interaction: discord.Interaction):
        # Validate link
        link = self.ps_link.value
        if "roblox.com" not in link.lower():
            return await interaction.response.send_message("❌ Please provide a valid Roblox private server link!", ephemeral=True)
        
        # Check cooldowns (anti-spam)
        # Create the duel
        duel_id = create_pending_duel(interaction.user.id, self.opponent.id, link)
        
        challenger_elo = get_elo(interaction.user.id)
        opponent_elo = get_elo(self.opponent.id)
        challenger_tier, _ = get_elo_tier(challenger_elo)
        opponent_tier, _ = get_elo_tier(opponent_elo)
        
        embed = discord.Embed(
            title="⚔️ DUEL CHALLENGE ⚔️",
            description=(
                f"**{interaction.user.display_name}** challenges **{self.opponent.display_name}**!\n\n"
                f"**ELO Ratings:**\n"
                f"• {interaction.user.mention}: **{challenger_elo}** {challenger_tier}\n"
                f"• {self.opponent.mention}: **{opponent_elo}** {opponent_tier}\n\n"
                f"🎮 **Private Server:** [Click Here]({link})\n\n"
                f"{self.opponent.mention}, do you accept this challenge?"
            ),
            color=0xe74c3c
        )
        embed.set_footer(text=f"Duel ID: {duel_id} • Expires in 5 minutes")
        
        await interaction.response.send_message(
            content=self.opponent.mention,
            embed=embed,
            view=DuelRequestView(duel_id, interaction.user, self.opponent, link)
        )

@bot.hybrid_command(name="duel", description="Challenge another player to a 1v1 duel")
@commands.cooldown(1, 60, commands.BucketType.user)  # 1 duel request per minute
async def duel_cmd(ctx, opponent: discord.Member):
    """Challenge another player to a duel"""
    if opponent.bot:
        return await ctx.send("❌ You can't duel a bot!", ephemeral=True)
    
    if opponent.id == ctx.author.id:
        return await ctx.send("❌ You can't duel yourself!", ephemeral=True)
    
    # Show modal for PS link
    await ctx.interaction.response.send_modal(DuelModal(opponent))

@bot.hybrid_command(name="elo", description="Check ELO rating")
@commands.cooldown(1, 5, commands.BucketType.user)
async def elo_cmd(ctx, member: discord.Member = None):
    """Check your or another player's ELO rating"""
    target = member or ctx.author
    
    elo = get_elo(target.id)
    tier, color = get_elo_tier(elo)
    
    # Get win/loss from history
    history = get_duel_history(target.id, 100)
    wins = sum(1 for d in history if d["winner"] == str(target.id))
    losses = len(history) - wins
    
    embed = discord.Embed(
        title=f"⚔️ {target.display_name}'s ELO",
        color=discord.Color.from_rgb(*color)
    )
    embed.add_field(name="Rating", value=f"**{elo}**", inline=True)
    embed.add_field(name="Rank", value=tier, inline=True)
    embed.add_field(name="Record", value=f"{wins}W - {losses}L", inline=True)
    embed.set_thumbnail(url=target.display_avatar.url)
    
    await ctx.send(embed=embed)

@bot.hybrid_command(name="elo_leaderboard", description="View the ELO leaderboard")
@commands.cooldown(1, 15, commands.BucketType.user)
async def elo_leaderboard_cmd(ctx):
    """View the top ELO players"""
    top_players = get_elo_leaderboard(10)
    
    if not top_players:
        return await ctx.send("❌ No ELO data yet! Challenge someone with `/duel`")
    
    embed = discord.Embed(
        title="🏆 ELO Leaderboard",
        color=0xFFD700
    )
    
    desc = ""
    for i, (uid, elo) in enumerate(top_players):
        member = ctx.guild.get_member(int(uid))
        name = member.display_name if member else f"User {uid[:8]}"
        tier, _ = get_elo_tier(elo)
        
        medal = ["🥇", "🥈", "🥉"][i] if i < 3 else f"#{i+1}"
        desc += f"{medal} **{name}** - {elo} {tier}\n"
    
    embed.description = desc
    embed.set_footer(text="Challenge others with /duel to climb!")
    
    await ctx.send(embed=embed)

@bot.hybrid_command(name="duel_history", description="View your duel history")
@commands.cooldown(1, 10, commands.BucketType.user)
async def duel_history_cmd(ctx, member: discord.Member = None):
    """View duel history"""
    target = member or ctx.author
    
    history = get_duel_history(target.id, 10)
    
    if not history:
        return await ctx.send(f"❌ {target.display_name} has no duel history yet!")
    
    embed = discord.Embed(
        title=f"⚔️ {target.display_name}'s Duel History",
        color=0x3498db
    )
    
    desc = ""
    for duel in history:
        won = duel["winner"] == str(target.id)
        opponent_id = duel["loser"] if won else duel["winner"]
        opponent = ctx.guild.get_member(int(opponent_id))
        opponent_name = opponent.display_name if opponent else f"User {opponent_id[:8]}"
        
        if won:
            elo_change = f"+{duel['winner_elo_after'] - duel['winner_elo_before']}"
            desc += f"✅ **Won** vs {opponent_name} ({elo_change})\n"
        else:
            elo_change = f"{duel['loser_elo_after'] - duel['loser_elo_before']}"
            desc += f"❌ **Lost** vs {opponent_name} ({elo_change})\n"
    
    embed.description = desc
    await ctx.send(embed=embed)

@bot.hybrid_command(name="elo_reset", description="Admin: Reset all ELO ratings")
@commands.has_permissions(administrator=True)
async def elo_reset_cmd(ctx, confirm: str = None):
    """Reset all ELO ratings"""
    if confirm != "confirm":
        return await ctx.send("⚠️ This will reset ALL ELO ratings! Use `/elo_reset confirm` to confirm.", ephemeral=True)
    
    data = load_duels_data()
    data["elo"] = {}
    save_duels_data(data)
    
    await ctx.send("✅ All ELO ratings have been reset!")

# ==========================================
# TOURNAMENT COMMANDS (Panel-based)
# ==========================================

@bot.hybrid_command(name="tournament_create", description="Admin: Create a new tournament")
@commands.has_permissions(administrator=True)
async def tournament_create_cmd(ctx, name: str, channel: discord.TextChannel = None):
    """Create a new tournament with signup panel"""
    tournament = create_tournament(name, ctx.author.id)
    
    if not tournament:
        return await ctx.send("❌ There's already an active tournament! End it first.", ephemeral=True)
    
    target_channel = channel or ctx.channel
    
    embed = discord.Embed(
        title=f"🏆 {name}",
        description=(
            f"**Status:** 🟢 Signups Open\n"
            f"**Participants:** 0\n\n"
            f"Click **Join Tournament** to participate!"
        ),
        color=0xFFD700
    )
    embed.set_footer(text="✝ The Fallen ✝ • Tournament System")
    
    await target_channel.send(embed=embed, view=TournamentPanelView())
    await ctx.send(f"✅ Tournament **{name}** created in {target_channel.mention}!", ephemeral=True)

@bot.hybrid_command(name="tournament_panel", description="Admin: Post tournament panel")
@commands.has_permissions(administrator=True)
async def tournament_panel_cmd(ctx):
    """Post the tournament panel"""
    tournament = get_active_tournament()
    
    if not tournament:
        return await ctx.send("❌ No active tournament! Create one with `/tournament_create`", ephemeral=True)
    
    participant_count = len(tournament["participants"])
    status_text = {
        "signup": "🟢 Signups Open",
        "active": "🔴 In Progress",
        "complete": "🏆 Complete"
    }.get(tournament["status"], "Unknown")
    
    embed = discord.Embed(
        title=f"🏆 {tournament['name']}",
        description=(
            f"**Status:** {status_text}\n"
            f"**Participants:** {participant_count}\n\n"
            f"Click **Join Tournament** to participate!"
        ),
        color=0xFFD700
    )
    embed.set_footer(text="✝ The Fallen ✝ • Tournament System")
    
    await ctx.send(embed=embed, view=TournamentPanelView())

@bot.hybrid_command(name="tournament_end", description="Admin: End the current tournament")
@commands.has_permissions(administrator=True)
async def tournament_end_cmd(ctx, confirm: str = None):
    """End the current tournament"""
    if confirm != "confirm":
        return await ctx.send("⚠️ Use `/tournament_end confirm` to confirm.", ephemeral=True)
    
    if end_tournament():
        await ctx.send("✅ Tournament ended!")
    else:
        await ctx.send("❌ No active tournament!", ephemeral=True)

# ==========================================
# INACTIVITY COMMANDS
# ==========================================

@bot.hybrid_command(name="inactivity_check", description="Staff: Run inactivity check on all ranked members")
@commands.has_any_role(*HIGH_STAFF_ROLES, STAFF_ROLE_NAME)
@commands.cooldown(1, 300, commands.BucketType.guild)  # Once per 5 minutes per server
async def inactivity_check_cmd(ctx):
    """Run inactivity check and give strikes to inactive ranked members"""
    await ctx.defer()
    
    # Add delay between each member check to avoid rate limits
    results = await run_inactivity_check(ctx.guild)
    
    embed = discord.Embed(
        title="📊 Inactivity Check Results",
        color=0xe67e22,
        timestamp=datetime.datetime.now(datetime.timezone.utc)
    )
    
    embed.add_field(name="👥 Members Checked", value=str(results["checked"]), inline=True)
    embed.add_field(name="⚠️ Strikes Given", value=str(results["strikes_given"]), inline=True)
    embed.add_field(name="📉 Demotions", value=str(results["demotions"]), inline=True)
    embed.add_field(name="👢 Kicks", value=str(results["kicks"]), inline=True)
    
    if results["details"]:
        details_text = ""
        for detail in results["details"][:10]:  # Show first 10
            action_emoji = {"strike": "⚠️", "demoted": "📉", "kicked": "👢", "kick_failed": "❌"}.get(detail["action"], "❓")
            rank_info = ""
            if detail.get("new_rank"):
                rank_info = f" → {detail['new_rank']}"
            details_text += f"{action_emoji} {detail['member'].mention} - Strike {detail['strikes']}/5 ({detail['days_inactive']}d){rank_info}\n"
        
        embed.add_field(name="📋 Details", value=details_text or "None", inline=False)
    
    embed.set_footer(text=f"Threshold: {INACTIVITY_CHECK_DAYS} days")
    await ctx.send(embed=embed)
    
    # Log to dashboard
    await log_to_dashboard(
        ctx.guild, "📊 INACTIVITY", "Inactivity Check Ran",
        f"Checked {results['checked']} ranked members\n{results['strikes_given']} strikes given",
        color=0xe67e22,
        fields={"Demotions": str(results["demotions"]), "Kicks": str(results["kicks"]), "By": ctx.author.mention}
    )

@bot.hybrid_command(name="inactivity_strikes", description="Check inactivity strikes for a user")
async def inactivity_strikes_cmd(ctx, member: discord.Member = None):
    """Check inactivity strikes for a user"""
    target = member or ctx.author
    
    # Staff can check anyone, members can only check themselves
    if member and member != ctx.author and not is_staff(ctx.author):
        return await ctx.send("❌ You can only check your own strikes.", ephemeral=True)
    
    strike_info = get_inactivity_strikes(target.id)
    user_data = get_user_data(target.id)
    
    # Get current rank
    current_rank = get_member_rank(target)
    
    # Calculate days since last active
    last_active = user_data.get("last_active")
    if last_active:
        try:
            last_active_date = datetime.datetime.fromisoformat(last_active.replace('Z', '+00:00'))
            days_ago = (datetime.datetime.now(datetime.timezone.utc) - last_active_date).days
            last_active_text = f"<t:{int(last_active_date.timestamp())}:R>"
        except:
            last_active_text = "Unknown"
            days_ago = 0
    else:
        last_active_text = "Never recorded"
        days_ago = 0
    
    embed = discord.Embed(
        title=f"📊 Inactivity Status - {target.display_name}",
        color=0xe74c3c if strike_info["count"] >= 3 else 0xf1c40f if strike_info["count"] > 0 else 0x2ecc71
    )
    
    # Current rank
    if current_rank:
        embed.add_field(name="🏅 Current Rank", value=current_rank, inline=False)
    
    # Strike visual
    strikes_visual = "🔴" * strike_info["count"] + "⚫" * (MAX_INACTIVITY_STRIKES - strike_info["count"])
    embed.add_field(name="⚠️ Strikes", value=f"{strikes_visual}\n**{strike_info['count']}/{MAX_INACTIVITY_STRIKES}**", inline=True)
    embed.add_field(name="📅 Last Active", value=last_active_text, inline=True)
    
    # Next demotion rank
    if current_rank:
        next_rank = get_next_demotion_rank(current_rank)
        if next_rank:
            embed.add_field(name="📉 Next Demotion", value=next_rank, inline=True)
        else:
            embed.add_field(name="📉 Next Demotion", value="At lowest rank", inline=True)
    
    # Warning levels
    if strike_info["count"] == 0:
        status = "✅ Good standing"
    elif strike_info["count"] < 3:
        status = "⚠️ Warning - Stay active!"
    elif strike_info["count"] < 5:
        status = "🚨 Critical - At risk of demotion/kick!"
    else:
        status = "⛔ Maximum strikes reached"
    
    embed.add_field(name="Status", value=status, inline=False)
    
    # Strike history (last 5)
    if strike_info.get("history"):
        history_text = ""
        for h in strike_info["history"][-5:]:
            try:
                date = datetime.datetime.fromisoformat(h["date"].replace('Z', '+00:00'))
                history_text += f"• <t:{int(date.timestamp())}:d> - {h['reason']}\n"
            except:
                history_text += f"• {h['reason']}\n"
        embed.add_field(name="📜 Recent History", value=history_text or "None", inline=False)
    
    embed.set_thumbnail(url=target.display_avatar.url)
    embed.set_footer(text=f"Threshold: {INACTIVITY_CHECK_DAYS} days of inactivity")
    
    await ctx.send(embed=embed)

@bot.hybrid_command(name="add_inactivity_strike", description="Staff: Add an inactivity strike to a user")
@commands.has_any_role(*HIGH_STAFF_ROLES, STAFF_ROLE_NAME)
async def add_inactivity_strike_cmd(ctx, member: discord.Member, reason: str = "Manual strike by staff"):
    """Manually add an inactivity strike"""
    new_count = add_inactivity_strike(member.id, reason)
    
    # Check if demotion needed
    demoted = False
    kicked = False
    
    if new_count >= 3:
        strike_info = get_inactivity_strikes(member.id)
        if not strike_info.get("demoted"):
            mainer_role = discord.utils.get(ctx.guild.roles, name=REQUIRED_ROLE_NAME)
            tryout_role = discord.utils.get(ctx.guild.roles, name="Tryout")
            
            if mainer_role and mainer_role in member.roles:
                try:
                    await member.remove_roles(mainer_role)
                    if tryout_role:
                        await member.add_roles(tryout_role)
                    mark_user_demoted(member.id)
                    demoted = True
                except:
                    pass
    
    if new_count >= MAX_INACTIVITY_STRIKES:
        try:
            await send_inactivity_strike_dm(member, new_count, kicked=True)
            await member.kick(reason=f"Inactivity: {MAX_INACTIVITY_STRIKES} strikes")
            kicked = True
        except:
            pass
    else:
        await send_inactivity_strike_dm(member, new_count, demoted=demoted)
    
    # Response
    action_text = ""
    if kicked:
        action_text = " → **KICKED**"
    elif demoted:
        action_text = " → **DEMOTED to Tryout**"
    
    embed = discord.Embed(
        title="⚠️ Inactivity Strike Added",
        description=f"{member.mention} now has **{new_count}/{MAX_INACTIVITY_STRIKES}** strikes{action_text}",
        color=0xe74c3c if new_count >= 3 else 0xf1c40f
    )
    embed.add_field(name="Reason", value=reason, inline=False)
    embed.set_footer(text=f"Added by {ctx.author}")
    
    await ctx.send(embed=embed)
    
    # Log
    await log_to_dashboard(
        ctx.guild, "⚠️ STRIKE", "Inactivity Strike Added",
        f"{member.mention} received strike {new_count}/5\nReason: {reason}",
        color=0xe67e22,
        fields={"By": ctx.author.mention, "Action": action_text or "None"}
    )

@bot.hybrid_command(name="remove_inactivity_strike", description="Staff: Remove an inactivity strike from a user")
@commands.has_any_role(*HIGH_STAFF_ROLES, STAFF_ROLE_NAME)
async def remove_inactivity_strike_cmd(ctx, member: discord.Member):
    """Remove one inactivity strike from a user"""
    new_count = remove_inactivity_strike(member.id)
    
    embed = discord.Embed(
        title="✅ Inactivity Strike Removed",
        description=f"{member.mention} now has **{new_count}/{MAX_INACTIVITY_STRIKES}** strikes",
        color=0x2ecc71
    )
    embed.set_footer(text=f"Removed by {ctx.author}")
    
    await ctx.send(embed=embed)
    
    # Try to DM user
    try:
        dm_embed = discord.Embed(
            title="✅ Inactivity Strike Removed",
            description=f"One of your inactivity strikes has been removed!\n\nYou now have **{new_count}/{MAX_INACTIVITY_STRIKES}** strikes.",
            color=0x2ecc71
        )
        dm_embed.set_footer(text="✝ The Fallen ✝ • Keep up the activity!")
        await member.send(embed=dm_embed)
    except:
        pass

@bot.hybrid_command(name="clear_inactivity_strikes", description="Admin: Clear all inactivity strikes from a user")
@commands.has_any_role(*HIGH_STAFF_ROLES)
async def clear_inactivity_strikes_cmd(ctx, member: discord.Member):
    """Clear all inactivity strikes from a user"""
    clear_inactivity_strikes(member.id)
    
    embed = discord.Embed(
        title="✅ All Strikes Cleared",
        description=f"{member.mention}'s inactivity strikes have been cleared.",
        color=0x2ecc71
    )
    embed.set_footer(text=f"Cleared by {ctx.author}")
    
    await ctx.send(embed=embed)
    
    # Try to DM user
    try:
        dm_embed = discord.Embed(
            title="✅ Inactivity Strikes Cleared",
            description="All of your inactivity strikes have been cleared!\n\nYou now have **0/5** strikes.",
            color=0x2ecc71
        )
        dm_embed.set_footer(text="✝ The Fallen ✝ • Fresh start!")
        await member.send(embed=dm_embed)
    except:
        pass
    
    # Log
    await log_to_dashboard(
        ctx.guild, "✅ CLEARED", "Inactivity Strikes Cleared",
        f"{member.mention}'s strikes were cleared",
        color=0x2ecc71,
        fields={"By": ctx.author.mention}
    )

@bot.hybrid_command(name="inactive_list", description="Staff: Show all members with inactivity strikes")
@commands.has_any_role(*HIGH_STAFF_ROLES, STAFF_ROLE_NAME)
async def inactive_list_cmd(ctx):
    """Show all members with inactivity strikes"""
    data = load_inactivity_data()
    
    striked_users = [(uid, info) for uid, info in data.get("strikes", {}).items() if info.get("count", 0) > 0]
    striked_users.sort(key=lambda x: x[1]["count"], reverse=True)
    
    if not striked_users:
        return await ctx.send("✅ No members have inactivity strikes!")
    
    embed = discord.Embed(
        title="📋 Inactivity Strike List",
        color=0xe67e22,
        timestamp=datetime.datetime.now(datetime.timezone.utc)
    )
    
    description = ""
    for uid, info in striked_users[:20]:  # Show top 20
        member = ctx.guild.get_member(int(uid))
        name = member.display_name if member else f"Unknown ({uid})"
        strikes = info["count"]
        demoted = "📉" if info.get("demoted") else ""
        
        strike_bar = "🔴" * strikes + "⚫" * (5 - strikes)
        description += f"{demoted}{name}: {strike_bar} ({strikes}/5)\n"
    
    embed.description = description
    
    # Summary
    total_striked = len(striked_users)
    critical = sum(1 for _, info in striked_users if info["count"] >= 3)
    embed.set_footer(text=f"Total: {total_striked} members with strikes | {critical} critical (3+ strikes)")
    
    await ctx.send(embed=embed)

@bot.hybrid_command(name="set_inactivity_days", description="Admin: Set days of inactivity before strike")
@commands.has_permissions(administrator=True)
async def set_inactivity_days(ctx, days: int):
    """Set the number of days before inactivity strike"""
    global INACTIVITY_CHECK_DAYS
    
    if days < 1 or days > 90:
        return await ctx.send("❌ Days must be between 1 and 90.", ephemeral=True)
    
    INACTIVITY_CHECK_DAYS = days
    await ctx.send(f"✅ Inactivity threshold set to **{days} days**.", ephemeral=True)

@bot.hybrid_command(name="immunity_add", description="Staff: Give inactivity immunity to a member")
@commands.has_any_role(*HIGH_STAFF_ROLES, STAFF_ROLE_NAME)
async def immunity_add(ctx, member: discord.Member, *, reason: str = "No reason provided"):
    """Give a member immunity from inactivity checks"""
    immunity_role = discord.utils.get(ctx.guild.roles, name=INACTIVITY_IMMUNITY_ROLE)
    
    if not immunity_role:
        return await ctx.send(
            f"❌ Role **{INACTIVITY_IMMUNITY_ROLE}** not found!\n"
            f"Please create a role named exactly `{INACTIVITY_IMMUNITY_ROLE}`",
            ephemeral=True
        )
    
    if immunity_role in member.roles:
        return await ctx.send(f"❌ {member.mention} already has immunity!", ephemeral=True)
    
    try:
        await member.add_roles(immunity_role)
        
        embed = discord.Embed(
            title="🛡️ Inactivity Immunity Granted",
            description=f"{member.mention} is now **immune** to inactivity checks.",
            color=0x2ecc71
        )
        embed.add_field(name="📝 Reason", value=reason, inline=False)
        embed.add_field(name="👤 Granted by", value=ctx.author.mention, inline=True)
        embed.set_footer(text="Use /immunity_remove when they return")
        
        await ctx.send(embed=embed)
        await log_action(ctx.guild, "🛡️ Immunity Granted", f"{member.mention} given immunity by {ctx.author.mention}\nReason: {reason}", 0x2ecc71)
        
        # DM the member
        try:
            dm_embed = discord.Embed(
                title="🛡️ Inactivity Immunity Granted",
                description=(
                    f"You've been given **inactivity immunity** in **{ctx.guild.name}**.\n\n"
                    f"**Reason:** {reason}\n\n"
                    f"This means you won't receive inactivity strikes while you're away.\n"
                    f"When you return, please let staff know so they can remove the immunity."
                ),
                color=0x2ecc71
            )
            await member.send(embed=dm_embed)
        except:
            pass  # DMs might be closed
            
    except discord.Forbidden:
        await ctx.send("❌ I don't have permission to add that role.", ephemeral=True)

@bot.hybrid_command(name="immunity_remove", description="Staff: Remove inactivity immunity from a member")
@commands.has_any_role(*HIGH_STAFF_ROLES, STAFF_ROLE_NAME)
async def immunity_remove(ctx, member: discord.Member):
    """Remove immunity from a member"""
    immunity_role = discord.utils.get(ctx.guild.roles, name=INACTIVITY_IMMUNITY_ROLE)
    
    if not immunity_role:
        return await ctx.send(f"❌ Role **{INACTIVITY_IMMUNITY_ROLE}** not found!", ephemeral=True)
    
    if immunity_role not in member.roles:
        return await ctx.send(f"❌ {member.mention} doesn't have immunity!", ephemeral=True)
    
    try:
        await member.remove_roles(immunity_role)
        
        # Reset their activity timestamp so they don't get immediately striked
        reset_member_activity(member.id)
        
        embed = discord.Embed(
            title="🛡️ Inactivity Immunity Removed",
            description=f"{member.mention}'s immunity has been **removed**.\n\nTheir activity timer has been reset.",
            color=0xe74c3c
        )
        embed.add_field(name="👤 Removed by", value=ctx.author.mention, inline=True)
        
        await ctx.send(embed=embed)
        await log_action(ctx.guild, "🛡️ Immunity Removed", f"{member.mention} immunity removed by {ctx.author.mention}", 0xe74c3c)
        
    except discord.Forbidden:
        await ctx.send("❌ I don't have permission to remove that role.", ephemeral=True)

@bot.hybrid_command(name="immunity_list", description="Staff: View all members with inactivity immunity")
@commands.has_any_role(*HIGH_STAFF_ROLES, STAFF_ROLE_NAME)
async def immunity_list(ctx):
    """View all members with immunity"""
    immunity_role = discord.utils.get(ctx.guild.roles, name=INACTIVITY_IMMUNITY_ROLE)
    
    if not immunity_role:
        return await ctx.send(f"❌ Role **{INACTIVITY_IMMUNITY_ROLE}** not found!", ephemeral=True)
    
    members_with_immunity = [m for m in ctx.guild.members if immunity_role in m.roles]
    
    if not members_with_immunity:
        embed = discord.Embed(
            title="🛡️ Immunity List",
            description="No members currently have inactivity immunity.",
            color=0x95a5a6
        )
    else:
        member_list = "\n".join([f"• {m.mention} ({m.display_name})" for m in members_with_immunity[:20]])
        if len(members_with_immunity) > 20:
            member_list += f"\n... and {len(members_with_immunity) - 20} more"
        
        embed = discord.Embed(
            title="🛡️ Immunity List",
            description=f"**{len(members_with_immunity)} members** have inactivity immunity:\n\n{member_list}",
            color=0x3498db
        )
    
    embed.set_footer(text="Use /immunity_remove @user to remove immunity")
    await ctx.send(embed=embed)

@bot.hybrid_command(name="db_status", description="Admin: Check database status")
@commands.has_permissions(administrator=True)
async def db_status(ctx):
    """Check database connection status"""
    embed = discord.Embed(title="📊 Database Status", color=0x3498db)
    
    embed.add_field(
        name="PostgreSQL",
        value=f"{'✅ Connected' if db_pool else '❌ Not connected'}\n{'Available' if POSTGRES_AVAILABLE else 'Not installed'}",
        inline=True
    )
    embed.add_field(
        name="Storage Mode",
        value="PostgreSQL" if db_pool else "JSON Files",
        inline=True
    )
    embed.add_field(
        name="DATABASE_URL",
        value="✅ Set" if DATABASE_URL else "❌ Not set",
        inline=True
    )
    
    # Count users
    data = load_data()
    user_count = len(data.get("users", {}))
    embed.add_field(name="Users", value=str(user_count), inline=True)
    
    await ctx.send(embed=embed)

@bot.hybrid_command(name="setup_logs", description="Admin: Setup the logging dashboard channel")
@commands.has_permissions(administrator=True)
async def setup_logs(ctx):
    """Create the logging dashboard channel"""
    # Check if channel exists
    existing = discord.utils.get(ctx.guild.text_channels, name=LOG_CHANNEL_NAME)
    if existing:
        return await ctx.send(f"✅ Log channel already exists: {existing.mention}", ephemeral=True)
    
    # Create the channel
    overwrites = {
        ctx.guild.default_role: discord.PermissionOverwrite(read_messages=False),
        ctx.guild.me: discord.PermissionOverwrite(read_messages=True, send_messages=True)
    }
    
    # Add staff access
    staff_role = discord.utils.get(ctx.guild.roles, name=STAFF_ROLE_NAME)
    if staff_role:
        overwrites[staff_role] = discord.PermissionOverwrite(read_messages=True)
    
    for role_name in HIGH_STAFF_ROLES:
        role = discord.utils.get(ctx.guild.roles, name=role_name)
        if role:
            overwrites[role] = discord.PermissionOverwrite(read_messages=True)
    
    channel = await ctx.guild.create_text_channel(
        name=LOG_CHANNEL_NAME,
        overwrites=overwrites,
        topic="📋 Fallen Bot Logging Dashboard - All bot activities are logged here"
    )
    
    # Send welcome message
    embed = discord.Embed(
        title="📋 Logging Dashboard",
        description="All bot activities will be logged here:\n\n• Member joins/leaves\n• Moderation actions\n• Level ups\n• Raid results\n• And more...",
        color=0x8B0000
    )
    await channel.send(embed=embed)
    await ctx.send(f"✅ Created logging channel: {channel.mention}", ephemeral=True)

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

# Global error handler for rate limits
@bot.event
async def on_command_error(ctx, error):
    """Handle command errors gracefully"""
    if isinstance(error, commands.CommandOnCooldown):
        await ctx.send(f"⏰ Command on cooldown. Try again in {error.retry_after:.1f}s", ephemeral=True)
    elif isinstance(error, commands.MissingPermissions):
        await ctx.send("❌ You don't have permission to use this command.", ephemeral=True)
    elif isinstance(error, commands.MissingRole):
        await ctx.send("❌ You don't have the required role.", ephemeral=True)
    elif isinstance(error, commands.MissingAnyRole):
        await ctx.send("❌ You don't have any of the required roles.", ephemeral=True)
    elif isinstance(error, commands.BotMissingPermissions):
        await ctx.send("❌ I don't have permission to do that.", ephemeral=True)
    elif "429" in str(error) or "rate limit" in str(error).lower():
        print(f"Rate limit hit: {error}")
        await asyncio.sleep(5)  # Wait before next action
    else:
        print(f"Command error: {error}")

@bot.tree.error
async def on_app_command_error(interaction: discord.Interaction, error):
    """Handle slash command errors"""
    if "429" in str(error) or "rate limit" in str(error).lower():
        print(f"Rate limit on slash command: {error}")
        try:
            await interaction.response.send_message("⏰ Please wait a moment and try again.", ephemeral=True)
        except:
            pass
    elif isinstance(error, discord.app_commands.errors.MissingPermissions):
        try:
            await interaction.response.send_message("❌ You don't have permission.", ephemeral=True)
        except:
            pass
    else:
        print(f"App command error: {error}")
        try:
            await interaction.response.send_message("❌ An error occurred. Please try again.", ephemeral=True)
        except:
            pass

# Run the bot with reconnect enabled
if __name__ == "__main__":
    import asyncio
    import time
    
    async def run_bot():
        """Run bot with automatic reconnection"""
        retry_count = 0
        max_retries = 5
        
        while retry_count < max_retries:
            try:
                print(f"Starting bot... (attempt {retry_count + 1})")
                await bot.start(TOKEN)
            except discord.errors.HTTPException as e:
                if e.status == 429:  # Rate limited
                    retry_after = getattr(e, 'retry_after', 60)
                    print(f"Rate limited! Waiting {retry_after} seconds...")
                    await asyncio.sleep(retry_after)
                    retry_count += 1
                else:
                    print(f"HTTP error: {e}")
                    await asyncio.sleep(30)
                    retry_count += 1
            except Exception as e:
                print(f"Bot error: {e}")
                await asyncio.sleep(30)
                retry_count += 1
            finally:
                if not bot.is_closed():
                    await bot.close()
        
        print("Max retries reached. Exiting.")
    
    # Run with asyncio
    try:
        asyncio.run(run_bot())
    except KeyboardInterrupt:
        print("Bot stopped by user.")
