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
WELCOME_CHANNEL_NAME = "╰・welcome"  # Welcome channel for new members

# --- DATA FILES ---
RECURRING_EVENTS_FILE = "recurring_events.json"
TRANSCRIPTS_FILE = "ticket_transcripts.json"
PRACTICE_FILE = "practice_sessions.json"
LEGACY_FILE = "legacy_data.json"
EMBEDS_FILE = "custom_embeds.json"      

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
    # Original Items
    {"id": "private_tryout", "name": "⚔️ Private Tryout Ticket", "price": 500, "desc": "Opens a private channel with hosts.", "type": "ticket"},
    {"id": "custom_role", "name": "🎨 Custom Role Request", "price": 2000, "desc": "Request a custom colored role.", "type": "ticket"},
    
    # New Cosmetic Items
    {"id": "custom_role_color", "name": "🎨 Custom Role Color", "price": 1500, "desc": "Change your custom role's color.", "type": "ticket"},
    {"id": "hoisted_role", "name": "👑 Hoisted Role", "price": 5000, "desc": "Your role displays separately on the member list.", "type": "ticket"},
    {"id": "custom_level_bg", "name": "🖼️ Custom Level Card BG", "price": 3000, "desc": "Set a custom background for your level card.", "type": "background"},
    
    # Gameplay Items
    {"id": "elo_shield", "name": "🛡️ ELO Shield", "price": 1000, "desc": "Prevents ELO loss on your next duel loss.", "type": "consumable", "uses": 1},
    {"id": "streak_saver", "name": "🔥 Streak Saver", "price": 1500, "desc": "Protects your attendance streak once if you miss.", "type": "consumable", "uses": 1},
    {"id": "training_reserve", "name": "📋 Training Slot Reserve", "price": 300, "desc": "Reserve your spot in the next training.", "type": "consumable", "uses": 1},
    
    # Special Access
    {"id": "coaching_session", "name": "🎯 1v1 Coaching Session", "price": 1500, "desc": "Book a private training with a coach.", "type": "coaching"},
]

# Coaching role - members with this role can be selected for coaching
COACHING_ROLE = "Coach"

# Preset Level Card Backgrounds
LEVEL_CARD_BACKGROUNDS = {
    "default": None,  # Default gradient
    "fallen_dark": "https://i.imgur.com/dark_fallen_bg.png",
    "crimson": "https://i.imgur.com/crimson_bg.png",
    "shadow": "https://i.imgur.com/shadow_bg.png",
    "flames": "https://i.imgur.com/flames_bg.png",
    "galaxy": "https://i.imgur.com/galaxy_bg.png",
}

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
            # Main users table with ALL fields
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
                    training_attendance INTEGER DEFAULT 0,
                    tryout_attendance INTEGER DEFAULT 0,
                    tryout_passes INTEGER DEFAULT 0,
                    tryout_fails INTEGER DEFAULT 0,
                    events_hosted INTEGER DEFAULT 0,
                    daily_streak INTEGER DEFAULT 0,
                    last_daily TIMESTAMP,
                    weekly_xp INTEGER DEFAULT 0,
                    monthly_xp INTEGER DEFAULT 0,
                    voice_time INTEGER DEFAULT 0,
                    messages INTEGER DEFAULT 0,
                    verified BOOLEAN DEFAULT FALSE,
                    roblox_username TEXT,
                    roblox_id BIGINT,
                    last_active TIMESTAMP DEFAULT NOW(),
                    elo_shield_active BOOLEAN DEFAULT FALSE,
                    streak_saver_active BOOLEAN DEFAULT FALSE,
                    training_reserved BOOLEAN DEFAULT FALSE,
                    custom_level_bg TEXT,
                    inventory TEXT[] DEFAULT ARRAY[]::TEXT[],
                    warnings JSONB DEFAULT '[]'::JSONB,
                    achievements TEXT[] DEFAULT ARRAY[]::TEXT[],
                    activity_log JSONB DEFAULT '[]'::JSONB,
                    created_at TIMESTAMP DEFAULT NOW()
                )
            ''')
            
            # Add new columns if they don't exist (for existing databases)
            new_columns = [
                ("training_attendance", "INTEGER DEFAULT 0"),
                ("tryout_attendance", "INTEGER DEFAULT 0"),
                ("tryout_passes", "INTEGER DEFAULT 0"),
                ("tryout_fails", "INTEGER DEFAULT 0"),
                ("events_hosted", "INTEGER DEFAULT 0"),
                ("voice_time", "INTEGER DEFAULT 0"),
                ("last_active", "TIMESTAMP DEFAULT NOW()"),
                ("elo_shield_active", "BOOLEAN DEFAULT FALSE"),
                ("streak_saver_active", "BOOLEAN DEFAULT FALSE"),
                ("training_reserved", "BOOLEAN DEFAULT FALSE"),
                ("custom_level_bg", "TEXT"),
                ("inventory", "TEXT[] DEFAULT ARRAY[]::TEXT[]"),
            ]
            
            for col_name, col_type in new_columns:
                try:
                    await conn.execute(f'ALTER TABLE users ADD COLUMN IF NOT EXISTS {col_name} {col_type}')
                except:
                    pass
            
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
            
            # JSON data backup table (stores entire JSON blobs)
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS json_data (
                    key TEXT PRIMARY KEY,
                    data JSONB NOT NULL,
                    updated_at TIMESTAMP DEFAULT NOW()
                )
            ''')
            
            # ELO/Duels table
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS duels (
                    key TEXT PRIMARY KEY,
                    data JSONB NOT NULL,
                    updated_at TIMESTAMP DEFAULT NOW()
                )
            ''')
            
            # Events table
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS events (
                    key TEXT PRIMARY KEY,
                    data JSONB NOT NULL,
                    updated_at TIMESTAMP DEFAULT NOW()
                )
            ''')
            
            # Inactivity table
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS inactivity (
                    key TEXT PRIMARY KEY,
                    data JSONB NOT NULL,
                    updated_at TIMESTAMP DEFAULT NOW()
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

# --- JSON DATA MANAGEMENT (with PostgreSQL backup) ---

# In-memory cache to reduce database calls
_data_cache = None
_cache_time = None
CACHE_DURATION = 5  # seconds

def load_data():
    """Load data from PostgreSQL if available, otherwise JSON file"""
    global _data_cache, _cache_time
    
    # Check cache first
    if _data_cache and _cache_time and (datetime.datetime.now() - _cache_time).seconds < CACHE_DURATION:
        return _data_cache
    
    # Try to load from JSON file (local copy)
    if not os.path.exists(LEADERBOARD_FILE):
        data = {"roster": [None]*10, "theme": DEFAULT_THEME, "users": {}}
    else:
        with open(LEADERBOARD_FILE, "r") as f:
            try:
                data = json.load(f)
                if "users" not in data: data["users"] = {}
                if "roster" not in data: data["roster"] = [None]*10
                if "theme" not in data: data["theme"] = DEFAULT_THEME
            except Exception as e:
                print(f"Error loading data: {e}")
                data = {"roster": [None]*10, "theme": DEFAULT_THEME, "users": {}}
    
    _data_cache = data
    _cache_time = datetime.datetime.now()
    return data

def save_data(data):
    """Save data to JSON file and PostgreSQL if available"""
    global _data_cache, _cache_time
    
    # Always save to local JSON file
    with open(LEADERBOARD_FILE, "w") as f:
        json.dump(data, f, indent=4)
    
    # Update cache
    _data_cache = data
    _cache_time = datetime.datetime.now()
    
    # Also save to PostgreSQL in background if available
    if db_pool:
        asyncio.create_task(save_data_to_postgres(data))

async def save_data_to_postgres(data):
    """Save main data to PostgreSQL json_data table"""
    if not db_pool:
        return
    
    try:
        async with db_pool.acquire() as conn:
            await conn.execute('''
                INSERT INTO json_data (key, data, updated_at)
                VALUES ('main_data', $1, NOW())
                ON CONFLICT (key) DO UPDATE SET data = $1, updated_at = NOW()
            ''', json.dumps(data))
    except Exception as e:
        print(f"PostgreSQL save error: {e}")

async def load_data_from_postgres():
    """Load main data from PostgreSQL - use on startup to restore data"""
    if not db_pool:
        return None
    
    try:
        async with db_pool.acquire() as conn:
            row = await conn.fetchrow("SELECT data FROM json_data WHERE key = 'main_data'")
            if row:
                return json.loads(row['data'])
    except Exception as e:
        print(f"PostgreSQL load error: {e}")
    return None

async def sync_data_from_postgres():
    """Sync local JSON with PostgreSQL data on startup"""
    global _data_cache, _cache_time
    
    if not db_pool:
        return False
    
    pg_data = await load_data_from_postgres()
    if pg_data:
        # PostgreSQL has data - use it
        with open(LEADERBOARD_FILE, "w") as f:
            json.dump(pg_data, f, indent=4)
        _data_cache = pg_data
        _cache_time = datetime.datetime.now()
        print("✅ Data synced from PostgreSQL!")
        return True
    else:
        # No data in PostgreSQL - upload current JSON
        local_data = load_data()
        await save_data_to_postgres(local_data)
        print("✅ Local data uploaded to PostgreSQL!")
        return True

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
        "training_attendance": 0, "tryout_attendance": 0, "tryout_passes": 0, "tryout_fails": 0,
        "warnings": [], "last_daily": None, "daily_streak": 0,
        "last_active": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        # Inventory & Shop
        "inventory": [],  # List of owned item IDs
        "elo_shield_active": False,  # ELO shield protection
        "streak_saver_active": False,  # Streak protection
        "training_reserved": False,  # Training slot reserved
        "custom_level_bg": None,  # Custom level card background URL
        "events_hosted": 0,  # Number of events hosted
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

def add_coins(user_id, amount):
    """Add coins to a user"""
    data = load_data()
    uid = str(user_id)
    data = ensure_user_structure(data, uid)
    data["users"][uid]["coins"] = data["users"][uid].get("coins", 0) + amount
    save_data(data)
    return data["users"][uid]["coins"]

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


# --- TOP 10 LEADERBOARD IMAGE GENERATION ---
# New horizontal background: 1920x1080
# Layout: Left column (1-5), Center (TOP PLAYER), Right column (6-10)
LEADERBOARD_BG_FILE = "leaderboard_bg.png"

# CENTER - TOP PLAYER (Rank 1 featured prominently)
TOP_PLAYER_POSITION = {
    "avatar_center": (960, 232),  # Center of the large circle
    "avatar_size": 140,           # Diameter to fit the circular frame
    "name_y": 650                 # Below the kanji characters
}

# LEFT COLUMN - Ranks 1-5
# Format: (center_x, center_y, avatar_size)
LEADERBOARD_AVATAR_POSITIONS = {
    1: (175, 200, 70),    # Rank 1
    2: (175, 312, 70),    # Rank 2
    3: (175, 425, 70),    # Rank 3
    4: (175, 538, 70),    # Rank 4
    5: (175, 685, 70),    # Rank 5
    # RIGHT COLUMN - Ranks 6-10
    6: (1745, 200, 70),   # Rank 6
    7: (1745, 312, 70),   # Rank 7
    8: (1745, 425, 70),   # Rank 8
    9: (1745, 538, 70),   # Rank 9
    10: (1745, 685, 70),  # Rank 10
}

# Name positions - (x, y, alignment)
# Left column: left-aligned, Right column: right-aligned
LEADERBOARD_NAME_POSITIONS = {
    1: (230, 200, "left"),
    2: (230, 312, "left"),
    3: (230, 425, "left"),
    4: (230, 538, "left"),
    5: (230, 685, "left"),
    6: (1710, 200, "right"),
    7: (1710, 312, "right"),
    8: (1710, 425, "right"),
    9: (1710, 538, "right"),
    10: (1710, 685, "right"),
}


async def create_top10_leaderboard_image(guild):
    """
    Create the visual Top 10 leaderboard image with player avatars
    Uses 1920x1080 horizontal "The Fallen - Top 10" background
    Returns BytesIO buffer with PNG image
    """
    if not PIL_AVAILABLE:
        return None
    
    # Load background - check multiple paths
    bg_paths = [
        LEADERBOARD_BG_FILE,
        "leaderboardbg.png",
        f"/home/container/{LEADERBOARD_BG_FILE}",
        f"/home/container/leaderboardbg.png",
        f"assets/{LEADERBOARD_BG_FILE}",
    ]
    
    bg_img = None
    for path in bg_paths:
        try:
            bg_img = Image.open(path).convert("RGBA")
            print(f"✅ Loaded leaderboard background from: {path}")
            break
        except:
            continue
    
    if not bg_img:
        print("⚠️ Leaderboard background not found!")
        return None
    
    # Ensure correct size for the new background
    if bg_img.size != (1920, 1080):
        bg_img = bg_img.resize((1920, 1080), Image.LANCZOS)
    
    draw = ImageDraw.Draw(bg_img)
    
    # Load fonts
    try:
        name_font_large = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf", 32)
        name_font = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf", 24)
    except:
        name_font_large = name_font = ImageFont.load_default()
    
    # Colors matching the red theme
    color_gold = (255, 215, 0)       # Gold for rank 1
    color_silver = (220, 220, 220)   # Silver for rank 2
    color_bronze = (205, 127, 50)    # Bronze for rank 3
    color_red = (255, 70, 70)        # Bright red for ranks 4-10
    color_dark = (120, 40, 40)       # Dark red for vacant/left
    
    # Load roster data
    roster = load_leaderboard()
    
    # =========================================================================
    # DRAW CENTER TOP PLAYER (RANK 1)
    # =========================================================================
    user_id = roster[0] if len(roster) > 0 else None
    if user_id:
        member = guild.get_member(int(user_id)) if isinstance(user_id, str) else guild.get_member(user_id)
        if member:
            # Draw avatar in center circle
            try:
                avatar_url = member.display_avatar.with_format('png').with_size(256).url
                async with aiohttp.ClientSession() as session:
                    async with session.get(avatar_url) as resp:
                        if resp.status == 200:
                            avatar_data = await resp.read()
                            avatar_img = Image.open(BytesIO(avatar_data)).convert("RGBA")
                            
                            # Resize avatar
                            size = TOP_PLAYER_POSITION["avatar_size"]
                            avatar_img = avatar_img.resize((size, size), Image.LANCZOS)
                            
                            # Create circular mask
                            mask = Image.new("L", (size, size), 0)
                            mask_draw = ImageDraw.Draw(mask)
                            mask_draw.ellipse((0, 0, size - 1, size - 1), fill=255)
                            
                            # Apply mask
                            output = Image.new("RGBA", (size, size), (0, 0, 0, 0))
                            output.paste(avatar_img, (0, 0))
                            output.putalpha(mask)
                            
                            # Paste avatar
                            center = TOP_PLAYER_POSITION["avatar_center"]
                            paste_x = center[0] - size // 2
                            paste_y = center[1] - size // 2
                            bg_img.paste(output, (paste_x, paste_y), output)
            except Exception as e:
                print(f"Error loading top player avatar: {e}")
            
            # Draw name below center
            name = member.display_name[:20]
            bbox = draw.textbbox((0, 0), name, font=name_font_large)
            text_w = bbox[2] - bbox[0]
            name_x = 960 - text_w // 2
            # Shadow
            draw.text((name_x + 2, TOP_PLAYER_POSITION["name_y"] + 2), name, fill=(0, 0, 0), font=name_font_large)
            draw.text((name_x, TOP_PLAYER_POSITION["name_y"]), name, fill=color_gold, font=name_font_large)
    
    # =========================================================================
    # DRAW LEFT AND RIGHT COLUMNS (RANKS 1-10)
    # =========================================================================
    for rank in range(1, 11):
        user_id = roster[rank - 1] if rank - 1 < len(roster) else None
        
        avatar_pos = LEADERBOARD_AVATAR_POSITIONS.get(rank)
        name_pos = LEADERBOARD_NAME_POSITIONS.get(rank)
        
        if not avatar_pos or not name_pos:
            continue
        
        center_x, center_y, avatar_size = avatar_pos
        name_x, name_y, alignment = name_pos
        
        # Determine name color based on rank
        if rank == 1:
            name_color = color_gold
        elif rank == 2:
            name_color = color_silver
        elif rank == 3:
            name_color = color_bronze
        else:
            name_color = color_red
        
        if user_id:
            member = guild.get_member(int(user_id)) if isinstance(user_id, str) else guild.get_member(user_id)
            
            if member:
                # Download and draw avatar
                try:
                    avatar_url = member.display_avatar.with_format('png').with_size(128).url
                    async with aiohttp.ClientSession() as session:
                        async with session.get(avatar_url) as resp:
                            if resp.status == 200:
                                avatar_data = await resp.read()
                                avatar_img = Image.open(BytesIO(avatar_data)).convert("RGBA")
                                avatar_img = avatar_img.resize((avatar_size, avatar_size), Image.LANCZOS)
                                
                                # Circular mask
                                mask = Image.new("L", (avatar_size, avatar_size), 0)
                                mask_draw = ImageDraw.Draw(mask)
                                mask_draw.ellipse((0, 0, avatar_size - 1, avatar_size - 1), fill=255)
                                
                                output = Image.new("RGBA", (avatar_size, avatar_size), (0, 0, 0, 0))
                                output.paste(avatar_img, (0, 0))
                                output.putalpha(mask)
                                
                                paste_x = center_x - avatar_size // 2
                                paste_y = center_y - avatar_size // 2
                                bg_img.paste(output, (paste_x, paste_y), output)
                except Exception as e:
                    print(f"Error loading avatar for rank {rank}: {e}")
                
                # Draw name
                name = member.display_name[:15]
                
                if alignment == "right":
                    # Right-aligned for right column
                    bbox = draw.textbbox((0, 0), name, font=name_font)
                    text_w = bbox[2] - bbox[0]
                    draw.text((name_x - text_w + 2, name_y + 2), name, fill=(0, 0, 0), font=name_font, anchor="lm")
                    draw.text((name_x - text_w, name_y), name, fill=name_color, font=name_font, anchor="lm")
                else:
                    # Left-aligned for left column
                    draw.text((name_x + 2, name_y + 2), name, fill=(0, 0, 0), font=name_font, anchor="lm")
                    draw.text((name_x, name_y), name, fill=name_color, font=name_font, anchor="lm")
            else:
                # Member left server
                name = "LEFT"
                if alignment == "right":
                    bbox = draw.textbbox((0, 0), name, font=name_font)
                    text_w = bbox[2] - bbox[0]
                    draw.text((name_x - text_w, name_y), name, fill=color_dark, font=name_font, anchor="lm")
                else:
                    draw.text((name_x, name_y), name, fill=color_dark, font=name_font, anchor="lm")
        else:
            # Vacant slot
            name = "VACANT"
            if alignment == "right":
                bbox = draw.textbbox((0, 0), name, font=name_font)
                text_w = bbox[2] - bbox[0]
                draw.text((name_x - text_w, name_y), name, fill=color_dark, font=name_font, anchor="lm")
            else:
                draw.text((name_x, name_y), name, fill=color_dark, font=name_font, anchor="lm")
    
    # Save to buffer
    buffer = BytesIO()
    bg_img.save(buffer, format="PNG", quality=95)
    buffer.seek(0)
    return buffer


async def update_top10_leaderboard_message(guild, channel_id=None, message_id=None):
    """Update the Top 10 leaderboard message with new image"""
    try:
        img_buffer = await create_top10_leaderboard_image(guild)
        if not img_buffer:
            return False
        
        if channel_id and message_id:
            channel = guild.get_channel(int(channel_id))
            if channel:
                try:
                    msg = await channel.fetch_message(int(message_id))
                    file = discord.File(img_buffer, filename="leaderboard.png")
                    await msg.edit(attachments=[file])
                    return True
                except:
                    pass
        
        return False
    except Exception as e:
        print(f"Failed to update leaderboard: {e}")
        return False

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
    
    # PRIORITY 1: Check for user's custom background (from shop purchase)
    user_custom_bg = user_data.get("custom_level_bg")
    if user_custom_bg:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(user_custom_bg) as resp:
                    if resp.status == 200:
                        img_data = await resp.read()
                        background = Image.open(BytesIO(img_data)).convert("RGBA")
                        background = background.resize((width, height), Image.Resampling.LANCZOS)
                        print(f"Loaded user custom background from URL")
        except Exception as e:
            print(f"Failed to load user custom background: {e}")
    
    # PRIORITY 2: Check for local file in multiple locations (Discloud compatibility)
    if background is None:
        for path in LEVEL_CARD_PATHS:
            if os.path.exists(path):
                try:
                    background = Image.open(path).convert("RGBA")
                    background = background.resize((width, height), Image.Resampling.LANCZOS)
                    print(f"Loaded level background from: {path}")
                    break
                except Exception as e:
                    print(f"Failed to load {path}: {e}")
    
    # PRIORITY 3: Check for global URL if no local file found
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

async def create_activity_results_image(guild, check, responses):
    """Create activity check results using custom Fallen background"""
    if not PIL_AVAILABLE:
        print("PIL not available for activity results image")
        return None
    
    # Load custom background image - check multiple possible locations
    img = None
    script_dir = os.path.dirname(os.path.abspath(__file__)) if '__file__' in dir() else os.getcwd()
    
    possible_paths = [
        "FallenCheck.png",
        os.path.join(script_dir, "FallenCheck.png"),
        os.path.join(os.getcwd(), "FallenCheck.png"),
        "/home/container/FallenCheck.png",
        "assets/FallenCheck.png",
        os.path.join(script_dir, "assets", "FallenCheck.png"),
    ]
    
    for bg_path in possible_paths:
        try:
            if os.path.exists(bg_path):
                img = Image.open(bg_path).convert("RGBA")
                print(f"✅ Loaded activity check background from: {bg_path}")
                break
        except Exception as e:
            print(f"Failed to load {bg_path}: {e}")
            continue
    
    # Fallback if image not found - create a nice looking fallback
    if img is None:
        print("⚠️ FallenCheck.png not found anywhere, creating fallback background")
        print(f"Searched in: {possible_paths}")
        
        # Create fallback with similar style
        img = Image.new("RGBA", (1280, 720), (25, 20, 30, 255))
        draw = ImageDraw.Draw(img)
        
        # Draw header area
        draw.rectangle([(0, 0), (1280, 165)], fill=(60, 20, 30))
        
        # Draw title
        try:
            title_font = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf", 42)
        except:
            title_font = ImageFont.load_default()
        draw.text((640, 80), "✦ FALLEN ACTIVITY CHECK ✦", font=title_font, fill=(255, 255, 255), anchor="mm")
        
        # Draw row placeholders
        row_y_positions = [207, 295, 383, 471, 559, 647]
        for y in row_y_positions:
            # Circle placeholder
            draw.ellipse([(22, y - 35), (92, y + 35)], outline=(139, 50, 50), width=2)
            # Bar placeholder
            draw.rounded_rectangle([(110, y - 30), (1250, y + 30)], radius=20, outline=(139, 50, 50), width=2)
    
    draw = ImageDraw.Draw(img)
    
    # Load fonts
    try:
        name_font = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf", 28)
        time_font = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf", 20)
    except:
        name_font = time_font = ImageFont.load_default()
    
    # Row positions (6 slots) - based on your image layout
    # Each row: avatar at X=57 (center), name at X=150, time at X=1200
    row_positions = [
        {"y": 207, "avatar_center": (57, 207)},
        {"y": 295, "avatar_center": (57, 295)},
        {"y": 383, "avatar_center": (57, 383)},
        {"y": 471, "avatar_center": (57, 471)},
        {"y": 559, "avatar_center": (57, 559)},
        {"y": 647, "avatar_center": (57, 647)},
    ]
    
    avatar_size = 60  # Size of avatar to fit in the circle
    
    # Draw up to 6 responses
    for i, uid in enumerate(responses[:6]):
        if i >= len(row_positions):
            break
            
        member = guild.get_member(int(uid))
        if not member:
            continue
        
        row = row_positions[i]
        center_x, center_y = row["avatar_center"]
        
        # Draw avatar (circular)
        try:
            avatar_url = member.display_avatar.url
            async with aiohttp.ClientSession() as session:
                async with session.get(str(avatar_url)) as resp:
                    if resp.status == 200:
                        avatar_data = await resp.read()
                        avatar = Image.open(BytesIO(avatar_data)).convert("RGBA")
                        avatar = avatar.resize((avatar_size, avatar_size), Image.Resampling.LANCZOS)
                        
                        # Circular mask
                        mask = Image.new("L", (avatar_size, avatar_size), 0)
                        mask_draw = ImageDraw.Draw(mask)
                        mask_draw.ellipse([(0, 0), (avatar_size, avatar_size)], fill=255)
                        
                        # Position avatar (center it on the circle)
                        avatar_x = center_x - avatar_size // 2
                        avatar_y = center_y - avatar_size // 2
                        
                        img.paste(avatar, (avatar_x, avatar_y), mask)
        except Exception as e:
            print(f"Avatar error: {e}")
        
        # Draw username (left side of the bar)
        name = member.display_name[:30]
        draw.text((150, center_y), name, font=name_font, fill=(255, 255, 255), anchor="lm")
        
        # Draw response time (right side)
        response_time = check.get("response_times", {}).get(uid)
        if response_time:
            try:
                rt = datetime.datetime.fromisoformat(response_time.replace('Z', '+00:00'))
                time_str = rt.strftime("%H:%M")
                draw.text((1200, center_y), time_str, font=time_font, fill=(180, 180, 180), anchor="rm")
            except:
                pass
    
    # Save to buffer
    buffer = BytesIO()
    img.save(buffer, format="PNG")
    buffer.seek(0)
    return buffer


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
    
    # Also save to PostgreSQL if available
    if db_pool:
        asyncio.create_task(save_duels_to_postgres(data))

async def save_duels_to_postgres(data):
    """Save duels data to PostgreSQL"""
    if not db_pool:
        return
    try:
        async with db_pool.acquire() as conn:
            await conn.execute('''
                INSERT INTO duels (key, data, updated_at)
                VALUES ('duels_data', $1, NOW())
                ON CONFLICT (key) DO UPDATE SET data = $1, updated_at = NOW()
            ''', json.dumps(data))
    except Exception as e:
        print(f"PostgreSQL duels save error: {e}")

async def load_duels_from_postgres():
    """Load duels data from PostgreSQL"""
    if not db_pool:
        return None
    try:
        async with db_pool.acquire() as conn:
            row = await conn.fetchrow("SELECT data FROM duels WHERE key = 'duels_data'")
            if row:
                return json.loads(row['data'])
    except Exception as e:
        print(f"PostgreSQL duels load error: {e}")
    return None

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
    
    # Check if loser has ELO shield
    loser_data = get_user_data(loser_id)
    shield_used = False
    if loser_data.get("elo_shield_active", False):
        # Shield protects from ELO loss
        loser_change = 0
        update_user_data(loser_id, "elo_shield_active", False)
        shield_used = True
    
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
        "shield_used": shield_used,
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
        "loser_new_elo": new_loser_elo,
        "shield_used": shield_used
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

def create_tournament(name, creator_id, required_role_id=None, required_role_name=None, channel_id=None, max_participants=16):
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
        "winner": None,
        "required_role_id": required_role_id,
        "required_role_name": required_role_name,
        "channel_id": channel_id,
        "message_id": None,
        "max_participants": max_participants
    }
    
    data["active"] = tournament
    save_tournaments(data)
    return tournament


def join_tournament(user_id, member_roles=None):
    """Join the active tournament"""
    data = load_tournaments()
    
    if not data["active"] or data["active"]["status"] != "signup":
        return False, "No active signup"
    
    tournament = data["active"]
    
    # Check role requirement
    if tournament.get("required_role_id") and member_roles:
        role_ids = [str(r.id) for r in member_roles]
        if tournament["required_role_id"] not in role_ids:
            return False, f"Requires {tournament.get('required_role_name', 'specific role')}"
    
    # Check max participants
    if len(tournament["participants"]) >= tournament.get("max_participants", 16):
        return False, "Tournament full"
    
    uid = str(user_id)
    if uid not in tournament["participants"]:
        tournament["participants"].append(uid)
        save_tournaments(data)
        return True, "Joined"
    return False, "Already joined"

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
        print(f"Tournament not active")
        return None
    
    bracket = data["active"]["bracket"]
    winner_uid = str(winner_id)
    match_id_upper = match_id.upper() if match_id else ""
    
    # Find the match
    for round_idx, round_matches in enumerate(bracket["rounds"]):
        for match_idx, match in enumerate(round_matches):
            # Case-insensitive match ID comparison
            if match["id"].upper() == match_id_upper:
                p1 = str(match.get("player1", "")) if match.get("player1") else None
                p2 = str(match.get("player2", "")) if match.get("player2") else None
                
                if winner_uid not in [p1, p2]:
                    print(f"Winner {winner_uid} not in match: {p1} vs {p2}")
                    return None
                
                match["winner"] = winner_uid
                
                # Advance winner to next round
                if round_idx < len(bracket["rounds"]) - 1:
                    next_round = bracket["rounds"][round_idx + 1]
                    next_match_idx = match_idx // 2
                    
                    if next_match_idx < len(next_round):
                        if match_idx % 2 == 0:
                            next_round[next_match_idx]["player1"] = winner_uid
                        else:
                            next_round[next_match_idx]["player2"] = winner_uid
                else:
                    # Final match - tournament complete
                    data["active"]["winner"] = winner_uid
                    data["active"]["status"] = "complete"
                
                save_tournaments(data)
                return match
    
    print(f"Match {match_id} not found")
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
    """Simplified tournament panel - all in one"""
    def __init__(self):
        super().__init__(timeout=None)
    
    @discord.ui.button(label="⚔️ Join", style=discord.ButtonStyle.success, custom_id="t_join")
    async def join(self, interaction: discord.Interaction, button: discord.ui.Button):
        tournament = get_active_tournament()
        if not tournament:
            return await interaction.response.send_message("❌ No tournament!", ephemeral=True)
        if tournament["status"] != "signup":
            return await interaction.response.send_message("❌ Signups closed!", ephemeral=True)
        if str(interaction.user.id) in tournament["participants"]:
            return await interaction.response.send_message("✅ Already joined!", ephemeral=True)
        
        success, msg = join_tournament(interaction.user.id, interaction.user.roles)
        if success:
            await interaction.response.send_message(f"✅ Joined!", ephemeral=True)
            await self.refresh_panel(interaction)
        else:
            await interaction.response.send_message(f"❌ {msg}", ephemeral=True)
    
    @discord.ui.button(label="🚪 Leave", style=discord.ButtonStyle.secondary, custom_id="t_leave")
    async def leave(self, interaction: discord.Interaction, button: discord.ui.Button):
        tournament = get_active_tournament()
        if not tournament or tournament["status"] != "signup":
            return await interaction.response.send_message("❌ Can't leave now!", ephemeral=True)
        if leave_tournament(interaction.user.id):
            await interaction.response.send_message("✅ Left tournament.", ephemeral=True)
            await self.refresh_panel(interaction)
        else:
            await interaction.response.send_message("❌ Not in tournament!", ephemeral=True)
    
    @discord.ui.button(label="🏆 Report Winner", style=discord.ButtonStyle.primary, custom_id="t_report")
    async def report(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Staff reports match winner - simplified"""
        if not is_staff(interaction.user):
            return await interaction.response.send_message("❌ Staff only!", ephemeral=True)
        
        tournament = get_active_tournament()
        if not tournament:
            return await interaction.response.send_message("❌ No tournament!", ephemeral=True)
        if tournament["status"] == "signup":
            return await interaction.response.send_message("❌ Start tournament first!", ephemeral=True)
        if tournament["status"] == "complete":
            return await interaction.response.send_message("✅ Tournament already complete!", ephemeral=True)
        
        # Find playable matches
        bracket = tournament.get("bracket", {})
        matches = []
        for rnd in bracket.get("rounds", []):
            for m in rnd:
                if m.get("player1") and m.get("player2") and not m.get("winner"):
                    matches.append(m)
        
        if not matches:
            return await interaction.response.send_message("❌ No matches to report!", ephemeral=True)
        
        # Create simple dropdown
        options = []
        for m in matches[:25]:
            p1 = interaction.guild.get_member(int(m["player1"]))
            p2 = interaction.guild.get_member(int(m["player2"]))
            n1 = p1.display_name[:12] if p1 else "?"
            n2 = p2.display_name[:12] if p2 else "?"
            options.append(discord.SelectOption(label=f"{m['id']}: {n1} vs {n2}", value=m["id"]))
        
        view = SimpleMatchSelect(matches, interaction.guild, interaction.message)
        await interaction.response.send_message("Select match:", view=view, ephemeral=True)
    
    @discord.ui.button(label="▶️ Start", style=discord.ButtonStyle.danger, custom_id="t_start", row=1)
    async def start(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not is_staff(interaction.user):
            return await interaction.response.send_message("❌ Staff only!", ephemeral=True)
        
        tournament = get_active_tournament()
        if not tournament:
            return await interaction.response.send_message("❌ No tournament!", ephemeral=True)
        if tournament["status"] != "signup":
            return await interaction.response.send_message("❌ Already started!", ephemeral=True)
        if len(tournament["participants"]) < 2:
            return await interaction.response.send_message("❌ Need 2+ players!", ephemeral=True)
        
        result = start_tournament()
        if result:
            await interaction.response.send_message(f"✅ Started with {len(result['participants'])} players!", ephemeral=True)
            await self.refresh_panel(interaction)
        else:
            await interaction.response.send_message("❌ Failed!", ephemeral=True)
    
    @discord.ui.button(label="🛑 End", style=discord.ButtonStyle.danger, custom_id="t_end", row=1)
    async def end(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not is_staff(interaction.user):
            return await interaction.response.send_message("❌ Staff only!", ephemeral=True)
        if end_tournament():
            await interaction.response.send_message("✅ Tournament ended!", ephemeral=True)
            try:
                embed = discord.Embed(title="🏆 Tournament Ended", color=0x95a5a6)
                await interaction.message.edit(embed=embed, view=None)
            except:
                pass
        else:
            await interaction.response.send_message("❌ No tournament!", ephemeral=True)
    
    async def refresh_panel(self, interaction):
        """Update the panel"""
        tournament = get_active_tournament()
        if not tournament:
            return
        
        bracket_text = create_visual_bracket_text(tournament, interaction.guild)
        status = {"signup": "📝 Signups", "active": "🔴 LIVE", "complete": "✅ Done"}
        
        embed = discord.Embed(
            title=f"🏆 {tournament['name']} - {status.get(tournament['status'], '')}",
            description=bracket_text,
            color=0xffd700 if tournament["status"] == "signup" else 0xe74c3c
        )
        
        max_p = tournament.get("max_participants", 16)
        embed.add_field(name="Players", value=f"{len(tournament['participants'])}/{max_p}", inline=True)
        if tournament.get("required_role_name"):
            embed.add_field(name="Role", value=tournament["required_role_name"], inline=True)
        embed.set_footer(text="✝ The Fallen ✝")
        
        try:
            await interaction.message.edit(embed=embed)
        except:
            pass


class SimpleMatchSelect(discord.ui.View):
    """Simple match selector"""
    def __init__(self, matches, guild, panel_msg):
        super().__init__(timeout=60)
        self.matches = matches
        self.guild = guild
        self.panel_msg = panel_msg
        
        options = []
        for m in matches[:25]:
            p1 = guild.get_member(int(m["player1"])) if m.get("player1") else None
            p2 = guild.get_member(int(m["player2"])) if m.get("player2") else None
            n1 = p1.display_name[:12] if p1 else "?"
            n2 = p2.display_name[:12] if p2 else "?"
            options.append(discord.SelectOption(label=f"{m['id']}: {n1} vs {n2}", value=m["id"]))
        
        if options:
            select = discord.ui.Select(placeholder="Pick match...", options=options)
            select.callback = self.match_selected
            self.add_item(select)
    
    async def match_selected(self, interaction: discord.Interaction):
        match_id = interaction.data["values"][0]
        match = next((m for m in self.matches if m["id"] == match_id), None)
        if not match:
            return await interaction.response.send_message("❌ Not found!", ephemeral=True)
        
        p1 = self.guild.get_member(int(match["player1"])) if match.get("player1") else None
        p2 = self.guild.get_member(int(match["player2"])) if match.get("player2") else None
        
        view = SimpleWinnerSelect(match, p1, p2, self.panel_msg, self.guild)
        n1 = p1.display_name if p1 else "P1"
        n2 = p2.display_name if p2 else "P2"
        await interaction.response.send_message(f"**{match_id}**: {n1} vs {n2}\nWho won?", view=view, ephemeral=True)


class SimpleWinnerSelect(discord.ui.View):
    """Simple winner buttons with player names"""
    def __init__(self, match, p1, p2, panel_msg, guild):
        super().__init__(timeout=60)
        self.match = match
        self.p1 = p1
        self.p2 = p2
        self.panel_msg = panel_msg
        self.guild = guild
        
        # Set button labels
        btn1 = discord.ui.Button(label=f"🏆 {p1.display_name[:20] if p1 else 'P1'}", style=discord.ButtonStyle.success)
        btn1.callback = self.p1_wins
        self.add_item(btn1)
        
        btn2 = discord.ui.Button(label=f"🏆 {p2.display_name[:20] if p2 else 'P2'}", style=discord.ButtonStyle.success)
        btn2.callback = self.p2_wins
        self.add_item(btn2)
    
    async def p1_wins(self, interaction: discord.Interaction):
        await self.report(interaction, self.match.get("player1"))
    
    async def p2_wins(self, interaction: discord.Interaction):
        await self.report(interaction, self.match.get("player2"))
    
    async def report(self, interaction: discord.Interaction, winner_id):
        if not winner_id:
            return await interaction.response.send_message("❌ Invalid!", ephemeral=True)
        
        result = report_tournament_match(self.match["id"], winner_id)
        if result:
            winner = self.guild.get_member(int(winner_id))
            name = winner.display_name if winner else "Unknown"
            
            tournament = get_active_tournament()
            if tournament and tournament.get("status") == "complete":
                add_coins(int(winner_id), 5000)
                add_xp_to_user(int(winner_id), 500)
                await interaction.response.send_message(f"🏆 **CHAMPION: {name}!**\n+5000 coins +500 XP!", ephemeral=True)
            else:
                await interaction.response.send_message(f"✅ {name} wins!", ephemeral=True)
            
            # Refresh panel
            try:
                tournament = get_active_tournament()
                if tournament and self.panel_msg:
                    bracket_text = create_visual_bracket_text(tournament, self.guild)
                    status = {"signup": "📝 Signups", "active": "🔴 LIVE", "complete": "✅ Done"}
                    embed = discord.Embed(
                        title=f"🏆 {tournament['name']} - {status.get(tournament['status'], '')}",
                        description=bracket_text,
                        color=0xffd700 if tournament["status"] == "signup" else 0xe74c3c
                    )
                    await self.panel_msg.edit(embed=embed)
            except:
                pass
        else:
            await interaction.response.send_message(f"❌ Failed! Match: {self.match['id']}, Winner: {winner_id}", ephemeral=True)


# Keep old views for compatibility but they won't be used
class TournamentManageStaffView(discord.ui.View):
    def __init__(self, panel_message=None):
        super().__init__(timeout=300)
        self.panel_message = panel_message
    
    @discord.ui.button(label="▶️ Start Tournament", style=discord.ButtonStyle.success)
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
            # Create visual bracket
            bracket_text = create_visual_bracket_text(result, interaction.guild)
            
            embed = discord.Embed(
                title=f"🏆 {result['name']} - LIVE",
                description=bracket_text,
                color=0xe74c3c
            )
            embed.set_footer(text="✝ The Fallen Tournament ✝")
            
            await interaction.response.send_message(
                f"✅ Tournament started with {len(result['participants'])} participants!\n"
                f"Bracket generated - use **Report Match** to record winners.",
                ephemeral=True
            )
            
            # Update panel
            try:
                if self.panel_message:
                    await self.panel_message.edit(embed=embed)
            except:
                pass
        else:
            await interaction.response.send_message("❌ Failed to start!", ephemeral=True)
    
    @discord.ui.button(label="🏆 Report Match", style=discord.ButtonStyle.primary)
    async def report(self, interaction: discord.Interaction, button: discord.ui.Button):
        tournament = get_active_tournament()
        
        if not tournament or tournament["status"] != "active":
            return await interaction.response.send_message("❌ No active tournament!", ephemeral=True)
        
        # Get current playable matches
        bracket = tournament.get("bracket", {})
        current_matches = []
        
        for r_idx, round_matches in enumerate(bracket.get("rounds", [])):
            for match in round_matches:
                # Match is playable if: has both players, no winner yet
                if match.get("player1") and match.get("player2") and not match.get("winner"):
                    current_matches.append(match)
        
        if not current_matches:
            return await interaction.response.send_message(
                "❌ No matches ready to report! Either waiting for players to advance or tournament is complete.",
                ephemeral=True
            )
        
        # Show match selector
        await interaction.response.send_message(
            "**Select a match to report:**",
            view=TournamentMatchSelectView(current_matches, interaction.guild),
            ephemeral=True
        )
    
    @discord.ui.button(label="📊 View Bracket", style=discord.ButtonStyle.secondary)
    async def view_bracket(self, interaction: discord.Interaction, button: discord.ui.Button):
        tournament = get_active_tournament()
        
        if not tournament:
            return await interaction.response.send_message("❌ No tournament!", ephemeral=True)
        
        bracket_text = create_visual_bracket_text(tournament, interaction.guild)
        
        embed = discord.Embed(
            title=f"🏆 {tournament['name']}",
            description=bracket_text,
            color=0xffd700 if tournament["status"] == "signup" else 0xe74c3c
        )
        
        if tournament.get("status") == "complete" and tournament.get("champion"):
            champ = interaction.guild.get_member(int(tournament["champion"]))
            champ_name = champ.display_name if champ else "Unknown"
            embed.add_field(name="🏆 Champion", value=f"**{champ_name}**", inline=False)
        
        embed.set_footer(text="✝ The Fallen Tournament ✝")
        await interaction.response.send_message(embed=embed, ephemeral=True)
    
    @discord.ui.button(label="🛑 End Tournament", style=discord.ButtonStyle.danger)
    async def end(self, interaction: discord.Interaction, button: discord.ui.Button):
        if end_tournament():
            await interaction.response.send_message("✅ Tournament ended!", ephemeral=True)
            
            try:
                embed = discord.Embed(
                    title="🏆 Tournament Ended",
                    description="No active tournament. Stay tuned for the next one!",
                    color=0x95a5a6
                )
                if self.panel_message:
                    await self.panel_message.edit(embed=embed, view=None)
            except:
                pass
        else:
            await interaction.response.send_message("❌ No tournament to end!", ephemeral=True)


def create_visual_bracket_text(tournament, guild):
    """Create visual text bracket"""
    if tournament["status"] == "signup":
        participants = tournament.get("participants", [])
        if not participants:
            return "*No participants yet - click Join to sign up!*"
        
        lines = ["**📋 Registered Participants:**\n"]
        for i, p_id in enumerate(participants[:20]):
            member = guild.get_member(int(p_id))
            name = member.display_name if member else "Unknown"
            lines.append(f"{i+1}. {name}")
        
        if len(participants) > 20:
            lines.append(f"*...and {len(participants) - 20} more*")
        
        return "\n".join(lines)
    
    # Active tournament - show bracket
    bracket = tournament.get("bracket", {})
    rounds = bracket.get("rounds", [])
    
    if not rounds:
        return "*Bracket not generated yet*"
    
    num_rounds = len(rounds)
    
    # Round names
    round_names = {}
    for i in range(1, num_rounds + 1):
        if i == num_rounds:
            round_names[i] = "🏆 Finals"
        elif i == num_rounds - 1:
            round_names[i] = "⚔️ Semifinals"
        elif i == num_rounds - 2:
            round_names[i] = "⚔️ Quarterfinals"
        else:
            round_names[i] = f"⚔️ Round {i}"
    
    lines = []
    
    for r_idx, round_matches in enumerate(rounds):
        r_num = r_idx + 1
        r_name = round_names.get(r_num, f"Round {r_num}")
        
        lines.append(f"\n**{r_name}**")
        
        for m_idx, match in enumerate(round_matches):
            p1 = match.get("player1")
            p2 = match.get("player2")
            winner = match.get("winner")
            match_id = match.get("id", f"M{m_idx+1}")
            
            # Get names
            if p1:
                m1 = guild.get_member(int(p1))
                n1 = m1.display_name if m1 else "???"
            else:
                n1 = "BYE" if r_num == 1 else "TBD"
            
            if p2:
                m2 = guild.get_member(int(p2))
                n2 = m2.display_name if m2 else "???"
            else:
                n2 = "BYE" if r_num == 1 else "TBD"
            
            # Format
            if winner:
                if winner == p1:
                    lines.append(f"`{match_id}` ✅ **{n1}** vs ~~{n2}~~")
                else:
                    lines.append(f"`{match_id}` ~~{n1}~~ vs ✅ **{n2}**")
            else:
                if p1 and p2:
                    lines.append(f"`{match_id}` ⚔️ {n1} vs {n2}")
                else:
                    lines.append(f"`{match_id}` ⏳ {n1} vs {n2}")
    
    # Champion
    if rounds and rounds[-1] and rounds[-1][0].get("winner"):
        w_id = rounds[-1][0]["winner"]
        w_member = guild.get_member(int(w_id))
        w_name = w_member.display_name if w_member else "Unknown"
        lines.append(f"\n**🏆 CHAMPION: {w_name} 🏆**")
    
    return "\n".join(lines)


class TournamentMatchSelectView(discord.ui.View):
    """Dropdown to select a match"""
    def __init__(self, matches, guild):
        super().__init__(timeout=120)
        self.matches = matches
        self.guild = guild
        
        options = []
        for m in matches[:25]:
            p1_id = m.get("player1")
            p2_id = m.get("player2")
            p1 = guild.get_member(int(p1_id)) if p1_id else None
            p2 = guild.get_member(int(p2_id)) if p2_id else None
            n1 = p1.display_name[:15] if p1 else "???"
            n2 = p2.display_name[:15] if p2 else "???"
            options.append(discord.SelectOption(
                label=f"{m['id']}: {n1} vs {n2}",
                value=m["id"]
            ))
        
        if options:
            self.select = discord.ui.Select(placeholder="Select a match...", options=options)
            self.select.callback = self.select_callback
            self.add_item(self.select)
    
    async def select_callback(self, interaction: discord.Interaction):
        match_id = self.select.values[0]
        match = next((m for m in self.matches if m["id"] == match_id), None)
        
        if not match:
            return await interaction.response.send_message("❌ Match not found!", ephemeral=True)
        
        p1_id = match.get("player1")
        p2_id = match.get("player2")
        p1 = self.guild.get_member(int(p1_id)) if p1_id else None
        p2 = self.guild.get_member(int(p2_id)) if p2_id else None
        
        p1_name = p1.display_name if p1 else "Player 1"
        p2_name = p2.display_name if p2 else "Player 2"
        
        await interaction.response.send_message(
            f"**{match_id}** - Who won?\n{p1_name} vs {p2_name}",
            view=TournamentWinnerSelectView(match, p1, p2),
            ephemeral=True
        )


class TournamentWinnerSelectView(discord.ui.View):
    """Select the winner - simplified"""
    def __init__(self, match, player1, player2):
        super().__init__(timeout=120)
        self.match = match
        self.player1 = player1
        self.player2 = player2
        
        # Set button labels to actual names
        p1_name = player1.display_name[:20] if player1 else "Player 1"
        p2_name = player2.display_name[:20] if player2 else "Player 2"
        self.children[0].label = f"🏆 {p1_name}"
        self.children[1].label = f"🏆 {p2_name}"
    
    @discord.ui.button(label="Player 1 Wins", style=discord.ButtonStyle.success)
    async def p1_wins(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.report_winner(interaction, self.match.get("player1"))
    
    @discord.ui.button(label="Player 2 Wins", style=discord.ButtonStyle.success)
    async def p2_wins(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.report_winner(interaction, self.match.get("player2"))
    
    async def report_winner(self, interaction: discord.Interaction, winner_id):
        if not winner_id:
            return await interaction.response.send_message("❌ Invalid player!", ephemeral=True)
        
        try:
            result = report_tournament_match(self.match["id"], winner_id)
            
            if result:
                winner = interaction.guild.get_member(int(winner_id))
                winner_name = winner.display_name if winner else "Unknown"
                
                # Check if complete
                tournament = get_active_tournament()
                if tournament and tournament.get("status") == "complete":
                    # Award champion using correct function
                    add_user_stat(int(winner_id), "coins", 5000)
                    add_xp_to_user(int(winner_id), 500)
                    
                    await interaction.response.send_message(
                        f"🏆 **TOURNAMENT COMPLETE!**\n\n"
                        f"**Champion:** {winner_name}\n"
                        f"**Prize:** 5,000 coins + 500 XP!",
                        ephemeral=True
                    )
                else:
                    await interaction.response.send_message(
                        f"✅ **{winner_name}** wins `{self.match['id']}`!",
                        ephemeral=True
                    )
                
                # Disable buttons
                for child in self.children:
                    child.disabled = True
                try:
                    await interaction.message.edit(view=self)
                except:
                    pass
            else:
                await interaction.response.send_message(
                    f"❌ Failed to report match!",
                    ephemeral=True
                )
        except Exception as e:
            print(f"Report error: {e}")
            await interaction.response.send_message(f"❌ Error: {e}", ephemeral=True)


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
    3: "♰ Shadow Initiate",         # 3 streak
    5: "♰ Rising Shadow",           # 5 streak
    10: "♰ Relentless",             # 10 streak
    20: "♰ Undying",                # 20 streak
    50: "♰ Eternal Fallen",         # 50 streak
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
    
    # Also save to PostgreSQL if available
    if db_pool:
        asyncio.create_task(save_events_to_postgres(data))

async def save_events_to_postgres(data):
    """Save events data to PostgreSQL"""
    if not db_pool:
        return
    try:
        async with db_pool.acquire() as conn:
            await conn.execute('''
                INSERT INTO events (key, data, updated_at)
                VALUES ('events_data', $1, NOW())
                ON CONFLICT (key) DO UPDATE SET data = $1, updated_at = NOW()
            ''', json.dumps(data))
    except Exception as e:
        print(f"PostgreSQL events save error: {e}")

async def load_events_from_postgres():
    """Load events data from PostgreSQL"""
    if not db_pool:
        return None
    try:
        async with db_pool.acquire() as conn:
            row = await conn.fetchrow("SELECT data FROM events WHERE key = 'events_data'")
            if row:
                return json.loads(row['data'])
    except Exception as e:
        print(f"PostgreSQL events load error: {e}")
    return None

# ==========================================
# RECURRING EVENTS SYSTEM
# ==========================================

def load_recurring_events():
    """Load recurring events configuration"""
    try:
        with open(RECURRING_EVENTS_FILE, "r") as f:
            return json.load(f)
    except:
        return {"recurring_events": [], "last_created": {}}

def save_recurring_events(data):
    """Save recurring events configuration"""
    with open(RECURRING_EVENTS_FILE, "w") as f:
        json.dump(data, f, indent=2)
    
    # Also save to PostgreSQL if available
    if db_pool:
        asyncio.create_task(save_recurring_to_postgres(data))

async def save_recurring_to_postgres(data):
    """Save recurring events to PostgreSQL"""
    if not db_pool:
        return
    try:
        async with db_pool.acquire() as conn:
            await conn.execute('''
                INSERT INTO json_data (key, data, updated_at)
                VALUES ('recurring_events', $1, NOW())
                ON CONFLICT (key) DO UPDATE SET data = $1, updated_at = NOW()
            ''', json.dumps(data))
    except Exception as e:
        print(f"PostgreSQL recurring save error: {e}")

async def load_recurring_from_postgres():
    """Load recurring events from PostgreSQL"""
    if not db_pool:
        return None
    try:
        async with db_pool.acquire() as conn:
            row = await conn.fetchrow("SELECT data FROM json_data WHERE key = 'recurring_events'")
            if row:
                return json.loads(row['data'])
    except Exception as e:
        print(f"PostgreSQL recurring load error: {e}")
    return None

def create_recurring_event(event_type, title, day_of_week, hour, minute, channel_id, created_by):
    """Create a new recurring event
    
    day_of_week: 0=Monday, 1=Tuesday, ..., 6=Sunday
    hour: 0-23 (UTC)
    minute: 0-59
    """
    data = load_recurring_events()
    
    recurring_id = f"recurring_{int(datetime.datetime.now().timestamp())}"
    
    recurring = {
        "id": recurring_id,
        "type": event_type,
        "title": title,
        "day_of_week": day_of_week,
        "hour": hour,
        "minute": minute,
        "channel_id": str(channel_id),
        "created_by": str(created_by),
        "enabled": True,
        "created_at": datetime.datetime.now(datetime.timezone.utc).isoformat()
    }
    
    data["recurring_events"].append(recurring)
    save_recurring_events(data)
    return recurring

def get_recurring_events():
    """Get all recurring events"""
    data = load_recurring_events()
    return data.get("recurring_events", [])

def delete_recurring_event(recurring_id):
    """Delete a recurring event"""
    data = load_recurring_events()
    
    for i, event in enumerate(data["recurring_events"]):
        if event["id"] == recurring_id:
            removed = data["recurring_events"].pop(i)
            save_recurring_events(data)
            return removed
    return None

def toggle_recurring_event(recurring_id, enabled):
    """Enable or disable a recurring event"""
    data = load_recurring_events()
    
    for event in data["recurring_events"]:
        if event["id"] == recurring_id:
            event["enabled"] = enabled
            save_recurring_events(data)
            return event
    return None

DAYS_OF_WEEK = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]

async def check_recurring_events(guild):
    """Check if any recurring events should be created now"""
    data = load_recurring_events()
    now = datetime.datetime.now(datetime.timezone.utc)
    current_day = now.weekday()  # 0=Monday, 6=Sunday
    current_hour = now.hour
    current_minute = now.minute
    
    for recurring in data.get("recurring_events", []):
        if not recurring.get("enabled", True):
            continue
        
        # Check if this event should trigger now
        if recurring["day_of_week"] == current_day and recurring["hour"] == current_hour:
            # Check if we're within the right minute window (0-5 mins past the hour)
            if 0 <= current_minute <= 5:
                # Check if we already created this event today
                last_key = f"{recurring['id']}_{now.strftime('%Y-%m-%d')}"
                if last_key in data.get("last_created", {}):
                    continue  # Already created today
                
                # Create the event!
                channel = guild.get_channel(int(recurring["channel_id"]))
                if not channel:
                    continue
                
                # Schedule event for right now (or a few minutes from now)
                scheduled_time = now + datetime.timedelta(minutes=30)  # Event starts in 30 mins
                
                event = create_event(
                    event_type=recurring["type"],
                    title=recurring["title"],
                    scheduled_time=scheduled_time.isoformat(),
                    host_id=recurring["created_by"],
                    channel_id=recurring["channel_id"]
                )
                
                # Mark as created
                if "last_created" not in data:
                    data["last_created"] = {}
                data["last_created"][last_key] = now.isoformat()
                save_recurring_events(data)
                
                # Post the event announcement
                ping_role_name = TRAINING_PING_ROLE if recurring["type"] == "training" else TRYOUT_PING_ROLE
                ping_role = discord.utils.get(guild.roles, name=ping_role_name)
                
                embed = await create_event_embed(event, guild)
                
                ping_text = ping_role.mention if ping_role else ""
                try:
                    await channel.send(content=f"📅 **Recurring Event Auto-Created!**\n{ping_text}", embed=embed, view=EventRSVPView(event["id"]))
                except Exception as e:
                    print(f"Failed to post recurring event: {e}")

async def recurring_events_loop():
    """Background loop to check recurring events"""
    await bot.wait_until_ready()
    await asyncio.sleep(120)  # Wait 2 minutes after startup
    
    while not bot.is_closed():
        try:
            for guild in bot.guilds:
                await check_recurring_events(guild)
        except Exception as e:
            print(f"Recurring events check error: {e}")
        
        await asyncio.sleep(300)  # Check every 5 minutes

def create_event(event_type, title, scheduled_time, host_id, ping_role=None, channel_id=None, server_link=None):
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
        "server_link": server_link,  # Private server link for Roblox
        "message_id": None,  # Will store the announcement message ID
        "rsvp_yes": [],
        "rsvp_maybe": [],
        "attendees": [],  # Logged after event
        "status": "scheduled",  # scheduled, active, completed, cancelled
        "created_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "reminder_30_sent": False,
        "reminder_5_sent": False,
        "link_posted": False  # Track if server link was posted
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
        
        # Get server link if available
        server_link = event.get("server_link")
        
        if minutes == 30:
            title = f"⏰ {emoji} {event['title']} - 30 MINUTES!"
            desc = f"**{event['title']}** starts in **30 minutes**!\n\nMake sure you're ready!"
            
            embed = discord.Embed(title=title, description=desc, color=0xf39c12)
            
            # Show who RSVPd
            if event.get("rsvp_yes"):
                mentions = " ".join([f"<@{uid}>" for uid in event["rsvp_yes"][:15]])
                embed.add_field(name="📋 Expected Attendees", value=mentions, inline=False)
            
            if server_link:
                embed.add_field(name="🔗 Server Link", value="Will be posted in 25 minutes!", inline=False)
            
            ping_text = ping_role.mention if ping_role else ""
            await channel.send(content=ping_text, embed=embed)
            
        else:  # 5 minute reminder - POST THE SERVER LINK!
            title = f"🚨 {emoji} {event['title']} - STARTING IN 5 MINUTES!"
            
            if server_link:
                desc = (
                    f"**{event['title']}** is starting in **5 MINUTES**!\n\n"
                    f"━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                    f"🎮 **JOIN THE SERVER NOW!**\n\n"
                    f"━━━━━━━━━━━━━━━━━━━━━━━━━━━"
                )
            else:
                desc = f"**{event['title']}** is starting in **5 MINUTES**!\n\n**Join the voice channel!**"
            
            embed = discord.Embed(title=title, description=desc, color=0xe74c3c)
            
            # Show who RSVPd
            if event.get("rsvp_yes"):
                mentions = " ".join([f"<@{uid}>" for uid in event["rsvp_yes"][:15]])
                embed.add_field(name="📋 Expected Attendees", value=mentions, inline=False)
            
            # Get host
            host = guild.get_member(int(event["host_id"]))
            if host:
                embed.add_field(name="👑 Host", value=host.mention, inline=True)
            
            embed.set_footer(text="✝ The Fallen ✝ • Be on time!")
            
            ping_text = ping_role.mention if ping_role else ""
            
            # Send the embed first
            await channel.send(content=ping_text, embed=embed)
            
            # Then send the server link separately so it's easy to click
            if server_link:
                link_embed = discord.Embed(
                    title="🔗 PRIVATE SERVER LINK",
                    description=f"**Click below to join:**\n\n{server_link}",
                    color=0x2ecc71
                )
                link_embed.set_footer(text="Link expires when the event ends!")
                await channel.send(embed=link_embed)
                
                # Mark link as posted
                update_event(event["id"], {"link_posted": True})
        
        break  # Only send to first guild found

# ==========================================
# INACTIVITY STRIKE SYSTEM
# ==========================================

INACTIVITY_FILE = "inactivity_data.json"
INACTIVITY_CHECK_DAYS = 3  # Days of inactivity before strike
MAX_INACTIVITY_STRIKES = 5  # Strikes before kick
INACTIVITY_IMMUNITY_ROLE = "Inactivity Immunity"  # Role that bypasses inactivity checks
INACTIVITY_REQUIRED_ROLE = "Mainer"  # ONLY check members with this role

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

def is_mainer(member):
    """Check if member has the Mainers role (required for inactivity tracking)"""
    mainers_role = discord.utils.get(member.roles, name=INACTIVITY_REQUIRED_ROLE)
    return mainers_role is not None

def should_check_inactivity(member):
    """Check if a member should be tracked for inactivity"""
    # Must have Mainers role
    if not is_mainer(member):
        return False
    # Must not have immunity
    if has_inactivity_immunity(member):
        return False
    # Must not be a bot
    if member.bot:
        return False
    return True

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
    
    # Also save to PostgreSQL if available
    if db_pool:
        asyncio.create_task(save_inactivity_to_postgres(data))

async def save_inactivity_to_postgres(data):
    """Save inactivity data to PostgreSQL"""
    if not db_pool:
        return
    try:
        async with db_pool.acquire() as conn:
            await conn.execute('''
                INSERT INTO inactivity (key, data, updated_at)
                VALUES ('inactivity_data', $1, NOW())
                ON CONFLICT (key) DO UPDATE SET data = $1, updated_at = NOW()
            ''', json.dumps(data))
    except Exception as e:
        print(f"PostgreSQL inactivity save error: {e}")

async def load_inactivity_from_postgres():
    """Load inactivity data from PostgreSQL"""
    if not db_pool:
        return None
    try:
        async with db_pool.acquire() as conn:
            row = await conn.fetchrow("SELECT data FROM inactivity WHERE key = 'inactivity_data'")
            if row:
                return json.loads(row['data'])
    except Exception as e:
        print(f"PostgreSQL inactivity load error: {e}")
    return None

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
    """Run inactivity check on Mainers with ranked roles (rate limit protected)"""
    results = {
        "checked": 0,
        "strikes_given": 0,
        "demotions": 0,
        "kicks": 0,
        "skipped_no_mainer": 0,
        "skipped_immunity": 0,
        "details": []
    }
    
    # Get Mainers role
    mainers_role = discord.utils.get(guild.roles, name=INACTIVITY_REQUIRED_ROLE)
    if not mainers_role:
        print(f"Warning: {INACTIVITY_REQUIRED_ROLE} role not found!")
        return results
    
    for member in guild.members:
        if member.bot:
            continue
        
        # MUST have Mainer role - that's the ONLY requirement
        if mainers_role not in member.roles:
            continue
        
        # Check for immunity
        if has_inactivity_immunity(member):
            results["skipped_immunity"] += 1
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
        
        await interaction.response.edit_message(content="📜 Generating transcript...", view=None)
        
        # Generate transcript using new system
        ticket_info = {
            "type": self.ticket_type,
            "creator_id": str(self.creator_id),
            "config": config["name"]
        }
        transcript = await generate_transcript(
            interaction.channel, 
            ticket_type=self.ticket_type,
            closer=interaction.user,
            ticket_info=ticket_info
        )
        
        # Update ticket status
        if interaction.channel.id in support_tickets:
            support_tickets[interaction.channel.id]["status"] = "closed"
            support_tickets[interaction.channel.id]["transcript_id"] = transcript["id"]
        
        # Send transcript to logs
        await send_transcript_log(interaction.guild, transcript, user=self.creator_id)
        
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
                dm_embed.add_field(name="📜 Transcript ID", value=f"`{transcript['id']}`", inline=False)
                await creator.send(embed=dm_embed)
            except:
                pass
        
        await log_action(
            interaction.guild,
            f"{config['emoji']} Ticket Closed",
            f"**Type:** {config['name']}\n**User:** <@{self.creator_id}>\n**Closed By:** {interaction.user.mention}\n**Messages:** {transcript['message_count']}\n**Transcript:** `{transcript['id']}`",
            0xe74c3c
        )
        
        await interaction.edit_original_response(content="🔒 Closing ticket in 5 seconds...")
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

# ==========================================
# STAGE TRANSFER SYSTEM
# ==========================================

# Stage transfer ranks (in order from highest to lowest)
STAGE_TRANSFER_RANKS = [
    "Stage 0〢FALLEN DEITY",
    "Stage 1〢FALLEN APEX", 
    "Stage 2〢FALLEN ASCENDANT",
    "Stage 3〢FORSAKEN WARRIOR",
    "Stage 4〢ABYSS-TOUCHED",
    "Stage 5〢BROKEN INITIATE",
]

# Rank levels for further detail
RANK_LEVELS = ["High", "Mid", "Low"]

# Strength evaluations
STRENGTH_LEVELS = ["Strong", "Stable", "Weak"]

# Clans that are accepted for proof
ACCEPTED_CLANS = ["TSBCC", "VALHALLA", "TSBER"]

class StageTransferView(discord.ui.View):
    """View for stage transfer panel"""
    def __init__(self):
        super().__init__(timeout=None)
    
    @discord.ui.button(label="Request Stage Transfer", style=discord.ButtonStyle.danger, emoji="📋", custom_id="stage_transfer_btn")
    async def request_transfer(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Open stage transfer ticket"""
        # Check if user already has an open transfer ticket
        category = discord.utils.get(interaction.guild.categories, name="Stage Transfers")
        if category:
            for channel in category.channels:
                if str(interaction.user.id) in channel.name:
                    return await interaction.response.send_message(
                        f"❌ You already have an open transfer request: {channel.mention}", 
                        ephemeral=True
                    )
        
        # Create ticket
        overwrites = {
            interaction.guild.default_role: discord.PermissionOverwrite(read_messages=False),
            interaction.user: discord.PermissionOverwrite(read_messages=True, send_messages=True, attach_files=True),
            interaction.guild.me: discord.PermissionOverwrite(read_messages=True, send_messages=True)
        }
        
        # Add staff access
        staff_role = discord.utils.get(interaction.guild.roles, name=STAFF_ROLE_NAME)
        if staff_role:
            overwrites[staff_role] = discord.PermissionOverwrite(read_messages=True, send_messages=True)
        
        # Add high staff access
        for role_name in HIGH_STAFF_ROLES:
            role = discord.utils.get(interaction.guild.roles, name=role_name)
            if role:
                overwrites[role] = discord.PermissionOverwrite(read_messages=True, send_messages=True)
        
        # Create or get category
        if not category:
            category = await interaction.guild.create_category("Stage Transfers")
        
        # Create ticket channel
        ticket_name = f"transfer-{interaction.user.name}"[:50].lower().replace(" ", "-")
        channel = await interaction.guild.create_text_channel(
            name=ticket_name,
            category=category,
            overwrites=overwrites
        )
        
        # Get user's current rank
        current_rank = "Unknown"
        for rank in STAGE_TRANSFER_RANKS:
            role = discord.utils.get(interaction.user.roles, name=rank)
            if role:
                current_rank = rank
                break
        
        embed = discord.Embed(
            title="📋 Stage Transfer Request",
            description=(
                f"**Requested by:** {interaction.user.mention}\n"
                f"**Current Rank:** {current_rank}\n\n"
                f"━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                f"**📸 Please upload proof of your rank from ONE of these clans:**\n"
                f"• **TSBCC**\n"
                f"• **VALHALLA**\n"
                f"• **TSBER**\n\n"
                f"**📌 Requirements:**\n"
                f"1️⃣ Screenshot must clearly show your **username**\n"
                f"2️⃣ Screenshot must show your **rank** in the clan\n"
                f"3️⃣ Screenshot must be **recent** (within 24 hours)\n\n"
                f"━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                f"**Upload your proof below and wait for staff to review.**"
            ),
            color=0x8B0000
        )
        embed.set_footer(text="Staff will approve or deny your request")
        
        await channel.send(
            content=f"{interaction.user.mention} {staff_role.mention if staff_role else ''}",
            embed=embed,
            view=StageTransferControlView()
        )
        
        await interaction.response.send_message(
            f"✅ Transfer request created! Go to {channel.mention}",
            ephemeral=True
        )
        
        await log_action(interaction.guild, "📋 Stage Transfer", f"{interaction.user.mention} opened a transfer request", 0x9b59b6)


class StageTransferControlView(discord.ui.View):
    """Control view for stage transfer tickets"""
    def __init__(self):
        super().__init__(timeout=None)
    
    @discord.ui.button(label="Approve", style=discord.ButtonStyle.success, emoji="✅", custom_id="transfer_approve")
    async def approve_transfer(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Approve the transfer - opens rank selection"""
        if not is_staff(interaction.user):
            return await interaction.response.send_message("❌ Staff only.", ephemeral=True)
        
        await interaction.response.send_message(
            "Select the rank to assign:",
            view=RankSelectView(interaction.channel),
            ephemeral=True
        )
    
    @discord.ui.button(label="Deny", style=discord.ButtonStyle.danger, emoji="❌", custom_id="transfer_deny")
    async def deny_transfer(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Deny the transfer"""
        if not is_staff(interaction.user):
            return await interaction.response.send_message("❌ Staff only.", ephemeral=True)
        
        # Send denial modal for reason
        await interaction.response.send_modal(TransferDenyModal(interaction.channel))
    
    @discord.ui.button(label="Close Ticket", style=discord.ButtonStyle.secondary, emoji="🔒", custom_id="transfer_close")
    async def close_ticket(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Close the ticket"""
        if not is_staff(interaction.user):
            return await interaction.response.send_message("❌ Staff only.", ephemeral=True)
        
        await interaction.response.send_message("🔒 Closing ticket in 5 seconds...")
        await asyncio.sleep(5)
        await interaction.channel.delete()


class RankSelectView(discord.ui.View):
    """View for selecting rank to assign - Stage, Rank Level, Strength"""
    def __init__(self, ticket_channel):
        super().__init__(timeout=300)
        self.ticket_channel = ticket_channel
        self.selected_stage = None
        self.selected_rank = None
        self.selected_strength = None
        
        # Stage dropdown
        stage_options = [
            discord.SelectOption(label="Stage 0", value="Stage 0〢FALLEN DEITY", description="FALLEN DEITY"),
            discord.SelectOption(label="Stage 1", value="Stage 1〢FALLEN APEX", description="FALLEN APEX"),
            discord.SelectOption(label="Stage 2", value="Stage 2〢FALLEN ASCENDANT", description="FALLEN ASCENDANT"),
            discord.SelectOption(label="Stage 3", value="Stage 3〢FORSAKEN WARRIOR", description="FORSAKEN WARRIOR"),
            discord.SelectOption(label="Stage 4", value="Stage 4〢ABYSS-TOUCHED", description="ABYSS-TOUCHED"),
            discord.SelectOption(label="Stage 5", value="Stage 5〢BROKEN INITIATE", description="BROKEN INITIATE"),
        ]
        
        self.stage_select = discord.ui.Select(
            placeholder="1️⃣ Select Stage (Required)...",
            options=stage_options,
            custom_id="stage_select_dropdown",
            row=0
        )
        self.stage_select.callback = self.select_stage
        self.add_item(self.stage_select)
        
        # Rank Level dropdown
        rank_options = [
            discord.SelectOption(label="High", value="High", description="High rank level"),
            discord.SelectOption(label="Mid", value="Mid", description="Mid rank level"),
            discord.SelectOption(label="Low", value="Low", description="Low rank level"),
            discord.SelectOption(label="Skip", value="skip", description="Don't assign rank level"),
        ]
        
        self.rank_select = discord.ui.Select(
            placeholder="2️⃣ Select Rank Level (Optional)...",
            options=rank_options,
            custom_id="rank_level_select_dropdown",
            row=1
        )
        self.rank_select.callback = self.select_rank_level
        self.add_item(self.rank_select)
        
        # Strength dropdown
        strength_options = [
            discord.SelectOption(label="Strong", value="Strong", description="Strong evaluation"),
            discord.SelectOption(label="Stable", value="Stable", description="Stable evaluation"),
            discord.SelectOption(label="Weak", value="Weak", description="Weak evaluation"),
            discord.SelectOption(label="Skip", value="skip", description="Don't assign strength"),
        ]
        
        self.strength_select = discord.ui.Select(
            placeholder="3️⃣ Select Strength (Optional)...",
            options=strength_options,
            custom_id="strength_select_dropdown",
            row=2
        )
        self.strength_select.callback = self.select_strength
        self.add_item(self.strength_select)
    
    async def select_stage(self, interaction: discord.Interaction):
        self.selected_stage = self.stage_select.values[0]
        await interaction.response.send_message(f"✅ Stage selected: **{self.selected_stage.split('〢')[0]}**\nNow select Rank Level and Strength, then click Confirm.", ephemeral=True)
    
    async def select_rank_level(self, interaction: discord.Interaction):
        value = self.rank_select.values[0]
        self.selected_rank = None if value == "skip" else value
        msg = f"✅ Rank Level: **{value}**" if value != "skip" else "✅ Rank Level: Skipped"
        await interaction.response.send_message(msg, ephemeral=True)
    
    async def select_strength(self, interaction: discord.Interaction):
        value = self.strength_select.values[0]
        self.selected_strength = None if value == "skip" else value
        msg = f"✅ Strength: **{value}**" if value != "skip" else "✅ Strength: Skipped"
        await interaction.response.send_message(msg, ephemeral=True)
    
    @discord.ui.button(label="✅ Confirm & Assign", style=discord.ButtonStyle.success, row=3)
    async def confirm_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Confirm and assign the selected roles"""
        if not self.selected_stage:
            return await interaction.response.send_message("❌ Please select a Stage first!", ephemeral=True)
        
        # Find the user who opened the ticket
        target_user = None
        async for message in self.ticket_channel.history(limit=5, oldest_first=True):
            if message.mentions:
                for mention in message.mentions:
                    if not mention.bot:
                        target_user = mention
                        break
            if target_user:
                break
        
        if not target_user:
            return await interaction.response.send_message("❌ Could not find the user who requested the transfer.", ephemeral=True)
        
        # All roles to potentially remove
        ALL_RESULT_ROLES = [
            "Stage 0〢FALLEN DEITY", "Stage 1〢FALLEN APEX", "Stage 2〢FALLEN ASCENDANT",
            "Stage 3〢FORSAKEN WARRIOR", "Stage 4〢ABYSS-TOUCHED", "Stage 5〢BROKEN INITIATE",
            "High", "Mid", "Low", "Strong", "Stable", "Weak"
        ]
        
        # Remove all current result roles
        roles_to_remove = []
        for role_name in ALL_RESULT_ROLES:
            role = discord.utils.get(interaction.guild.roles, name=role_name)
            if role and role in target_user.roles:
                roles_to_remove.append(role)
        
        if roles_to_remove:
            try:
                await target_user.remove_roles(*roles_to_remove)
                await asyncio.sleep(0.5)
            except Exception as e:
                print(f"Error removing roles: {e}")
        
        # Add new roles
        roles_to_add = []
        result_parts = []
        
        # Stage role (required)
        stage_role = discord.utils.get(interaction.guild.roles, name=self.selected_stage)
        if stage_role:
            roles_to_add.append(stage_role)
            result_parts.append(self.selected_stage.split("〢")[0])
        else:
            return await interaction.response.send_message(f"❌ Role `{self.selected_stage}` not found!", ephemeral=True)
        
        # Rank level (optional)
        if self.selected_rank:
            rank_role = discord.utils.get(interaction.guild.roles, name=self.selected_rank)
            if rank_role:
                roles_to_add.append(rank_role)
                result_parts.append(self.selected_rank)
        
        # Strength (optional)
        if self.selected_strength:
            strength_role = discord.utils.get(interaction.guild.roles, name=self.selected_strength)
            if strength_role:
                roles_to_add.append(strength_role)
                result_parts.append(self.selected_strength)
        
        # Add all roles
        try:
            await target_user.add_roles(*roles_to_add)
        except Exception as e:
            return await interaction.response.send_message(f"❌ Failed to add roles: {e}", ephemeral=True)
        
        result_str = ", ".join(result_parts)
        
        # Send approval message
        embed = discord.Embed(
            title="✅ Transfer Approved!",
            description=(
                f"**User:** {target_user.mention}\n"
                f"**Result:** {result_str}\n"
                f"**Approved by:** {interaction.user.mention}"
            ),
            color=0x2ecc71
        )
        embed.add_field(name="📊 Stage", value=self.selected_stage, inline=True)
        if self.selected_rank:
            embed.add_field(name="📈 Rank Level", value=self.selected_rank, inline=True)
        if self.selected_strength:
            embed.add_field(name="💪 Strength", value=self.selected_strength, inline=True)
        
        await self.ticket_channel.send(embed=embed)
        await interaction.response.send_message(f"✅ Assigned **{result_str}** to {target_user.display_name}", ephemeral=True)
        
        # Disable this view
        for item in self.children:
            item.disabled = True
        await interaction.message.edit(view=self)
        
        # Try to DM user
        try:
            dm_embed = discord.Embed(
                title="✅ Stage Transfer Approved!",
                description=f"Your transfer request has been approved!\n\n**Result:** {result_str}",
                color=0x2ecc71
            )
            await target_user.send(embed=dm_embed)
        except:
            pass
        
        await log_action(interaction.guild, "✅ Transfer Approved", f"{target_user.mention} → **{result_str}**\nApproved by: {interaction.user.mention}", 0x2ecc71)
    
    @discord.ui.button(label="❌ Cancel", style=discord.ButtonStyle.secondary, row=3)
    async def cancel_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_message("Cancelled.", ephemeral=True)
        await interaction.message.delete()


class TransferDenyModal(discord.ui.Modal, title="Deny Transfer Request"):
    """Modal for denying transfer with reason"""
    
    reason = discord.ui.TextInput(
        label="Reason for denial",
        placeholder="Enter the reason for denying this transfer...",
        style=discord.TextStyle.paragraph,
        required=True,
        max_length=500
    )
    
    def __init__(self, ticket_channel):
        super().__init__()
        self.ticket_channel = ticket_channel
    
    async def on_submit(self, interaction: discord.Interaction):
        # Find the user
        target_user = None
        async for message in self.ticket_channel.history(limit=5, oldest_first=True):
            if message.mentions:
                for mention in message.mentions:
                    if not mention.bot:
                        target_user = mention
                        break
            if target_user:
                break
        
        embed = discord.Embed(
            title="❌ Transfer Denied",
            description=(
                f"**User:** {target_user.mention if target_user else 'Unknown'}\n"
                f"**Denied by:** {interaction.user.mention}\n\n"
                f"**Reason:**\n{self.reason.value}"
            ),
            color=0xe74c3c
        )
        
        await self.ticket_channel.send(embed=embed)
        await interaction.response.send_message("❌ Transfer denied. Ticket will close in 30 seconds.", ephemeral=True)
        
        # Try to DM user
        if target_user:
            try:
                dm_embed = discord.Embed(
                    title="❌ Stage Transfer Denied",
                    description=f"Your transfer request has been denied.\n\n**Reason:** {self.reason.value}",
                    color=0xe74c3c
                )
                await target_user.send(embed=dm_embed)
            except:
                pass
        
        await log_action(interaction.guild, "❌ Transfer Denied", f"{target_user.mention if target_user else 'Unknown'}\nReason: {self.reason.value}", 0xe74c3c)
        
        # Close ticket after delay
        await asyncio.sleep(30)
        try:
            await self.ticket_channel.delete()
        except:
            pass


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
        
        # Deduct coins
        add_user_stat(interaction.user.id, "coins", -item["price"])
        
        # Handle different item types
        item_type = item.get("type", "ticket")
        
        if item_type == "ticket":
            # Opens a ticket for staff assistance
            await self.open_shop_ticket(interaction, item_id, item["name"])
        
        elif item_type == "coaching":
            # Open coaching selection
            await self.open_coaching_ticket(interaction)
        
        elif item_type == "consumable":
            # Add to inventory
            await self.add_to_inventory(interaction, item_id, item)
        
        elif item_type == "background":
            # Open background selection
            await interaction.response.send_message(
                "🖼️ **Custom Level Card Background**\n\n"
                "Use `/setbackground <url>` to set your custom background!\n"
                "• Image must be a direct URL (ends in .png, .jpg, etc.)\n"
                "• Recommended size: 900x300 pixels\n"
                "• Or use `/setbackground default` to reset",
                ephemeral=True
            )
            # Mark that they own the background feature
            data = load_data()
            uid = str(interaction.user.id)
            data = ensure_user_structure(data, uid)
            if "custom_level_bg" not in data["users"][uid].get("inventory", []):
                if "inventory" not in data["users"][uid]:
                    data["users"][uid]["inventory"] = []
                data["users"][uid]["inventory"].append("custom_level_bg")
            save_data(data)

    async def add_to_inventory(self, interaction: discord.Interaction, item_id: str, item: dict):
        """Add consumable item to user's inventory"""
        data = load_data()
        uid = str(interaction.user.id)
        data = ensure_user_structure(data, uid)
        
        if "inventory" not in data["users"][uid]:
            data["users"][uid]["inventory"] = []
        
        data["users"][uid]["inventory"].append(item_id)
        save_data(data)
        
        # Special handling for certain items
        if item_id == "elo_shield":
            update_user_data(interaction.user.id, "elo_shield_active", True)
            await interaction.response.send_message(
                "🛡️ **ELO Shield Activated!**\n"
                "Your next duel loss will not affect your ELO rating.\n"
                "*Shield is consumed after one loss.*",
                ephemeral=True
            )
        elif item_id == "streak_saver":
            update_user_data(interaction.user.id, "streak_saver_active", True)
            await interaction.response.send_message(
                "🔥 **Streak Saver Activated!**\n"
                "If you miss the next training, your streak will be protected.\n"
                "*Saver is consumed after one missed event.*",
                ephemeral=True
            )
        elif item_id == "training_reserve":
            update_user_data(interaction.user.id, "training_reserved", True)
            await interaction.response.send_message(
                "📋 **Training Slot Reserved!**\n"
                "You have a guaranteed spot in the next training.\n"
                "*Staff will be notified of your reservation.*",
                ephemeral=True
            )
            # Notify staff
            await log_action(interaction.guild, "📋 Training Reserve", f"{interaction.user.mention} reserved a training slot!", 0x3498db)
        else:
            await interaction.response.send_message(f"✅ **{item['name']}** added to your inventory!", ephemeral=True)

    async def open_shop_ticket(self, interaction: discord.Interaction, item_id: str, title: str):
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
            name=f"{item_id}-{interaction.user.name}"[:50], 
            category=cat, 
            overwrites=overwrites
        )
        
        embed = discord.Embed(
            title=title, 
            description=f"Purchase confirmed by {interaction.user.mention}.\nStaff will assist you shortly.", 
            color=0x2ecc71
        )
        await ch.send(f"{staff.mention if staff else ''}", embed=embed, view=TicketControlView(item_id))
        await interaction.response.send_message(f"✅ **Purchased!** Check {ch.mention}", ephemeral=True)
        await log_action(interaction.guild, "🛒 Purchase", f"User: {interaction.user.mention}\nItem: {title}", 0xF1C40F)

    async def open_coaching_ticket(self, interaction: discord.Interaction):
        """Open a coaching session ticket with coach selection"""
        # Find all coaches
        coach_role = discord.utils.get(interaction.guild.roles, name=COACHING_ROLE)
        
        if not coach_role or len(coach_role.members) == 0:
            # Refund if no coaches available
            add_user_stat(interaction.user.id, "coins", 1500)
            return await interaction.response.send_message(
                "❌ No coaches are currently available. You have been refunded.",
                ephemeral=True
            )
        
        # Create selection view
        await interaction.response.send_message(
            "🎯 **Select Your Coach**\n\nChoose who you'd like to train with:",
            view=CoachSelectView(interaction.user, coach_role.members),
            ephemeral=True
        )


class CoachSelectView(discord.ui.View):
    """View for selecting a coach for 1v1 session"""
    def __init__(self, buyer: discord.Member, coaches: list):
        super().__init__(timeout=120)
        self.buyer = buyer
        self.coaches = coaches[:25]  # Discord limit
        
        # Add coach select dropdown
        options = [
            discord.SelectOption(
                label=coach.display_name[:100],
                value=str(coach.id),
                description=f"Book session with {coach.display_name}"[:100]
            )
            for coach in self.coaches
        ]
        
        self.coach_select = discord.ui.Select(
            placeholder="Select a coach...",
            options=options,
            custom_id="coach_select"
        )
        self.coach_select.callback = self.select_coach
        self.add_item(self.coach_select)
    
    async def select_coach(self, interaction: discord.Interaction):
        if interaction.user.id != self.buyer.id:
            return await interaction.response.send_message("❌ This isn't your purchase!", ephemeral=True)
        
        coach_id = int(self.coach_select.values[0])
        coach = interaction.guild.get_member(coach_id)
        
        if not coach:
            return await interaction.response.send_message("❌ Coach not found!", ephemeral=True)
        
        # Create coaching ticket
        overwrites = {
            interaction.guild.default_role: discord.PermissionOverwrite(read_messages=False),
            self.buyer: discord.PermissionOverwrite(read_messages=True, send_messages=True),
            coach: discord.PermissionOverwrite(read_messages=True, send_messages=True),
            interaction.guild.me: discord.PermissionOverwrite(read_messages=True)
        }
        
        staff = discord.utils.get(interaction.guild.roles, name=STAFF_ROLE_NAME)
        if staff:
            overwrites[staff] = discord.PermissionOverwrite(read_messages=True, send_messages=True)
        
        cat = discord.utils.get(interaction.guild.categories, name="Coaching")
        if not cat:
            cat = await interaction.guild.create_category("Coaching")
        
        ch = await interaction.guild.create_text_channel(
            name=f"coaching-{self.buyer.name}-{coach.name}"[:50],
            category=cat,
            overwrites=overwrites
        )
        
        embed = discord.Embed(
            title="🎯 1v1 Coaching Session",
            description=(
                f"**Student:** {self.buyer.mention}\n"
                f"**Coach:** {coach.mention}\n\n"
                f"Coordinate your training session here!\n"
                f"Coach, please schedule a time that works for both of you."
            ),
            color=0x9b59b6
        )
        embed.set_footer(text="Close this ticket when the session is complete")
        
        await ch.send(f"{self.buyer.mention} {coach.mention}", embed=embed, view=TicketControlView("coaching"))
        await interaction.response.edit_message(content=f"✅ **Coaching booked!** Check {ch.mention}", view=None)
        
        # Notify coach
        try:
            dm_embed = discord.Embed(
                title="🎯 New Coaching Session!",
                description=f"**{self.buyer.display_name}** has booked a 1v1 coaching session with you!\n\nCheck {ch.mention} to coordinate.",
                color=0x9b59b6
            )
            await coach.send(embed=dm_embed)
        except:
            pass
        
        await log_action(interaction.guild, "🎯 Coaching Booked", f"Student: {self.buyer.mention}\nCoach: {coach.mention}", 0x9b59b6)


class ShopSelectView(discord.ui.View):
    """Dropdown shop for all items"""
    def __init__(self):
        super().__init__(timeout=None)
        
        options = [
            discord.SelectOption(
                label=item["name"].replace("⚔️ ", "").replace("🎨 ", "").replace("🛡️ ", "").replace("🔥 ", "").replace("📋 ", "").replace("🎯 ", "").replace("👑 ", "").replace("🖼️ ", "")[:100],
                value=item["id"],
                description=f"{item['price']} coins - {item['desc'][:50]}",
                emoji=item["name"][0] if item["name"][0] in "⚔️🎨🛡️🔥📋🎯👑🖼️" else "🛒"
            )
            for item in SHOP_ITEMS
        ]
        
        self.shop_select = discord.ui.Select(
            placeholder="Select an item to purchase...",
            options=options,
            custom_id="shop_select_menu"
        )
        self.shop_select.callback = self.purchase_item
        self.add_item(self.shop_select)
    
    async def purchase_item(self, interaction: discord.Interaction):
        item_id = self.shop_select.values[0]
        shop_view = ShopView()
        await shop_view.buy_item(interaction, item_id)

    @discord.ui.button(label="Buy Private Tryout (500 💰)", style=discord.ButtonStyle.primary, custom_id="shop_tryout", row=1)
    async def buy_tryout(self, interaction: discord.Interaction, button: discord.ui.Button): 
        shop_view = ShopView()
        await shop_view.buy_item(interaction, "private_tryout")
    
    @discord.ui.button(label="Buy Custom Role (2000 💰)", style=discord.ButtonStyle.secondary, custom_id="shop_role", row=1)
    async def buy_role(self, interaction: discord.Interaction, button: discord.ui.Button): 
        shop_view = ShopView()
        await shop_view.buy_item(interaction, "custom_role")

class HelpSelect(discord.ui.Select):
    def __init__(self):
        options = [
            discord.SelectOption(label="Member", emoji="👤", description="Basic member commands"),
            discord.SelectOption(label="Profile & Stats", emoji="📊", description="Profile, rank, stats"),
            discord.SelectOption(label="Events", emoji="📅", description="Trainings & tryouts"),
            discord.SelectOption(label="Duels & ELO", emoji="⚔️", description="1v1 duels & rankings"),
            discord.SelectOption(label="Spar Finder", emoji="⚔️", description="Tier-based spar matchmaking"),
            discord.SelectOption(label="Tournaments", emoji="🏆", description="Tournament system"),
            discord.SelectOption(label="Economy & Shop", emoji="💰", description="Coins, shop & items"),
            discord.SelectOption(label="Backup", emoji="🆘", description="Request backup help"),
            discord.SelectOption(label="Stage Transfer", emoji="📋", description="Rank transfers & results"),
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
                "`/verify` - Link your Roblox account\n\n"
                "**📊 Quick Stats**\n"
                "`/level` - View your level card\n"
                "`/rank` - View your rank card\n"
                "`/profile` - Full profile with all stats\n"
                "`/fcoins` - Check coin balance\n"
                "`/inventory` - View purchased items\n\n"
                "**🎁 Daily & Streaks**\n"
                "`/daily` - Claim daily reward (streak bonus!)\n"
                "`/attendance_streak` - View event streak\n\n"
                "**📅 Events**\n"
                "`/schedule` - View upcoming events\n"
                "Click RSVP buttons on event posts!"
            )
            
        elif self.values[0] == "Profile & Stats":
            e.title="📊 Profile & Statistics"
            e.description=(
                "**🖼️ Visual Cards**\n"
                "`/profile` - Full profile card with avatar\n"
                "`/rank` - Rank card with XP bar\n"
                "`/level` - Level card\n\n"
                "**📈 Statistics**\n"
                "`/stats` - Combat stats (W/L)\n"
                "`!mystats` - Detailed stats breakdown\n"
                "`!achievements` - View your badges\n"
                "`!activity` - Activity graph\n\n"
                "**🏆 Leaderboards**\n"
                "`/leaderboard` - XP leaderboard\n"
                "`!voicetop` - Voice time leaders\n"
                "`!topactive` - Most active this week\n"
                "`!serverstats` - Server statistics\n"
                "`!compare @user` - Compare with someone"
            )
        
        elif self.values[0] == "Duels & ELO":
            e.title="⚔️ Duels & ELO System"
            e.description=(
                "**⚔️ Duel Commands**\n"
                "`/duel @user` - Challenge to 1v1\n"
                "`/elo` - Check your ELO rating\n"
                "`/elo @user` - Check someone's ELO\n"
                "`!elo_leaderboard` - Top ranked players\n"
                "`!duel_history` - Your match history\n\n"
                "**🛡️ ELO Shield (Shop Item)**\n"
                "Protects you from ELO loss once!\n\n"
                "**🏅 ELO Ranks**\n"
                "🏆 Grandmaster (2000+)\n"
                "💎 Diamond (1800+)\n"
                "🥇 Platinum (1600+)\n"
                "🥈 Gold (1400+)\n"
                "🥉 Silver (1200+)\n"
                "⚔️ Bronze (1000+)\n\n"
                "*Win duels to climb!*"
            )
        
        elif self.values[0] == "Spar Finder":
            e.title="⚔️ Spar Finder"
            e.description=(
                "**⚔️ Tier-Based Matchmaking**\n"
                "Your Stage + Rank + Strength = Your Tier\n"
                "54 tiers total (lower = stronger)\n\n"
                "**Match Types:**\n"
                "⭐ Perfect (±1 tier)\n"
                "✅ Good (±2-3 tiers)\n"
                "⚠️ Fair (±4-6 tiers)\n\n"
                "**📋 Panel Buttons**\n"
                "• **🎯 Find Spar** - Join queue\n"
                "• **📋 View Queue** - See who's waiting\n"
                "• **🔍 Find Match** - Auto-find opponents\n"
                "• **⚔️ Challenge** - Pick directly\n\n"
                "**🎮 In Match Channel**\n"
                "• Post private server link\n"
                "• Play your set (FT5, FT10, etc.)\n"
                "• Post proof & submit result\n"
                "• Rate your partner!\n\n"
                "**📊 Commands**\n"
                "`/practice_stats` - View your stats"
            )
        
        elif self.values[0] == "Tournaments":
            e.title="🏆 Tournament System V3"
            e.description=(
                "**👤 How to Participate**\n"
                "• Click **Register** on tournament portal\n"
                "• Click **Leave** to withdraw\n"
                "• Click **Spectate** to watch\n\n"
                "**⚔️ During Matches**\n"
                "• Match threads created automatically\n"
                "• Staff click **Report Score** button\n"
                "• Bracket image updates live!\n\n"
                "**🛠️ Staff Commands**\n"
                "`!tournament` — Create new tournament\n"
                "`!bracket` — View current bracket\n"
                "`!tparticipants` — View registered players\n"
                "`!tstatus` — Tournament status\n"
                "`!tsetwinner @user` — Set winner manually\n"
                "`!tendtournament` — End tournament\n"
                "`!tdeletetournament` — Delete tournament\n\n"
                "**🎁 End of Tournament**\n"
                "• 🏆 Update Top 10 with winner\n"
                "• 🎁 Give rewards (coins + XP)\n"
                "• 📢 Publish results\n\n"
                "**💰 Reward Amounts**\n"
                "🥇 1st: 5,000 coins + 500 XP\n"
                "🥈 2nd: 2,500 coins + 250 XP\n"
                "🥉 3rd: 1,000 coins + 100 XP"
            )
            
        elif self.values[0] == "Events":
            e.title="📅 Events (Trainings & Tryouts)"
            e.description=(
                "**👤 Member Commands**\n"
                "`/schedule` - View upcoming events\n"
                "`/event list` - All scheduled events\n"
                "`/attendance_streak` - Your streak\n\n"
                "**💰 Attendance Rewards**\n"
                "• Training: 100 coins + 50 XP\n"
                "• Tryout: 150 coins + 75 XP\n"
                "• Host: 300 coins + 100 XP\n\n"
                "**🔥 Streak Bonuses**\n"
                "• 3 streak: +50 | 5: +100\n"
                "• 7 streak: +200 | 10: +500\n\n"
                "**🎖️ Attendance Roles**\n"
                "5→Fallen Initiate | 15→Disciple\n"
                "30→Warrior | 50→Slayer | 100→Immortal\n\n"
                "**🔥 Streak Roles**\n"
                "3→♰Shadow Initiate | 5→♰Rising Shadow\n"
                "10→♰Relentless | 20→♰Undying | 50→♰Eternal"
            )
            
        elif self.values[0] == "Economy & Shop": 
            e.title="💰 Economy & Shop"
            e.description=(
                "**💵 Earning Coins**\n"
                "• Chat messages & reactions\n"
                "• Voice channel time\n"
                "• Attend trainings/tryouts\n"
                "• `/daily` rewards & streaks\n"
                "• Win duels & raids\n\n"
                "**📜 Commands**\n"
                "`/fcoins` - Check balance\n"
                "`/inventory` - View items\n"
                "`/setbackground <url>` - Custom bg\n\n"
                "**🛒 Shop Items**\n"
                "• Private Tryout (500)\n"
                "• Custom Role (2000)\n"
                "• Custom Role Color (1500)\n"
                "• Hoisted Role (5000)\n"
                "• Custom Level BG (3000)\n"
                "• ELO Shield (1000)\n"
                "• Streak Saver (1500)\n"
                "• Training Reserve (300)\n"
                "• Coaching Session (1500)"
            )
            
        elif self.values[0] == "Backup":
            e.title="🆘 Backup System"
            e.description=(
                "**🆘 Request Backup**\n"
                "`/backup` or `!backup`\n"
                "Opens a form to request backup!\n\n"
                "**📋 Requirements:**\n"
                "• List at least **3 enemies**\n"
                "• Include a valid **invite link**\n\n"
                "**🔗 Valid Links:**\n"
                "• Roblox Invite: `roblox.com/share?code=...`\n"
                "• RO-PRO: `ro.pro/XXXXXX`\n\n"
                "**📢 What Happens:**\n"
                "Your request pings @Backup Ping\n"
                "so members can join and help!\n\n"
                "**⚠️ Rules:**\n"
                "• Don't spam backup requests\n"
                "• Only use for real situations\n"
                "• Include accurate enemy count"
            )
        
        elif self.values[0] == "Stage Transfer":
            e.title="📋 Stage Transfer & Results"
            e.description=(
                "**📋 Request a Transfer**\n"
                "Click **Stage Transfer** button\n"
                "Upload proof from: TSBCC, VALHALLA, TSBER\n"
                "Staff will approve/deny\n\n"
                "**📸 Proof Requirements**\n"
                "• Shows your username + rank\n"
                "• Recent (within 24 hours)\n\n"
                "**📊 Stage Ranks**\n"
                "Stage 0 - FALLEN DEITY\n"
                "Stage 1 - FALLEN APEX\n"
                "Stage 2 - FALLEN ASCENDANT\n"
                "Stage 3 - FORSAKEN WARRIOR\n"
                "Stage 4 - ABYSS-TOUCHED\n"
                "Stage 5 - BROKEN INITIATE\n\n"
                "**📈 Rank Levels:** High/Mid/Low/Stable\n"
                "**💪 Strength:** Strong/Moderate/Weak\n\n"
                "**🛡️ Staff:** `/result @user <stage> [rank] [str]`"
            )
            
        elif self.values[0] == "Staff": 
            e.title="🛡️ Staff Commands"
            e.description=(
                "**🏆 Tournaments**\n"
                "`!setup_tournament` - Setup panel with modal\n"
                "`!tournament_panel` `!tournament_end confirm`\n\n"
                "**📢 Activity Checks**\n"
                "`!activitycheck [duration] [msg]`\n"
                "View Results shows visual image!\n"
                "`!activitystats` `!checkparticipation @user`\n\n"
                "**🎉 Giveaways**\n"
                "`!giveaway <duration> <winners> <prize>`\n"
                "`!giveaway_req <dur> <win> <lvl> <prize>`\n\n"
                "**🎭 Mass Roles**\n"
                "`!massrole add/remove @Role target`\n"
                "`!giverole @Role` `!takerole @Role`\n"
                "`!inrole @Role` `!roleinfo @Role`\n\n"
                "**📊 Levels & Economy**\n"
                "`!addxp` `!removexp` `!setlevel`\n"
                "`!addfcoins` `!removefcoins`\n\n"
                "**🛡️ Inactivity (Mainer only)**\n"
                "`!inactivity_check` - Check all Mainers\n"
                "`!inactivity_strikes @user` - View strikes\n\n"
                "**🔨 Moderation**\n"
                "`/warn` `/warnings` `!promote` `!demote`"
            )
            
        elif self.values[0] == "Admin":
            e.title="⚙️ Admin Commands"
            e.description=(
                "**📋 Setup Panels** (prefix: `!`)\n"
                "`!setup_verify` `!setup_tickets`\n"
                "`!setup_shop` `!setup_transfer`\n"
                "`!setup_practice` `!setup_attendance`\n"
                "`!setup_staffpanel` `!setup_applications`\n"
                "`!setup_tournament` `!setup_modlog`\n\n"
                "**🏆 Tournament Setup Panel**\n"
                "`!setup_tournament` creates a panel\n"
                "Click 'Create Tournament' → Fill modal:\n"
                "• Name, Channel ID, Role, Max, Top10\n\n"
                "**🛡️ Inactivity Config**\n"
                "Only tracks members with **Mainer** role\n"
                "No stage required - just Mainer!\n\n"
                "**📊 Management**\n"
                "`!archive_old_apps <days>`\n"
                "`!db_status` - Database status\n\n"
                "**⚙️ Sync**\n"
                "`!sync` `!clearsync`"
            )
        
        e.set_footer(text="The Fallen Bot • / = slash • ! = prefix")
        await interaction.response.edit_message(embed=e)

class HelpView(discord.ui.View):
    def __init__(self): 
        super().__init__(timeout=180)
        self.add_item(HelpSelect())

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


# ==========================================
# SERVER INFO PANEL SYSTEM
# ==========================================

class ServerInfoView(discord.ui.View):
    """Main server info panel with category buttons"""
    def __init__(self):
        super().__init__(timeout=None)
    
    @discord.ui.button(label="🎖️ High Ranks", style=discord.ButtonStyle.secondary, custom_id="info_high_ranks", row=0)
    async def high_ranks(self, interaction: discord.Interaction, button: discord.ui.Button):
        embed = discord.Embed(
            title="🎖️ ✦ HIGH RANKS ✦",
            description="**The leadership of The Fallen.**",
            color=0x8B0000
        )
        embed.add_field(
            name="👑 Owner",
            value="Supreme authority over The Fallen. Final say on all matters.",
            inline=False
        )
        embed.add_field(
            name="⚔️ Co-Owner",
            value="Second in command. Manages high-level operations and staff.",
            inline=False
        )
        embed.add_field(
            name="🛡️ Head Staff",
            value="Oversees all staff members. Handles promotions and demotions.",
            inline=False
        )
        embed.add_field(
            name="📋 Staff",
            value="Moderates the server, hosts events, manages members.",
            inline=False
        )
        embed.add_field(
            name="🎯 Trial Staff",
            value="Probationary staff. Proving their worth before full promotion.",
            inline=False
        )
        embed.add_field(
            name="⚠️ How to Become Staff",
            value="• Be active and trusted\n• Apply when applications open\n• Show leadership qualities\n• No begging or asking",
            inline=False
        )
        embed.set_footer(text="✝ The Fallen ✝ • Staff roles are earned through trust")
        await interaction.response.send_message(embed=embed, ephemeral=True)
    
    @discord.ui.button(label="💀 Low Ranks", style=discord.ButtonStyle.secondary, custom_id="info_low_ranks", row=0)
    async def low_ranks(self, interaction: discord.Interaction, button: discord.ui.Button):
        embed = discord.Embed(
            title="💀 ✦ MEMBER RANKS (STAGES) ✦",
            description="**Combat ranks within The Fallen.**\nEarned through tryouts and performance.",
            color=0x8B0000
        )
        embed.add_field(
            name="⭐ Stage 5 — Elite",
            value="The best of the best. Top performers.",
            inline=False
        )
        embed.add_field(
            name="⭐ Stage 4 — Veteran",
            value="Highly skilled and proven in combat.",
            inline=False
        )
        embed.add_field(
            name="⭐ Stage 3 — Experienced",
            value="Solid skill level, consistent performer.",
            inline=False
        )
        embed.add_field(
            name="⭐ Stage 2 — Intermediate",
            value="Developing skills, shows potential.",
            inline=False
        )
        embed.add_field(
            name="⭐ Stage 1 — Beginner",
            value="New to competitive play, learning.",
            inline=False
        )
        embed.add_field(
            name="⭐ Stage 0 — Unranked",
            value="Just joined, needs to tryout.",
            inline=False
        )
        embed.add_field(
            name="🎮 Mainer",
            value="Full member of The Fallen. Base rank for all members.",
            inline=False
        )
        embed.add_field(
            name="📈 How to Rank Up",
            value="• Attend tryouts\n• Perform well in scrims/wars\n• Show consistency\n• Be active",
            inline=False
        )
        embed.set_footer(text="✝ The Fallen ✝ • Ranks are earned, not requested")
        await interaction.response.send_message(embed=embed, ephemeral=True)
    
    @discord.ui.button(label="⚔️ Raid Ranks", style=discord.ButtonStyle.secondary, custom_id="info_raid_ranks", row=0)
    async def raid_ranks(self, interaction: discord.Interaction, button: discord.ui.Button):
        embed = discord.Embed(
            title="⚔️ ✦ RAID/WAR RANKS ✦",
            description="**Performance-based roles for clan battles.**",
            color=0x8B0000
        )
        embed.add_field(
            name="🔥 Raid Leader",
            value="Leads raids and coordinates attacks. Calls strats.",
            inline=False
        )
        embed.add_field(
            name="⚔️ War Veteran",
            value="Experienced in clan wars. Reliable in battle.",
            inline=False
        )
        embed.add_field(
            name="🎯 Raider",
            value="Active participant in raids and wars.",
            inline=False
        )
        embed.add_field(
            name="📊 How to Earn",
            value="• Participate in clan wars\n• Show up consistently\n• Perform well in raids\n• Follow raid leader calls",
            inline=False
        )
        embed.set_footer(text="✝ The Fallen ✝ • War roles are earned through battle")
        await interaction.response.send_message(embed=embed, ephemeral=True)
    
    @discord.ui.button(label="📊 Activity Ranks", style=discord.ButtonStyle.secondary, custom_id="info_activity_ranks", row=0)
    async def activity_ranks(self, interaction: discord.Interaction, button: discord.ui.Button):
        embed = discord.Embed(
            title="📊 ✦ ACTIVITY RANKS ✦",
            description="**Level roles earned through activity.**\nGain XP by chatting, joining VC, and participating.",
            color=0x8B0000
        )
        embed.add_field(
            name="How XP Works",
            value="• **Chat:** 15-25 XP per message (60s cooldown)\n• **Voice:** 10-20 XP per 2 minutes\n• **Events:** Bonus XP for attending\n• **Daily:** Claim daily rewards",
            inline=False
        )
        embed.add_field(
            name="Level Milestones",
            value="• Level 5 → Faint Emberling\n• Level 10 → Initiate of Shadows\n• Level 20 → Abysswalk Student\n• Level 50 → Bearer of Abyssal Echo\n• Level 100 → Abyssforged Warden\n• Level 200 → Eternal Shadow Sovereign",
            inline=False
        )
        embed.add_field(
            name="Benefits",
            value="• Higher levels = more recognition\n• Coin rewards at milestones\n• Special channel access\n• Flex on the leaderboard",
            inline=False
        )
        embed.set_footer(text="✝ The Fallen ✝ • Check !level to see your progress")
        await interaction.response.send_message(embed=embed, ephemeral=True)


class ServerInfoLevelsView(discord.ui.View):
    """Level perks buttons"""
    def __init__(self):
        super().__init__(timeout=None)
    
    @discord.ui.button(label="Lower Levels (5 - 80)", style=discord.ButtonStyle.success, custom_id="info_levels_low", row=0)
    async def lower_levels(self, interaction: discord.Interaction, button: discord.ui.Button):
        embed = discord.Embed(
            title="📈 ✦ LOWER LEVELS (5 - 80) ✦",
            description="**Early level rewards and roles.**",
            color=0x2ecc71
        )
        embed.add_field(
            name="Level 5 — Faint Emberling",
            value="💰 50 coins",
            inline=True
        )
        embed.add_field(
            name="Level 10 — Initiate of Shadows",
            value="💰 100 coins",
            inline=True
        )
        embed.add_field(
            name="Level 20 — Abysswalk Student",
            value="💰 200 coins",
            inline=True
        )
        embed.add_field(
            name="Level 30 — Twilight Disciple",
            value="💰 400 coins",
            inline=True
        )
        embed.add_field(
            name="Level 40 — Duskforged Aspirant",
            value="💰 600 coins",
            inline=True
        )
        embed.add_field(
            name="Level 50 — Bearer of Abyssal Echo",
            value="💰 1,000 coins",
            inline=True
        )
        embed.add_field(
            name="Level 60 — Nightwoven Adept",
            value="💰 1,500 coins",
            inline=True
        )
        embed.add_field(
            name="Level 70 — Veilmarked Veteran",
            value="💰 2,000 coins",
            inline=True
        )
        embed.add_field(
            name="Level 80 — Shadowborn Ascendant",
            value="💰 2,500 coins",
            inline=True
        )
        embed.set_footer(text="✝ The Fallen ✝ • Keep grinding!")
        await interaction.response.send_message(embed=embed, ephemeral=True)
    
    @discord.ui.button(label="Higher Levels (100 - 200)", style=discord.ButtonStyle.danger, custom_id="info_levels_high", row=0)
    async def higher_levels(self, interaction: discord.Interaction, button: discord.ui.Button):
        embed = discord.Embed(
            title="🔥 ✦ HIGHER LEVELS (100 - 200) ✦",
            description="**Elite level rewards and roles.**",
            color=0xe74c3c
        )
        embed.add_field(
            name="Level 100 — Abyssforged Warden",
            value="💰 5,000 coins\n⭐ Elite recognition",
            inline=True
        )
        embed.add_field(
            name="Level 120 — Eclipsed Oathbearer",
            value="💰 7,500 coins\n⭐ Veteran status",
            inline=True
        )
        embed.add_field(
            name="Level 140 — Harbinger of Dusk",
            value="💰 10,000 coins\n⭐ Respected member",
            inline=True
        )
        embed.add_field(
            name="Level 160 — Ascended Dreadkeeper",
            value="💰 15,000 coins\n⭐ Top tier",
            inline=True
        )
        embed.add_field(
            name="Level 200 — Eternal Shadow Sovereign",
            value="💰 50,000 coins\n👑 Maximum prestige\n🏆 The highest honor",
            inline=False
        )
        embed.add_field(
            name="💡 Tips for High Levels",
            value="• Stay consistently active\n• Attend all events\n• Use daily rewards\n• Chat in voice channels",
            inline=False
        )
        embed.set_footer(text="✝ The Fallen ✝ • Only the dedicated reach 200")
        await interaction.response.send_message(embed=embed, ephemeral=True)


class ServerInfoBoosterView(discord.ui.View):
    """Booster info button"""
    def __init__(self):
        super().__init__(timeout=None)
    
    @discord.ui.button(label="💎 Booster Info", style=discord.ButtonStyle.secondary, custom_id="info_booster", row=0)
    async def booster_info(self, interaction: discord.Interaction, button: discord.ui.Button):
        embed = discord.Embed(
            title="💎 ✦ BOOSTER PERKS ✦",
            description="**Support The Fallen and be rewarded.**\nBoost the server to unlock exclusive benefits.",
            color=0xf47fff
        )
        embed.add_field(
            name="🎭 Exclusive Role",
            value="Special Booster role with unique color",
            inline=False
        )
        embed.add_field(
            name="⚡ Priority Access",
            value="• First pick for events & trainings\n• Priority in tryout queues\n• Access to booster-only events",
            inline=False
        )
        embed.add_field(
            name="💬 Special Channels",
            value="• Booster lounge access\n• Behind-the-scenes chat\n• Direct line to staff",
            inline=False
        )
        embed.add_field(
            name="🏆 Recognition",
            value="• Special mention in announcements\n• Booster badge on profile\n• Appreciation from the clan",
            inline=False
        )
        embed.add_field(
            name="📋 Faster Response",
            value="• Applications reviewed first\n• Support tickets prioritized\n• Questions answered faster",
            inline=False
        )
        embed.add_field(
            name="💰 Bonus Rewards",
            value="• 2x daily coin bonus\n• Extra XP multiplier\n• Exclusive shop items",
            inline=False
        )
        embed.set_footer(text="✝ The Fallen ✝ • Thank you for supporting us!")
        await interaction.response.send_message(embed=embed, ephemeral=True)


class ServerInfoBotView(discord.ui.View):
    """Bot info button"""
    def __init__(self):
        super().__init__(timeout=None)
    
    @discord.ui.button(label="🤖 Bot Info", style=discord.ButtonStyle.secondary, custom_id="info_bot", row=0)
    async def bot_info(self, interaction: discord.Interaction, button: discord.ui.Button):
        embed = discord.Embed(
            title="🤖 ✦ BOT COMMANDS ✦",
            description="**The Fallen Bot — Your clan companion.**",
            color=0x3498db
        )
        embed.add_field(
            name="📊 Profile & Stats",
            value="`!profile` — View your profile\n`!level` — Check your level\n`!rank` — See leaderboard position\n`!stats` — Detailed statistics",
            inline=False
        )
        embed.add_field(
            name="💰 Economy",
            value="`!daily` — Claim daily reward\n`!balance` — Check coins\n`!shop` — Buy items\n`!inventory` — View owned items",
            inline=False
        )
        embed.add_field(
            name="🏆 Leaderboards",
            value="`!leaderboard` — XP rankings\n`!top10` — Combat top 10\n`!elolb` — ELO rankings\n`!coinlb` — Richest members",
            inline=False
        )
        embed.add_field(
            name="⚔️ Competitive",
            value="`!duel @user` — 1v1 duel\n`!elo` — Check ELO rating\n`!record` — Win/loss record",
            inline=False
        )
        embed.add_field(
            name="🎮 Fun",
            value="`!achievements` — View badges\n`!coinflip` — Gamble coins\n`!train` — Training mode",
            inline=False
        )
        embed.add_field(
            name="📋 Info",
            value="`!help` — Full command list\n`!roster` — Clan roster\n`!events` — Upcoming events",
            inline=False
        )
        embed.set_footer(text="✝ The Fallen ✝ • Use !help for full command list")
        await interaction.response.send_message(embed=embed, ephemeral=True)


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
        try:
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
            
            # Defer FIRST before any API calls
            await interaction.response.defer(ephemeral=True)
            
            # Try to look up the Roblox user to get their ID (with timeout protection)
            roblox_id = None
            try:
                roblox_user = await asyncio.wait_for(
                    get_roblox_user_by_username(roblox_username),
                    timeout=5.0  # 5 second timeout
                )
                if roblox_user:
                    roblox_username = roblox_user["name"]  # Use correct capitalization
                    roblox_id = roblox_user["id"]
            except asyncio.TimeoutError:
                print(f"Roblox API timeout for {roblox_username}")
            except Exception as e:
                print(f"Roblox API error: {e}")
            
            # Give roles
            roles_given = []
            roles_removed = []
            
            # Remove Unverified
            unverified = discord.utils.get(guild.roles, name=UNVERIFIED_ROLE_NAME)
            if unverified and unverified in member.roles:
                try:
                    await member.remove_roles(unverified)
                    roles_removed.append(unverified.name)
                    await asyncio.sleep(0.5)  # Small delay between role operations
                except:
                    pass
            
            # Add Verified (Fallen's own verified role, can be different from Bloxlink's)
            fallen_verified = discord.utils.get(guild.roles, name=FALLEN_VERIFIED_ROLE)
            if fallen_verified and fallen_verified not in member.roles:
                try:
                    await member.add_roles(fallen_verified)
                    roles_given.append(fallen_verified.name)
                    await asyncio.sleep(0.5)  # Small delay between role operations
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
            
            # Check achievements (don't wait for this)
            asyncio.create_task(check_new_achievements(member.id, guild))
            
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
            
            # Log to dashboard (don't wait for this)
            asyncio.create_task(log_to_dashboard(
                guild, "✅ VERIFY", "Member Verified",
                f"{member.mention} verified as **{roblox_username}**",
                color=0x2ecc71,
                fields={"Method": "Bloxlink Quick Verify", "Roblox": roblox_username}
            ))
        
        except discord.errors.NotFound:
            # Interaction expired
            pass
        except Exception as e:
            print(f"Verify button error: {e}")
            try:
                await interaction.followup.send(
                    f"❌ An error occurred during verification. Please try again.",
                    ephemeral=True
                )
            except:
                pass

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
        self.add_view(TournamentAdminView(""))
        self.add_view(TournamentRegistrationView(""))
        self.add_view(MatchScoreView("", ""))
        self.add_view(TournamentEndView(""))
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
        self.add_view(StageTransferView())
        self.add_view(StageTransferControlView())
        self.add_view(PracticeQueueView())
        self.add_view(AttendanceLoggingView())
        self.add_view(StaffQuickActionsView())
        self.add_view(ActivityCheckView())
        self.add_view(ActivityCheckControlView())
        self.add_view(GiveawayView())
        self.add_view(GiveawayControlView())
        self.add_view(ServerInfoView())
        self.add_view(ServerInfoLevelsView())
        self.add_view(ServerInfoBoosterView())
        self.add_view(ServerInfoBotView())
        
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
        minutes_from_now="Minutes from now (e.g., 30 = starts in 30 mins)",
        server_link="Private server link (posted 5 mins before event)"
    )
    @app_commands.choices(event_type=[
        app_commands.Choice(name="Training", value="training"),
        app_commands.Choice(name="Tryout", value="tryout"),
    ])
    async def event_create(self, interaction: discord.Interaction, event_type: str, title: str, minutes_from_now: int, server_link: str = None):
        """Create a scheduled event with RSVP and reminders"""
        try:
            if not is_staff(interaction.user):
                return await interaction.response.send_message("❌ Staff only.", ephemeral=True)
            
            if minutes_from_now < 1:
                return await interaction.response.send_message("❌ Time must be at least 1 minute from now.", ephemeral=True)
            
            if minutes_from_now > 10080:  # 7 days max
                return await interaction.response.send_message("❌ Time must be within 7 days.", ephemeral=True)
            
            # Validate server link if provided
            if server_link and not server_link.startswith("https://"):
                return await interaction.response.send_message("❌ Server link must start with https://", ephemeral=True)
            
            # Calculate the actual scheduled time
            scheduled_time = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(minutes=minutes_from_now)
            
            event = create_event(
                event_type=event_type.lower(),
                title=title,
                scheduled_time=scheduled_time.isoformat(),
                host_id=interaction.user.id,
                channel_id=interaction.channel.id,
                server_link=server_link
            )
            
            ping_role_name = TRAINING_PING_ROLE if event_type.lower() == "training" else TRYOUT_PING_ROLE
            ping_role = discord.utils.get(interaction.guild.roles, name=ping_role_name)
            
            embed = await create_event_embed(event, interaction.guild)
            
            # Add server link info if provided
            if server_link:
                embed.add_field(name="🔗 Server Link", value="Will be posted 5 minutes before event!", inline=False)
            
            ping_text = ping_role.mention if ping_role else ""
            await interaction.response.send_message(content=ping_text, embed=embed, view=EventRSVPView(event["id"]))
        except Exception as e:
            print(f"Event create error: {e}")
            import traceback
            traceback.print_exc()
            if not interaction.response.is_done():
                await interaction.response.send_message(f"❌ Error creating event: {str(e)}", ephemeral=True)
            else:
                await interaction.followup.send(f"❌ Error creating event: {str(e)}", ephemeral=True)
    
    @app_commands.command(name="schedule", description="Staff: Schedule event at specific time")
    @app_commands.describe(
        event_type="Type of event",
        title="Event title",
        hour="Hour (0-23, in UTC)",
        minute="Minute (0-59)",
        server_link="Private server link (posted 5 mins before event)"
    )
    @app_commands.choices(event_type=[
        app_commands.Choice(name="Training", value="training"),
        app_commands.Choice(name="Tryout", value="tryout"),
    ])
    async def event_schedule(self, interaction: discord.Interaction, event_type: str, title: str, hour: int, minute: int = 0, server_link: str = None):
        """Schedule event at a specific UTC time today or tomorrow"""
        if not is_staff(interaction.user):
            return await interaction.response.send_message("❌ Staff only.", ephemeral=True)
        
        if hour < 0 or hour > 23:
            return await interaction.response.send_message("❌ Hour must be 0-23.", ephemeral=True)
        if minute < 0 or minute > 59:
            return await interaction.response.send_message("❌ Minute must be 0-59.", ephemeral=True)
        
        # Validate server link if provided
        if server_link and not server_link.startswith("https://"):
            return await interaction.response.send_message("❌ Server link must start with https://", ephemeral=True)
        
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
            channel_id=interaction.channel.id,
            server_link=server_link
        )
        
        ping_role_name = TRAINING_PING_ROLE if event_type.lower() == "training" else TRYOUT_PING_ROLE
        ping_role = discord.utils.get(interaction.guild.roles, name=ping_role_name)
        
        embed = await create_event_embed(event, interaction.guild)
        
        # Add server link info if provided
        if server_link:
            embed.add_field(name="🔗 Server Link", value="Will be posted 5 minutes before event!", inline=False)
        
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
    
    @app_commands.command(name="recurring_add", description="Admin: Create a recurring weekly event")
    @app_commands.describe(
        event_type="Type of event",
        title="Event title",
        day="Day of the week",
        hour="Hour (0-23, UTC)",
        minute="Minute (0-59)"
    )
    @app_commands.choices(event_type=[
        app_commands.Choice(name="Training", value="training"),
        app_commands.Choice(name="Tryout", value="tryout"),
    ])
    @app_commands.choices(day=[
        app_commands.Choice(name="Monday", value=0),
        app_commands.Choice(name="Tuesday", value=1),
        app_commands.Choice(name="Wednesday", value=2),
        app_commands.Choice(name="Thursday", value=3),
        app_commands.Choice(name="Friday", value=4),
        app_commands.Choice(name="Saturday", value=5),
        app_commands.Choice(name="Sunday", value=6),
    ])
    @app_commands.checks.has_permissions(administrator=True)
    async def event_recurring_add(self, interaction: discord.Interaction, event_type: str, title: str, day: int, hour: int, minute: int = 0):
        """Create a recurring weekly event"""
        if hour < 0 or hour > 23:
            return await interaction.response.send_message("❌ Hour must be 0-23.", ephemeral=True)
        if minute < 0 or minute > 59:
            return await interaction.response.send_message("❌ Minute must be 0-59.", ephemeral=True)
        
        recurring = create_recurring_event(
            event_type=event_type,
            title=title,
            day_of_week=day,
            hour=hour,
            minute=minute,
            channel_id=interaction.channel.id,
            created_by=interaction.user.id
        )
        
        day_name = DAYS_OF_WEEK[day]
        time_str = f"{hour:02d}:{minute:02d} UTC"
        
        embed = discord.Embed(
            title="🔁 Recurring Event Created!",
            description=(
                f"**Type:** {event_type.title()}\n"
                f"**Title:** {title}\n"
                f"**Schedule:** Every **{day_name}** at **{time_str}**\n"
                f"**Channel:** {interaction.channel.mention}\n\n"
                f"The bot will automatically create this event every week!"
            ),
            color=0x2ecc71
        )
        embed.set_footer(text=f"ID: {recurring['id']}")
        
        await interaction.response.send_message(embed=embed)
    
    @app_commands.command(name="recurring_list", description="View all recurring events")
    async def event_recurring_list(self, interaction: discord.Interaction):
        """View all recurring events"""
        recurring_events = get_recurring_events()
        
        if not recurring_events:
            return await interaction.response.send_message("📅 No recurring events set up!", ephemeral=True)
        
        embed = discord.Embed(
            title="🔁 Recurring Events",
            color=0x3498db
        )
        
        for event in recurring_events:
            day_name = DAYS_OF_WEEK[event["day_of_week"]]
            time_str = f"{event['hour']:02d}:{event['minute']:02d} UTC"
            status = "✅ Enabled" if event.get("enabled", True) else "❌ Disabled"
            emoji = "📚" if event["type"] == "training" else "🎯"
            
            channel = interaction.guild.get_channel(int(event["channel_id"]))
            channel_str = channel.mention if channel else "Unknown"
            
            embed.add_field(
                name=f"{emoji} {event['title']}",
                value=(
                    f"**When:** {day_name} @ {time_str}\n"
                    f"**Channel:** {channel_str}\n"
                    f"**Status:** {status}\n"
                    f"**ID:** `{event['id']}`"
                ),
                inline=False
            )
        
        await interaction.response.send_message(embed=embed)
    
    @app_commands.command(name="recurring_remove", description="Admin: Remove a recurring event")
    @app_commands.describe(recurring_id="The recurring event ID to remove")
    @app_commands.checks.has_permissions(administrator=True)
    async def event_recurring_remove(self, interaction: discord.Interaction, recurring_id: str):
        """Remove a recurring event"""
        removed = delete_recurring_event(recurring_id)
        
        if not removed:
            return await interaction.response.send_message("❌ Recurring event not found!", ephemeral=True)
        
        await interaction.response.send_message(f"✅ Recurring event **{removed['title']}** has been removed.")
    
    @app_commands.command(name="recurring_toggle", description="Admin: Enable or disable a recurring event")
    @app_commands.describe(recurring_id="The recurring event ID", enabled="Enable or disable")
    @app_commands.checks.has_permissions(administrator=True)
    async def event_recurring_toggle(self, interaction: discord.Interaction, recurring_id: str, enabled: bool):
        """Toggle a recurring event on/off"""
        event = toggle_recurring_event(recurring_id, enabled)
        
        if not event:
            return await interaction.response.send_message("❌ Recurring event not found!", ephemeral=True)
        
        status = "enabled" if enabled else "disabled"
        await interaction.response.send_message(f"✅ Recurring event **{event['title']}** has been **{status}**.")


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


# OLD TOURNAMENT COG REMOVED - Using V3 system at end of file
# class TournamentCommands - DISABLED


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
    # TournamentCommands removed - using new V3 system
    await bot.add_cog(LevelCommands(bot))

@bot.event
async def on_ready():
    print("=" * 50)
    print(f"✅ Logged in as {bot.user} (ID: {bot.user.id})")
    print(f"✅ Connected to {len(bot.guilds)} guild(s)")
    print(f"✅ PIL Available: {PIL_AVAILABLE}")
    print(f"✅ PostgreSQL Available: {POSTGRES_AVAILABLE}")
    print("=" * 50)
    
    # Add startup delay to avoid rate limits
    print("⏳ Waiting 5 seconds before initializing...")
    await asyncio.sleep(5)
    
    # Initialize PostgreSQL database
    if POSTGRES_AVAILABLE and DATABASE_URL:
        print("Connecting to PostgreSQL database...")
        db_connected = await init_database()
        
        if db_connected:
            # Sync data from PostgreSQL (restore after redeploy)
            print("Syncing data from PostgreSQL...")
            await sync_data_from_postgres()
            await asyncio.sleep(1)  # Small delay
            
            # Sync other data files
            duels_data = await load_duels_from_postgres()
            if duels_data:
                with open(DUELS_FILE, "w") as f:
                    json.dump(duels_data, f, indent=2)
                print("✅ Duels data synced from PostgreSQL!")
            
            await asyncio.sleep(1)  # Small delay
            
            events_data = await load_events_from_postgres()
            if events_data:
                with open(EVENTS_FILE, "w") as f:
                    json.dump(events_data, f, indent=2)
                print("✅ Events data synced from PostgreSQL!")
            
            await asyncio.sleep(1)  # Small delay
            
            inactivity_data = await load_inactivity_from_postgres()
            if inactivity_data:
                with open(INACTIVITY_FILE, "w") as f:
                    json.dump(inactivity_data, f, indent=2)
                print("✅ Inactivity data synced from PostgreSQL!")
    else:
        print("📁 Using JSON file storage (no PostgreSQL)")
    
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
    
    # DON'T auto-sync on startup - use !sync command instead to avoid rate limits
    print("⚠️ Slash commands NOT auto-synced. Use !sync to sync manually.")
    
    # Start event reminder background task
    if not hasattr(bot, 'event_reminder_task_started'):
        bot.loop.create_task(check_event_reminders())
        bot.event_reminder_task_started = True
        print("✅ Event reminder task started!")
    
    # Start recurring events background task
    if not hasattr(bot, 'recurring_events_task_started'):
        bot.loop.create_task(recurring_events_loop())
        bot.recurring_events_task_started = True
        print("✅ Recurring events task started!")
    
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
    welcome_channel = discord.utils.get(member.guild.text_channels, name=WELCOME_CHANNEL_NAME) or \
                      discord.utils.get(member.guild.text_channels, name="welcome") or \
                      discord.utils.get(member.guild.text_channels, name="welcomes")
    
    if welcome_channel:
        try:
            # Generate welcome card image
            welcome_card = await create_welcome_card(member)
            
            # Fallen-themed welcome message
            welcome_messages = [
                f"The shadows welcome you, {member.mention}...",
                f"Another soul descends... Welcome, {member.mention}.",
                f"From the ashes, {member.mention} rises to join The Fallen.",
                f"The abyss has claimed another... Welcome, {member.mention}.",
                f"{member.mention} has answered the call of The Fallen.",
            ]
            
            import random
            welcome_text = random.choice(welcome_messages)
            
            embed = discord.Embed(
                title="✝ WELCOME TO THE FALLEN ✝",
                description=(
                    f"{welcome_text}\n\n"
                    f"━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                    f"*Through shattered skies and broken crowns,*\n"
                    f"*The descent carves its mark.*\n"
                    f"*Fallen endures — not erased, but remade.*\n"
                    f"*In ruin lies the seed of power.*\n\n"
                    f"━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                    f"**🔒 To gain access:**\n"
                    f"1️⃣ Verify with **Bloxlink** (`/verify`)\n"
                    f"2️⃣ Click the **Verify** button in verification channel\n\n"
                    f"**⚔️ What awaits you:**\n"
                    f"• Trainings & Tryouts\n"
                    f"• Ranked Duels & ELO System\n"
                    f"• Leveling & Rewards\n"
                    f"• Clan Wars & Raids\n\n"
                    f"You are member **#{member.guild.member_count}**"
                ),
                color=0x8B0000
            )
            
            if welcome_card:
                file = discord.File(welcome_card, filename="welcome.png")
                embed.set_image(url="attachment://welcome.png")
                await welcome_channel.send(file=file, embed=embed)
            else:
                embed.set_thumbnail(url=member.display_avatar.url)
                if member.guild.icon:
                    embed.set_footer(text="✝ The Fallen ✝", icon_url=member.guild.icon.url)
                else:
                    embed.set_footer(text="✝ The Fallen ✝")
                await welcome_channel.send(embed=embed)
                
        except Exception as e:
            print(f"Welcome card error: {e}")
            # Simple fallback
            try:
                await welcome_channel.send(f"✝ Welcome to The Fallen, {member.mention}! ✝")
            except:
                pass
    
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
    
    # Alt account detection
    try:
        score, reasons = calculate_alt_score(member)
        
        if score >= 50:
            alt_flags[str(member.id)] = {
                "score": score,
                "reasons": reasons,
                "flagged_at": datetime.datetime.now(datetime.timezone.utc).isoformat()
            }
            
            # Alert staff
            log_channel = discord.utils.get(member.guild.text_channels, name=LOG_CHANNEL_NAME)
            if not log_channel:
                log_channel = discord.utils.get(member.guild.text_channels, name="fallen-logs")
            
            if log_channel:
                embed = discord.Embed(
                    title="🔍 Potential Alt Account Detected",
                    description=f"{member.mention} has been flagged as a potential alt account.",
                    color=0xe74c3c if score >= 70 else 0xf39c12
                )
                embed.add_field(name="👤 User", value=f"{member} ({member.id})", inline=True)
                embed.add_field(name="⚠️ Risk Score", value=f"{score}/100", inline=True)
                embed.add_field(name="📅 Account Age", value=f"{(datetime.datetime.now(datetime.timezone.utc) - member.created_at).days} days", inline=True)
                embed.add_field(name="🚩 Flags", value="\n".join(f"• {r}" for r in reasons), inline=False)
                embed.set_thumbnail(url=member.display_avatar.url)
                embed.set_footer(text="Use !altcheck @user for detailed check")
                
                await log_channel.send(embed=embed)
    except Exception as e:
        print(f"Alt check error: {e}")

@bot.event
async def on_message(message):
    if not message.author.bot and message.guild:
        # === XP & ACTIVITY ===
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

# ==========================================
# MASS ROLE MANAGEMENT (Rate Limited & Safe)
# ==========================================

@bot.command(name="massrole")
@commands.has_permissions(administrator=True)
@commands.cooldown(1, 300, commands.BucketType.guild)  # Once per 5 minutes
async def mass_role_add(ctx, action: str, role: discord.Role, target: str = "everyone"):
    """
    Add or remove a role from multiple members
    
    Usage:
    !massrole add @Role everyone - Add role to all members
    !massrole remove @Role everyone - Remove role from all members
    !massrole add @Role humans - Add to non-bots only
    !massrole add @Role bots - Add to bots only
    !massrole add @Role @OtherRole - Add to members with @OtherRole
    """
    if action.lower() not in ["add", "remove"]:
        return await ctx.send("❌ Action must be `add` or `remove`!\nUsage: `!massrole add @Role everyone`")
    
    # Determine target members
    if target.lower() == "everyone":
        members = [m for m in ctx.guild.members if not m.bot]
    elif target.lower() == "humans":
        members = [m for m in ctx.guild.members if not m.bot]
    elif target.lower() == "bots":
        members = [m for m in ctx.guild.members if m.bot]
    elif target.lower() == "all":
        members = ctx.guild.members
    else:
        # Check if target is a role mention
        target_role = None
        for r in ctx.guild.roles:
            if r.mention == target or r.name.lower() == target.lower():
                target_role = r
                break
        
        if target_role:
            members = [m for m in ctx.guild.members if target_role in m.roles and not m.bot]
        else:
            return await ctx.send("❌ Invalid target! Use: `everyone`, `humans`, `bots`, `all`, or `@Role`")
    
    if not members:
        return await ctx.send("❌ No members found matching that target!")
    
    # Check bot permissions
    if role >= ctx.guild.me.top_role:
        return await ctx.send("❌ I can't manage that role! It's higher than or equal to my highest role.")
    
    # Filter members who need the change
    if action.lower() == "add":
        members = [m for m in members if role not in m.roles]
        action_word = "Adding"
        action_past = "added to"
    else:
        members = [m for m in members if role in m.roles]
        action_word = "Removing"
        action_past = "removed from"
    
    if not members:
        if action.lower() == "add":
            return await ctx.send("✅ No members need this change - they already have the role!")
        else:
            return await ctx.send("✅ No members need this change - they don't have the role!")
    
    # Confirmation
    confirm_embed = discord.Embed(
        title="⚠️ Mass Role Confirmation",
        description=(
            f"**Action:** {action_word} {role.mention}\n"
            f"**Target:** {len(members)} members\n\n"
            f"⏱️ **Estimated time:** ~{len(members) * 1.5:.0f} seconds\n\n"
            f"React with ✅ to confirm or ❌ to cancel."
        ),
        color=0xf39c12
    )
    confirm_msg = await ctx.send(embed=confirm_embed)
    await confirm_msg.add_reaction("✅")
    await confirm_msg.add_reaction("❌")
    
    def check(reaction, user):
        return user == ctx.author and str(reaction.emoji) in ["✅", "❌"] and reaction.message.id == confirm_msg.id
    
    try:
        reaction, user = await bot.wait_for("reaction_add", timeout=30.0, check=check)
    except asyncio.TimeoutError:
        await confirm_msg.edit(embed=discord.Embed(title="❌ Timed out", color=0xe74c3c))
        return
    
    if str(reaction.emoji) == "❌":
        await confirm_msg.edit(embed=discord.Embed(title="❌ Cancelled", color=0xe74c3c))
        return
    
    # Start the mass role operation
    progress_embed = discord.Embed(
        title=f"🔄 {action_word} Role...",
        description=f"Progress: 0/{len(members)}",
        color=0x3498db
    )
    await confirm_msg.edit(embed=progress_embed)
    
    success = 0
    failed = 0
    
    for i, member in enumerate(members):
        try:
            if action.lower() == "add":
                await member.add_roles(role, reason=f"Mass role add by {ctx.author}")
            else:
                await member.remove_roles(role, reason=f"Mass role remove by {ctx.author}")
            success += 1
        except Exception as e:
            failed += 1
            print(f"Failed to modify role for {member}: {e}")
        
        # Update progress every 10 members
        if (i + 1) % 10 == 0 or i == len(members) - 1:
            progress_embed.description = f"Progress: {i + 1}/{len(members)}\n✅ Success: {success} | ❌ Failed: {failed}"
            try:
                await confirm_msg.edit(embed=progress_embed)
            except:
                pass
        
        # Rate limit protection - wait between each role change
        await asyncio.sleep(1.2)
    
    # Final result
    result_embed = discord.Embed(
        title="✅ Mass Role Complete!",
        description=(
            f"**Role:** {role.mention}\n"
            f"**Action:** {action_past}\n\n"
            f"✅ **Success:** {success}\n"
            f"❌ **Failed:** {failed}"
        ),
        color=0x2ecc71 if failed == 0 else 0xf39c12
    )
    await confirm_msg.edit(embed=result_embed)


@bot.command(name="giverole")
@commands.has_permissions(administrator=True)
@commands.cooldown(1, 300, commands.BucketType.guild)
async def give_role_all(ctx, role: discord.Role):
    """Quick command to add a role to all humans. Use !massrole for more options."""
    # Redirect to massrole
    await mass_role_add(ctx, "add", role, "humans")


@bot.command(name="takerole")
@commands.has_permissions(administrator=True)
@commands.cooldown(1, 300, commands.BucketType.guild)
async def take_role_all(ctx, role: discord.Role):
    """Quick command to remove a role from all humans. Use !massrole for more options."""
    await mass_role_add(ctx, "remove", role, "humans")


@bot.command(name="inrole")
@commands.has_permissions(manage_roles=True)
async def in_role(ctx, role: discord.Role):
    """See how many members have a specific role"""
    members = [m for m in ctx.guild.members if role in m.roles]
    humans = [m for m in members if not m.bot]
    bots = [m for m in members if m.bot]
    
    embed = discord.Embed(
        title=f"📊 Members with {role.name}",
        color=role.color
    )
    embed.add_field(name="Total", value=str(len(members)), inline=True)
    embed.add_field(name="Humans", value=str(len(humans)), inline=True)
    embed.add_field(name="Bots", value=str(len(bots)), inline=True)
    
    if humans and len(humans) <= 20:
        embed.add_field(
            name="Members",
            value=", ".join([m.display_name for m in humans[:20]]),
            inline=False
        )
    
    await ctx.send(embed=embed)


@bot.command(name="roleinfo")
@commands.has_permissions(manage_roles=True)
async def role_info(ctx, role: discord.Role):
    """Get detailed info about a role"""
    embed = discord.Embed(
        title=f"📋 Role Info: {role.name}",
        color=role.color
    )
    
    embed.add_field(name="ID", value=f"`{role.id}`", inline=True)
    embed.add_field(name="Color", value=f"`{role.color}`", inline=True)
    embed.add_field(name="Position", value=f"{role.position}/{len(ctx.guild.roles)}", inline=True)
    embed.add_field(name="Members", value=str(len(role.members)), inline=True)
    embed.add_field(name="Hoisted", value="✅" if role.hoist else "❌", inline=True)
    embed.add_field(name="Mentionable", value="✅" if role.mentionable else "❌", inline=True)
    
    # Key permissions
    perms = []
    if role.permissions.administrator:
        perms.append("Administrator")
    if role.permissions.manage_guild:
        perms.append("Manage Server")
    if role.permissions.manage_roles:
        perms.append("Manage Roles")
    if role.permissions.manage_channels:
        perms.append("Manage Channels")
    if role.permissions.kick_members:
        perms.append("Kick")
    if role.permissions.ban_members:
        perms.append("Ban")
    if role.permissions.manage_messages:
        perms.append("Manage Messages")
    
    embed.add_field(
        name="Key Permissions",
        value=", ".join(perms) if perms else "None",
        inline=False
    )
    
    embed.add_field(name="Created", value=f"<t:{int(role.created_at.timestamp())}:R>", inline=True)
    embed.add_field(name="Mention", value=role.mention, inline=True)
    
    await ctx.send(embed=embed)


@bot.command(name="sync")
@commands.has_permissions(administrator=True)
@commands.cooldown(1, 60, commands.BucketType.guild)  # Once per minute per server
async def sync_cmd(ctx):
    """Sync slash commands to this server"""
    msg = await ctx.send("🔄 Syncing slash commands to this server... (this may take a moment)")
    
    try:
        # Add delay before syncing to avoid rate limits
        await asyncio.sleep(3)
        synced = await bot.tree.sync(guild=ctx.guild)
        await asyncio.sleep(2)
        await msg.edit(content=f"✅ Synced {len(synced)} slash commands to this server!")
    except discord.HTTPException as e:
        if e.status == 429:
            retry = getattr(e, 'retry_after', 60)
            await msg.edit(content=f"⚠️ Rate limited! Please wait {int(retry)} seconds and try again.")
        else:
            await msg.edit(content=f"❌ Sync failed: {e}")
    except Exception as e:
        await msg.edit(content=f"❌ Sync failed: {e}")


@bot.command(name="syncglobal")
@commands.has_permissions(administrator=True)
@commands.cooldown(1, 300, commands.BucketType.guild)  # Once per 5 minutes
async def sync_global_cmd(ctx):
    """Sync slash commands globally (takes up to 1 hour to propagate)"""
    msg = await ctx.send("🔄 Syncing slash commands globally... (this may take a moment)")
    
    try:
        await asyncio.sleep(3)
        synced = await bot.tree.sync()
        await asyncio.sleep(2)
        await msg.edit(content=f"✅ Synced {len(synced)} slash commands globally!\n⚠️ Global sync can take up to 1 hour to show everywhere.")
    except discord.HTTPException as e:
        if e.status == 429:
            retry = getattr(e, 'retry_after', 60)
            await msg.edit(content=f"⚠️ Rate limited! Please wait {int(retry)} seconds and try again.")
        else:
            await msg.edit(content=f"❌ Sync failed: {e}")
    except Exception as e:
        await msg.edit(content=f"❌ Sync failed: {e}")


@bot.command(name="clearsync")
@commands.has_permissions(administrator=True)
@commands.cooldown(1, 300, commands.BucketType.guild)  # Once per 5 minutes
async def clear_sync_cmd(ctx):
    """Clear ALL slash commands from this server and re-sync fresh"""
    msg = await ctx.send("🗑️ Clearing all slash commands from this server... (please wait ~15 seconds)")
    
    try:
        # Clear guild commands
        bot.tree.clear_commands(guild=ctx.guild)
        await asyncio.sleep(5)  # Longer delay
        
        # Sync empty (this removes old commands)
        await bot.tree.sync(guild=ctx.guild)
        await msg.edit(content="✅ Cleared old commands! Waiting before re-sync...")
        
        await asyncio.sleep(5)  # Longer delay between operations
        
        # Copy global commands to guild and sync
        bot.tree.copy_global_to(guild=ctx.guild)
        await asyncio.sleep(3)
        synced = await bot.tree.sync(guild=ctx.guild)
        
        await msg.edit(content=f"✅ Done! Cleared and re-synced {len(synced)} slash commands to this server!")
    except discord.HTTPException as e:
        if e.status == 429:
            retry = getattr(e, 'retry_after', 60)
            await msg.edit(content=f"⚠️ Rate limited! Please wait {int(retry)} seconds and try again.")
        else:
            await msg.edit(content=f"❌ Failed: {e}")
    except Exception as e:
        await msg.edit(content=f"❌ Failed: {e}")


@bot.command(name="clearglobal")
@commands.has_permissions(administrator=True)
@commands.cooldown(1, 600, commands.BucketType.guild)  # Once per 10 minutes
async def clear_global_cmd(ctx):
    """Clear ALL global slash commands and re-sync (DANGEROUS - use carefully)"""
    msg = await ctx.send("⚠️ Clearing ALL global slash commands... This will take ~20 seconds.")
    
    try:
        # Clear global commands
        bot.tree.clear_commands(guild=None)
        await asyncio.sleep(5)
        
        # Sync empty globally
        await bot.tree.sync()
        await msg.edit(content="✅ Cleared global commands! Waiting before re-sync...")
        
        await asyncio.sleep(10)  # Long delay for global operations
        
        # Re-register all commands and sync
        synced = await bot.tree.sync()
        
        await msg.edit(content=f"✅ Done! Cleared and re-synced {len(synced)} slash commands globally!\n⚠️ May take up to 1 hour to fully propagate.")
    except discord.HTTPException as e:
        if e.status == 429:
            await msg.edit(content=f"⚠️ Rate limited! Please wait a few minutes and try again.")
        else:
            await msg.edit(content=f"❌ Failed: {e}")
    except Exception as e:
        await msg.edit(content=f"❌ Failed: {e}")

@bot.command(name="test_welcome")
@commands.has_permissions(administrator=True)
async def test_welcome_cmd(ctx, member: discord.Member = None):
    """Test the welcome message - !test_welcome or !test_welcome @user"""
    target = member or ctx.author
    
    try:
        # Generate welcome card image
        welcome_card = await create_welcome_card(target)
        
        # Fallen-themed welcome message
        welcome_messages = [
            f"The shadows welcome you, {target.mention}...",
            f"Another soul descends... Welcome, {target.mention}.",
            f"From the ashes, {target.mention} rises to join The Fallen.",
            f"The abyss has claimed another... Welcome, {target.mention}.",
            f"{target.mention} has answered the call of The Fallen.",
        ]
        
        import random
        welcome_text = random.choice(welcome_messages)
        
        embed = discord.Embed(
            title="✝ WELCOME TO THE FALLEN ✝",
            description=(
                f"{welcome_text}\n\n"
                f"━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                f"*Through shattered skies and broken crowns,*\n"
                f"*The descent carves its mark.*\n"
                f"*Fallen endures — not erased, but remade.*\n"
                f"*In ruin lies the seed of power.*\n\n"
                f"━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                f"**🔒 To gain access:**\n"
                f"1️⃣ Verify with **Bloxlink** (`/verify`)\n"
                f"2️⃣ Click the **Verify** button in verification channel\n\n"
                f"**⚔️ What awaits you:**\n"
                f"• Trainings & Tryouts\n"
                f"• Ranked Duels & ELO System\n"
                f"• Leveling & Rewards\n"
                f"• Clan Wars & Raids\n\n"
                f"You are member **#{ctx.guild.member_count}**"
            ),
            color=0x8B0000
        )
        
        if welcome_card:
            file = discord.File(welcome_card, filename="welcome.png")
            embed.set_image(url="attachment://welcome.png")
            await ctx.send(content="**🧪 Welcome Message Test:**", file=file, embed=embed)
        else:
            embed.set_thumbnail(url=target.display_avatar.url)
            if ctx.guild.icon:
                embed.set_footer(text="✝ The Fallen ✝", icon_url=ctx.guild.icon.url)
            else:
                embed.set_footer(text="✝ The Fallen ✝")
            await ctx.send(content="**🧪 Welcome Message Test:**", embed=embed)
            
    except Exception as e:
        await ctx.send(f"❌ Error testing welcome: {e}")

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

@bot.command(name="setlevelbackground", description="Admin: Set the level card banner image")
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

@bot.hybrid_command(name="inventory", description="View your purchased items")
@commands.cooldown(1, 10, commands.BucketType.user)
async def inventory_cmd(ctx):
    """Display your inventory of purchased items"""
    user_data = get_user_data(ctx.author.id)
    inventory = user_data.get("inventory", [])
    
    embed = discord.Embed(
        title=f"🎒 {ctx.author.display_name}'s Inventory",
        color=0x9b59b6
    )
    
    if not inventory:
        embed.description = "Your inventory is empty!\nVisit the shop to buy items."
    else:
        # Count items
        item_counts = {}
        for item_id in inventory:
            item_counts[item_id] = item_counts.get(item_id, 0) + 1
        
        inv_text = ""
        for item_id, count in item_counts.items():
            item = next((i for i in SHOP_ITEMS if i["id"] == item_id), None)
            if item:
                inv_text += f"• **{item['name']}** x{count}\n"
            else:
                inv_text += f"• {item_id} x{count}\n"
        
        embed.description = inv_text
    
    # Show active effects
    effects = []
    if user_data.get("elo_shield_active"):
        effects.append("🛡️ ELO Shield (Active)")
    if user_data.get("streak_saver_active"):
        effects.append("🔥 Streak Saver (Active)")
    if user_data.get("training_reserved"):
        effects.append("📋 Training Reserved")
    if user_data.get("custom_level_bg"):
        effects.append("🖼️ Custom Background Set")
    
    if effects:
        embed.add_field(name="✨ Active Effects", value="\n".join(effects), inline=False)
    
    await ctx.send(embed=embed)

@bot.hybrid_command(name="setbackground", description="Set your custom level card background")
@commands.cooldown(1, 30, commands.BucketType.user)
async def setbackground_cmd(ctx, url: str):
    """Set a custom background for your level card"""
    user_data = get_user_data(ctx.author.id)
    
    # Check if user owns the background feature
    if "custom_level_bg" not in user_data.get("inventory", []):
        return await ctx.send("❌ You need to purchase **Custom Level Card BG** from the shop first!", ephemeral=True)
    
    # Handle reset
    if url.lower() == "default" or url.lower() == "reset":
        update_user_data(ctx.author.id, "custom_level_bg", None)
        return await ctx.send("✅ Your level card background has been reset to default!")
    
    # Validate URL
    valid_extensions = ('.png', '.jpg', '.jpeg', '.gif', '.webp')
    if not url.startswith(('http://', 'https://')) or not any(url.lower().endswith(ext) for ext in valid_extensions):
        return await ctx.send(
            "❌ Invalid URL! Please provide a direct image link.\n"
            "• Must start with `http://` or `https://`\n"
            "• Must end with `.png`, `.jpg`, `.jpeg`, `.gif`, or `.webp`",
            ephemeral=True
        )
    
    # Save the background
    update_user_data(ctx.author.id, "custom_level_bg", url)
    
    embed = discord.Embed(
        title="🖼️ Background Set!",
        description="Your level card will now use your custom background.",
        color=0x2ecc71
    )
    embed.set_image(url=url)
    embed.set_footer(text="Use /setbackground default to reset")
    
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
            "💰 **Economy & Shop** - Coins & items"
        ),
        inline=False
    )
    
    embed.add_field(
        name="━━━━━ Activities ━━━━━",
        value=(
            "📅 **Events** - Trainings & tryouts\n"
            "⚔️ **Duels & ELO** - 1v1 battles\n"
            "🏆 **Tournaments** - Competitions\n"
            "🆘 **Backup** - Request help"
        ),
        inline=False
    )
    
    embed.add_field(
        name="━━━━━ Ranking ━━━━━",
        value=(
            "📋 **Stage Transfer** - Rank transfers\n"
            "🛡️ **Staff** - Moderation tools\n"
            "⚙️ **Admin** - Server management"
        ),
        inline=False
    )
    
    embed.set_thumbnail(url=ctx.guild.icon.url if ctx.guild.icon else None)
    embed.set_footer(text="Use the dropdown below to view commands • / or ! prefix")
    
    await ctx.send(embed=embed, view=HelpView())

# --- STAFF COMMANDS ---

@bot.command(name="checklevel", description="Staff: Check another user's level")
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

@bot.command(name="addxp", description="Staff: Add XP to a user")
async def addxp(ctx, member: discord.Member, amount: int):
    """Add XP to a user"""
    if not is_staff(ctx.author):
        return await ctx.send("❌ Staff only.", ephemeral=True)
    
    new_xp = add_xp_to_user(member.id, amount)
    await ctx.send(f"✅ Added **{amount:,} XP** to {member.mention}. Total: **{new_xp:,} XP**")
    await log_action(ctx.guild, "✨ XP Added", f"{member.mention} received +{amount:,} XP from {ctx.author.mention}", 0xF1C40F)
    await check_level_up(member.id, ctx.guild)

@bot.command(name="removexp", description="Staff: Remove XP from a user")
async def removexp(ctx, member: discord.Member, amount: int):
    """Remove XP from a user"""
    if not is_staff(ctx.author):
        return await ctx.send("❌ Staff only.", ephemeral=True)
    
    new_xp = add_xp_to_user(member.id, -amount)
    await ctx.send(f"✅ Removed **{amount:,} XP** from {member.mention}. Total: **{new_xp:,} XP**")
    await log_action(ctx.guild, "✨ XP Removed", f"{member.mention} lost -{amount:,} XP by {ctx.author.mention}", 0xe74c3c)

@bot.command(name="setlevel", description="Staff: Set a user's level directly")
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

@bot.command(name="setxp", description="Staff: Set a user's total XP directly")
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

@bot.command(name="importlevel", description="Staff: Import level from Arcane bot")
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

@bot.command(name="bulkimport", description="Admin: Show bulk import instructions")
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

@bot.command(name="addfcoins", description="Staff: Add coins to a user")
async def addfcoins(ctx, member: discord.Member, amount: int):
    """Add coins to a user"""
    if not is_staff(ctx.author):
        return await ctx.send("❌ Staff only.", ephemeral=True)
    
    new_coins = add_user_stat(member.id, "coins", amount)
    await ctx.send(f"✅ Added **{amount:,} coins** to {member.mention}. Total: **{new_coins:,}**")
    await log_action(ctx.guild, "💰 Coins Added", f"{member.mention} received +{amount:,} coins from {ctx.author.mention}", 0xF1C40F)

@bot.command(name="removefcoins", description="Staff: Remove coins from a user")
async def removefcoins(ctx, member: discord.Member, amount: int):
    """Remove coins from a user"""
    if not is_staff(ctx.author):
        return await ctx.send("❌ Staff only.", ephemeral=True)
    
    new_coins = add_user_stat(member.id, "coins", -amount)
    await ctx.send(f"✅ Removed **{amount:,} coins** from {member.mention}. Total: **{new_coins:,}**")
    await log_action(ctx.guild, "💰 Coins Removed", f"{member.mention} lost -{amount:,} coins by {ctx.author.mention}", 0xe74c3c)

@bot.command(name="levelchange", description="Staff: Set a user's level")
async def levelchange(ctx, member: discord.Member, level: int):
    """Set a user's level directly"""
    if not is_staff(ctx.author):
        return await ctx.send("❌ Staff only.", ephemeral=True)
    
    update_user_data(member.id, "level", level)
    await ctx.send(f"✅ Set {member.mention}'s level to **{level}**")
    await log_action(ctx.guild, "📊 Level Changed", f"{member.mention}'s level set to {level} by {ctx.author.mention}", 0xF1C40F)

# --- ADMIN COMMANDS ---

@bot.command(name="wipedata", description="Owner: Complete data wipe")
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

@bot.command(name="setup_verify", description="Admin: Set up the verification panel")
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

@bot.command(name="set_bloxlink_role", description="Admin: Set the Bloxlink verified role name")
@commands.has_permissions(administrator=True)
async def set_bloxlink_role(ctx, role: discord.Role):
    """Set which role Bloxlink gives when users verify"""
    global BLOXLINK_VERIFIED_ROLE
    BLOXLINK_VERIFIED_ROLE = role.name
    await ctx.send(f"✅ Bloxlink verified role set to **{role.name}**\n\nUsers with this role can now click the verify button to get Fallen access.", ephemeral=True)

@bot.command(name="setup_shop", description="Admin: Set up the shop panel")
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
                await ch.send(file=file, embed=embed, view=ShopSelectView())
                await ctx.send(f"✅ Shop panel posted in {ch.mention}", ephemeral=True)
                return
        except Exception as e:
            print(f"Shop image error: {e}")
    
    # Fallback to embed with all items
    embed = discord.Embed(
        title="🛒 The Fallen Shop",
        description="Spend your hard-earned Fallen Coins here!",
        color=0xDC143C
    )
    
    # Group items by type
    cosmetic_items = [i for i in SHOP_ITEMS if i.get("type") in ["ticket", "background"] and "role" in i["id"].lower() or "bg" in i["id"].lower() or "hoisted" in i["id"].lower()]
    gameplay_items = [i for i in SHOP_ITEMS if i.get("type") == "consumable"]
    special_items = [i for i in SHOP_ITEMS if i.get("type") in ["coaching", "ticket"] and i["id"] in ["private_tryout", "coaching_session"]]
    
    embed.add_field(
        name="🎨 Cosmetic Items",
        value="\n".join([f"**{i['name']}** — {i['price']:,} 💰\n*{i['desc']}*" for i in SHOP_ITEMS if i["id"] in ["custom_role", "custom_role_color", "hoisted_role", "custom_level_bg"]]) or "None",
        inline=False
    )
    
    embed.add_field(
        name="⚔️ Gameplay Items",
        value="\n".join([f"**{i['name']}** — {i['price']:,} 💰\n*{i['desc']}*" for i in SHOP_ITEMS if i.get("type") == "consumable"]) or "None",
        inline=False
    )
    
    embed.add_field(
        name="📜 Special Access",
        value="\n".join([f"**{i['name']}** — {i['price']:,} 💰\n*{i['desc']}*" for i in SHOP_ITEMS if i["id"] in ["private_tryout", "coaching_session"]]) or "None",
        inline=False
    )
    
    embed.set_footer(text="Use the dropdown or buttons below to purchase!")
    
    await ch.send(embed=embed, view=ShopSelectView())
    await ctx.send(f"✅ Shop panel posted in {ch.mention}", ephemeral=True)

@bot.command(name="setup_transfer", description="Admin: Set up stage transfer panel")
@commands.has_permissions(administrator=True)
async def setup_transfer(ctx):
    """Set up the stage transfer request panel"""
    embed = discord.Embed(
        title="📋 Stage Transfer Request",
        description=(
            "**Request a rank transfer from another clan!**\n\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            "**📌 Accepted Clans:**\n"
            "• TSBCC\n"
            "• VALHALLA\n"
            "• TSBER\n\n"
            "**📸 Requirements:**\n"
            "• Screenshot of your rank in the clan\n"
            "• Must show your username clearly\n"
            "• Must be recent (within 24 hours)\n\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            "**Click the button below to open a transfer request.**"
        ),
        color=0x8B0000
    )
    embed.set_footer(text="✝ The Fallen ✝ • Stage Transfers")
    
    await ctx.send(embed=embed, view=StageTransferView())
    await ctx.send("✅ Stage transfer panel created!", ephemeral=True)

@bot.hybrid_command(name="result", description="Staff: Assign tryout/transfer result with stage, rank, and strength")
@app_commands.describe(
    member="Member to give the rank to",
    stage="Stage (0-5 or 'stage 2')",
    rank_level="Rank level (High/Mid/Low)",
    strength="Strength (Strong/Stable/Weak)"
)
@commands.has_any_role(*HIGH_STAFF_ROLES, STAFF_ROLE_NAME, TRYOUT_HOST_ROLE)
async def result_transfer(ctx, member: discord.Member, stage: str, rank_level: str = None, strength: str = None):
    """Assign tryout/transfer result - stage required, rank and strength optional
    
    Examples:
    /result @user stage 2
    /result @user 3 high strong
    /result @user stage 0 high stable
    """
    await ctx.defer()
    
    # Stage mapping - convert simple input to full role name
    STAGE_MAP = {
        "0": "Stage 0〢FALLEN DEITY",
        "stage 0": "Stage 0〢FALLEN DEITY",
        "stage0": "Stage 0〢FALLEN DEITY",
        "deity": "Stage 0〢FALLEN DEITY",
        "1": "Stage 1〢FALLEN APEX",
        "stage 1": "Stage 1〢FALLEN APEX",
        "stage1": "Stage 1〢FALLEN APEX",
        "apex": "Stage 1〢FALLEN APEX",
        "2": "Stage 2〢FALLEN ASCENDANT",
        "stage 2": "Stage 2〢FALLEN ASCENDANT",
        "stage2": "Stage 2〢FALLEN ASCENDANT",
        "ascendant": "Stage 2〢FALLEN ASCENDANT",
        "3": "Stage 3〢FORSAKEN WARRIOR",
        "stage 3": "Stage 3〢FORSAKEN WARRIOR",
        "stage3": "Stage 3〢FORSAKEN WARRIOR",
        "warrior": "Stage 3〢FORSAKEN WARRIOR",
        "forsaken": "Stage 3〢FORSAKEN WARRIOR",
        "4": "Stage 4〢ABYSS-TOUCHED",
        "stage 4": "Stage 4〢ABYSS-TOUCHED",
        "stage4": "Stage 4〢ABYSS-TOUCHED",
        "abyss": "Stage 4〢ABYSS-TOUCHED",
        "5": "Stage 5〢BROKEN INITIATE",
        "stage 5": "Stage 5〢BROKEN INITIATE",
        "stage5": "Stage 5〢BROKEN INITIATE",
        "initiate": "Stage 5〢BROKEN INITIATE",
        "broken": "Stage 5〢BROKEN INITIATE",
    }
    
    # Rank level mapping
    RANK_MAP = {
        "high": "High",
        "h": "High",
        "mid": "Mid",
        "m": "Mid",
        "medium": "Mid",
        "low": "Low",
        "l": "Low",
    }
    
    # Strength mapping
    STRENGTH_MAP = {
        "strong": "Strong",
        "s": "Strong",
        "stable": "Stable",
        "st": "Stable",
        "weak": "Weak",
        "w": "Weak",
    }
    
    # Parse stage input
    stage_lower = stage.lower().strip()
    stage_role_name = STAGE_MAP.get(stage_lower)
    
    if not stage_role_name:
        # Try to find partial match
        for key, value in STAGE_MAP.items():
            if key in stage_lower or stage_lower in key:
                stage_role_name = value
                break
    
    if not stage_role_name:
        return await ctx.send(
            f"❌ Invalid stage: `{stage}`\n\n"
            f"**Valid options:**\n"
            f"• `0` or `stage 0` → Stage 0〢FALLEN DEITY\n"
            f"• `1` or `stage 1` → Stage 1〢FALLEN APEX\n"
            f"• `2` or `stage 2` → Stage 2〢FALLEN ASCENDANT\n"
            f"• `3` or `stage 3` → Stage 3〢FORSAKEN WARRIOR\n"
            f"• `4` or `stage 4` → Stage 4〢ABYSS-TOUCHED\n"
            f"• `5` or `stage 5` → Stage 5〢BROKEN INITIATE"
        )
    
    # Parse rank level if provided
    rank_role_name = None
    if rank_level:
        rank_lower = rank_level.lower().strip()
        rank_role_name = RANK_MAP.get(rank_lower, rank_level.capitalize())
        if rank_role_name not in ["High", "Mid", "Low"]:
            return await ctx.send(f"❌ Invalid rank level: `{rank_level}`\n**Valid:** High, Mid, Low")
    
    # Parse strength if provided
    strength_role_name = None
    if strength:
        strength_lower = strength.lower().strip()
        strength_role_name = STRENGTH_MAP.get(strength_lower, strength.capitalize())
        if strength_role_name not in ["Strong", "Stable", "Weak"]:
            return await ctx.send(f"❌ Invalid strength: `{strength}`\n**Valid:** Strong, Stable, Weak")
    
    # All possible stage/rank roles to remove
    ALL_RESULT_ROLES = [
        "Stage 0〢FALLEN DEITY",
        "Stage 1〢FALLEN APEX", 
        "Stage 2〢FALLEN ASCENDANT",
        "Stage 3〢FORSAKEN WARRIOR",
        "Stage 4〢ABYSS-TOUCHED",
        "Stage 5〢BROKEN INITIATE",
        "High", "Mid", "Low",
        "Strong", "Stable", "Weak"
    ]
    
    # Get their current roles
    old_stage = "None"
    old_rank = "None"
    old_strength = "None"
    
    for role_name in ["Stage 0〢FALLEN DEITY", "Stage 1〢FALLEN APEX", "Stage 2〢FALLEN ASCENDANT", 
                      "Stage 3〢FORSAKEN WARRIOR", "Stage 4〢ABYSS-TOUCHED", "Stage 5〢BROKEN INITIATE"]:
        role = discord.utils.get(member.roles, name=role_name)
        if role:
            old_stage = role_name
            break
    
    for role_name in ["High", "Mid", "Low"]:
        role = discord.utils.get(member.roles, name=role_name)
        if role:
            old_rank = role_name
            break
    
    for role_name in ["Strong", "Stable", "Weak"]:
        role = discord.utils.get(member.roles, name=role_name)
        if role:
            old_strength = role_name
            break
    
    # Remove all current result roles
    roles_to_remove = []
    for role_name in ALL_RESULT_ROLES:
        role = discord.utils.get(ctx.guild.roles, name=role_name)
        if role and role in member.roles:
            roles_to_remove.append(role)
    
    if roles_to_remove:
        try:
            await member.remove_roles(*roles_to_remove)
            await asyncio.sleep(0.5)
        except Exception as e:
            print(f"Error removing roles: {e}")
    
    # Add new roles
    roles_to_add = []
    roles_added = []
    
    # Stage role (required)
    stage_role = discord.utils.get(ctx.guild.roles, name=stage_role_name)
    if stage_role:
        roles_to_add.append(stage_role)
        roles_added.append(stage_role_name)
    else:
        return await ctx.send(f"❌ Role `{stage_role_name}` not found! Please create it first.")
    
    # Rank level role (optional)
    if rank_role_name:
        rank_role = discord.utils.get(ctx.guild.roles, name=rank_role_name)
        if rank_role:
            roles_to_add.append(rank_role)
            roles_added.append(rank_role_name)
        else:
            await ctx.send(f"⚠️ Role `{rank_role_name}` not found, skipping...")
    
    # Strength role (optional)
    if strength_role_name:
        strength_role = discord.utils.get(ctx.guild.roles, name=strength_role_name)
        if strength_role:
            roles_to_add.append(strength_role)
            roles_added.append(strength_role_name)
        else:
            await ctx.send(f"⚠️ Role `{strength_role_name}` not found, skipping...")
    
    # Add all roles at once
    try:
        await member.add_roles(*roles_to_add)
    except Exception as e:
        return await ctx.send(f"❌ Failed to add roles: {e}")
    
    # Build result string
    result_parts = [stage_role_name.split("〢")[0] if "〢" in stage_role_name else stage_role_name]
    if rank_role_name:
        result_parts.append(rank_role_name)
    if strength_role_name:
        result_parts.append(strength_role_name)
    result_str = ", ".join(result_parts)
    
    # Build old rank string
    old_parts = []
    if old_stage != "None":
        old_parts.append(old_stage.split("〢")[0] if "〢" in old_stage else old_stage)
    if old_rank != "None":
        old_parts.append(old_rank)
    if old_strength != "None":
        old_parts.append(old_strength)
    old_str = ", ".join(old_parts) if old_parts else "None"
    
    # Create result embed
    embed = discord.Embed(
        title="✅ Result Assigned",
        description=(
            f"**Member:** {member.mention}\n\n"
            f"**Previous:** {old_str}\n"
            f"**New Result:** {result_str}\n\n"
            f"**Assigned by:** {ctx.author.mention}"
        ),
        color=0x2ecc71
    )
    
    # Add breakdown
    embed.add_field(name="📊 Stage", value=stage_role_name, inline=True)
    if rank_role_name:
        embed.add_field(name="📈 Rank Level", value=rank_role_name, inline=True)
    if strength_role_name:
        embed.add_field(name="💪 Strength", value=strength_role_name, inline=True)
    
    embed.set_thumbnail(url=member.display_avatar.url)
    embed.set_footer(text="✝ The Fallen ✝")
    
    await ctx.send(embed=embed)
    
    # Try to DM the user
    try:
        dm_embed = discord.Embed(
            title="✅ Your Result Has Been Assigned!",
            description=(
                f"Your rank in **The Fallen** has been updated!\n\n"
                f"**Result:** {result_str}"
            ),
            color=0x2ecc71
        )
        if rank_level:
            dm_embed.add_field(name="📊 Stage", value=stage, inline=True)
            dm_embed.add_field(name="📈 Rank Level", value=rank_level, inline=True)
        if strength:
            dm_embed.add_field(name="💪 Strength", value=strength, inline=True)
        await member.send(embed=dm_embed)
    except:
        pass
    
    await log_action(ctx.guild, "✅ Result Assigned", f"{member.mention}\n{old_str} → **{result_str}**\nBy: {ctx.author.mention}", 0x2ecc71)

@bot.command(name="voicetop", description="View voice time leaderboard")
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

@bot.command(name="top10_setup", description="Admin: Set up the Top 10 ranked leaderboard panel")
@commands.has_permissions(administrator=True)
async def top10_setup(ctx):
    """Create the Top 10 ranked leaderboard panel with visual image"""
    async with ctx.typing():
        # Try to create visual image first
        img_buffer = await create_top10_leaderboard_image(ctx.guild)
        
        if img_buffer:
            file = discord.File(img_buffer, filename="leaderboard.png")
            msg = await ctx.send(file=file, view=LeaderboardView())
            
            # Save message info for future updates
            full_data = load_data()
            full_data["leaderboard_message"] = {
                "channel_id": str(ctx.channel.id),
                "message_id": str(msg.id)
            }
            save_data(full_data)
            
            await ctx.send("✅ Visual leaderboard posted! It will auto-update when tournaments end.", delete_after=10)
        else:
            # Fallback to embed
            embed = create_leaderboard_embed(ctx.guild)
            await ctx.send(embed=embed, view=LeaderboardView())


@bot.command(name="top10_refresh", aliases=["refreshtop10"])
@commands.has_permissions(administrator=True) 
async def top10_refresh(ctx):
    """Refresh the Top 10 leaderboard image"""
    async with ctx.typing():
        img_buffer = await create_top10_leaderboard_image(ctx.guild)
        
        if img_buffer:
            file = discord.File(img_buffer, filename="leaderboard.png")
            await ctx.send(file=file)
        else:
            await ctx.send("❌ Failed to generate leaderboard image. Make sure `leaderboard_bg.png` exists.")


@bot.command(name="top10_update")
@commands.has_permissions(administrator=True)
async def top10_update(ctx):
    """Update the existing Top 10 leaderboard message"""
    full_data = load_data()
    lb_info = full_data.get("leaderboard_message", {})
    
    if not lb_info.get("channel_id") or not lb_info.get("message_id"):
        return await ctx.send("❌ No leaderboard message saved. Use `!top10_setup` first.")
    
    async with ctx.typing():
        img_buffer = await create_top10_leaderboard_image(ctx.guild)
        
        if img_buffer:
            try:
                channel = ctx.guild.get_channel(int(lb_info["channel_id"]))
                msg = await channel.fetch_message(int(lb_info["message_id"]))
                file = discord.File(img_buffer, filename="leaderboard.png")
                await msg.edit(attachments=[file])
                await ctx.send("✅ Leaderboard updated!", delete_after=5)
            except Exception as e:
                await ctx.send(f"❌ Failed to update: {e}")
        else:
            await ctx.send("❌ Failed to generate image.")

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

@bot.command(name="setup_roster", description="Admin: Set up the clan roster panel (EU Roster style)")
@commands.has_permissions(administrator=True)
async def setup_roster(ctx, channel: discord.TextChannel = None):
    """Create the clan roster panel"""
    target_channel = channel or ctx.channel
    
    embed = create_clan_roster_embed(ctx.guild)
    await target_channel.send(embed=embed, view=ClanRosterView())
    await ctx.send(f"✅ Clan roster posted in {target_channel.mention}", ephemeral=True)

@bot.command(name="roster_add", description="Admin: Add member to clan roster")
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

@bot.command(name="roster_remove", description="Admin: Remove member from clan roster")
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

@bot.command(name="roster_set_title", description="Admin: Set roster title")
@commands.has_permissions(administrator=True)
async def roster_set_title(ctx, *, title: str):
    """Set the clan roster title"""
    roster_data = load_clan_roster()
    roster_data["title"] = title
    save_clan_roster(roster_data)
    await ctx.send(f"✅ Roster title set to: **{title}**", ephemeral=True)

@bot.command(name="roster_set_description", description="Admin: Set roster description")
@commands.has_permissions(administrator=True)
async def roster_set_description(ctx, *, description: str):
    """Set the clan roster description (use \\n for new lines)"""
    roster_data = load_clan_roster()
    roster_data["description"] = description.replace("\\n", "\n")
    save_clan_roster(roster_data)
    await ctx.send(f"✅ Roster description updated!", ephemeral=True)

@bot.command(name="roster_set_image", description="Admin: Set roster banner image URL")
@commands.has_permissions(administrator=True)
async def roster_set_image(ctx, url: str):
    """Set the clan roster banner image"""
    roster_data = load_clan_roster()
    roster_data["image_url"] = url
    save_clan_roster(roster_data)
    await ctx.send(f"✅ Roster image set!", ephemeral=True)

@bot.command(name="roster_set_role", description="Admin: Set the roster role name")
@commands.has_permissions(administrator=True)
async def roster_set_role(ctx, role_name: str):
    """Set the role shown on the roster"""
    roster_data = load_clan_roster()
    roster_data["role_name"] = role_name
    save_clan_roster(roster_data)
    await ctx.send(f"✅ Roster role set to: **{role_name}**", ephemeral=True)

@bot.command(name="roster_list", description="View all roster members")
async def roster_list(ctx):
    """View the current clan roster"""
    embed = create_clan_roster_embed(ctx.guild)
    await ctx.send(embed=embed)

@bot.command(name="ticket_panel", description="Admin: Set up the challenge ticket panel")
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

@bot.command(name="apply_panel", description="Admin: Set up the application panel")
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

@bot.command(name="app_status", description="Check your application status")
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

# ==========================================
# BACKUP SYSTEM
# ==========================================

class BackupModal(discord.ui.Modal, title="🆘 Request Backup"):
    """Modal for requesting backup"""
    
    enemies = discord.ui.TextInput(
        label="Enemy Names (at least 3)",
        placeholder="List enemy usernames, separated by commas...",
        style=discord.TextStyle.paragraph,
        min_length=5,
        max_length=500,
        required=True
    )
    
    invite_link = discord.ui.TextInput(
        label="Invite Link (Roblox or RO-PRO)",
        placeholder="https://www.roblox.com/share?code=... or ro.pro/XXXXX",
        style=discord.TextStyle.short,
        min_length=10,
        max_length=200,
        required=True
    )
    
    additional_info = discord.ui.TextInput(
        label="Additional Info (Optional)",
        placeholder="Any extra details about the situation...",
        style=discord.TextStyle.paragraph,
        required=False,
        max_length=300
    )
    
    async def on_submit(self, interaction: discord.Interaction):
        # Validate enemies (at least 3)
        enemy_list = [e.strip() for e in self.enemies.value.replace('\n', ',').split(',') if e.strip()]
        
        if len(enemy_list) < 3:
            return await interaction.response.send_message(
                "❌ You must list at least **3 enemies**! Please try again.",
                ephemeral=True
            )
        
        # Validate link
        link = self.invite_link.value.strip()
        valid_link = False
        
        # Check for valid Roblox invite link
        if "roblox.com/share" in link.lower() and "code=" in link.lower():
            valid_link = True
        # Check for RO-PRO link
        elif "ro.pro/" in link.lower():
            valid_link = True
        # Also accept direct roblox game links with privateServerLinkCode
        elif "roblox.com/games" in link.lower() and "privateserverlinkcode" in link.lower():
            valid_link = True
        
        if not valid_link:
            return await interaction.response.send_message(
                "❌ Invalid link! Please provide a valid:\n"
                "• **Roblox Invite:** `https://www.roblox.com/share?code=...`\n"
                "• **RO-PRO Link:** `ro.pro/XXXXXX`",
                ephemeral=True
            )
        
        # Find backup ping role
        backup_role = discord.utils.get(interaction.guild.roles, name="Backup Ping")
        ping_text = backup_role.mention if backup_role else "@Backup Ping"
        
        # Create backup request embed
        embed = discord.Embed(
            title="🆘 BACKUP REQUESTED",
            description=f"**{interaction.user.mention} needs backup!**",
            color=0xFF0000,
            timestamp=datetime.datetime.now(datetime.timezone.utc)
        )
        
        # Format enemy list nicely
        enemy_display = "\n".join([f"• {enemy}" for enemy in enemy_list[:10]])  # Max 10 displayed
        if len(enemy_list) > 10:
            enemy_display += f"\n• ... and {len(enemy_list) - 10} more"
        
        embed.add_field(
            name=f"⚔️ Enemies ({len(enemy_list)})",
            value=enemy_display,
            inline=False
        )
        
        embed.add_field(
            name="🔗 Join Link",
            value=f"**[CLICK TO JOIN]({link})**",
            inline=False
        )
        
        if self.additional_info.value:
            embed.add_field(
                name="📝 Additional Info",
                value=self.additional_info.value,
                inline=False
            )
        
        embed.set_thumbnail(url=interaction.user.display_avatar.url)
        embed.set_footer(text="✝ The Fallen • Backup System ✝")
        
        # Send the backup request
        await interaction.response.send_message(
            content=f"🚨 {ping_text} 🚨",
            embed=embed
        )
        
        # Log the backup request
        await log_action(
            interaction.guild,
            "🆘 Backup Requested",
            f"**By:** {interaction.user.mention}\n**Enemies:** {len(enemy_list)}\n**Link:** [Join]({link})",
            0xFF0000
        )


@bot.command(name="backup", description="Request backup from clan members")
async def backup_request(ctx):
    """Request backup - shows instructions for prefix, modal for slash"""
    # For prefix command, show instructions since modals don't work
    embed = discord.Embed(
        title="🆘 Request Backup",
        description=(
            "To request backup, use the **slash command**:\n"
            "```/backup```\n\n"
            "This will open a form where you need to:\n"
            "• List at least **3 enemies**\n"
            "• Provide a valid **invite link**\n\n"
            "**Valid Links:**\n"
            "• `https://www.roblox.com/share?code=...`\n"
            "• `ro.pro/XXXXXX`"
        ),
        color=0xFF0000
    )
    embed.set_footer(text="✝ The Fallen ✝")
    await ctx.send(embed=embed)


@bot.tree.command(name="backup", description="Request backup from clan members")
async def backup_slash(interaction: discord.Interaction):
    """Request backup - opens a form to fill out"""
    modal = BackupModal()
    await interaction.response.send_modal(modal)


# ==========================================
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

@bot.command(name="quick_training", description="Staff: Quick announce training (no RSVP)")
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

@bot.command(name="quick_tryout", description="Staff: Quick announce tryout (no RSVP)")
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

@bot.hybrid_command(name="log_training", description="Staff: Log training attendance for members")
@app_commands.describe(
    member1="First attendee",
    member2="Second attendee (optional)",
    member3="Third attendee (optional)",
    member4="Fourth attendee (optional)",
    member5="Fifth attendee (optional)",
    member6="Sixth attendee (optional)",
    member7="Seventh attendee (optional)",
    member8="Eighth attendee (optional)",
    member9="Ninth attendee (optional)",
    member10="Tenth attendee (optional)"
)
@commands.has_any_role(*HIGH_STAFF_ROLES, STAFF_ROLE_NAME)
async def log_training(ctx, 
                       member1: discord.Member,
                       member2: discord.Member = None,
                       member3: discord.Member = None,
                       member4: discord.Member = None,
                       member5: discord.Member = None,
                       member6: discord.Member = None,
                       member7: discord.Member = None,
                       member8: discord.Member = None,
                       member9: discord.Member = None,
                       member10: discord.Member = None):
    """Log training attendance - add up to 10 members, or use mentions for more"""
    # Collect all provided members
    members = [m for m in [member1, member2, member3, member4, member5, 
                           member6, member7, member8, member9, member10] if m is not None]
    
    # Also check for mentions in message (for prefix command with more than 10)
    if hasattr(ctx, 'message') and ctx.message and ctx.message.mentions:
        for m in ctx.message.mentions:
            if m not in members and m != ctx.bot.user:
                members.append(m)
    
    if not members:
        return await ctx.send("❌ Please specify at least one member!", ephemeral=True)
    
    # Remove duplicates while preserving order
    seen = set()
    unique_members = []
    for m in members:
        if m.id not in seen:
            seen.add(m.id)
            unique_members.append(m)
    members = unique_members
    
    await ctx.defer()  # May take a while with many members
    
    rewards = ATTENDANCE_REWARDS["training"]
    streak_bonuses = []
    role_rewards = []
    
    for m in members:
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
        await asyncio.sleep(0.3)
    
    # Host rewards
    host_rewards = ATTENDANCE_REWARDS["host"]
    add_user_stat(ctx.author.id, "coins", host_rewards["coins"])
    add_xp_to_user(ctx.author.id, host_rewards["xp"])
    add_user_stat(ctx.author.id, "events_hosted", 1)
    reset_member_activity(ctx.author.id)  # Reset host activity too
    
    # Build attendee list
    attendee_names = [m.display_name for m in members[:15]]
    attendee_list = ", ".join(attendee_names)
    if len(members) > 15:
        attendee_list += f" +{len(members) - 15} more"
    
    embed = discord.Embed(
        title="📚 Training Attendance Logged",
        description=f"**{len(members)} attendees** rewarded!",
        color=0x2ecc71
    )
    embed.add_field(name="👥 Attendees", value=attendee_list, inline=False)
    embed.add_field(name="💰 Each Received", value=f"{rewards['coins']} coins + {rewards['xp']} XP", inline=True)
    embed.add_field(name="👑 Host Received", value=f"{host_rewards['coins']} coins + {host_rewards['xp']} XP", inline=True)
    
    if streak_bonuses:
        embed.add_field(name="🔥 Streak Bonuses", value="\n".join(streak_bonuses[:5]), inline=False)
    
    if role_rewards:
        embed.add_field(name="🎉 Role Rewards Earned!", value="\n".join(role_rewards[:5]), inline=False)
    
    await ctx.send(embed=embed)

@bot.hybrid_command(name="log_tryout", description="Staff: Log tryout attendance for members")
@app_commands.describe(
    member1="First attendee",
    member2="Second attendee (optional)",
    member3="Third attendee (optional)",
    member4="Fourth attendee (optional)",
    member5="Fifth attendee (optional)",
    member6="Sixth attendee (optional)",
    member7="Seventh attendee (optional)",
    member8="Eighth attendee (optional)",
    member9="Ninth attendee (optional)",
    member10="Tenth attendee (optional)"
)
@commands.has_any_role(*HIGH_STAFF_ROLES, STAFF_ROLE_NAME)
async def log_tryout(ctx,
                     member1: discord.Member,
                     member2: discord.Member = None,
                     member3: discord.Member = None,
                     member4: discord.Member = None,
                     member5: discord.Member = None,
                     member6: discord.Member = None,
                     member7: discord.Member = None,
                     member8: discord.Member = None,
                     member9: discord.Member = None,
                     member10: discord.Member = None):
    """Log tryout attendance - add up to 10 members, or use mentions for more"""
    # Collect all provided members
    members = [m for m in [member1, member2, member3, member4, member5, 
                           member6, member7, member8, member9, member10] if m is not None]
    
    # Also check for mentions in message (for prefix command with more than 10)
    if hasattr(ctx, 'message') and ctx.message and ctx.message.mentions:
        for m in ctx.message.mentions:
            if m not in members and m != ctx.bot.user:
                members.append(m)
    
    if not members:
        return await ctx.send("❌ Please specify at least one member!", ephemeral=True)
    
    # Remove duplicates while preserving order
    seen = set()
    unique_members = []
    for m in members:
        if m.id not in seen:
            seen.add(m.id)
            unique_members.append(m)
    members = unique_members
    
    await ctx.defer()  # May take a while with many members
    
    rewards = ATTENDANCE_REWARDS["tryout"]
    streak_bonuses = []
    role_rewards = []
    
    for m in members:
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
        await asyncio.sleep(0.3)
    
    # Host rewards
    host_rewards = ATTENDANCE_REWARDS["host"]
    add_user_stat(ctx.author.id, "coins", host_rewards["coins"])
    add_xp_to_user(ctx.author.id, host_rewards["xp"])
    add_user_stat(ctx.author.id, "events_hosted", 1)
    reset_member_activity(ctx.author.id)  # Reset host activity too
    
    # Build attendee list
    attendee_names = [m.display_name for m in members[:15]]
    attendee_list = ", ".join(attendee_names)
    if len(members) > 15:
        attendee_list += f" +{len(members) - 15} more"
    
    embed = discord.Embed(
        title="🎯 Tryout Attendance Logged",
        description=f"**{len(members)} attendees** rewarded!",
        color=0x2ecc71
    )
    embed.add_field(name="👥 Attendees", value=attendee_list, inline=False)
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

@bot.command(name="promote", description="Staff: Promote a user")
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

@bot.command(name="demote", description="Staff: Demote a user")
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

@bot.command(name="announce", description="Staff: Send an announcement")
async def announce(ctx, channel: discord.TextChannel, *, message: str):
    """Send formatted announcement"""
    if not is_staff(ctx.author):
        return await ctx.send("❌ Staff only.", ephemeral=True)
    
    embed = discord.Embed(title="📢 Announcement", description=message, color=0xF1C40F, timestamp=datetime.datetime.now(datetime.timezone.utc))
    embed.set_footer(text=f"By {ctx.author.display_name}", icon_url=ctx.author.display_avatar.url)
    await channel.send(embed=embed)
    await ctx.send(f"✅ Sent to {channel.mention}", ephemeral=True)

@bot.command(name="top10_add", description="Admin: Add to Top 10 leaderboard")
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

@bot.command(name="top10_remove", description="Admin: Remove from Top 10 leaderboard")
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

@bot.command(name="reset_weekly", description="Admin: Reset weekly XP")
@commands.has_permissions(administrator=True)
async def reset_weekly(ctx):
    """Reset weekly XP for all users"""
    data = load_data()
    for uid in data["users"]:
        data["users"][uid]["weekly_xp"] = 0
    save_data(data)
    await ctx.send("✅ Weekly XP reset!")
    await log_action(ctx.guild, "🔄 Weekly Reset", f"By {ctx.author.mention}", 0x3498db)

@bot.command(name="reset_monthly", description="Admin: Reset monthly XP")
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

@bot.command(name="setup_tickets", description="Admin: Setup the support ticket panel")
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

@bot.command(name="ticket_stats", description="Staff: View ticket statistics")
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

@bot.command(name="close_ticket", description="Close the current ticket")
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

@bot.command(name="serverstats", description="View server statistics")
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

@bot.command(name="topactive", description="View most active members this week")
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

@bot.command(name="achievements", description="View your achievements")
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

@bot.command(name="activity", description="View your activity graph")
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

@bot.command(name="elo_leaderboard", description="View the ELO leaderboard")
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

@bot.command(name="duel_history", description="View your duel history")
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

@bot.command(name="elo_reset", description="Admin: Reset all ELO ratings")
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

# OLD_TOURNEY: @bot.command(name="tournament_create")
# OLD_TOURNEY: @commands.has_permissions(administrator=True)
# OLD_TOURNEY: async def tournament_create_cmd(ctx, name: str, channel: discord.TextChannel = None, required_role: discord.Role = None, max_participants: int = 16):
# OLD_TOURNEY:     """
# OLD_TOURNEY:     Create a new tournament
# OLD_TOURNEY:     Usage: !tournament_create "Tournament Name" #channel @role max_participants
# OLD_TOURNEY:     Example: !tournament_create "Top 10 Showdown" #tournaments @Mainers 10
# OLD_TOURNEY:     """
# OLD_TOURNEY:     tournament = create_tournament(
# OLD_TOURNEY:         name, 
# OLD_TOURNEY:         ctx.author.id,
# OLD_TOURNEY:         required_role_id=str(required_role.id) if required_role else None,
# OLD_TOURNEY:         required_role_name=required_role.name if required_role else None,
# OLD_TOURNEY:         channel_id=str((channel or ctx.channel).id),
# OLD_TOURNEY:         max_participants=max(2, min(32, max_participants))
# OLD_TOURNEY:     )
# OLD_TOURNEY:     
# OLD_TOURNEY:     if not tournament:
# OLD_TOURNEY:         return await ctx.send("❌ There's already an active tournament! End it first with `!tournament_end confirm`")
# OLD_TOURNEY:     
# OLD_TOURNEY:     target_channel = channel or ctx.channel
# OLD_TOURNEY:     
# OLD_TOURNEY:     # Create embed with visual bracket text
# OLD_TOURNEY:     bracket_text = create_visual_bracket_text(tournament, ctx.guild)
# OLD_TOURNEY:     
# OLD_TOURNEY:     embed = discord.Embed(
# OLD_TOURNEY:         title=f"🏆 {name}",
# OLD_TOURNEY:         description=bracket_text,
# OLD_TOURNEY:         color=0xffd700
# OLD_TOURNEY:     )
# OLD_TOURNEY:     
# OLD_TOURNEY:     embed.add_field(name="Status", value="📝 Signups Open", inline=True)
# OLD_TOURNEY:     embed.add_field(name="Participants", value=f"0/{max_participants}", inline=True)
# OLD_TOURNEY:     
# OLD_TOURNEY:     if required_role:
# OLD_TOURNEY:         embed.add_field(name="🎭 Requirement", value=required_role.name, inline=True)
# OLD_TOURNEY:     
# OLD_TOURNEY:     embed.set_footer(text="✝ The Fallen Tournament ✝")
# OLD_TOURNEY:     
# OLD_TOURNEY:     msg = await target_channel.send(embed=embed, view=TournamentPanelView())
# OLD_TOURNEY:     
# OLD_TOURNEY:     # Save message ID
# OLD_TOURNEY:     data = load_tournaments()
# OLD_TOURNEY:     if data["active"]:
# OLD_TOURNEY:         data["active"]["message_id"] = str(msg.id)
# OLD_TOURNEY:         save_tournaments(data)
# OLD_TOURNEY:     
# OLD_TOURNEY:     role_text = f" (Requires **{required_role.name}**)" if required_role else ""
# OLD_TOURNEY:     await ctx.send(f"✅ Tournament **{name}** created in {target_channel.mention}!{role_text}")
# OLD_TOURNEY:     
# OLD_TOURNEY:     try:
# OLD_TOURNEY:         await ctx.message.delete()
# OLD_TOURNEY:     except:
# OLD_TOURNEY:         pass
# OLD_TOURNEY: 
# OLD_TOURNEY: 
# OLD_TOURNEY: @bot.command(name="setup_tournament")
# OLD_TOURNEY: @commands.has_permissions(administrator=True)
# OLD_TOURNEY: async def setup_tournament_panel(ctx):
# OLD_TOURNEY:     """Setup a tournament creation panel for staff"""
# OLD_TOURNEY:     embed = discord.Embed(
# OLD_TOURNEY:         title="🏆 Tournament System",
# OLD_TOURNEY:         description=(
# OLD_TOURNEY:             "Create and manage tournaments with visual brackets!\n\n"
# OLD_TOURNEY:             "**Features:**\n"
# OLD_TOURNEY:             "• Visual bracket display\n"
# OLD_TOURNEY:             "• Role restrictions (e.g., Mainer only)\n"
# OLD_TOURNEY:             "• Auto bracket generation\n"
# OLD_TOURNEY:             "• Easy winner reporting via dropdown\n"
# OLD_TOURNEY:             "• Champion rewards (5,000 coins + 500 XP)\n"
# OLD_TOURNEY:             "• Optional Top 10 auto-update\n\n"
# OLD_TOURNEY:             "Click **Create Tournament** below to start!"
# OLD_TOURNEY:         ),
# OLD_TOURNEY:         color=0xffd700
# OLD_TOURNEY:     )
# OLD_TOURNEY:     embed.set_footer(text="✝ The Fallen Tournament System ✝")
# OLD_TOURNEY:     
# OLD_TOURNEY:     await ctx.send(embed=embed, view=TournamentSetupView())
# OLD_TOURNEY:     await ctx.message.delete()
# OLD_TOURNEY: 
# OLD_TOURNEY: 
# OLD_TOURNEY: class TournamentSetupView(discord.ui.View):
# OLD_TOURNEY:     """Panel for staff to create tournaments"""
# OLD_TOURNEY:     def __init__(self):
# OLD_TOURNEY:         super().__init__(timeout=None)
# OLD_TOURNEY:     
# OLD_TOURNEY:     @discord.ui.button(label="➕ Create Tournament", style=discord.ButtonStyle.success, custom_id="tournament_setup_create")
# OLD_TOURNEY:     async def create_tournament_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
# OLD_TOURNEY:         if not is_staff(interaction.user):
# OLD_TOURNEY:             return await interaction.response.send_message("❌ Staff only!", ephemeral=True)
# OLD_TOURNEY:         
# OLD_TOURNEY:         data = load_tournaments()
# OLD_TOURNEY:         if data.get("active"):
# OLD_TOURNEY:             return await interaction.response.send_message(
# OLD_TOURNEY:                 "❌ There's already an active tournament! End it first with `!tournament_end confirm`",
# OLD_TOURNEY:                 ephemeral=True
# OLD_TOURNEY:             )
# OLD_TOURNEY:         
# OLD_TOURNEY:         await interaction.response.send_modal(TournamentCreateModal())
# OLD_TOURNEY:     
# OLD_TOURNEY:     @discord.ui.button(label="📊 View Current", style=discord.ButtonStyle.primary, custom_id="tournament_setup_view")
# OLD_TOURNEY:     async def view_current_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
# OLD_TOURNEY:         tournament = get_active_tournament()
# OLD_TOURNEY:         
# OLD_TOURNEY:         if not tournament:
# OLD_TOURNEY:             return await interaction.response.send_message("❌ No active tournament!", ephemeral=True)
# OLD_TOURNEY:         
# OLD_TOURNEY:         # Create visual bracket image
# OLD_TOURNEY:         if PIL_AVAILABLE and tournament.get("bracket"):
# OLD_TOURNEY:             try:
# OLD_TOURNEY:                 img = await create_tournament_bracket_image(tournament, interaction.guild)
# OLD_TOURNEY:                 if img:
# OLD_TOURNEY:                     file = discord.File(img, filename="bracket.png")
# OLD_TOURNEY:                     await interaction.response.send_message(file=file, ephemeral=True)
# OLD_TOURNEY:                     return
# OLD_TOURNEY:             except Exception as e:
# OLD_TOURNEY:                 print(f"Bracket image error: {e}")
# OLD_TOURNEY:         
# OLD_TOURNEY:         # Fallback to text
# OLD_TOURNEY:         bracket_text = create_visual_bracket_text(tournament, interaction.guild)
# OLD_TOURNEY:         embed = discord.Embed(
# OLD_TOURNEY:             title=f"🏆 {tournament['name']}",
# OLD_TOURNEY:             description=bracket_text,
# OLD_TOURNEY:             color=0xffd700 if tournament["status"] == "signup" else 0xe74c3c
# OLD_TOURNEY:         )
# OLD_TOURNEY:         await interaction.response.send_message(embed=embed, ephemeral=True)
# OLD_TOURNEY: 
# OLD_TOURNEY: 
# OLD_TOURNEY: class TournamentCreateModal(discord.ui.Modal, title="🏆 Create Tournament"):
# OLD_TOURNEY:     name = discord.ui.TextInput(
# OLD_TOURNEY:         label="Tournament Name",
# OLD_TOURNEY:         placeholder="e.g., Weekly 1v1, Top 10 Showdown",
# OLD_TOURNEY:         style=discord.TextStyle.short,
# OLD_TOURNEY:         required=True,
# OLD_TOURNEY:         max_length=50
# OLD_TOURNEY:     )
# OLD_TOURNEY:     
# OLD_TOURNEY:     channel_id = discord.ui.TextInput(
# OLD_TOURNEY:         label="Channel ID (where to post)",
# OLD_TOURNEY:         placeholder="Right-click channel > Copy Channel ID",
# OLD_TOURNEY:         style=discord.TextStyle.short,
# OLD_TOURNEY:         required=True,
# OLD_TOURNEY:         max_length=20
# OLD_TOURNEY:     )
# OLD_TOURNEY:     
# OLD_TOURNEY:     role_name = discord.ui.TextInput(
# OLD_TOURNEY:         label="Required Role Name (leave blank for open)",
# OLD_TOURNEY:         placeholder="e.g., Mainer, Stage 2",
# OLD_TOURNEY:         style=discord.TextStyle.short,
# OLD_TOURNEY:         required=False,
# OLD_TOURNEY:         max_length=50
# OLD_TOURNEY:     )
# OLD_TOURNEY:     
# OLD_TOURNEY:     max_participants = discord.ui.TextInput(
# OLD_TOURNEY:         label="Max Participants (2-32)",
# OLD_TOURNEY:         placeholder="16",
# OLD_TOURNEY:         style=discord.TextStyle.short,
# OLD_TOURNEY:         required=False,
# OLD_TOURNEY:         max_length=2
# OLD_TOURNEY:     )
# OLD_TOURNEY:     
# OLD_TOURNEY:     update_top10 = discord.ui.TextInput(
# OLD_TOURNEY:         label="Update Top 10 on win? (yes/no)",
# OLD_TOURNEY:         placeholder="no",
# OLD_TOURNEY:         style=discord.TextStyle.short,
# OLD_TOURNEY:         required=False,
# OLD_TOURNEY:         max_length=3
# OLD_TOURNEY:     )
# OLD_TOURNEY:     
# OLD_TOURNEY:     async def on_submit(self, interaction: discord.Interaction):
# OLD_TOURNEY:         # Parse inputs
# OLD_TOURNEY:         try:
# OLD_TOURNEY:             channel = interaction.guild.get_channel(int(self.channel_id.value))
# OLD_TOURNEY:             if not channel:
# OLD_TOURNEY:                 return await interaction.response.send_message("❌ Channel not found! Make sure you copied the Channel ID correctly.", ephemeral=True)
# OLD_TOURNEY:         except:
# OLD_TOURNEY:             return await interaction.response.send_message("❌ Invalid Channel ID!", ephemeral=True)
# OLD_TOURNEY:         
# OLD_TOURNEY:         # Find role if specified
# OLD_TOURNEY:         required_role = None
# OLD_TOURNEY:         role_name = self.role_name.value.strip() if self.role_name.value else None
# OLD_TOURNEY:         if role_name:
# OLD_TOURNEY:             required_role = discord.utils.get(interaction.guild.roles, name=role_name)
# OLD_TOURNEY:             if not required_role:
# OLD_TOURNEY:                 return await interaction.response.send_message(f"❌ Role '{role_name}' not found!", ephemeral=True)
# OLD_TOURNEY:         
# OLD_TOURNEY:         # Parse max participants
# OLD_TOURNEY:         try:
# OLD_TOURNEY:             max_p = int(self.max_participants.value) if self.max_participants.value else 16
# OLD_TOURNEY:             max_p = max(2, min(32, max_p))
# OLD_TOURNEY:         except:
# OLD_TOURNEY:             max_p = 16
# OLD_TOURNEY:         
# OLD_TOURNEY:         # Parse top 10 update
# OLD_TOURNEY:         update_top10 = self.update_top10.value.lower() in ["yes", "y", "true", "1"] if self.update_top10.value else False
# OLD_TOURNEY:         
# OLD_TOURNEY:         # Create tournament
# OLD_TOURNEY:         tournament = create_tournament(
# OLD_TOURNEY:             self.name.value,
# OLD_TOURNEY:             interaction.user.id,
# OLD_TOURNEY:             required_role_id=str(required_role.id) if required_role else None,
# OLD_TOURNEY:             required_role_name=required_role.name if required_role else None,
# OLD_TOURNEY:             channel_id=str(channel.id),
# OLD_TOURNEY:             max_participants=max_p
# OLD_TOURNEY:         )
# OLD_TOURNEY:         
# OLD_TOURNEY:         if not tournament:
# OLD_TOURNEY:             return await interaction.response.send_message("❌ Failed to create tournament!", ephemeral=True)
# OLD_TOURNEY:         
# OLD_TOURNEY:         # Add top 10 flag
# OLD_TOURNEY:         data = load_tournaments()
# OLD_TOURNEY:         if data["active"]:
# OLD_TOURNEY:             data["active"]["update_top10"] = update_top10
# OLD_TOURNEY:             save_tournaments(data)
# OLD_TOURNEY:         
# OLD_TOURNEY:         # Create signup embed
# OLD_TOURNEY:         bracket_text = create_visual_bracket_text(tournament, interaction.guild)
# OLD_TOURNEY:         
# OLD_TOURNEY:         embed = discord.Embed(
# OLD_TOURNEY:             title=f"🏆 {tournament['name']}",
# OLD_TOURNEY:             description=bracket_text,
# OLD_TOURNEY:             color=0xffd700
# OLD_TOURNEY:         )
# OLD_TOURNEY:         
# OLD_TOURNEY:         embed.add_field(name="Status", value="📝 Signups Open", inline=True)
# OLD_TOURNEY:         embed.add_field(name="Participants", value=f"0/{max_p}", inline=True)
# OLD_TOURNEY:         
# OLD_TOURNEY:         if required_role:
# OLD_TOURNEY:             embed.add_field(name="🎭 Requirement", value=required_role.name, inline=True)
# OLD_TOURNEY:         
# OLD_TOURNEY:         if update_top10:
# OLD_TOURNEY:             embed.add_field(name="🏅 Top 10", value="Winner updates Top 10!", inline=True)
# OLD_TOURNEY:         
# OLD_TOURNEY:         embed.set_footer(text="✝ The Fallen Tournament ✝")
# OLD_TOURNEY:         
# OLD_TOURNEY:         # Post to channel and ping role if set
# OLD_TOURNEY:         ping_content = required_role.mention if required_role else None
# OLD_TOURNEY:         msg = await channel.send(content=ping_content, embed=embed, view=TournamentPanelView())
# OLD_TOURNEY:         
# OLD_TOURNEY:         # Save message ID
# OLD_TOURNEY:         data = load_tournaments()
# OLD_TOURNEY:         if data["active"]:
# OLD_TOURNEY:             data["active"]["message_id"] = str(msg.id)
# OLD_TOURNEY:             save_tournaments(data)
# OLD_TOURNEY:         
# OLD_TOURNEY:         await interaction.response.send_message(
# OLD_TOURNEY:             f"✅ Tournament **{tournament['name']}** created!\n"
# OLD_TOURNEY:             f"📍 Posted in {channel.mention}\n"
# OLD_TOURNEY:             f"👥 Max: {max_p} participants\n"
# OLD_TOURNEY:             f"🎭 Role: {required_role.name if required_role else 'Open to all'}\n"
# OLD_TOURNEY:             f"🏅 Top 10 Update: {'Yes' if update_top10 else 'No'}",
# OLD_TOURNEY:             ephemeral=True
# OLD_TOURNEY:         )
# OLD_TOURNEY: 
# OLD_TOURNEY: 
# OLD_TOURNEY: async def create_tournament_bracket_image(tournament, guild):
# OLD_TOURNEY:     """Create a visual tournament bracket image"""
# OLD_TOURNEY:     if not PIL_AVAILABLE:
# OLD_TOURNEY:         return None
# OLD_TOURNEY:     
# OLD_TOURNEY:     bracket = tournament.get("bracket")
# OLD_TOURNEY:     if not bracket:
# OLD_TOURNEY:         return None
# OLD_TOURNEY:     
# OLD_TOURNEY:     rounds = bracket.get("rounds", [])
# OLD_TOURNEY:     if not rounds:
# OLD_TOURNEY:         return None
# OLD_TOURNEY:     
# OLD_TOURNEY:     num_rounds = len(rounds)
# OLD_TOURNEY:     first_round_matches = len(rounds[0])
# OLD_TOURNEY:     
# OLD_TOURNEY:     # Calculate dimensions
# OLD_TOURNEY:     match_width = 180
# OLD_TOURNEY:     match_height = 55
# OLD_TOURNEY:     round_spacing = 220
# OLD_TOURNEY:     
# OLD_TOURNEY:     width = num_rounds * round_spacing + 100
# OLD_TOURNEY:     height = max(400, first_round_matches * (match_height + 30) + 180)
# OLD_TOURNEY:     
# OLD_TOURNEY:     # Create dark background
# OLD_TOURNEY:     img = Image.new("RGBA", (width, height), (20, 20, 30, 255))
# OLD_TOURNEY:     draw = ImageDraw.Draw(img)
# OLD_TOURNEY:     
# OLD_TOURNEY:     # Load fonts
# OLD_TOURNEY:     try:
# OLD_TOURNEY:         title_font = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf", 24)
# OLD_TOURNEY:         round_font = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf", 14)
# OLD_TOURNEY:         name_font = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf", 13)
# OLD_TOURNEY:         small_font = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf", 10)
# OLD_TOURNEY:     except:
# OLD_TOURNEY:         title_font = round_font = name_font = small_font = ImageFont.load_default()
# OLD_TOURNEY:     
# OLD_TOURNEY:     # Header
# OLD_TOURNEY:     draw.rectangle([(0, 0), (width, 60)], fill=(139, 0, 0))
# OLD_TOURNEY:     title = f"🏆 {tournament['name'].upper()}"
# OLD_TOURNEY:     draw.text((width // 2, 30), title, font=title_font, fill=(255, 255, 255), anchor="mm")
# OLD_TOURNEY:     
# OLD_TOURNEY:     # Round names
# OLD_TOURNEY:     round_names = []
# OLD_TOURNEY:     for i in range(num_rounds):
# OLD_TOURNEY:         if i == num_rounds - 1:
# OLD_TOURNEY:             round_names.append("🏆 Finals")
# OLD_TOURNEY:         elif i == num_rounds - 2:
# OLD_TOURNEY:             round_names.append("Semifinals")
# OLD_TOURNEY:         elif i == num_rounds - 3:
# OLD_TOURNEY:             round_names.append("Quarterfinals")
# OLD_TOURNEY:         else:
# OLD_TOURNEY:             round_names.append(f"Round {i + 1}")
# OLD_TOURNEY:     
# OLD_TOURNEY:     # Draw connecting lines and matches
# OLD_TOURNEY:     match_positions = {}  # Store positions for drawing lines
# OLD_TOURNEY:     
# OLD_TOURNEY:     for round_idx, round_matches in enumerate(rounds):
# OLD_TOURNEY:         x = 50 + round_idx * round_spacing
# OLD_TOURNEY:         num_matches = len(round_matches)
# OLD_TOURNEY:         
# OLD_TOURNEY:         available_height = height - 140
# OLD_TOURNEY:         if num_matches > 0:
# OLD_TOURNEY:             spacing = available_height / num_matches
# OLD_TOURNEY:         else:
# OLD_TOURNEY:             spacing = available_height
# OLD_TOURNEY:         
# OLD_TOURNEY:         # Round label
# OLD_TOURNEY:         round_name = round_names[round_idx] if round_idx < len(round_names) else f"Round {round_idx + 1}"
# OLD_TOURNEY:         draw.text((x + match_width // 2, 80), round_name, font=round_font, fill=(200, 200, 200), anchor="mm")
# OLD_TOURNEY:         
# OLD_TOURNEY:         for match_idx, match in enumerate(round_matches):
# OLD_TOURNEY:             y = 100 + match_idx * spacing + (spacing - match_height) / 2
# OLD_TOURNEY:             
# OLD_TOURNEY:             # Store position
# OLD_TOURNEY:             match_positions[(round_idx, match_idx)] = (x + match_width, y + match_height // 2)
# OLD_TOURNEY:             
# OLD_TOURNEY:             # Draw connecting line to next round
# OLD_TOURNEY:             if round_idx < num_rounds - 1:
# OLD_TOURNEY:                 next_match_idx = match_idx // 2
# OLD_TOURNEY:                 next_x = 50 + (round_idx + 1) * round_spacing
# OLD_TOURNEY:                 next_y = 100 + next_match_idx * (available_height / len(rounds[round_idx + 1])) + ((available_height / len(rounds[round_idx + 1])) - match_height) / 2 + match_height // 2
# OLD_TOURNEY:                 
# OLD_TOURNEY:                 # Horizontal line from match
# OLD_TOURNEY:                 draw.line([(x + match_width, y + match_height // 2), (x + match_width + 15, y + match_height // 2)], fill=(80, 80, 80), width=2)
# OLD_TOURNEY:                 # Vertical line
# OLD_TOURNEY:                 mid_x = x + match_width + 15
# OLD_TOURNEY:                 draw.line([(mid_x, y + match_height // 2), (mid_x, next_y)], fill=(80, 80, 80), width=2)
# OLD_TOURNEY:             
# OLD_TOURNEY:             # Match box
# OLD_TOURNEY:             winner = match.get("winner")
# OLD_TOURNEY:             if winner:
# OLD_TOURNEY:                 box_fill = (40, 80, 40)  # Green for completed
# OLD_TOURNEY:             elif match.get("player1") and match.get("player2"):
# OLD_TOURNEY:                 box_fill = (80, 40, 40)  # Red for active
# OLD_TOURNEY:             else:
# OLD_TOURNEY:                 box_fill = (50, 50, 60)  # Grey for waiting
# OLD_TOURNEY:             
# OLD_TOURNEY:             draw.rectangle([(x, y), (x + match_width, y + match_height)], fill=box_fill, outline=(100, 100, 100))
# OLD_TOURNEY:             
# OLD_TOURNEY:             # Player 1
# OLD_TOURNEY:             p1_id = match.get("player1")
# OLD_TOURNEY:             if p1_id:
# OLD_TOURNEY:                 member1 = guild.get_member(int(p1_id))
# OLD_TOURNEY:                 p1_name = member1.display_name[:18] if member1 else "Unknown"
# OLD_TOURNEY:             else:
# OLD_TOURNEY:                 p1_name = "BYE" if round_idx == 0 else "TBD"
# OLD_TOURNEY:             
# OLD_TOURNEY:             p1_color = (255, 215, 0) if winner == p1_id else (255, 255, 255)
# OLD_TOURNEY:             draw.text((x + 10, y + 12), p1_name, font=name_font, fill=p1_color)
# OLD_TOURNEY:             
# OLD_TOURNEY:             # VS divider
# OLD_TOURNEY:             draw.line([(x + 5, y + match_height // 2), (x + match_width - 5, y + match_height // 2)], fill=(80, 80, 80), width=1)
# OLD_TOURNEY:             
# OLD_TOURNEY:             # Player 2
# OLD_TOURNEY:             p2_id = match.get("player2")
# OLD_TOURNEY:             if p2_id:
# OLD_TOURNEY:                 member2 = guild.get_member(int(p2_id))
# OLD_TOURNEY:                 p2_name = member2.display_name[:18] if member2 else "Unknown"
# OLD_TOURNEY:             else:
# OLD_TOURNEY:                 p2_name = "BYE" if round_idx == 0 else "TBD"
# OLD_TOURNEY:             
# OLD_TOURNEY:             p2_color = (255, 215, 0) if winner == p2_id else (255, 255, 255)
# OLD_TOURNEY:             draw.text((x + 10, y + match_height - 18), p2_name, font=name_font, fill=p2_color)
# OLD_TOURNEY:             
# OLD_TOURNEY:             # Match ID
# OLD_TOURNEY:             draw.text((x + match_width - 5, y + 5), match.get("id", ""), font=small_font, fill=(100, 100, 100), anchor="rt")
# OLD_TOURNEY:     
# OLD_TOURNEY:     # Champion box if complete
# OLD_TOURNEY:     if rounds[-1] and rounds[-1][0].get("winner"):
# OLD_TOURNEY:         winner_id = rounds[-1][0]["winner"]
# OLD_TOURNEY:         winner_member = guild.get_member(int(winner_id))
# OLD_TOURNEY:         winner_name = winner_member.display_name if winner_member else "Unknown"
# OLD_TOURNEY:         
# OLD_TOURNEY:         # Champion highlight
# OLD_TOURNEY:         champ_x = width - 150
# OLD_TOURNEY:         champ_y = height // 2 - 30
# OLD_TOURNEY:         draw.rectangle([(champ_x, champ_y), (champ_x + 120, champ_y + 60)], fill=(139, 0, 0), outline=(255, 215, 0), width=3)
# OLD_TOURNEY:         draw.text((champ_x + 60, champ_y + 15), "🏆 CHAMPION", font=small_font, fill=(255, 215, 0), anchor="mm")
# OLD_TOURNEY:         draw.text((champ_x + 60, champ_y + 40), winner_name[:15], font=name_font, fill=(255, 255, 255), anchor="mm")
# OLD_TOURNEY:     
# OLD_TOURNEY:     # Footer
# OLD_TOURNEY:     draw.text((width // 2, height - 15), "✝ The Fallen Tournament ✝", font=small_font, fill=(100, 100, 100), anchor="mm")
# OLD_TOURNEY:     
# OLD_TOURNEY:     # Save to buffer
# OLD_TOURNEY:     buffer = BytesIO()
# OLD_TOURNEY:     img.save(buffer, format="PNG")
# OLD_TOURNEY:     buffer.seek(0)
# OLD_TOURNEY:     return buffer
# OLD_TOURNEY: 
# OLD_TOURNEY: 
# OLD_TOURNEY: @bot.command(name="tournament_panel")
# OLD_TOURNEY: @commands.has_permissions(administrator=True)
# OLD_TOURNEY: async def tournament_panel_cmd(ctx):
# OLD_TOURNEY:     """Post the tournament panel"""
# OLD_TOURNEY:     tournament = get_active_tournament()
# OLD_TOURNEY:     
# OLD_TOURNEY:     if not tournament:
# OLD_TOURNEY:         return await ctx.send("❌ No active tournament! Create one with `!tournament_create`")
# OLD_TOURNEY:     
# OLD_TOURNEY:     bracket_text = create_visual_bracket_text(tournament, ctx.guild)
# OLD_TOURNEY:     
# OLD_TOURNEY:     embed = discord.Embed(
# OLD_TOURNEY:         title=f"🏆 {tournament['name']}",
# OLD_TOURNEY:         description=bracket_text,
# OLD_TOURNEY:         color=0xffd700 if tournament["status"] == "signup" else 0xe74c3c
# OLD_TOURNEY:     )
# OLD_TOURNEY:     
# OLD_TOURNEY:     status_map = {"signup": "📝 Signups Open", "active": "🔴 In Progress", "complete": "✅ Complete"}
# OLD_TOURNEY:     max_p = tournament.get("max_participants", 16)
# OLD_TOURNEY:     
# OLD_TOURNEY:     embed.add_field(name="Status", value=status_map.get(tournament["status"], tournament["status"]), inline=True)
# OLD_TOURNEY:     embed.add_field(name="Participants", value=f"{len(tournament['participants'])}/{max_p}", inline=True)
# OLD_TOURNEY:     
# OLD_TOURNEY:     if tournament.get("required_role_name"):
# OLD_TOURNEY:         embed.add_field(name="🎭 Requirement", value=tournament["required_role_name"], inline=True)
# OLD_TOURNEY:     
# OLD_TOURNEY:     embed.set_footer(text="✝ The Fallen Tournament ✝")
# OLD_TOURNEY:     
# OLD_TOURNEY:     await ctx.send(embed=embed, view=TournamentPanelView())
# OLD_TOURNEY: 
# OLD_TOURNEY: 
# OLD_TOURNEY: @bot.command(name="tournament_end")
# OLD_TOURNEY: @commands.has_permissions(administrator=True)
# OLD_TOURNEY: async def tournament_end_cmd(ctx, confirm: str = None):
# OLD_TOURNEY:     """End the current tournament"""
# OLD_TOURNEY:     if confirm != "confirm":
# OLD_TOURNEY:         return await ctx.send("⚠️ Use `!tournament_end confirm` to end the tournament.")
# OLD_TOURNEY:     
# OLD_TOURNEY:     if end_tournament():
# OLD_TOURNEY:         await ctx.send("✅ Tournament ended!")
# OLD_TOURNEY:     else:
# OLD_TOURNEY:         await ctx.send("❌ No active tournament!")

# ==========================================
# INACTIVITY COMMANDS
# ==========================================

@bot.command(name="inactivity_check", description="Staff: Run inactivity check on all ranked members")
@commands.has_any_role(*HIGH_STAFF_ROLES, STAFF_ROLE_NAME)
@commands.cooldown(1, 300, commands.BucketType.guild)  # Once per 5 minutes per server
async def inactivity_check_cmd(ctx):
    """Run inactivity check on Mainers with ranked roles"""
    await ctx.defer()
    
    # Add delay between each member check to avoid rate limits
    results = await run_inactivity_check(ctx.guild)
    
    embed = discord.Embed(
        title="📊 Inactivity Check Results",
        description=f"*Only checking members with **{INACTIVITY_REQUIRED_ROLE}** role*",
        color=0xe67e22,
        timestamp=datetime.datetime.now(datetime.timezone.utc)
    )
    
    embed.add_field(name="👥 Mainers Checked", value=str(results["checked"]), inline=True)
    embed.add_field(name="⚠️ Strikes Given", value=str(results["strikes_given"]), inline=True)
    embed.add_field(name="🛡️ Immune (Skipped)", value=str(results.get("skipped_immunity", 0)), inline=True)
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
    
    embed.set_footer(text=f"Threshold: {INACTIVITY_CHECK_DAYS} days • Mainers only")
    await ctx.send(embed=embed)
    
    # Log to dashboard
    await log_to_dashboard(
        ctx.guild, "📊 INACTIVITY", "Inactivity Check Ran",
        f"Checked {results['checked']} Mainers\n{results['strikes_given']} strikes given",
        color=0xe67e22,
        fields={"Demotions": str(results["demotions"]), "Kicks": str(results["kicks"]), "By": ctx.author.mention}
    )

@bot.command(name="inactivity_strikes", description="Check inactivity strikes for a user")
async def inactivity_strikes_cmd(ctx, member: discord.Member = None):
    """Check inactivity strikes for a user"""
    target = member or ctx.author
    
    # Staff can check anyone, members can only check themselves
    if member and member != ctx.author and not is_staff(ctx.author):
        return await ctx.send("❌ You can only check your own strikes.", ephemeral=True)
    
    strike_info = get_inactivity_strikes(target.id)
    user_data = get_user_data(target.id)
    
    # Check if they're a Mainer
    is_mainer_member = is_mainer(target)
    has_immunity = has_inactivity_immunity(target)
    
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
    
    # Tracking status
    if not is_mainer_member:
        tracking_status = "❌ Not Tracked (Not a Mainer)"
        embed.color = 0x95a5a6  # Grey
    elif has_immunity:
        tracking_status = "🛡️ Immune (Has Immunity Role)"
        embed.color = 0x3498db  # Blue
    else:
        tracking_status = "✅ Being Tracked"
    
    embed.add_field(name="📋 Tracking Status", value=tracking_status, inline=False)
    
    # Current rank
    if current_rank:
        embed.add_field(name="🏅 Current Rank", value=current_rank, inline=True)
    
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

@bot.command(name="add_inactivity_strike", description="Staff: Add an inactivity strike to a user")
@commands.has_any_role(*HIGH_STAFF_ROLES, STAFF_ROLE_NAME)
async def add_inactivity_strike_cmd(ctx, member: discord.Member, reason: str = "Manual strike by staff"):
    """Manually add an inactivity strike"""
    new_count = add_inactivity_strike(member.id, reason)
    
    # Check if demotion needed
    demoted = False
    kicked = False
    demoted_to_rank = None
    old_rank = None
    
    if new_count >= 3:
        strike_info = get_inactivity_strikes(member.id)
        if not strike_info.get("demoted"):
            # Get current rank and next rank using the stage system
            old_rank = get_member_rank(member)
            
            if old_rank:
                next_rank = get_next_demotion_rank(old_rank)
                
                if next_rank:
                    old_role = discord.utils.get(ctx.guild.roles, name=old_rank)
                    new_role = discord.utils.get(ctx.guild.roles, name=next_rank)
                    
                    try:
                        if old_role:
                            await member.remove_roles(old_role)
                            await asyncio.sleep(0.5)
                        if new_role:
                            await member.add_roles(new_role)
                        mark_user_demoted(member.id)
                        demoted = True
                        demoted_to_rank = next_rank
                    except:
                        pass
                else:
                    # Already at lowest rank, just mark as demoted
                    mark_user_demoted(member.id)
                    demoted = True
                    demoted_to_rank = old_rank  # Stays at same rank
    
    if new_count >= MAX_INACTIVITY_STRIKES:
        try:
            await send_inactivity_strike_dm(member, new_count, kicked=True)
            await member.kick(reason=f"Inactivity: {MAX_INACTIVITY_STRIKES} strikes")
            kicked = True
        except:
            pass
    else:
        await send_inactivity_strike_dm(member, new_count, demoted=demoted, old_rank=old_rank, new_rank=demoted_to_rank)
    
    # Response
    action_text = ""
    if kicked:
        action_text = " → **KICKED**"
    elif demoted and demoted_to_rank:
        # Show just the stage name without the full role name
        rank_display = demoted_to_rank.split("〢")[0] if "〢" in demoted_to_rank else demoted_to_rank
        action_text = f" → **DEMOTED to {rank_display}**"
    
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

@bot.command(name="remove_inactivity_strike", description="Staff: Remove an inactivity strike from a user")
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

@bot.command(name="clear_inactivity_strikes", description="Admin: Clear all inactivity strikes from a user")
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

@bot.command(name="inactive_list", description="Staff: Show all members with inactivity strikes")
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

@bot.command(name="set_inactivity_days", description="Admin: Set days of inactivity before strike")
@commands.has_permissions(administrator=True)
async def set_inactivity_days(ctx, days: int):
    """Set the number of days before inactivity strike"""
    global INACTIVITY_CHECK_DAYS
    
    if days < 1 or days > 90:
        return await ctx.send("❌ Days must be between 1 and 90.", ephemeral=True)
    
    INACTIVITY_CHECK_DAYS = days
    await ctx.send(f"✅ Inactivity threshold set to **{days} days**.", ephemeral=True)

@bot.command(name="immunity_add", description="Staff: Give inactivity immunity to a member")
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

@bot.command(name="immunity_remove", description="Staff: Remove inactivity immunity from a member")
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

@bot.command(name="immunity_list", description="Staff: View all members with inactivity immunity")
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

@bot.command(name="db_status", description="Admin: Check database status")
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

@bot.command(name="setup_logs", description="Admin: Setup the logging dashboard channel")
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

@bot.command(name="compare", description="Compare stats with another member")
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

@bot.command(name="leaderboards", description="View various leaderboards")
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
    """Handle command errors gracefully with friendly messages"""
    
    # Cooldown errors - show time remaining
    if isinstance(error, commands.CommandOnCooldown):
        minutes = int(error.retry_after // 60)
        seconds = int(error.retry_after % 60)
        
        if minutes > 0:
            time_str = f"{minutes}m {seconds}s"
        else:
            time_str = f"{seconds}s"
        
        embed = discord.Embed(
            title="⏰ Cooldown Active",
            description=(
                f"This command is on cooldown!\n\n"
                f"**Try again in:** {time_str}\n\n"
                f"*Cooldowns help keep the bot running smoothly for everyone.*"
            ),
            color=0xf39c12
        )
        embed.set_footer(text="✝ The Fallen ✝")
        
        try:
            await ctx.send(embed=embed, delete_after=10)
        except:
            pass
        return
    
    # Permission errors
    elif isinstance(error, commands.MissingPermissions):
        missing = ", ".join(error.missing_permissions)
        embed = discord.Embed(
            title="🔒 Permission Denied",
            description=f"You need the following permissions:\n`{missing}`",
            color=0xe74c3c
        )
        try:
            await ctx.send(embed=embed, delete_after=10)
        except:
            pass
        return
    
    elif isinstance(error, commands.MissingRole):
        embed = discord.Embed(
            title="🔒 Role Required",
            description="You don't have the required role for this command.",
            color=0xe74c3c
        )
        try:
            await ctx.send(embed=embed, delete_after=10)
        except:
            pass
        return
    
    elif isinstance(error, commands.MissingAnyRole):
        embed = discord.Embed(
            title="🔒 Role Required",
            description="You need one of the required roles to use this command.",
            color=0xe74c3c
        )
        try:
            await ctx.send(embed=embed, delete_after=10)
        except:
            pass
        return
    
    elif isinstance(error, commands.BotMissingPermissions):
        missing = ", ".join(error.missing_permissions)
        embed = discord.Embed(
            title="⚠️ Bot Missing Permissions",
            description=f"I need the following permissions to do that:\n`{missing}`\n\nPlease contact a server admin.",
            color=0xe74c3c
        )
        try:
            await ctx.send(embed=embed, delete_after=15)
        except:
            pass
        return
    
    # Rate limit errors
    elif isinstance(error, commands.CommandInvokeError):
        original = error.original
        
        # Check for rate limits (429 errors)
        if hasattr(original, 'status') and original.status == 429:
            retry_after = getattr(original, 'retry_after', 60)
            minutes = int(retry_after // 60) + 1
            
            embed = discord.Embed(
                title="🚫 Rate Limited",
                description=(
                    f"Discord is rate limiting the bot to prevent spam.\n\n"
                    f"**Please wait:** ~{minutes} minute(s)\n\n"
                    f"*This is a Discord protection - not a bug!*"
                ),
                color=0xe74c3c
            )
            embed.set_footer(text="✝ The Fallen ✝")
            
            try:
                await ctx.send(embed=embed, delete_after=30)
            except:
                pass
            
            print(f"[RATE LIMIT] Command: {ctx.command}, Retry after: {retry_after}s")
            return
        
        # Check for HTTPException with rate limit
        elif isinstance(original, discord.HTTPException):
            if original.status == 429 or "rate limit" in str(original).lower():
                embed = discord.Embed(
                    title="🚫 Rate Limited",
                    description=(
                        f"Discord is temporarily limiting requests.\n\n"
                        f"**Please wait:** ~1-2 minutes\n\n"
                        f"*The bot is fine - just need to slow down!*"
                    ),
                    color=0xe74c3c
                )
                try:
                    await ctx.send(embed=embed, delete_after=30)
                except:
                    pass
                return
    
    # Generic rate limit check in error string
    elif "429" in str(error) or "rate limit" in str(error).lower():
        embed = discord.Embed(
            title="🚫 Rate Limited",
            description=(
                f"Discord is temporarily limiting requests.\n\n"
                f"**Please wait:** ~1-2 minutes\n\n"
                f"*Try again shortly!*"
            ),
            color=0xe74c3c
        )
        try:
            await ctx.send(embed=embed, delete_after=30)
        except:
            pass
        print(f"[RATE LIMIT] {error}")
        return
    
    # Member not found
    elif isinstance(error, commands.MemberNotFound):
        embed = discord.Embed(
            title="❌ Member Not Found",
            description=f"Could not find that member. Make sure you're mentioning them correctly.",
            color=0xe74c3c
        )
        try:
            await ctx.send(embed=embed, delete_after=10)
        except:
            pass
        return
    
    # Role not found
    elif isinstance(error, commands.RoleNotFound):
        embed = discord.Embed(
            title="❌ Role Not Found",
            description=f"Could not find that role. Make sure you're mentioning it correctly.",
            color=0xe74c3c
        )
        try:
            await ctx.send(embed=embed, delete_after=10)
        except:
            pass
        return
    
    # Channel not found
    elif isinstance(error, commands.ChannelNotFound):
        embed = discord.Embed(
            title="❌ Channel Not Found",
            description=f"Could not find that channel.",
            color=0xe74c3c
        )
        try:
            await ctx.send(embed=embed, delete_after=10)
        except:
            pass
        return
    
    # Bad argument
    elif isinstance(error, commands.BadArgument):
        embed = discord.Embed(
            title="❌ Invalid Input",
            description=f"One of your inputs was invalid. Check the command usage.",
            color=0xe74c3c
        )
        try:
            await ctx.send(embed=embed, delete_after=10)
        except:
            pass
        return
    
    # Missing required argument
    elif isinstance(error, commands.MissingRequiredArgument):
        embed = discord.Embed(
            title="❌ Missing Argument",
            description=f"You're missing a required input: `{error.param.name}`",
            color=0xe74c3c
        )
        try:
            await ctx.send(embed=embed, delete_after=10)
        except:
            pass
        return
    
    # Command not found - ignore silently
    elif isinstance(error, commands.CommandNotFound):
        return
    
    # Log other errors
    else:
        print(f"[ERROR] Command: {ctx.command}, Error: {error}")


@bot.tree.error
async def on_app_command_error(interaction: discord.Interaction, error):
    """Handle slash command errors with friendly messages"""
    
    # Check if already responded
    responded = interaction.response.is_done()
    
    async def send_error(embed):
        try:
            if responded:
                await interaction.followup.send(embed=embed, ephemeral=True)
            else:
                await interaction.response.send_message(embed=embed, ephemeral=True)
        except:
            pass
    
    # Cooldown
    if isinstance(error, discord.app_commands.errors.CommandOnCooldown):
        minutes = int(error.retry_after // 60)
        seconds = int(error.retry_after % 60)
        
        if minutes > 0:
            time_str = f"{minutes}m {seconds}s"
        else:
            time_str = f"{seconds}s"
        
        embed = discord.Embed(
            title="⏰ Cooldown Active",
            description=(
                f"This command is on cooldown!\n\n"
                f"**Try again in:** {time_str}"
            ),
            color=0xf39c12
        )
        await send_error(embed)
        return
    
    # Missing permissions
    elif isinstance(error, discord.app_commands.errors.MissingPermissions):
        embed = discord.Embed(
            title="🔒 Permission Denied",
            description="You don't have permission to use this command.",
            color=0xe74c3c
        )
        await send_error(embed)
        return
    
    # Missing role
    elif isinstance(error, discord.app_commands.errors.MissingRole):
        embed = discord.Embed(
            title="🔒 Role Required",
            description="You need a specific role to use this command.",
            color=0xe74c3c
        )
        await send_error(embed)
        return
    
    # Bot missing permissions
    elif isinstance(error, discord.app_commands.errors.BotMissingPermissions):
        embed = discord.Embed(
            title="⚠️ Bot Missing Permissions",
            description="I don't have the required permissions. Please contact an admin.",
            color=0xe74c3c
        )
        await send_error(embed)
        return
    
    # Rate limits
    elif "429" in str(error) or "rate limit" in str(error).lower():
        embed = discord.Embed(
            title="🚫 Rate Limited",
            description=(
                f"Discord is temporarily limiting requests.\n\n"
                f"**Please wait:** ~1-2 minutes\n\n"
                f"*This protects the server from spam!*"
            ),
            color=0xe74c3c
        )
        await send_error(embed)
        print(f"[RATE LIMIT] Slash command error: {error}")
        return
    
    # Generic error
    else:
        embed = discord.Embed(
            title="❌ Something Went Wrong",
            description=(
                f"An error occurred while processing your request.\n\n"
                f"**What to do:**\n"
                f"• Wait a few seconds and try again\n"
                f"• If it persists, contact staff\n\n"
                f"*The issue has been logged.*"
            ),
            color=0xe74c3c
        )
        await send_error(embed)
        print(f"[ERROR] Slash command: {interaction.command}, Error: {error}")


# Global interaction error handler for buttons/modals
@bot.event
async def on_error(event, *args, **kwargs):
    """Global error handler for all events"""
    import traceback
    error = traceback.format_exc()
    
    if "429" in error or "rate limit" in error.lower():
        print(f"[RATE LIMIT] Event: {event}")
    else:
        print(f"[ERROR] Event: {event}\n{error}")


# ==========================================
# TICKET TRANSCRIPT SYSTEM
# ==========================================

def load_transcripts():
    """Load ticket transcripts from file"""
    try:
        with open(TRANSCRIPTS_FILE, "r") as f:
            return json.load(f)
    except:
        return {"transcripts": []}

def save_transcripts(data):
    """Save ticket transcripts to file"""
    with open(TRANSCRIPTS_FILE, "w") as f:
        json.dump(data, f, indent=2)

async def generate_transcript(channel, ticket_type="support", closer=None, ticket_info=None):
    """Generate a transcript of all messages in a ticket channel"""
    messages = []
    try:
        # channel.history() is a single paginated API call, not multiple calls
        # Limit to 200 messages to be safe
        async for msg in channel.history(limit=200, oldest_first=True):
            timestamp = msg.created_at.strftime("%Y-%m-%d %H:%M:%S")
            content = msg.content or ""
            
            # Handle attachments
            attachments = [att.url for att in msg.attachments] if msg.attachments else []
            
            # Handle embeds
            embed_texts = []
            for embed in msg.embeds:
                if embed.title:
                    embed_texts.append(f"[Embed: {embed.title}]")
                if embed.description:
                    embed_texts.append(embed.description[:200])
            
            messages.append({
                "timestamp": timestamp,
                "author": str(msg.author),
                "author_id": str(msg.author.id),
                "content": content,
                "attachments": attachments,
                "embeds": embed_texts
            })
    except Exception as e:
        print(f"Error generating transcript: {e}")
    
    # Create transcript entry
    transcript = {
        "id": f"transcript_{int(datetime.datetime.now().timestamp())}",
        "channel_name": channel.name,
        "ticket_type": ticket_type,
        "created_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "closed_by": str(closer) if closer else "Unknown",
        "closed_by_id": str(closer.id) if closer else None,
        "ticket_info": ticket_info or {},
        "message_count": len(messages),
        "messages": messages
    }
    
    # Save transcript
    data = load_transcripts()
    data["transcripts"].append(transcript)
    
    # Keep only last 500 transcripts
    if len(data["transcripts"]) > 500:
        data["transcripts"] = data["transcripts"][-500:]
    
    save_transcripts(data)
    
    return transcript

async def send_transcript_log(guild, transcript, user=None):
    """Send transcript summary to logs channel"""
    log_channel = discord.utils.get(guild.text_channels, name=LOG_CHANNEL_NAME)
    if not log_channel:
        log_channel = discord.utils.get(guild.text_channels, name="fallen-logs")
    
    if log_channel:
        embed = discord.Embed(
            title="📜 Ticket Transcript Saved",
            color=0x3498db,
            timestamp=datetime.datetime.now(datetime.timezone.utc)
        )
        embed.add_field(name="📋 Ticket", value=transcript["channel_name"], inline=True)
        embed.add_field(name="🏷️ Type", value=transcript["ticket_type"].title(), inline=True)
        embed.add_field(name="💬 Messages", value=str(transcript["message_count"]), inline=True)
        embed.add_field(name="🔒 Closed By", value=transcript["closed_by"], inline=True)
        embed.add_field(name="🆔 Transcript ID", value=f"`{transcript['id']}`", inline=False)
        
        if user:
            embed.add_field(name="👤 Ticket Owner", value=f"<@{user}>", inline=True)
        
        embed.set_footer(text="Use !transcript <id> to view full transcript")
        
        await log_channel.send(embed=embed)

@bot.command(name="transcript")
@commands.has_any_role(*HIGH_STAFF_ROLES, STAFF_ROLE_NAME)
@commands.cooldown(1, 10, commands.BucketType.user)  # 10 second cooldown
async def view_transcript(ctx, transcript_id: str):
    """View a ticket transcript by ID"""
    data = load_transcripts()
    
    transcript = None
    for t in data["transcripts"]:
        if t["id"] == transcript_id:
            transcript = t
            break
    
    if not transcript:
        return await ctx.send(f"❌ Transcript `{transcript_id}` not found.")
    
    # Create transcript text file
    lines = [
        f"═══════════════════════════════════════════════════",
        f"TICKET TRANSCRIPT - {transcript['channel_name']}",
        f"═══════════════════════════════════════════════════",
        f"Type: {transcript['ticket_type']}",
        f"Created: {transcript['created_at']}",
        f"Closed By: {transcript['closed_by']}",
        f"Messages: {transcript['message_count']}",
        f"═══════════════════════════════════════════════════",
        ""
    ]
    
    for msg in transcript["messages"]:
        lines.append(f"[{msg['timestamp']}] {msg['author']}:")
        if msg["content"]:
            lines.append(f"  {msg['content']}")
        if msg["attachments"]:
            lines.append(f"  📎 Attachments: {', '.join(msg['attachments'])}")
        if msg["embeds"]:
            lines.append(f"  📋 Embeds: {' | '.join(msg['embeds'])}")
        lines.append("")
    
    transcript_text = "\n".join(lines)
    
    # Send as file
    file = discord.File(
        BytesIO(transcript_text.encode()),
        filename=f"{transcript['id']}.txt"
    )
    
    embed = discord.Embed(
        title=f"📜 Transcript: {transcript['channel_name']}",
        description=f"**Type:** {transcript['ticket_type']}\n**Messages:** {transcript['message_count']}",
        color=0x3498db
    )
    
    await ctx.send(embed=embed, file=file)

@bot.command(name="transcripts")
@commands.has_any_role(*HIGH_STAFF_ROLES, STAFF_ROLE_NAME)
@commands.cooldown(1, 15, commands.BucketType.user)  # 15 second cooldown
async def list_transcripts(ctx, limit: int = 10):
    """List recent ticket transcripts"""
    data = load_transcripts()
    
    if not data["transcripts"]:
        return await ctx.send("📜 No transcripts found.")
    
    recent = data["transcripts"][-limit:][::-1]  # Most recent first
    
    embed = discord.Embed(
        title="📜 Recent Ticket Transcripts",
        color=0x3498db
    )
    
    lines = []
    for t in recent:
        date = t["created_at"][:10] if t.get("created_at") else "Unknown"
        lines.append(f"`{t['id']}` - {t['channel_name']} ({date})")
    
    embed.description = "\n".join(lines)
    embed.set_footer(text="Use !transcript <id> to view full transcript")
    
    await ctx.send(embed=embed)


# ==========================================
# ALT DETECTION SYSTEM
# ==========================================

# Store flagged users for alt detection
alt_flags = {}

def calculate_alt_score(member):
    """Calculate likelihood of account being an alt (0-100)"""
    score = 0
    reasons = []
    
    # Account age
    account_age = (datetime.datetime.now(datetime.timezone.utc) - member.created_at).days
    if account_age < 1:
        score += 40
        reasons.append("Account less than 1 day old")
    elif account_age < 7:
        score += 30
        reasons.append("Account less than 1 week old")
    elif account_age < 30:
        score += 15
        reasons.append("Account less than 1 month old")
    
    # No avatar
    if member.avatar is None:
        score += 15
        reasons.append("No profile picture")
    
    # Default username pattern (random letters/numbers)
    if re.match(r'^[a-z]+_?\d{4,}$', member.name.lower()):
        score += 20
        reasons.append("Default username pattern")
    
    # No bio/about me (can't check this via API easily)
    
    # Joined very recently
    if member.joined_at:
        time_in_server = (datetime.datetime.now(datetime.timezone.utc) - member.joined_at).total_seconds()
        if time_in_server < 60:  # Less than 1 minute
            score += 10
            reasons.append("Just joined")
    
    return min(score, 100), reasons

@bot.command(name="altcheck")
@commands.has_any_role(*HIGH_STAFF_ROLES, STAFF_ROLE_NAME)
@commands.cooldown(1, 5, commands.BucketType.user)  # 5 second cooldown
async def alt_check_cmd(ctx, member: discord.Member):
    """Check if a user might be an alt account"""
    score, reasons = calculate_alt_score(member)
    
    # Determine risk level
    if score >= 70:
        risk = "🔴 HIGH"
        color = 0xe74c3c
    elif score >= 50:
        risk = "🟠 MEDIUM"
        color = 0xf39c12
    elif score >= 30:
        risk = "🟡 LOW"
        color = 0xf1c40f
    else:
        risk = "🟢 MINIMAL"
        color = 0x2ecc71
    
    embed = discord.Embed(
        title="🔍 Alt Account Analysis",
        description=f"Analysis for {member.mention}",
        color=color
    )
    
    embed.add_field(name="⚠️ Risk Level", value=risk, inline=True)
    embed.add_field(name="📊 Score", value=f"{score}/100", inline=True)
    embed.add_field(name="📅 Account Age", value=f"{(datetime.datetime.now(datetime.timezone.utc) - member.created_at).days} days", inline=True)
    embed.add_field(name="📅 Account Created", value=f"<t:{int(member.created_at.timestamp())}:F>", inline=False)
    
    if member.joined_at:
        embed.add_field(name="📥 Joined Server", value=f"<t:{int(member.joined_at.timestamp())}:F>", inline=False)
    
    if reasons:
        embed.add_field(name="🚩 Risk Factors", value="\n".join(f"• {r}" for r in reasons), inline=False)
    else:
        embed.add_field(name="✅ Status", value="No risk factors detected", inline=False)
    
    embed.set_thumbnail(url=member.display_avatar.url)
    
    await ctx.send(embed=embed)

@bot.command(name="altflags")
@commands.has_any_role(*HIGH_STAFF_ROLES, STAFF_ROLE_NAME)
async def alt_flags_cmd(ctx):
    """View all flagged potential alt accounts"""
    if not alt_flags:
        return await ctx.send("✅ No accounts currently flagged.")
    
    embed = discord.Embed(
        title="🔍 Flagged Alt Accounts",
        color=0xe74c3c
    )
    
    lines = []
    for uid, data in list(alt_flags.items())[-15:]:  # Last 15
        member = ctx.guild.get_member(int(uid))
        name = member.mention if member else f"User {uid}"
        lines.append(f"{name} - Score: {data['score']}/100")
    
    embed.description = "\n".join(lines) or "No flags"
    embed.set_footer(text="Use !altcheck @user for details")
    
    await ctx.send(embed=embed)


# ==========================================
# LEGACY SYSTEM
# ==========================================

def load_legacy_data():
    """Load legacy data"""
    try:
        with open(LEGACY_FILE, "r") as f:
            return json.load(f)
    except:
        return {"members": {}, "milestones": []}

def save_legacy_data(data):
    """Save legacy data"""
    with open(LEGACY_FILE, "w") as f:
        json.dump(data, f, indent=2)

def get_legacy_status(member):
    """Calculate legacy status based on join date"""
    if not member.joined_at:
        return None, 0
    
    days_in_server = (datetime.datetime.now(datetime.timezone.utc) - member.joined_at).days
    
    # Legacy tiers
    if days_in_server >= 730:  # 2 years
        return "Eternal Legend", days_in_server
    elif days_in_server >= 365:  # 1 year
        return "Fallen Veteran", days_in_server
    elif days_in_server >= 180:  # 6 months
        return "Loyal Guardian", days_in_server
    elif days_in_server >= 90:  # 3 months
        return "Rising Fallen", days_in_server
    elif days_in_server >= 30:  # 1 month
        return "New Blood", days_in_server
    else:
        return "Fresh Recruit", days_in_server

def get_legacy_perks(legacy_tier):
    """Get perks for each legacy tier"""
    perks = {
        "Eternal Legend": ["2x Daily Coins", "Exclusive Legacy Role", "Custom Role Color", "Priority Support", "Legacy Badge"],
        "Fallen Veteran": ["1.5x Daily Coins", "Veteran Role", "Legacy Badge", "Priority Support"],
        "Loyal Guardian": ["1.25x Daily Coins", "Guardian Role", "Legacy Badge"],
        "Rising Fallen": ["1.1x Daily Coins", "Rising Role"],
        "New Blood": ["Standard perks"],
        "Fresh Recruit": ["Welcome bonus"]
    }
    return perks.get(legacy_tier, [])

def get_legacy_multiplier(legacy_tier):
    """Get coin multiplier for legacy tier"""
    multipliers = {
        "Eternal Legend": 2.0,
        "Fallen Veteran": 1.5,
        "Loyal Guardian": 1.25,
        "Rising Fallen": 1.1,
        "New Blood": 1.0,
        "Fresh Recruit": 1.0
    }
    return multipliers.get(legacy_tier, 1.0)

@bot.hybrid_command(name="legacy", description="View your legacy status")
async def legacy_cmd(ctx, member: discord.Member = None):
    """View legacy status and perks"""
    target = member or ctx.author
    
    tier, days = get_legacy_status(target)
    perks = get_legacy_perks(tier)
    multiplier = get_legacy_multiplier(tier)
    
    # Tier colors
    tier_colors = {
        "Eternal Legend": 0xFFD700,  # Gold
        "Fallen Veteran": 0x9B59B6,  # Purple
        "Loyal Guardian": 0x3498DB,  # Blue
        "Rising Fallen": 0x2ECC71,   # Green
        "New Blood": 0x95A5A6,       # Gray
        "Fresh Recruit": 0x7F8C8D    # Dark Gray
    }
    
    # Progress to next tier
    tier_days = {
        "Fresh Recruit": 30,
        "New Blood": 90,
        "Rising Fallen": 180,
        "Loyal Guardian": 365,
        "Fallen Veteran": 730,
        "Eternal Legend": None
    }
    
    next_tier_days = tier_days.get(tier)
    
    embed = discord.Embed(
        title=f"📜 {target.display_name}'s Legacy",
        color=tier_colors.get(tier, 0x8B0000)
    )
    
    embed.add_field(name="🏆 Legacy Tier", value=f"**{tier}**", inline=True)
    embed.add_field(name="📅 Days in Server", value=f"{days:,} days", inline=True)
    embed.add_field(name="💰 Coin Multiplier", value=f"{multiplier}x", inline=True)
    
    if target.joined_at:
        embed.add_field(name="📥 Joined", value=f"<t:{int(target.joined_at.timestamp())}:D>", inline=True)
    
    embed.add_field(name="✨ Perks", value="\n".join(f"• {p}" for p in perks), inline=False)
    
    # Progress bar to next tier
    if next_tier_days:
        current_tier_start = {
            "Fresh Recruit": 0,
            "New Blood": 30,
            "Rising Fallen": 90,
            "Loyal Guardian": 180,
            "Fallen Veteran": 365
        }.get(tier, 0)
        
        progress = min((days - current_tier_start) / (next_tier_days - current_tier_start) * 100, 100)
        bar_filled = int(progress / 10)
        bar = "█" * bar_filled + "░" * (10 - bar_filled)
        
        days_left = next_tier_days - days
        next_tier_name = list(tier_days.keys())[list(tier_days.values()).index(next_tier_days) + 1] if next_tier_days else "Max"
        
        embed.add_field(
            name=f"📈 Progress to {next_tier_name}",
            value=f"`{bar}` {progress:.1f}%\n{days_left} days remaining",
            inline=False
        )
    else:
        embed.add_field(name="🎉 Status", value="**MAX TIER ACHIEVED!**", inline=False)
    
    embed.set_thumbnail(url=target.display_avatar.url)
    embed.set_footer(text="✝ The Fallen Legacy System ✝")
    
    await ctx.send(embed=embed)

@bot.command(name="legacytop")
@commands.cooldown(1, 30, commands.BucketType.user)  # 30 second cooldown (iterates members)
async def legacy_top(ctx, limit: int = 10):
    """View members with longest tenure"""
    members_with_dates = []
    
    for member in ctx.guild.members:
        if member.joined_at and not member.bot:
            days = (datetime.datetime.now(datetime.timezone.utc) - member.joined_at).days
            tier, _ = get_legacy_status(member)
            members_with_dates.append((member, days, tier))
    
    # Sort by days
    members_with_dates.sort(key=lambda x: x[1], reverse=True)
    
    embed = discord.Embed(
        title="📜 Legacy Leaderboard",
        description="Members with longest tenure",
        color=0xFFD700
    )
    
    lines = []
    for i, (member, days, tier) in enumerate(members_with_dates[:limit], 1):
        medal = "🥇" if i == 1 else "🥈" if i == 2 else "🥉" if i == 3 else f"#{i}"
        lines.append(f"{medal} **{member.display_name}** - {days:,} days ({tier})")
    
    embed.description = "\n".join(lines)
    embed.set_footer(text="Use /legacy to view your status")
    
    await ctx.send(embed=embed)


# ==========================================
# PRACTICE MODE / SPARRING SYSTEM
# ==========================================

def load_practice_data():
    """Load practice session data"""
    try:
        with open(PRACTICE_FILE, "r") as f:
            return json.load(f)
    except:
        return {"sessions": [], "ratings": {}, "queue": [], "stats": {}}

def save_practice_data(data):
    """Save practice session data"""
    with open(PRACTICE_FILE, "w") as f:
        json.dump(data, f, indent=2)

# Practice queue
practice_queue = []  # [{user_id, skill_level, queued_at, server_link}]

class PracticeQueueView(discord.ui.View):
    """Spar finder based on Stage/Rank/Strength"""
    def __init__(self):
        super().__init__(timeout=None)
    
    @discord.ui.button(label="🎯 Find Spar", style=discord.ButtonStyle.success, custom_id="practice_join_queue")
    async def join_queue(self, interaction: discord.Interaction, button: discord.ui.Button):
        global practice_queue
        user_id = str(interaction.user.id)
        
        # Check if already in queue
        for entry in practice_queue:
            if entry["user_id"] == user_id:
                return await interaction.response.send_message("❌ You're already in the queue!", ephemeral=True)
        
        # Get user's stage/rank/strength from roles
        member_rank = get_member_spar_rank(interaction.user)
        
        if not member_rank["stage"]:
            # No stage role - show as Unranked
            member_rank = {"stage": None, "stage_num": 99, "rank": None, "strength": None, "display": "Unranked"}
        
        # Show their detected rank and confirm
        await interaction.response.send_message(
            f"**🎯 Join Spar Queue**\n\n"
            f"**Your Detected Rank:**\n"
            f"• Stage: **{member_rank['display'] if member_rank['stage'] else 'Unranked'}**\n"
            f"• Rank Level: **{member_rank['rank'] or 'Not Set'}**\n"
            f"• Strength: **{member_rank['strength'] or 'Not Set'}**\n\n"
            f"Is this correct?",
            view=ConfirmJoinQueueView(member_rank),
            ephemeral=True
        )
    
    @discord.ui.button(label="📋 View Queue", style=discord.ButtonStyle.primary, custom_id="practice_view_queue")
    async def view_queue(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not practice_queue:
            return await interaction.response.send_message("📋 The spar queue is currently empty!", ephemeral=True)
        
        embed = discord.Embed(
            title="⚔️ Spar Queue",
            color=0x8B0000
        )
        
        lines = []
        now = datetime.datetime.now(datetime.timezone.utc)
        for i, entry in enumerate(practice_queue[:15], 1):
            user = interaction.guild.get_member(int(entry["user_id"]))
            name = user.display_name if user else f"User {entry['user_id']}"
            
            # Build rank display
            rank_display = entry.get("display", "Unranked")
            rank_level = entry.get("rank", "")
            strength = entry.get("strength", "")
            tier = entry.get("tier", get_spar_tier(entry))
            
            extra = []
            if rank_level:
                extra.append(rank_level)
            if strength:
                extra.append(strength)
            extra_str = f" ({'/'.join(extra)})" if extra else ""
            
            # Tier display
            tier_str = f"T{tier}" if tier != 99 else "Unranked"
            
            # Calculate wait time
            try:
                queued_at = datetime.datetime.fromisoformat(entry["queued_at"].replace('Z', '+00:00'))
                wait_mins = int((now - queued_at).total_seconds() / 60)
                wait_str = f"{wait_mins}m" if wait_mins > 0 else "<1m"
            except:
                wait_str = "?"
            
            lines.append(f"{i}. **{name}** - {rank_display}{extra_str} | **{tier_str}** | ⏱️ {wait_str}")
        
        embed.description = "\n".join(lines)
        embed.set_footer(text=f"{len(practice_queue)} player(s) in queue • Tiers 1-54 (lower = stronger)")
        
        await interaction.response.send_message(embed=embed, view=SparChallengeSelectView(practice_queue, interaction.user.id), ephemeral=True)
    
    @discord.ui.button(label="⚔️ Challenge", style=discord.ButtonStyle.secondary, custom_id="practice_challenge")
    async def challenge_player(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not practice_queue:
            return await interaction.response.send_message("📋 No one is in the queue to challenge!", ephemeral=True)
        
        # Filter out self
        available = [p for p in practice_queue if p["user_id"] != str(interaction.user.id)]
        if not available:
            return await interaction.response.send_message("📋 No other players in queue to challenge!", ephemeral=True)
        
        await interaction.response.send_message(
            "**Select a player to challenge:**",
            view=SparChallengeSelectView(available, interaction.user.id),
            ephemeral=True
        )
    
    @discord.ui.button(label="🔍 Find Match", style=discord.ButtonStyle.success, custom_id="practice_find_match")
    async def find_match(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Auto-find a suitable opponent"""
        user_id = str(interaction.user.id)
        
        # Check if user is in queue
        user_entry = None
        for entry in practice_queue:
            if entry["user_id"] == user_id:
                user_entry = entry
                break
        
        if not user_entry:
            return await interaction.response.send_message("❌ You need to join the queue first!", ephemeral=True)
        
        # Find suitable opponents
        suitable = find_suitable_opponents(user_entry, practice_queue)
        
        user_tier = get_spar_tier(user_entry)
        
        if not suitable:
            return await interaction.response.send_message(
                f"❌ No suitable opponents found in queue.\n"
                f"Your Tier: **{user_tier}** ({user_entry.get('display', 'Unranked')})\n\n"
                f"Try challenging someone directly or wait for more players!",
                ephemeral=True
            )
        
        # Show suitable opponents
        embed = discord.Embed(
            title="⚔️ Suitable Opponents",
            description=f"**Your Tier:** {user_tier} ({user_entry.get('display', 'Unranked')})\n\nThese players match your skill level:",
            color=0x2ecc71
        )
        
        lines = []
        for opp in suitable[:5]:
            user = interaction.guild.get_member(int(opp["user_id"]))
            name = user.display_name if user else "Unknown"
            rank_display = opp.get("display", "Unranked")
            opp_tier = opp.get("tier", get_spar_tier(opp))
            compatibility = opp.get("compatibility", "Unknown")
            tier_diff = opp.get("tier_diff", abs(user_tier - opp_tier))
            lines.append(f"{compatibility}\n  **{name}** - {rank_display} (T{opp_tier}, {tier_diff} tiers apart)")
        
        embed.description += "\n\n" + "\n".join(lines)
        embed.set_footer(text="Select a player to challenge!")
        
        await interaction.response.send_message(
            embed=embed,
            view=SparChallengeSelectView(suitable, interaction.user.id),
            ephemeral=True
        )
    
    @discord.ui.button(label="🚪 Leave Queue", style=discord.ButtonStyle.danger, custom_id="practice_leave_queue")
    async def leave_queue(self, interaction: discord.Interaction, button: discord.ui.Button):
        global practice_queue
        
        user_id = str(interaction.user.id)
        initial_len = len(practice_queue)
        practice_queue = [p for p in practice_queue if p["user_id"] != user_id]
        
        if len(practice_queue) < initial_len:
            await interaction.response.send_message("✅ You've left the spar queue.", ephemeral=True)
        else:
            await interaction.response.send_message("❌ You're not in the queue.", ephemeral=True)


def get_member_spar_rank(member):
    """Get a member's stage/rank/strength from their roles"""
    result = {
        "stage": None,
        "stage_num": 99,  # Default high for unranked
        "rank": None,
        "strength": None,
        "display": "Unranked"
    }
    
    stage_mapping = {
        "Stage 0": (0, "Stage 0〢FALLEN DEITY"),
        "Stage 1": (1, "Stage 1〢FALLEN APEX"),
        "Stage 2": (2, "Stage 2〢FALLEN ASCENDANT"),
        "Stage 3": (3, "Stage 3〢FORSAKEN WARRIOR"),
        "Stage 4": (4, "Stage 4〢ABYSS-TOUCHED"),
        "Stage 5": (5, "Stage 5〢BROKEN INITIATE"),
    }
    
    for role in member.roles:
        role_name = role.name
        
        # Check for stage
        for stage_key, (stage_num, full_name) in stage_mapping.items():
            if stage_key in role_name or full_name in role_name:
                result["stage"] = full_name
                result["stage_num"] = stage_num
                result["display"] = f"Stage {stage_num}"
                break
        
        # Check for rank level
        if role_name in RANK_LEVELS:
            result["rank"] = role_name
        
        # Check for strength
        if role_name in STRENGTH_LEVELS:
            result["strength"] = role_name
    
    return result


def get_spar_tier(entry):
    """
    Convert Stage/Rank/Strength to a numerical tier for matchmaking.
    
    Full Hierarchy (54 possible combinations):
    Stage 0 (FALLEN DEITY) - Tiers 1-9
    Stage 1 (FALLEN APEX) - Tiers 10-18
    Stage 2 (FALLEN ASCENDANT) - Tiers 19-27
    Stage 3 (FORSAKEN WARRIOR) - Tiers 28-36
    Stage 4 (ABYSS-TOUCHED) - Tiers 37-45
    Stage 5 (BROKEN INITIATE) - Tiers 46-54
    Unranked - Tier 99
    """
    stage_num = entry.get("stage_num", 99)
    rank = entry.get("rank")  # High, Mid, Low
    strength = entry.get("strength")  # Strong, Stable, Weak
    
    # If unranked
    if stage_num == 99 or stage_num is None:
        return 99
    
    # Base tier from stage (each stage has 9 tiers)
    base_tier = stage_num * 9
    
    # Rank adjustment (0-6)
    rank_tiers = {"High": 0, "Mid": 3, "Low": 6}
    rank_add = rank_tiers.get(rank, 3)  # Default to Mid if not set
    
    # Strength adjustment (0-2)
    strength_tiers = {"Strong": 0, "Stable": 1, "Weak": 2}
    strength_add = strength_tiers.get(strength, 1)  # Default to Stable if not set
    
    return base_tier + rank_add + strength_add + 1  # +1 so tiers start at 1


# Complete tier mapping for reference
TIER_MAP = {
    # Stage 0 - FALLEN DEITY (Tiers 1-9)
    (0, "High", "Strong"): 1,
    (0, "High", "Stable"): 2,
    (0, "High", "Weak"): 3,
    (0, "Mid", "Strong"): 4,
    (0, "Mid", "Stable"): 5,
    (0, "Mid", "Weak"): 6,
    (0, "Low", "Strong"): 7,
    (0, "Low", "Stable"): 8,
    (0, "Low", "Weak"): 9,
    
    # Stage 1 - FALLEN APEX (Tiers 10-18)
    (1, "High", "Strong"): 10,
    (1, "High", "Stable"): 11,
    (1, "High", "Weak"): 12,
    (1, "Mid", "Strong"): 13,
    (1, "Mid", "Stable"): 14,
    (1, "Mid", "Weak"): 15,
    (1, "Low", "Strong"): 16,
    (1, "Low", "Stable"): 17,
    (1, "Low", "Weak"): 18,
    
    # Stage 2 - FALLEN ASCENDANT (Tiers 19-27)
    (2, "High", "Strong"): 19,
    (2, "High", "Stable"): 20,
    (2, "High", "Weak"): 21,
    (2, "Mid", "Strong"): 22,
    (2, "Mid", "Stable"): 23,
    (2, "Mid", "Weak"): 24,
    (2, "Low", "Strong"): 25,
    (2, "Low", "Stable"): 26,
    (2, "Low", "Weak"): 27,
    
    # Stage 3 - FORSAKEN WARRIOR (Tiers 28-36)
    (3, "High", "Strong"): 28,
    (3, "High", "Stable"): 29,
    (3, "High", "Weak"): 30,
    (3, "Mid", "Strong"): 31,
    (3, "Mid", "Stable"): 32,
    (3, "Mid", "Weak"): 33,
    (3, "Low", "Strong"): 34,
    (3, "Low", "Stable"): 35,
    (3, "Low", "Weak"): 36,
    
    # Stage 4 - ABYSS-TOUCHED (Tiers 37-45)
    (4, "High", "Strong"): 37,
    (4, "High", "Stable"): 38,
    (4, "High", "Weak"): 39,
    (4, "Mid", "Strong"): 40,
    (4, "Mid", "Stable"): 41,
    (4, "Mid", "Weak"): 42,
    (4, "Low", "Strong"): 43,
    (4, "Low", "Stable"): 44,
    (4, "Low", "Weak"): 45,
    
    # Stage 5 - BROKEN INITIATE (Tiers 46-54)
    (5, "High", "Strong"): 46,
    (5, "High", "Stable"): 47,
    (5, "High", "Weak"): 48,
    (5, "Mid", "Strong"): 49,
    (5, "Mid", "Stable"): 50,
    (5, "Mid", "Weak"): 51,
    (5, "Low", "Strong"): 52,
    (5, "Low", "Stable"): 53,
    (5, "Low", "Weak"): 54,
}


def get_tier_display(tier):
    """Get display name for a tier"""
    if tier == 99:
        return "Unranked"
    
    for (stage, rank, strength), t in TIER_MAP.items():
        if t == tier:
            stage_names = {
                0: "FALLEN DEITY",
                1: "FALLEN APEX", 
                2: "FALLEN ASCENDANT",
                3: "FORSAKEN WARRIOR",
                4: "ABYSS-TOUCHED",
                5: "BROKEN INITIATE"
            }
            return f"Stage {stage} / {rank} / {strength}"
    
    return f"Tier {tier}"


def find_suitable_opponents(user_entry, queue):
    """
    Find suitable opponents based on Stage/Rank/Strength tier system.
    
    Matching Rules:
    - Perfect Match: Same tier or ±1 tier
    - Good Match: ±2 to ±3 tiers  
    - Fair Match: ±4 to ±6 tiers (within same stage usually)
    - Cross-stage matches allowed if within 6 tiers
    - Unranked can match with anyone in Stage 4-5 or other Unranked
    """
    user_tier = get_spar_tier(user_entry)
    user_id = user_entry["user_id"]
    user_stage = user_entry.get("stage_num", 99)
    
    suitable = []
    
    for entry in queue:
        if entry["user_id"] == user_id:
            continue
        
        opponent_tier = get_spar_tier(entry)
        opponent_stage = entry.get("stage_num", 99)
        tier_diff = abs(user_tier - opponent_tier)
        
        # Special handling for Unranked
        if user_tier == 99 or opponent_tier == 99:
            # Unranked can match with:
            # - Other Unranked (perfect)
            # - Stage 4-5 players (fair)
            if user_tier == 99 and opponent_tier == 99:
                compatibility = "⭐ Perfect Match"
                priority = 0
            elif opponent_stage in [4, 5] or user_stage in [4, 5]:
                compatibility = "⚠️ Fair Match"
                priority = 3
            else:
                continue  # Skip - too big skill gap
        else:
            # Determine compatibility based on tier difference
            if tier_diff <= 1:
                compatibility = "⭐ Perfect Match"
                priority = 0
            elif tier_diff <= 3:
                compatibility = "✅ Good Match"
                priority = 1
            elif tier_diff <= 6:
                compatibility = "⚠️ Fair Match"
                priority = 2
            elif tier_diff <= 9:  # Allow up to one full stage difference
                compatibility = "⚠️ Challenging"
                priority = 3
            else:
                continue  # Skip - too big skill gap
        
        entry_copy = entry.copy()
        entry_copy["compatibility"] = compatibility
        entry_copy["tier_diff"] = tier_diff
        entry_copy["priority"] = priority
        entry_copy["tier"] = opponent_tier
        suitable.append(entry_copy)
    
    # Sort by priority first, then tier difference
    suitable.sort(key=lambda x: (x["priority"], x["tier_diff"]))
    
    return suitable


def get_full_rank_display(entry):
    """Get full rank display string"""
    stage_num = entry.get("stage_num", 99)
    rank = entry.get("rank")
    strength = entry.get("strength")
    
    if stage_num == 99:
        return "Unranked"
    
    stage_names = {
        0: "FALLEN DEITY",
        1: "FALLEN APEX",
        2: "FALLEN ASCENDANT", 
        3: "FORSAKEN WARRIOR",
        4: "ABYSS-TOUCHED",
        5: "BROKEN INITIATE"
    }
    
    stage_name = stage_names.get(stage_num, f"Stage {stage_num}")
    
    parts = [f"Stage {stage_num}"]
    if rank:
        parts.append(rank)
    if strength:
        parts.append(strength)
    
    return " / ".join(parts)


class ConfirmJoinQueueView(discord.ui.View):
    def __init__(self, member_rank: dict):
        super().__init__(timeout=60)
        self.member_rank = member_rank
    
    @discord.ui.button(label="✅ Join Queue", style=discord.ButtonStyle.success)
    async def confirm_join(self, interaction: discord.Interaction, button: discord.ui.Button):
        global practice_queue
        user_id = str(interaction.user.id)
        
        # Double check not already in queue
        for entry in practice_queue:
            if entry["user_id"] == user_id:
                return await interaction.response.edit_message(content="❌ You're already in the queue!", view=None)
        
        # Get partner rating
        data = load_practice_data()
        rating_data = data["ratings"].get(user_id, {"total": 0, "count": 0})
        avg_rating = (rating_data["total"] / rating_data["count"]) if rating_data["count"] > 0 else 0
        
        # Calculate tier
        tier = get_spar_tier(self.member_rank)
        
        # Add to queue
        practice_queue.append({
            "user_id": user_id,
            "stage": self.member_rank.get("stage"),
            "stage_num": self.member_rank.get("stage_num", 99),
            "rank": self.member_rank.get("rank"),
            "strength": self.member_rank.get("strength"),
            "display": self.member_rank.get("display", "Unranked"),
            "tier": tier,
            "avg_rating": avg_rating,
            "queued_at": datetime.datetime.now(datetime.timezone.utc).isoformat()
        })
        
        tier_display = f"Tier {tier}" if tier != 99 else "Unranked"
        
        await interaction.response.edit_message(
            content=(
                f"✅ You've joined the spar queue!\n\n"
                f"**Your Rank:** {self.member_rank.get('display', 'Unranked')}\n"
                f"**Tier:** {tier_display}\n"
                f"**Position:** #{len(practice_queue)}\n\n"
                f"Use **🔍 Find Match** to find suitable opponents!"
            ),
            view=None
        )
    
    @discord.ui.button(label="❌ Cancel", style=discord.ButtonStyle.secondary)
    async def cancel(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.edit_message(content="❌ Cancelled.", view=None)


class SparChallengeSelectView(discord.ui.View):
    def __init__(self, queue_entries, challenger_id):
        super().__init__(timeout=60)
        self.challenger_id = challenger_id
        
        # Create dropdown options
        options = []
        for entry in queue_entries[:25]:  # Max 25 options
            if entry["user_id"] != str(challenger_id):
                display = entry.get("display", "Unranked")
                rank = entry.get("rank", "")
                strength = entry.get("strength", "")
                compatibility = entry.get("compatibility", "")
                
                desc_parts = []
                if rank:
                    desc_parts.append(rank)
                if strength:
                    desc_parts.append(strength)
                if compatibility:
                    desc_parts.append(compatibility)
                
                options.append(discord.SelectOption(
                    label=f"{display}",
                    value=entry["user_id"],
                    description=" | ".join(desc_parts)[:100] if desc_parts else "Challenge this player"
                ))
        
        if options:
            self.select = discord.ui.Select(
                placeholder="Select a player to challenge...",
                options=options
            )
            self.select.callback = self.select_callback
            self.add_item(self.select)
    
    async def select_callback(self, interaction: discord.Interaction):
        target_id = self.select.values[0]
        target = interaction.guild.get_member(int(target_id))
        
        if not target:
            return await interaction.response.send_message("❌ Player not found!", ephemeral=True)
        
        # Get challenger's rank
        challenger_rank = get_member_spar_rank(interaction.user)
        
        # Send challenge
        await interaction.response.edit_message(
            content=f"⚔️ Challenge sent to **{target.display_name}**!",
            view=None
        )
        
        # DM the target
        challenge_embed = discord.Embed(
            title="⚔️ Spar Challenge!",
            description=f"**{interaction.user.display_name}** has challenged you to a spar!",
            color=0xf39c12
        )
        
        challenge_embed.add_field(
            name="Challenger's Rank",
            value=(
                f"**{challenger_rank.get('display', 'Unranked')}**\n"
                f"Rank: {challenger_rank.get('rank', 'N/A')} | Strength: {challenger_rank.get('strength', 'N/A')}"
            ),
            inline=False
        )
        challenge_embed.set_footer(text="Accept or Decline below")
        
        try:
            await target.send(
                embed=challenge_embed,
                view=SparChallengeResponseView(str(interaction.user.id), target_id, interaction.guild.id)
            )
        except:
            await interaction.followup.send(
                f"⚠️ Couldn't DM {target.mention}. They may have DMs disabled.",
                ephemeral=True
            )


class SparChallengeResponseView(discord.ui.View):
    def __init__(self, challenger_id: str, target_id: str, guild_id: int):
        super().__init__(timeout=300)  # 5 minute timeout
        self.challenger_id = challenger_id
        self.target_id = target_id
        self.guild_id = guild_id
    
    @discord.ui.button(label="✅ Accept", style=discord.ButtonStyle.success)
    async def accept(self, interaction: discord.Interaction, button: discord.ui.Button):
        global practice_queue
        
        # Remove both from queue if they're in it
        practice_queue = [p for p in practice_queue if p["user_id"] not in [self.challenger_id, self.target_id]]
        
        guild = interaction.client.get_guild(self.guild_id)
        if not guild:
            return await interaction.response.send_message("❌ Server not found!", ephemeral=True)
        
        challenger = guild.get_member(int(self.challenger_id))
        target = guild.get_member(int(self.target_id))
        
        if not challenger or not target:
            return await interaction.response.send_message("❌ Players not found!", ephemeral=True)
        
        await interaction.response.edit_message(content="✅ Challenge accepted! Creating match...", view=None)
        
        await create_spar_match(guild, challenger, target)
    
    @discord.ui.button(label="❌ Decline", style=discord.ButtonStyle.danger)
    async def decline(self, interaction: discord.Interaction, button: discord.ui.Button):
        guild = interaction.client.get_guild(self.guild_id)
        challenger = guild.get_member(int(self.challenger_id)) if guild else None
        
        await interaction.response.edit_message(content="❌ Challenge declined.", view=None)
        
        if challenger:
            try:
                await challenger.send(f"❌ {interaction.user.display_name} declined your spar challenge.")
            except:
                pass


async def create_spar_match(guild, player1, player2):
    """Create a spar match between two players"""
    global practice_queue
    
    # Get ranks
    p1_rank = get_member_spar_rank(player1)
    p2_rank = get_member_spar_rank(player2)
    
    # Get partner ratings
    data = load_practice_data()
    p1_rating = data["ratings"].get(str(player1.id), {"total": 0, "count": 0})
    p2_rating = data["ratings"].get(str(player2.id), {"total": 0, "count": 0})
    p1_avg = (p1_rating["total"] / p1_rating["count"]) if p1_rating["count"] > 0 else 0
    p2_avg = (p2_rating["total"] / p2_rating["count"]) if p2_rating["count"] > 0 else 0
    
    # Create session
    session_id = f"spar_{int(datetime.datetime.now().timestamp())}"
    session_data = {
        "id": session_id,
        "player1": str(player1.id),
        "player2": str(player2.id),
        "player1_rank": p1_rank,
        "player2_rank": p2_rank,
        "player1_rating": p1_avg,
        "player2_rating": p2_avg,
        "status": "active",
        "created_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "rounds_won": 0,
        "rounds_lost": 0,
        "ratings": {},
        "result_confirmed": False
    }
    data["sessions"].append(session_data)
    save_practice_data(data)
    
    # Create ticket channel
    try:
        cat = discord.utils.get(guild.categories, name="Spar Matches")
        if not cat:
            cat = await guild.create_category("Spar Matches", overwrites={
                guild.default_role: discord.PermissionOverwrite(read_messages=False)
            })
            await asyncio.sleep(1)
        
        overwrites = {
            guild.default_role: discord.PermissionOverwrite(read_messages=False),
            player1: discord.PermissionOverwrite(read_messages=True, send_messages=True, attach_files=True),
            player2: discord.PermissionOverwrite(read_messages=True, send_messages=True, attach_files=True),
            guild.me: discord.PermissionOverwrite(read_messages=True, send_messages=True, manage_channels=True)
        }
        
        staff_role = discord.utils.get(guild.roles, name=STAFF_ROLE_NAME)
        if staff_role:
            overwrites[staff_role] = discord.PermissionOverwrite(read_messages=True, send_messages=True)
        
        for role_name in HIGH_STAFF_ROLES:
            role = discord.utils.get(guild.roles, name=role_name)
            if role:
                overwrites[role] = discord.PermissionOverwrite(read_messages=True, send_messages=True)
        
        await asyncio.sleep(0.5)
        channel = await guild.create_text_channel(
            name=f"spar-{player1.name[:8]}-vs-{player2.name[:8]}",
            category=cat,
            overwrites=overwrites
        )
        
        # Update session with channel ID
        for s in data["sessions"]:
            if s["id"] == session_id:
                s["channel_id"] = channel.id
                break
        save_practice_data(data)
        
        # Create match embed
        match_embed = discord.Embed(
            title="⚔️ Spar Match",
            description=f"**{player1.mention}** vs **{player2.mention}**",
            color=0x8B0000,
            timestamp=datetime.datetime.now(datetime.timezone.utc)
        )
        
        # Player 1 info
        p1_stars = "⭐" * round(p1_avg) + "☆" * (5 - round(p1_avg)) if p1_avg > 0 else "No ratings"
        p1_extra = []
        if p1_rank.get("rank"):
            p1_extra.append(p1_rank["rank"])
        if p1_rank.get("strength"):
            p1_extra.append(p1_rank["strength"])
        
        match_embed.add_field(
            name=f"👤 {player1.display_name}",
            value=(
                f"**{p1_rank.get('display', 'Unranked')}**\n"
                f"{' / '.join(p1_extra) if p1_extra else 'No rank details'}\n"
                f"Rating: {p1_stars}"
            ),
            inline=True
        )
        
        # Player 2 info
        p2_stars = "⭐" * round(p2_avg) + "☆" * (5 - round(p2_avg)) if p2_avg > 0 else "No ratings"
        p2_extra = []
        if p2_rank.get("rank"):
            p2_extra.append(p2_rank["rank"])
        if p2_rank.get("strength"):
            p2_extra.append(p2_rank["strength"])
        
        match_embed.add_field(
            name=f"👤 {player2.display_name}",
            value=(
                f"**{p2_rank.get('display', 'Unranked')}**\n"
                f"{' / '.join(p2_extra) if p2_extra else 'No rank details'}\n"
                f"Rating: {p2_stars}"
            ),
            inline=True
        )
        
        match_embed.add_field(
            name="🔗 Server Link",
            value="**Post your private server link below!**",
            inline=False
        )
        
        match_embed.add_field(
            name="📋 Instructions",
            value=(
                "1️⃣ Post your **private server link** below\n"
                "2️⃣ Complete your spar (e.g., FT5, FT10)\n"
                "3️⃣ Post **screenshot/video proof** of the final score\n"
                "4️⃣ Click **📊 Submit Result** to log the score\n"
                "5️⃣ Rate your partner after!"
            ),
            inline=False
        )
        
        match_embed.add_field(name="🆔 Session", value=f"`{session_id}`", inline=True)
        match_embed.add_field(name="⏱️ Started", value=f"<t:{int(datetime.datetime.now().timestamp())}:R>", inline=True)
        
        match_embed.set_footer(text="✝ The Fallen Spar System ✝")
        
        await asyncio.sleep(0.5)
        await channel.send(
            f"{player1.mention} {player2.mention}",
            embed=match_embed,
            view=SparControlView(session_id, player1.id, player2.id)
        )
        
        # DM both players
        dm_embed = discord.Embed(
            title="⚔️ Spar Match Created!",
            description=f"Your spar match is ready!\n\n**Go to:** {channel.mention}",
            color=0x2ecc71
        )
        
        await asyncio.sleep(0.5)
        try:
            await player1.send(embed=dm_embed)
            await asyncio.sleep(1)
        except:
            pass
        try:
            await player2.send(embed=dm_embed)
        except:
            pass
        
        return channel
        
    except Exception as e:
        print(f"Error creating spar match: {e}")
        return None


class SparControlView(discord.ui.View):
    def __init__(self, session_id: str, player1_id: int, player2_id: int):
        super().__init__(timeout=None)
        self.session_id = session_id
        self.player1_id = player1_id
        self.player2_id = player2_id
    
    @discord.ui.button(label="📊 Submit Result", style=discord.ButtonStyle.success, custom_id="spar_submit_result", row=0)
    async def submit_result(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user.id not in [self.player1_id, self.player2_id] and not is_staff(interaction.user):
            return await interaction.response.send_message("❌ Only match participants or staff can submit results!", ephemeral=True)
        
        await interaction.response.send_modal(SparResultModal(self.session_id, self.player1_id, self.player2_id))
    
    @discord.ui.button(label="🚨 Request Staff", style=discord.ButtonStyle.secondary, custom_id="spar_request_staff", row=0)
    async def request_staff(self, interaction: discord.Interaction, button: discord.ui.Button):
        staff_role = discord.utils.get(interaction.guild.roles, name=STAFF_ROLE_NAME)
        
        embed = discord.Embed(
            title="🚨 Staff Assistance Requested",
            description=f"{interaction.user.mention} needs staff help with this spar.",
            color=0xe74c3c
        )
        
        if staff_role:
            await interaction.channel.send(f"{staff_role.mention}", embed=embed)
        else:
            await interaction.channel.send(embed=embed)
        
        await interaction.response.send_message("✅ Staff has been notified!", ephemeral=True)
    
    @discord.ui.button(label="⭐ Rate Partner", style=discord.ButtonStyle.primary, custom_id="spar_rate_partner", row=1)
    async def rate_partner(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user.id == self.player1_id:
            rated_id = self.player2_id
        elif interaction.user.id == self.player2_id:
            rated_id = self.player1_id
        else:
            return await interaction.response.send_message("❌ Only participants can rate!", ephemeral=True)
        
        await interaction.response.send_modal(SparRatingModal(self.session_id, rated_id, interaction.user.id))
    
    @discord.ui.button(label="⏱️ Match Time", style=discord.ButtonStyle.secondary, custom_id="spar_match_time", row=1)
    async def match_time(self, interaction: discord.Interaction, button: discord.ui.Button):
        data = load_practice_data()
        
        session = None
        for s in data["sessions"]:
            if s["id"] == self.session_id:
                session = s
                break
        
        if not session:
            return await interaction.response.send_message("❌ Session not found!", ephemeral=True)
        
        try:
            created = datetime.datetime.fromisoformat(session["created_at"].replace('Z', '+00:00'))
            elapsed = datetime.datetime.now(datetime.timezone.utc) - created
            mins = int(elapsed.total_seconds() / 60)
            secs = int(elapsed.total_seconds() % 60)
            
            await interaction.response.send_message(f"⏱️ **Match Duration:** {mins}m {secs}s", ephemeral=True)
        except:
            await interaction.response.send_message("⏱️ Unable to calculate time", ephemeral=True)
    
    @discord.ui.button(label="🔒 Close (Staff)", style=discord.ButtonStyle.danger, custom_id="spar_close", row=1)
    async def close_channel(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not is_staff(interaction.user):
            return await interaction.response.send_message("❌ Only staff can close!", ephemeral=True)
        
        await interaction.response.send_message("🔒 Closing in 5 seconds...")
        await asyncio.sleep(5)
        
        try:
            await interaction.channel.delete()
        except:
            pass


class SparResultModal(discord.ui.Modal, title="📊 Submit Spar Result"):
    winner = discord.ui.TextInput(
        label="Who won? (1 or 2)",
        placeholder="Enter 1 for Player 1, or 2 for Player 2",
        style=discord.TextStyle.short,
        required=True,
        min_length=1,
        max_length=1
    )
    
    winner_rounds = discord.ui.TextInput(
        label="Winner's Rounds (e.g., 5 for FT5)",
        placeholder="How many rounds did the winner win?",
        style=discord.TextStyle.short,
        required=True,
        max_length=3
    )
    
    loser_rounds = discord.ui.TextInput(
        label="Loser's Rounds",
        placeholder="How many rounds did the loser win?",
        style=discord.TextStyle.short,
        required=True,
        max_length=3
    )
    
    def __init__(self, session_id: str, player1_id: int, player2_id: int):
        super().__init__()
        self.session_id = session_id
        self.player1_id = player1_id
        self.player2_id = player2_id
    
    async def on_submit(self, interaction: discord.Interaction):
        try:
            winner_num = int(self.winner.value)
            won = int(self.winner_rounds.value)
            lost = int(self.loser_rounds.value)
        except ValueError:
            return await interaction.response.send_message("❌ Please enter valid numbers!", ephemeral=True)
        
        if winner_num not in [1, 2]:
            return await interaction.response.send_message("❌ Winner must be 1 or 2!", ephemeral=True)
        
        if won < 0 or lost < 0:
            return await interaction.response.send_message("❌ Rounds cannot be negative!", ephemeral=True)
        
        if won <= lost:
            return await interaction.response.send_message("❌ Winner must have more rounds!", ephemeral=True)
        
        winner_id = self.player1_id if winner_num == 1 else self.player2_id
        loser_id = self.player2_id if winner_num == 1 else self.player1_id
        
        data = load_practice_data()
        
        session = None
        for s in data["sessions"]:
            if s["id"] == self.session_id:
                session = s
                break
        
        if not session:
            return await interaction.response.send_message("❌ Session not found!", ephemeral=True)
        
        if session.get("result_confirmed"):
            return await interaction.response.send_message("❌ Result already confirmed!", ephemeral=True)
        
        # Update session
        session["status"] = "completed"
        session["winner"] = str(winner_id)
        session["loser"] = str(loser_id)
        session["rounds_won"] = won
        session["rounds_lost"] = lost
        session["completed_at"] = datetime.datetime.now(datetime.timezone.utc).isoformat()
        session["submitted_by"] = str(interaction.user.id)
        session["result_confirmed"] = True
        
        # Update stats
        w_id = str(winner_id)
        l_id = str(loser_id)
        
        if w_id not in data["stats"]:
            data["stats"][w_id] = {"wins": 0, "losses": 0, "rounds_won": 0, "rounds_lost": 0, "sessions": 0}
        if l_id not in data["stats"]:
            data["stats"][l_id] = {"wins": 0, "losses": 0, "rounds_won": 0, "rounds_lost": 0, "sessions": 0}
        
        data["stats"][w_id]["wins"] += 1
        data["stats"][w_id]["rounds_won"] += won
        data["stats"][w_id]["rounds_lost"] += lost
        data["stats"][w_id]["sessions"] += 1
        
        data["stats"][l_id]["losses"] += 1
        data["stats"][l_id]["rounds_won"] += lost
        data["stats"][l_id]["rounds_lost"] += won
        data["stats"][l_id]["sessions"] += 1
        
        save_practice_data(data)
        
        winner = interaction.guild.get_member(winner_id)
        loser = interaction.guild.get_member(loser_id)
        
        # Calculate duration
        try:
            created = datetime.datetime.fromisoformat(session["created_at"].replace('Z', '+00:00'))
            elapsed = datetime.datetime.now(datetime.timezone.utc) - created
            duration = f"{int(elapsed.total_seconds() / 60)}m {int(elapsed.total_seconds() % 60)}s"
        except:
            duration = "Unknown"
        
        result_embed = discord.Embed(
            title="🏆 Spar Complete!",
            color=0x2ecc71
        )
        result_embed.add_field(name="🥇 Winner", value=winner.mention if winner else "Unknown", inline=True)
        result_embed.add_field(name="🥈 Loser", value=loser.mention if loser else "Unknown", inline=True)
        result_embed.add_field(name="📊 Score", value=f"**{won}** - {lost}", inline=True)
        result_embed.add_field(name="⏱️ Duration", value=duration, inline=True)
        result_embed.add_field(name="📝 Submitted By", value=interaction.user.mention, inline=True)
        result_embed.set_footer(text="Don't forget to rate your partner!")
        
        await interaction.response.send_message(embed=result_embed)
        
        # Check auto-close
        await check_spar_auto_close(interaction.channel, self.session_id, data)


class SparRatingModal(discord.ui.Modal, title="⭐ Rate Your Partner"):
    rating = discord.ui.TextInput(
        label="Rating (1-5 stars)",
        placeholder="Enter 1, 2, 3, 4, or 5",
        style=discord.TextStyle.short,
        required=True,
        min_length=1,
        max_length=1
    )
    
    feedback = discord.ui.TextInput(
        label="Feedback (Optional)",
        placeholder="How was your spar? Good sportsmanship?",
        style=discord.TextStyle.paragraph,
        required=False,
        max_length=200
    )
    
    def __init__(self, session_id: str, rated_user_id: int, rater_id: int):
        super().__init__()
        self.session_id = session_id
        self.rated_user_id = rated_user_id
        self.rater_id = rater_id
    
    async def on_submit(self, interaction: discord.Interaction):
        try:
            rating_val = int(self.rating.value)
        except ValueError:
            return await interaction.response.send_message("❌ Please enter 1-5!", ephemeral=True)
        
        if rating_val < 1 or rating_val > 5:
            return await interaction.response.send_message("❌ Rating must be 1-5!", ephemeral=True)
        
        data = load_practice_data()
        
        session = None
        for s in data["sessions"]:
            if s["id"] == self.session_id:
                session = s
                break
        
        if not session:
            return await interaction.response.send_message("❌ Session not found!", ephemeral=True)
        
        if "ratings" not in session:
            session["ratings"] = {}
        
        if str(self.rater_id) in session["ratings"]:
            return await interaction.response.send_message("❌ You've already rated!", ephemeral=True)
        
        # Save rating
        session["ratings"][str(self.rater_id)] = {
            "rated_user": str(self.rated_user_id),
            "rating": rating_val,
            "feedback": self.feedback.value or "",
            "rated_at": datetime.datetime.now(datetime.timezone.utc).isoformat()
        }
        
        # Update overall rating
        rated_id = str(self.rated_user_id)
        if rated_id not in data["ratings"]:
            data["ratings"][rated_id] = {"total": 0, "count": 0, "feedback": []}
        
        data["ratings"][rated_id]["total"] += rating_val
        data["ratings"][rated_id]["count"] += 1
        if self.feedback.value:
            data["ratings"][rated_id]["feedback"].append({
                "from": str(self.rater_id),
                "rating": rating_val,
                "feedback": self.feedback.value[:200]
            })
        
        save_practice_data(data)
        
        stars = "⭐" * rating_val + "☆" * (5 - rating_val)
        rated_user = interaction.guild.get_member(self.rated_user_id)
        
        # DM the rated user
        if rated_user:
            try:
                dm_embed = discord.Embed(
                    title="⭐ You Received a Rating!",
                    description=f"**{interaction.user.display_name}** rated you after your spar!",
                    color=0xf1c40f
                )
                dm_embed.add_field(name="Rating", value=stars, inline=True)
                if self.feedback.value:
                    dm_embed.add_field(name="Feedback", value=self.feedback.value, inline=False)
                
                await rated_user.send(embed=dm_embed)
            except:
                pass
        
        await interaction.response.send_message(f"✅ You rated {rated_user.mention if rated_user else 'your partner'} {stars}", ephemeral=True)
        
        # Post in channel
        await interaction.channel.send(
            embed=discord.Embed(
                title="⭐ Rating Submitted",
                description=f"{interaction.user.mention} rated their partner {stars}",
                color=0xf1c40f
            )
        )
        
        # Check auto-close
        await check_spar_auto_close(interaction.channel, self.session_id, data)


async def check_spar_auto_close(channel, session_id, data):
    """Check if spar channel should auto-close"""
    session = None
    for s in data["sessions"]:
        if s["id"] == session_id:
            session = s
            break
    
    if not session:
        return
    
    if not session.get("result_confirmed"):
        return
    
    ratings = session.get("ratings", {})
    if len(ratings) >= 2:
        await asyncio.sleep(3)
        
        await channel.send(
            embed=discord.Embed(
                title="✅ Spar Complete!",
                description="Both players have rated. Channel closing in 10 seconds...",
                color=0x2ecc71
            )
        )
        
        await asyncio.sleep(10)
        try:
            await channel.delete()
        except:
            pass
        
        if won < 0 or lost < 0:
            return await interaction.response.send_message("❌ Rounds cannot be negative!", ephemeral=True)
        
        if won <= lost:
            return await interaction.response.send_message("❌ Winner must have more rounds!", ephemeral=True)
        
        winner_id = self.player1_id if winner_num == 1 else self.player2_id
        loser_id = self.player2_id if winner_num == 1 else self.player1_id
        
        data = load_practice_data()
        
        session = None
        for s in data["sessions"]:
            if s["id"] == self.session_id:
                session = s
                break
        
        if not session:
            return await interaction.response.send_message("❌ Session not found!", ephemeral=True)
        
        if session.get("result_confirmed"):
            return await interaction.response.send_message("❌ Result already confirmed!", ephemeral=True)
        
        # Update session
        session["status"] = "completed"
        session["winner"] = str(winner_id)
        session["loser"] = str(loser_id)
        session["rounds_won"] = won
        session["rounds_lost"] = lost
        session["completed_at"] = datetime.datetime.now(datetime.timezone.utc).isoformat()
        session["submitted_by"] = str(interaction.user.id)
        session["result_confirmed"] = True
        
        # Update stats
        w_id = str(winner_id)
        l_id = str(loser_id)
        
        if w_id not in data["stats"]:
            data["stats"][w_id] = {"wins": 0, "losses": 0, "rounds_won": 0, "rounds_lost": 0, "sessions": 0}
        if l_id not in data["stats"]:
            data["stats"][l_id] = {"wins": 0, "losses": 0, "rounds_won": 0, "rounds_lost": 0, "sessions": 0}
        
        data["stats"][w_id]["wins"] += 1
        data["stats"][w_id]["rounds_won"] += won
        data["stats"][w_id]["rounds_lost"] += lost
        data["stats"][w_id]["sessions"] += 1
        
        data["stats"][l_id]["losses"] += 1
        data["stats"][l_id]["rounds_won"] += lost
        data["stats"][l_id]["rounds_lost"] += won
        data["stats"][l_id]["sessions"] += 1
        
        save_practice_data(data)
        
        winner = interaction.guild.get_member(winner_id)
        loser = interaction.guild.get_member(loser_id)
        
        # Calculate match duration
        try:
            created = datetime.datetime.fromisoformat(session["created_at"].replace('Z', '+00:00'))
            elapsed = datetime.datetime.now(datetime.timezone.utc) - created
            duration = f"{int(elapsed.total_seconds() / 60)}m {int(elapsed.total_seconds() % 60)}s"
        except:
            duration = "Unknown"
        
        result_embed = discord.Embed(
            title="🏆 Match Complete!",
            color=0x2ecc71
        )
        result_embed.add_field(name="🥇 Winner", value=winner.mention if winner else "Unknown", inline=True)
        result_embed.add_field(name="🥈 Loser", value=loser.mention if loser else "Unknown", inline=True)
        result_embed.add_field(name="📊 Score", value=f"**{won}** - {lost}", inline=True)
        result_embed.add_field(name="⏱️ Duration", value=duration, inline=True)
        result_embed.add_field(name="📝 Submitted By", value=interaction.user.mention, inline=True)
        result_embed.add_field(
            name="⭐ Rate Your Partner",
            value="Click the **Rate Partner** button to rate your opponent!",
            inline=False
        )
        result_embed.set_footer(text="Channel will auto-close once both players have rated (or click Skip Rating)")
        
        await interaction.response.send_message(embed=result_embed)
        
        # Check if should auto-close (both rated or skipped)
        await check_practice_auto_close(interaction.channel, self.session_id, data)



@bot.hybrid_command(name="practice_stats", description="View your practice statistics")
async def practice_stats(ctx, member: discord.Member = None):
    """View practice session statistics"""
    target = member or ctx.author
    target_id = str(target.id)
    
    data = load_practice_data()
    
    stats = data["stats"].get(target_id, {"wins": 0, "losses": 0, "rounds_won": 0, "rounds_lost": 0, "sessions": 0})
    rating_data = data["ratings"].get(target_id, {"total": 0, "count": 0})
    
    total_sessions = stats["sessions"]
    wins = stats["wins"]
    losses = stats["losses"]
    win_rate = (wins / total_sessions * 100) if total_sessions > 0 else 0
    
    rounds_won = stats["rounds_won"]
    rounds_lost = stats["rounds_lost"]
    total_rounds = rounds_won + rounds_lost
    round_win_rate = (rounds_won / total_rounds * 100) if total_rounds > 0 else 0
    
    avg_rating = (rating_data["total"] / rating_data["count"]) if rating_data["count"] > 0 else 0
    stars = "⭐" * round(avg_rating) + "☆" * (5 - round(avg_rating)) if avg_rating > 0 else "No ratings yet"
    
    embed = discord.Embed(
        title=f"🎯 {target.display_name}'s Practice Stats",
        color=0x3498db
    )
    
    embed.add_field(name="📊 Sessions", value=f"{total_sessions}", inline=True)
    embed.add_field(name="🏆 Wins", value=f"{wins}", inline=True)
    embed.add_field(name="💀 Losses", value=f"{losses}", inline=True)
    embed.add_field(name="📈 Win Rate", value=f"{win_rate:.1f}%", inline=True)
    embed.add_field(name="🎯 Rounds Won", value=f"{rounds_won}", inline=True)
    embed.add_field(name="❌ Rounds Lost", value=f"{rounds_lost}", inline=True)
    embed.add_field(name="📊 Round Win Rate", value=f"{round_win_rate:.1f}%", inline=True)
    embed.add_field(name="⭐ Partner Rating", value=f"{stars} ({avg_rating:.1f}/5)" if avg_rating > 0 else "No ratings", inline=True)
    embed.add_field(name="📝 Reviews", value=f"{rating_data['count']}", inline=True)
    
    embed.set_thumbnail(url=target.display_avatar.url)
    embed.set_footer(text="✝ The Fallen Practice System ✝")
    
    await ctx.send(embed=embed)

@bot.command(name="setup_practice")
@commands.has_permissions(administrator=True)
async def setup_practice(ctx):
    """Setup spar finder panel"""
    embed = discord.Embed(
        title="⚔️ Spar Finder",
        description=(
            "Find sparring partners matched to your skill level!\n\n"
            "**Tier-Based Matchmaking:**\n"
            "Your **Stage + Rank + Strength** determines your tier.\n"
            "You'll be matched with players of similar skill!\n\n"
            "**Match Types:**\n"
            "⭐ **Perfect** - Same tier or ±1\n"
            "✅ **Good** - ±2-3 tiers apart\n"
            "⚠️ **Fair** - ±4-6 tiers apart\n"
            "⚠️ **Challenging** - Up to 1 stage apart\n\n"
            "**How it works:**\n"
            "1️⃣ Click **🎯 Find Spar** to join queue\n"
            "2️⃣ Click **🔍 Find Match** to see suitable opponents\n"
            "3️⃣ Click **⚔️ Challenge** to pick someone directly\n"
            "4️⃣ A private channel is created for your match\n"
            "5️⃣ Post server link, play, then submit results!\n\n"
            "**Unranked Players:**\n"
            "Can match with other Unranked or Stage 4-5 players."
        ),
        color=0x8B0000
    )
    embed.set_footer(text="✝ The Fallen Spar System ✝")
    
    await ctx.send(embed=embed, view=PracticeQueueView())
    await ctx.message.delete()


# ==========================================
# ACTIVITY CHECK SYSTEM
# ==========================================

ACTIVITY_CHECK_FILE = "activity_checks.json"

def load_activity_checks():
    """Load activity check data"""
    try:
        with open(ACTIVITY_CHECK_FILE, "r") as f:
            return json.load(f)
    except:
        return {"checks": [], "current": None}

def save_activity_checks(data):
    """Save activity check data"""
    with open(ACTIVITY_CHECK_FILE, "w") as f:
        json.dump(data, f, indent=2)


class ActivityCheckView(discord.ui.View):
    """Activity check with counter and auto-expire"""
    def __init__(self):
        super().__init__(timeout=None)
    
    @discord.ui.button(label="✅ I'm Active! (0)", style=discord.ButtonStyle.success, custom_id="activity_check_btn")
    async def check_in(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            data = load_activity_checks()
            check_id = data.get("current")
            
            if not check_id:
                return await interaction.response.send_message("❌ No active check!", ephemeral=True)
            
            check = next((c for c in data["checks"] if c["id"] == check_id), None)
            if not check:
                return await interaction.response.send_message("❌ Check not found!", ephemeral=True)
            
            # Check if time expired
            is_expired = False
            try:
                ends_at = datetime.datetime.fromisoformat(check["ends_at"].replace('Z', '+00:00'))
                if datetime.datetime.now(datetime.timezone.utc) > ends_at:
                    is_expired = True
            except:
                pass
            
            if check.get("ended") or is_expired:
                # Auto-end if expired
                if is_expired and not check.get("ended"):
                    check["ended"] = True
                    check["auto_ended"] = True
                    data["current"] = None
                    save_activity_checks(data)
                
                # RESPOND FIRST
                await interaction.response.send_message("❌ This check has ended!", ephemeral=True)
                
                # Then try to update message
                try:
                    count = len(check.get("responses", []))
                    embed = interaction.message.embeds[0].copy() if interaction.message.embeds else None
                    if embed:
                        embed.title = "📢 ACTIVITY CHECK ENDED"
                        embed.color = 0x95a5a6
                        embed.set_footer(text=f"✝ Ended • {count} responses ✝")
                    
                    new_view = discord.ui.View(timeout=None)
                    ended_btn = discord.ui.Button(label=f"⏰ Ended ({count})", style=discord.ButtonStyle.secondary, disabled=True)
                    new_view.add_item(ended_btn)
                    await interaction.message.edit(embed=embed, view=new_view)
                except:
                    pass
                return
            
            user_id = str(interaction.user.id)
            
            if user_id in check.get("responses", []):
                return await interaction.response.send_message("✅ Already checked in!", ephemeral=True)
            
            # Add response
            if "responses" not in check:
                check["responses"] = []
            check["responses"].append(user_id)
            
            if "response_times" not in check:
                check["response_times"] = {}
            check["response_times"][user_id] = datetime.datetime.now(datetime.timezone.utc).isoformat()
            
            save_activity_checks(data)
            
            # Give rewards
            add_coins(interaction.user.id, 25)
            add_xp_to_user(interaction.user.id, 15)
            update_user_data(interaction.user.id, "last_active", datetime.datetime.now(datetime.timezone.utc).isoformat())
            
            count = len(check["responses"])
            
            # RESPOND FIRST - always
            await interaction.response.send_message(
                f"✅ **Checked in!** +25 💰 +15 XP\nYou are #{count}!",
                ephemeral=True
            )
            
            # Then update button counter and embed
            try:
                embed = interaction.message.embeds[0].copy() if interaction.message.embeds else None
                if embed:
                    embed.set_footer(text=f"✝ {count} responses ✝")
                
                new_view = ActivityCheckView()
                new_view.children[0].label = f"✅ I'm Active! ({count})"
                
                await interaction.message.edit(embed=embed, view=new_view)
            except Exception as e:
                print(f"Failed to update activity embed: {e}")
                
        except Exception as e:
            print(f"Activity check error: {e}")
            try:
                await interaction.response.send_message("❌ Error!", ephemeral=True)
            except:
                pass


class ActivityCheckControlView(discord.ui.View):
    """Staff controls for activity check"""
    def __init__(self, check_id: str = None):
        super().__init__(timeout=None)
        self.check_id = check_id
    
    @discord.ui.button(label="📊 View Results", style=discord.ButtonStyle.primary, custom_id="activity_check_results")
    async def view_results(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not is_staff(interaction.user):
            return await interaction.response.send_message("❌ Staff only!", ephemeral=True)
        
        await interaction.response.defer(ephemeral=True)
        
        data = load_activity_checks()
        check_id = self.check_id or data.get("current")
        
        check = None
        for c in data["checks"]:
            if c["id"] == check_id:
                check = c
                break
        
        if not check:
            return await interaction.followup.send("❌ Check not found!", ephemeral=True)
        
        responses = check.get("responses", [])
        
        # Try to create visual image
        if PIL_AVAILABLE and responses:
            try:
                img = await create_activity_results_image(interaction.guild, check, responses)
                if img:
                    file = discord.File(img, filename="activity_results.png")
                    await interaction.followup.send(file=file, ephemeral=True)
                    return
            except Exception as e:
                print(f"Activity results image error: {e}")
        
        # Fallback to embed
        embed = discord.Embed(
            title="📊 Activity Check Results",
            color=0x2ecc71
        )
        
        embed.add_field(name="✅ Responses", value=str(len(responses)), inline=True)
        embed.add_field(name="🆔 Check ID", value=f"`{check_id}`", inline=True)
        
        # Show recent responders
        if responses:
            recent = responses[-15:]  # Last 15
            names = []
            for uid in recent:
                member = interaction.guild.get_member(int(uid))
                if member:
                    names.append(member.display_name)
            if names:
                embed.add_field(name="Recent Check-ins", value="\n".join(names), inline=False)
        
        await interaction.followup.send(embed=embed, ephemeral=True)
    
    @discord.ui.button(label="🛑 End Check", style=discord.ButtonStyle.danger, custom_id="activity_check_end")
    async def end_check(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not is_staff(interaction.user):
            return await interaction.response.send_message("❌ Staff only!", ephemeral=True)
        
        data = load_activity_checks()
        check_id = self.check_id or data.get("current")
        
        check = None
        for c in data["checks"]:
            if c["id"] == check_id:
                check = c
                break
        
        if not check:
            return await interaction.response.send_message("❌ Check not found!", ephemeral=True)
        
        if check.get("ended"):
            return await interaction.response.send_message("❌ Already ended!", ephemeral=True)
        
        check["ended"] = True
        check["ended_at"] = datetime.datetime.now(datetime.timezone.utc).isoformat()
        check["ended_by"] = str(interaction.user.id)
        data["current"] = None
        save_activity_checks(data)
        
        responses = check.get("responses", [])
        
        # Find who didn't respond
        all_members = [m for m in interaction.guild.members if not m.bot]
        responded_ids = set(responses)
        not_responded = [m for m in all_members if str(m.id) not in responded_ids]
        
        # Create results embed
        results_embed = discord.Embed(
            title="📊 Activity Check Ended",
            description=f"Ended by {interaction.user.mention}",
            color=0xe74c3c,
            timestamp=datetime.datetime.now(datetime.timezone.utc)
        )
        
        total_members = len(all_members)
        results_embed.add_field(name="✅ Responded", value=str(len(responses)), inline=True)
        results_embed.add_field(name="❌ No Response", value=str(len(not_responded)), inline=True)
        rate = (len(responses)/total_members*100) if total_members > 0 else 0
        results_embed.add_field(name="📈 Response Rate", value=f"{rate:.1f}%", inline=True)
        
        # Try to find and update the original activity check message
        try:
            if check.get("message_id") and check.get("channel_id"):
                channel = interaction.guild.get_channel(int(check["channel_id"]))
                if channel:
                    ac_msg = await channel.fetch_message(int(check["message_id"]))
                    if ac_msg:
                        ac_embed = ac_msg.embeds[0] if ac_msg.embeds else None
                        if ac_embed:
                            ac_embed.title = "📢 ACTIVITY CHECK ENDED"
                            ac_embed.color = 0x95a5a6
                            ac_embed.set_footer(text=f"✝ Ended • {len(responses)} responses ✝")
                        
                        # Create disabled view
                        disabled_view = discord.ui.View(timeout=None)
                        disabled_btn = discord.ui.Button(
                            label=f"⏰ Ended ({len(responses)})",
                            style=discord.ButtonStyle.secondary,
                            disabled=True
                        )
                        disabled_view.add_item(disabled_btn)
                        
                        await ac_msg.edit(embed=ac_embed, view=disabled_view)
        except Exception as e:
            print(f"Failed to update activity check message: {e}")
        
        # Update the control panel message (remove buttons)
        try:
            await interaction.message.edit(view=None)
        except:
            pass
        
        await interaction.response.send_message(embed=results_embed)
    
    @discord.ui.button(label="📋 Export No-Shows", style=discord.ButtonStyle.secondary, custom_id="activity_check_export")
    async def export_noshows(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not is_staff(interaction.user):
            return await interaction.response.send_message("❌ Staff only!", ephemeral=True)
        
        await interaction.response.defer(ephemeral=True)
        
        data = load_activity_checks()
        check_id = self.check_id or data.get("current")
        
        check = None
        for c in data["checks"]:
            if c["id"] == check_id:
                check = c
                break
        
        if not check:
            return await interaction.followup.send("❌ Check not found!", ephemeral=True)
        
        responses = set(check.get("responses", []))
        
        # Find who didn't respond (exclude bots and staff optionally)
        all_members = [m for m in interaction.guild.members if not m.bot]
        not_responded = [m for m in all_members if str(m.id) not in responses]
        
        if not not_responded:
            return await interaction.followup.send("✅ Everyone responded!", ephemeral=True)
        
        # Create embed with no-shows
        embed = discord.Embed(
            title="❌ Members Who Didn't Respond",
            description=f"**{len(not_responded)}** members didn't check in",
            color=0xe74c3c
        )
        
        # Group by role/stage if possible
        no_show_names = []
        for member in not_responded[:50]:  # Limit to 50
            # Get their stage
            stage = "?"
            for role in member.roles:
                if "Stage" in role.name:
                    stage = role.name.split("〢")[0] if "〢" in role.name else role.name
                    break
            no_show_names.append(f"• {member.display_name} ({stage})")
        
        # Split into chunks if needed
        chunk = "\n".join(no_show_names[:25])
        embed.add_field(name="No-Shows (1-25)", value=chunk or "None", inline=False)
        
        if len(no_show_names) > 25:
            chunk2 = "\n".join(no_show_names[25:50])
            embed.add_field(name="No-Shows (26-50)", value=chunk2, inline=False)
        
        if len(not_responded) > 50:
            embed.set_footer(text=f"Showing 50 of {len(not_responded)} no-shows")
        
        await interaction.followup.send(embed=embed, ephemeral=True)


@bot.command(name="activitycheck")
@commands.has_any_role(*HIGH_STAFF_ROLES, STAFF_ROLE_NAME)
async def start_activity_check(ctx, duration: str = "1h", *, message: str = None):
    """
    Start an activity check
    Usage: !activitycheck [duration] [message]
    Duration: 30m, 1h, 2h, 6h, 12h, 24h
    """
    # Parse duration
    duration_map = {
        "30m": 30, "1h": 60, "2h": 120, "3h": 180,
        "6h": 360, "12h": 720, "24h": 1440
    }
    
    minutes = duration_map.get(duration.lower(), 60)
    
    data = load_activity_checks()
    
    # Auto-clear expired checks
    if data.get("current"):
        current_check = next((c for c in data["checks"] if c["id"] == data["current"]), None)
        if current_check:
            try:
                ends_at = datetime.datetime.fromisoformat(current_check["ends_at"].replace('Z', '+00:00'))
                if datetime.datetime.now(datetime.timezone.utc) > ends_at or current_check.get("ended"):
                    # Auto-end expired check
                    current_check["ended"] = True
                    current_check["auto_ended"] = True
                    data["current"] = None
                    save_activity_checks(data)
                    data = load_activity_checks()  # Reload
                else:
                    return await ctx.send("❌ There's an active activity check! Use `!endactivitycheck` to end it.")
            except:
                # If we can't parse the date, clear it anyway
                data["current"] = None
                save_activity_checks(data)
                data = load_activity_checks()
    
    # Create check
    check_id = f"ac_{int(datetime.datetime.now().timestamp())}"
    end_time = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(minutes=minutes)
    
    check_data = {
        "id": check_id,
        "started_by": str(ctx.author.id),
        "started_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "ends_at": end_time.isoformat(),
        "duration_minutes": minutes,
        "message": message,
        "channel_id": str(ctx.channel.id),
        "responses": [],
        "response_times": {},
        "ended": False
    }
    
    # Save FIRST before sending
    data["checks"].append(check_data)
    data["current"] = check_id
    save_activity_checks(data)
    
    # Create embed
    custom_msg = message or "Click button to confirm you're active!"
    
    embed = discord.Embed(
        title="📢 ACTIVITY CHECK",
        description=(
            f"{custom_msg}\n\n"
            f"⏰ **Ends:** <t:{int(end_time.timestamp())}:R>\n"
            f"🎁 **Reward:** 25 coins + 15 XP"
        ),
        color=0x2ecc71
    )
    embed.set_footer(text=f"✝ 0 responses ✝")
    
    # Send with ping
    msg = await ctx.send("@everyone", embed=embed, view=ActivityCheckView())
    
    # Update message ID
    data = load_activity_checks()
    for c in data["checks"]:
        if c["id"] == check_id:
            c["message_id"] = str(msg.id)
            break
    save_activity_checks(data)
    
    # Staff controls
    await ctx.send(
        "**Staff Controls:**",
        view=ActivityCheckControlView()
    )
    
    try:
        await ctx.message.delete()
    except:
        pass


@bot.command(name="endactivitycheck")
@commands.has_any_role(*HIGH_STAFF_ROLES, STAFF_ROLE_NAME)
async def force_end_activity_check(ctx):
    """Force end any active activity check"""
    data = load_activity_checks()
    
    if not data.get("current"):
        return await ctx.send("✅ No active activity check to end.")
    
    check = next((c for c in data["checks"] if c["id"] == data["current"]), None)
    if check:
        check["ended"] = True
        check["ended_by"] = str(ctx.author.id)
    
    data["current"] = None
    save_activity_checks(data)
    
    await ctx.send("✅ Activity check ended! You can now start a new one.")


@bot.command(name="activitystats")
@commands.has_any_role(*HIGH_STAFF_ROLES, STAFF_ROLE_NAME)
async def activity_check_stats(ctx):
    """View activity check history"""
    data = load_activity_checks()
    
    if not data["checks"]:
        return await ctx.send("📊 No activity checks have been run yet!")
    
    embed = discord.Embed(
        title="📊 Activity Check History",
        color=0x3498db
    )
    
    # Show last 5 checks
    recent = data["checks"][-5:]
    recent.reverse()
    
    for check in recent:
        responses = len(check.get("responses", []))
        status = "🟢 Active" if not check.get("ended") else "🔴 Ended"
        
        try:
            started = datetime.datetime.fromisoformat(check["started_at"].replace('Z', '+00:00'))
            date_str = started.strftime("%m/%d %H:%M")
        except:
            date_str = "Unknown"
        
        embed.add_field(
            name=f"{status} {date_str}",
            value=f"✅ {responses} responses\nID: `{check['id']}`",
            inline=True
        )
    
    # Current check
    if data.get("current"):
        embed.set_footer(text=f"Current active check: {data['current']}")
    
    await ctx.send(embed=embed)


@bot.command(name="checkparticipation")
@commands.has_any_role(*HIGH_STAFF_ROLES, STAFF_ROLE_NAME)
async def check_member_participation(ctx, member: discord.Member):
    """Check how many activity checks a member has responded to"""
    data = load_activity_checks()
    
    user_id = str(member.id)
    total_checks = len([c for c in data["checks"] if c.get("ended")])
    responded = len([c for c in data["checks"] if user_id in c.get("responses", [])])
    
    rate = (responded / total_checks * 100) if total_checks > 0 else 0
    
    embed = discord.Embed(
        title=f"📊 {member.display_name}'s Participation",
        color=0x3498db
    )
    embed.add_field(name="Activity Checks Responded", value=f"{responded}/{total_checks}", inline=True)
    embed.add_field(name="Response Rate", value=f"{rate:.1f}%", inline=True)
    
    # Rating
    if rate >= 80:
        rating = "⭐ Excellent"
    elif rate >= 60:
        rating = "✅ Good"
    elif rate >= 40:
        rating = "⚠️ Fair"
    else:
        rating = "❌ Poor"
    
    embed.add_field(name="Rating", value=rating, inline=True)
    embed.set_thumbnail(url=member.display_avatar.url)
    
    await ctx.send(embed=embed)


# ==========================================
# GIVEAWAY SYSTEM
# ==========================================

GIVEAWAY_FILE = "giveaways.json"

def load_giveaways():
    """Load giveaway data"""
    try:
        with open(GIVEAWAY_FILE, "r") as f:
            return json.load(f)
    except:
        return {"giveaways": [], "current": []}

def save_giveaways(data):
    """Save giveaway data"""
    with open(GIVEAWAY_FILE, "w") as f:
        json.dump(data, f, indent=2)


class GiveawayView(discord.ui.View):
    """View for giveaway entries"""
    def __init__(self, giveaway_id: str = None):
        super().__init__(timeout=None)
        self.giveaway_id = giveaway_id
    
    @discord.ui.button(label="🎉 Enter Giveaway", style=discord.ButtonStyle.success, custom_id="giveaway_enter")
    async def enter_giveaway(self, interaction: discord.Interaction, button: discord.ui.Button):
        data = load_giveaways()
        
        # Find giveaway
        giveaway = None
        for g in data["giveaways"]:
            if g.get("message_id") == str(interaction.message.id) or g.get("id") == self.giveaway_id:
                giveaway = g
                break
        
        if not giveaway:
            return await interaction.response.send_message("❌ This giveaway has ended!", ephemeral=True)
        
        if giveaway.get("ended"):
            return await interaction.response.send_message("❌ This giveaway has ended!", ephemeral=True)
        
        user_id = str(interaction.user.id)
        
        # Check requirements
        min_level = giveaway.get("min_level", 0)
        if min_level > 0:
            user_data = get_user_data(interaction.user.id)
            if user_data.get("level", 1) < min_level:
                return await interaction.response.send_message(
                    f"❌ You need to be level {min_level}+ to enter!",
                    ephemeral=True
                )
        
        # Check if already entered
        if user_id in giveaway.get("entries", []):
            return await interaction.response.send_message("✅ You're already entered!", ephemeral=True)
        
        # Add entry
        if "entries" not in giveaway:
            giveaway["entries"] = []
        giveaway["entries"].append(user_id)
        save_giveaways(data)
        
        entry_count = len(giveaway["entries"])
        
        await interaction.response.send_message(
            f"🎉 **You're in!**\n"
            f"You've entered the giveaway for **{giveaway.get('prize', 'Unknown Prize')}**!\n"
            f"Total entries: {entry_count}",
            ephemeral=True
        )
        
        # Update embed with entry count
        try:
            embed = interaction.message.embeds[0]
            # Update footer with entry count
            embed.set_footer(text=f"✝ {entry_count} entries • ID: {giveaway['id']} ✝")
            await interaction.message.edit(embed=embed)
        except:
            pass


class GiveawayControlView(discord.ui.View):
    """Staff controls for giveaway"""
    def __init__(self, giveaway_id: str = None):
        super().__init__(timeout=None)
        self.giveaway_id = giveaway_id
    
    @discord.ui.button(label="🏆 Draw Winner", style=discord.ButtonStyle.success, custom_id="giveaway_draw")
    async def draw_winner(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not is_staff(interaction.user):
            return await interaction.response.send_message("❌ Staff only!", ephemeral=True)
        
        data = load_giveaways()
        
        giveaway = None
        for g in data["giveaways"]:
            if g.get("id") == self.giveaway_id:
                giveaway = g
                break
        
        if not giveaway:
            return await interaction.response.send_message("❌ Giveaway not found!", ephemeral=True)
        
        entries = giveaway.get("entries", [])
        if not entries:
            return await interaction.response.send_message("❌ No entries yet!", ephemeral=True)
        
        # Draw winners
        num_winners = giveaway.get("winners", 1)
        winners = random.sample(entries, min(num_winners, len(entries)))
        
        giveaway["ended"] = True
        giveaway["ended_at"] = datetime.datetime.now(datetime.timezone.utc).isoformat()
        giveaway["winner_ids"] = winners
        save_giveaways(data)
        
        # Build winner mentions
        winner_mentions = []
        for w_id in winners:
            member = interaction.guild.get_member(int(w_id))
            if member:
                winner_mentions.append(member.mention)
            else:
                winner_mentions.append(f"<@{w_id}>")
        
        # Create winner embed
        winner_embed = discord.Embed(
            title="🎉 GIVEAWAY ENDED!",
            description=f"**Prize:** {giveaway.get('prize', 'Unknown')}\n\n**Winner(s):**\n" + "\n".join(winner_mentions),
            color=0xffd700,
            timestamp=datetime.datetime.now(datetime.timezone.utc)
        )
        winner_embed.add_field(name="Total Entries", value=str(len(entries)), inline=True)
        winner_embed.set_footer(text="✝ The Fallen Giveaways ✝")
        
        await interaction.response.send_message(
            f"🎉 Congratulations {', '.join(winner_mentions)}!",
            embed=winner_embed
        )
        
        # Update original giveaway message
        try:
            # Find the giveaway message
            if giveaway.get("channel_id") and giveaway.get("message_id"):
                channel = interaction.guild.get_channel(int(giveaway["channel_id"]))
                if channel:
                    msg = await channel.fetch_message(int(giveaway["message_id"]))
                    embed = msg.embeds[0] if msg.embeds else None
                    if embed:
                        embed.color = 0x95a5a6
                        embed.title = "🎉 GIVEAWAY ENDED"
                        embed.add_field(name="🏆 Winner(s)", value="\n".join(winner_mentions), inline=False)
                        await msg.edit(embed=embed, view=None)
        except:
            pass
    
    @discord.ui.button(label="🔄 Reroll", style=discord.ButtonStyle.primary, custom_id="giveaway_reroll")
    async def reroll(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not is_staff(interaction.user):
            return await interaction.response.send_message("❌ Staff only!", ephemeral=True)
        
        data = load_giveaways()
        
        giveaway = None
        for g in data["giveaways"]:
            if g.get("id") == self.giveaway_id:
                giveaway = g
                break
        
        if not giveaway:
            return await interaction.response.send_message("❌ Giveaway not found!", ephemeral=True)
        
        entries = giveaway.get("entries", [])
        previous_winners = giveaway.get("winner_ids", [])
        
        # Remove previous winners from pool
        available = [e for e in entries if e not in previous_winners]
        
        if not available:
            return await interaction.response.send_message("❌ No more entries to reroll from!", ephemeral=True)
        
        # Draw new winner
        new_winner = random.choice(available)
        member = interaction.guild.get_member(int(new_winner))
        
        giveaway["winner_ids"].append(new_winner)
        save_giveaways(data)
        
        await interaction.response.send_message(
            f"🔄 **Reroll Winner:** {member.mention if member else f'<@{new_winner}>'}\n"
            f"Congratulations! 🎉"
        )
    
    @discord.ui.button(label="📊 View Entries", style=discord.ButtonStyle.secondary, custom_id="giveaway_entries")
    async def view_entries(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not is_staff(interaction.user):
            return await interaction.response.send_message("❌ Staff only!", ephemeral=True)
        
        data = load_giveaways()
        
        giveaway = None
        for g in data["giveaways"]:
            if g.get("id") == self.giveaway_id:
                giveaway = g
                break
        
        if not giveaway:
            return await interaction.response.send_message("❌ Giveaway not found!", ephemeral=True)
        
        entries = giveaway.get("entries", [])
        
        embed = discord.Embed(
            title=f"📊 Giveaway Entries",
            description=f"**Prize:** {giveaway.get('prize', 'Unknown')}\n**Entries:** {len(entries)}",
            color=0x3498db
        )
        
        if entries:
            # Show last 10 entries
            recent = entries[-10:]
            names = []
            for uid in recent:
                member = interaction.guild.get_member(int(uid))
                names.append(member.display_name if member else f"User {uid}")
            embed.add_field(name="Recent Entries", value="\n".join(names), inline=False)
        
        await interaction.response.send_message(embed=embed, ephemeral=True)
    
    @discord.ui.button(label="🗑️ Cancel", style=discord.ButtonStyle.danger, custom_id="giveaway_cancel")
    async def cancel_giveaway(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not is_staff(interaction.user):
            return await interaction.response.send_message("❌ Staff only!", ephemeral=True)
        
        data = load_giveaways()
        
        for g in data["giveaways"]:
            if g.get("id") == self.giveaway_id:
                g["ended"] = True
                g["cancelled"] = True
                break
        
        save_giveaways(data)
        
        await interaction.response.send_message("🗑️ Giveaway cancelled!", ephemeral=True)
        
        # Update original message
        try:
            if interaction.message:
                await interaction.message.edit(view=None)
        except:
            pass


@bot.command(name="giveaway")
@commands.has_any_role(*HIGH_STAFF_ROLES, STAFF_ROLE_NAME)
async def start_giveaway(ctx, duration: str, winners: int, *, prize: str):
    """
    Start a giveaway
    Usage: !giveaway <duration> <winners> <prize>
    Example: !giveaway 1h 1 500 Coins
    Example: !giveaway 24h 3 Nitro Classic
    """
    # Parse duration
    duration_map = {
        "30m": 30, "1h": 60, "2h": 120, "3h": 180,
        "6h": 360, "12h": 720, "24h": 1440, "48h": 2880, "7d": 10080
    }
    
    minutes = duration_map.get(duration.lower())
    if not minutes:
        return await ctx.send("❌ Invalid duration! Use: 30m, 1h, 2h, 3h, 6h, 12h, 24h, 48h, 7d")
    
    if winners < 1 or winners > 10:
        return await ctx.send("❌ Winners must be between 1 and 10!")
    
    data = load_giveaways()
    
    # Create giveaway
    giveaway_id = f"gw_{int(datetime.datetime.now().timestamp())}"
    end_time = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(minutes=minutes)
    
    giveaway_data = {
        "id": giveaway_id,
        "prize": prize,
        "winners": winners,
        "host_id": str(ctx.author.id),
        "channel_id": str(ctx.channel.id),
        "started_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "ends_at": end_time.isoformat(),
        "duration_minutes": minutes,
        "entries": [],
        "ended": False
    }
    
    # Create embed
    embed = discord.Embed(
        title="🎉 GIVEAWAY!",
        description=(
            f"**Prize:** {prize}\n\n"
            f"**Click the button below to enter!**\n\n"
            f"⏰ **Ends:** <t:{int(end_time.timestamp())}:R>\n"
            f"🏆 **Winners:** {winners}\n"
            f"👤 **Hosted by:** {ctx.author.mention}"
        ),
        color=0xffd700,
        timestamp=datetime.datetime.now(datetime.timezone.utc)
    )
    embed.set_footer(text=f"✝ 0 entries • ID: {giveaway_id} ✝")
    
    # Send giveaway message
    msg = await ctx.send(embed=embed, view=GiveawayView(giveaway_id))
    
    # Update with message ID
    giveaway_data["message_id"] = str(msg.id)
    data["giveaways"].append(giveaway_data)
    data["current"].append(giveaway_id)
    save_giveaways(data)
    
    # Send staff controls (ephemeral-like, in same channel)
    staff_embed = discord.Embed(
        title="🔧 Giveaway Controls",
        description=f"Controls for giveaway `{giveaway_id}`",
        color=0x3498db
    )
    await ctx.send(embed=staff_embed, view=GiveawayControlView(giveaway_id))
    
    await ctx.message.delete()


@bot.command(name="giveaway_req")
@commands.has_any_role(*HIGH_STAFF_ROLES, STAFF_ROLE_NAME)
async def giveaway_with_requirements(ctx, duration: str, winners: int, min_level: int, *, prize: str):
    """
    Start a giveaway with level requirement
    Usage: !giveaway_req <duration> <winners> <min_level> <prize>
    Example: !giveaway_req 24h 1 10 VIP Role
    """
    duration_map = {
        "30m": 30, "1h": 60, "2h": 120, "3h": 180,
        "6h": 360, "12h": 720, "24h": 1440, "48h": 2880, "7d": 10080
    }
    
    minutes = duration_map.get(duration.lower())
    if not minutes:
        return await ctx.send("❌ Invalid duration!")
    
    data = load_giveaways()
    
    giveaway_id = f"gw_{int(datetime.datetime.now().timestamp())}"
    end_time = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(minutes=minutes)
    
    giveaway_data = {
        "id": giveaway_id,
        "prize": prize,
        "winners": winners,
        "min_level": min_level,
        "host_id": str(ctx.author.id),
        "channel_id": str(ctx.channel.id),
        "started_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "ends_at": end_time.isoformat(),
        "entries": [],
        "ended": False
    }
    
    embed = discord.Embed(
        title="🎉 GIVEAWAY!",
        description=(
            f"**Prize:** {prize}\n\n"
            f"**Click the button below to enter!**\n\n"
            f"⏰ **Ends:** <t:{int(end_time.timestamp())}:R>\n"
            f"🏆 **Winners:** {winners}\n"
            f"📊 **Requirement:** Level {min_level}+\n"
            f"👤 **Hosted by:** {ctx.author.mention}"
        ),
        color=0xffd700
    )
    embed.set_footer(text=f"✝ 0 entries • ID: {giveaway_id} ✝")
    
    msg = await ctx.send(embed=embed, view=GiveawayView(giveaway_id))
    
    giveaway_data["message_id"] = str(msg.id)
    data["giveaways"].append(giveaway_data)
    save_giveaways(data)
    
    await ctx.send(
        embed=discord.Embed(title="🔧 Controls", color=0x3498db),
        view=GiveawayControlView(giveaway_id)
    )
    await ctx.message.delete()


# ==========================================
# ATTENDANCE LOGGING PANEL (Efficient Staff Tool)
# ==========================================

class AttendanceLoggingView(discord.ui.View):
    """Staff panel for efficiently logging attendance"""
    def __init__(self):
        super().__init__(timeout=None)
    
    @discord.ui.button(label="📚 Log Training", style=discord.ButtonStyle.success, custom_id="attendance_log_training", row=0)
    async def log_training_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not is_staff(interaction.user):
            return await interaction.response.send_message("❌ Staff only!", ephemeral=True)
        
        await interaction.response.send_message(
            "**📚 Log Training Attendance**\n\n"
            "Select members who attended (up to 25 at a time).\n"
            "You can run this multiple times for large trainings.",
            view=AttendanceMemberSelectView("training", interaction.user.id),
            ephemeral=True
        )
    
    @discord.ui.button(label="🎯 Log Tryout", style=discord.ButtonStyle.primary, custom_id="attendance_log_tryout", row=0)
    async def log_tryout_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not is_staff(interaction.user):
            return await interaction.response.send_message("❌ Staff only!", ephemeral=True)
        
        await interaction.response.send_message(
            "**🎯 Log Tryout Attendance**\n\n"
            "Select members who attended (up to 25 at a time).\n"
            "You can run this multiple times for large tryouts.",
            view=AttendanceMemberSelectView("tryout", interaction.user.id),
            ephemeral=True
        )
    
    @discord.ui.button(label="📊 Quick Stats", style=discord.ButtonStyle.secondary, custom_id="attendance_quick_stats", row=0)
    async def quick_stats_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not is_staff(interaction.user):
            return await interaction.response.send_message("❌ Staff only!", ephemeral=True)
        
        # Show today's logging stats
        embed = discord.Embed(
            title="📊 Attendance Quick Stats",
            color=0x3498db
        )
        
        # Get global stats
        total_members = len([m for m in interaction.guild.members if not m.bot])
        
        embed.add_field(name="👥 Total Members", value=str(total_members), inline=True)
        embed.add_field(name="📚 Training Reward", value="100 coins + 50 XP", inline=True)
        embed.add_field(name="🎯 Tryout Reward", value="150 coins + 75 XP", inline=True)
        embed.add_field(name="👑 Host Reward", value="300 coins + 100 XP", inline=True)
        
        embed.add_field(
            name="🔥 Streak Bonuses",
            value="3: +50 | 5: +100 | 7: +200 | 10: +500",
            inline=False
        )
        
        embed.set_footer(text="Use the buttons above to log attendance!")
        
        await interaction.response.send_message(embed=embed, ephemeral=True)


class AttendanceMemberSelectView(discord.ui.View):
    """View with member select dropdown for attendance logging"""
    def __init__(self, event_type: str, staff_id: int):
        super().__init__(timeout=300)  # 5 minute timeout
        self.event_type = event_type
        self.staff_id = staff_id
        self.selected_members = []
        
        # Add the user select
        self.member_select = discord.ui.UserSelect(
            placeholder="Select attendees...",
            min_values=1,
            max_values=25,
            row=0
        )
        self.member_select.callback = self.member_select_callback
        self.add_item(self.member_select)
    
    async def member_select_callback(self, interaction: discord.Interaction):
        if interaction.user.id != self.staff_id:
            return await interaction.response.send_message("❌ Not your panel!", ephemeral=True)
        
        self.selected_members = list(self.member_select.values)
        
        # Show confirmation
        member_names = [m.display_name for m in self.selected_members[:10]]
        preview = ", ".join(member_names)
        if len(self.selected_members) > 10:
            preview += f" +{len(self.selected_members) - 10} more"
        
        await interaction.response.edit_message(
            content=(
                f"**{'📚 Training' if self.event_type == 'training' else '🎯 Tryout'} Attendance**\n\n"
                f"**Selected ({len(self.selected_members)}):** {preview}\n\n"
                f"Click **✅ Confirm** to log attendance, or select different members."
            ),
            view=self
        )
    
    @discord.ui.button(label="✅ Confirm & Log", style=discord.ButtonStyle.success, row=1)
    async def confirm_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user.id != self.staff_id:
            return await interaction.response.send_message("❌ Not your panel!", ephemeral=True)
        
        if not self.selected_members:
            return await interaction.response.send_message("❌ Select at least one member first!", ephemeral=True)
        
        await interaction.response.edit_message(
            content=f"⏳ Logging {len(self.selected_members)} attendees... Please wait.",
            view=None
        )
        
        # Process attendance with rate limiting
        await process_attendance_batch(
            interaction, 
            self.selected_members, 
            self.event_type,
            interaction.user
        )
    
    @discord.ui.button(label="❌ Cancel", style=discord.ButtonStyle.danger, row=1)
    async def cancel_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user.id != self.staff_id:
            return await interaction.response.send_message("❌ Not your panel!", ephemeral=True)
        
        await interaction.response.edit_message(content="❌ Cancelled.", view=None)


async def process_attendance_batch(interaction, members: list, event_type: str, host):
    """Process attendance for a batch of members with rate limiting"""
    rewards = ATTENDANCE_REWARDS.get(event_type, ATTENDANCE_REWARDS["training"])
    
    streak_bonuses = []
    role_rewards = []
    processed = 0
    
    # Process in batches with delays
    batch_size = 5
    for i in range(0, len(members), batch_size):
        batch = members[i:i + batch_size]
        
        for m in batch:
            if m.bot:
                continue
                
            # Add rewards
            add_user_stat(m.id, "coins", rewards["coins"])
            add_xp_to_user(m.id, rewards["xp"])
            
            if event_type == "training":
                add_user_stat(m.id, "training_attendance", 1)
            else:
                add_user_stat(m.id, "tryout_attendance", 1)
            
            # Reset activity
            reset_member_activity(m.id)
            
            # Update streak
            streak = update_attendance_streak(m.id)
            bonus = get_streak_bonus(streak)
            if bonus > 0:
                add_user_stat(m.id, "coins", bonus)
                if len(streak_bonuses) < 5:
                    streak_bonuses.append(f"🔥 {m.display_name}: {streak} streak (+{bonus})")
            
            # Check for attendance role rewards (with rate limiting)
            try:
                new_roles = await check_attendance_roles(m, interaction.guild)
                if new_roles and len(role_rewards) < 5:
                    role_rewards.append(f"🎖️ {m.display_name}: **{new_roles[0]}**")
                await asyncio.sleep(0.3)  # Rate limit protection for role changes
            except:
                pass
            
            # Check for streak role rewards
            try:
                streak_roles = await check_streak_roles(m, interaction.guild, streak)
                if streak_roles and len(role_rewards) < 5:
                    role_rewards.append(f"🔥 {m.display_name}: **{streak_roles[0]}**")
            except:
                pass
            
            # Level up check
            await check_level_up(m.id, interaction.guild)
            
            processed += 1
        
        # Delay between batches
        if i + batch_size < len(members):
            await asyncio.sleep(1)  # 1 second between batches
    
    # Host rewards
    host_rewards = ATTENDANCE_REWARDS["host"]
    add_user_stat(host.id, "coins", host_rewards["coins"])
    add_xp_to_user(host.id, host_rewards["xp"])
    add_user_stat(host.id, "events_hosted", 1)
    reset_member_activity(host.id)
    
    # Build result embed
    attendee_names = [m.display_name for m in members[:15] if not m.bot]
    attendee_list = ", ".join(attendee_names)
    if len(members) > 15:
        attendee_list += f" +{len(members) - 15} more"
    
    title = "📚 Training Attendance Logged" if event_type == "training" else "🎯 Tryout Attendance Logged"
    
    embed = discord.Embed(
        title=title,
        description=f"**{processed} attendees** rewarded!",
        color=0x2ecc71
    )
    embed.add_field(name="👥 Attendees", value=attendee_list, inline=False)
    embed.add_field(name="💰 Each Received", value=f"{rewards['coins']} coins + {rewards['xp']} XP", inline=True)
    embed.add_field(name="👑 Host Received", value=f"{host_rewards['coins']} coins + {host_rewards['xp']} XP", inline=True)
    
    if streak_bonuses:
        embed.add_field(name="🔥 Streak Bonuses", value="\n".join(streak_bonuses), inline=False)
    
    if role_rewards:
        embed.add_field(name="🎉 Role Rewards Earned!", value="\n".join(role_rewards), inline=False)
    
    embed.set_footer(text=f"Logged by {host.display_name}")
    
    # Send to channel (not ephemeral) so everyone can see
    await interaction.channel.send(embed=embed)
    
    # Update the ephemeral message
    await interaction.edit_original_response(
        content=f"✅ Successfully logged {processed} attendees!"
    )


@bot.command(name="setup_attendance")
@commands.has_permissions(administrator=True)
async def setup_attendance_panel(ctx):
    """Setup the attendance logging panel for staff"""
    embed = discord.Embed(
        title="📋 Staff Attendance Logger",
        description=(
            "Use this panel to efficiently log training & tryout attendance!\n\n"
            "**How to use:**\n"
            "1️⃣ Click **Log Training** or **Log Tryout**\n"
            "2️⃣ Select members from the dropdown (up to 25)\n"
            "3️⃣ Click **Confirm** to reward them\n\n"
            "**Rewards:**\n"
            "• 📚 Training: 100 coins + 50 XP\n"
            "• 🎯 Tryout: 150 coins + 75 XP\n"
            "• 👑 Host: 300 coins + 100 XP\n\n"
            "**Streak Bonuses:** 3→+50 | 5→+100 | 7→+200 | 10→+500"
        ),
        color=0x8B0000
    )
    embed.set_footer(text="✝ The Fallen Staff Tools ✝")
    
    await ctx.send(embed=embed, view=AttendanceLoggingView())
    await ctx.message.delete()

# STAFF QUICK ACTIONS PANEL
# ==========================================

class StaffQuickActionsView(discord.ui.View):
    """Quick actions panel for staff"""
    def __init__(self):
        super().__init__(timeout=None)
    
    @discord.ui.button(label="⚠️ Quick Warn", style=discord.ButtonStyle.danger, custom_id="staff_quick_warn", row=0)
    async def quick_warn(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not is_staff(interaction.user):
            return await interaction.response.send_message("❌ Staff only!", ephemeral=True)
        await interaction.response.send_modal(QuickWarnModal())
    
    @discord.ui.button(label="📈 Promote", style=discord.ButtonStyle.success, custom_id="staff_quick_promote", row=0)
    async def quick_promote(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not is_staff(interaction.user):
            return await interaction.response.send_message("❌ Staff only!", ephemeral=True)
        await interaction.response.send_modal(QuickPromoteModal())
    
    @discord.ui.button(label="📉 Demote", style=discord.ButtonStyle.secondary, custom_id="staff_quick_demote", row=0)
    async def quick_demote(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not is_staff(interaction.user):
            return await interaction.response.send_message("❌ Staff only!", ephemeral=True)
        await interaction.response.send_modal(QuickDemoteModal())
    
    @discord.ui.button(label="📊 Check Stats", style=discord.ButtonStyle.primary, custom_id="staff_check_stats", row=1)
    async def check_stats(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not is_staff(interaction.user):
            return await interaction.response.send_message("❌ Staff only!", ephemeral=True)
        await interaction.response.send_modal(CheckStatsModal())
    
    @discord.ui.button(label="😴 Inactivity List", style=discord.ButtonStyle.secondary, custom_id="staff_inactivity_list", row=1)
    async def inactivity_list(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not is_staff(interaction.user):
            return await interaction.response.send_message("❌ Staff only!", ephemeral=True)
        
        await interaction.response.defer(ephemeral=True)
        
        # Get inactive members
        inactive_members = []
        threshold_days = 3  # Default
        now = datetime.datetime.now(datetime.timezone.utc)
        
        for member in interaction.guild.members:
            if member.bot:
                continue
            
            user_data = get_user_data(member.id)
            last_active = user_data.get("last_active")
            
            if last_active:
                try:
                    last_dt = datetime.datetime.fromisoformat(last_active.replace('Z', '+00:00'))
                    days_inactive = (now - last_dt).days
                    if days_inactive >= threshold_days:
                        strikes = user_data.get("inactivity_strikes", 0)
                        inactive_members.append((member, days_inactive, strikes))
                except:
                    pass
        
        # Sort by days inactive
        inactive_members.sort(key=lambda x: x[1], reverse=True)
        
        if not inactive_members:
            return await interaction.followup.send("✅ No inactive members found!", ephemeral=True)
        
        embed = discord.Embed(
            title="😴 Inactive Members",
            description=f"Members inactive for {threshold_days}+ days",
            color=0xe74c3c
        )
        
        lines = []
        for member, days, strikes in inactive_members[:15]:
            strike_emoji = "🔴" if strikes >= 3 else "🟡" if strikes >= 1 else "🟢"
            lines.append(f"{strike_emoji} **{member.display_name}** - {days}d ({strikes}/5 strikes)")
        
        embed.description = "\n".join(lines)
        embed.set_footer(text=f"Showing {min(15, len(inactive_members))} of {len(inactive_members)} inactive")
        
        await interaction.followup.send(embed=embed, ephemeral=True)
    
    @discord.ui.button(label="📋 Recent Mod Actions", style=discord.ButtonStyle.secondary, custom_id="staff_mod_log", row=1)
    async def mod_log(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not is_staff(interaction.user):
            return await interaction.response.send_message("❌ Staff only!", ephemeral=True)
        
        embed = discord.Embed(
            title="📋 Mod Log Info",
            description=(
                f"Mod actions are logged to **#{MOD_LOG_CHANNEL_NAME}**\n\n"
                f"**What's Logged:**\n"
                f"• Manual warns, mutes, kicks, bans\n"
                f"• Promotions & demotions\n"
                f"• Quick panel actions\n\n"
                f"**Auto-Moderation:**\n"
                f"Use **Wick Bot** for auto-mod!\n"
                f"• Link filtering\n"
                f"• Spam detection\n"
                f"• Anti-raid & anti-nuke\n"
                f"• Dashboard: wick.bot"
            ),
            color=0x3498db
        )
        
        await interaction.response.send_message(embed=embed, ephemeral=True)


class QuickWarnModal(discord.ui.Modal, title="⚠️ Quick Warn"):
    user_id = discord.ui.TextInput(
        label="User ID or @mention",
        placeholder="123456789 or username",
        style=discord.TextStyle.short,
        required=True
    )
    
    reason = discord.ui.TextInput(
        label="Reason",
        placeholder="Why are you warning this user?",
        style=discord.TextStyle.paragraph,
        required=True,
        max_length=500
    )
    
    async def on_submit(self, interaction: discord.Interaction):
        # Parse user
        user_input = self.user_id.value.strip()
        member = None
        
        # Try as ID
        if user_input.isdigit():
            member = interaction.guild.get_member(int(user_input))
        
        # Try as mention
        if not member and user_input.startswith("<@"):
            try:
                uid = int(user_input.replace("<@", "").replace(">", "").replace("!", ""))
                member = interaction.guild.get_member(uid)
            except:
                pass
        
        # Try as username
        if not member:
            member = discord.utils.get(interaction.guild.members, name=user_input)
            if not member:
                member = discord.utils.get(interaction.guild.members, display_name=user_input)
        
        if not member:
            return await interaction.response.send_message("❌ User not found!", ephemeral=True)
        
        # Add warning
        user_data = get_user_data(member.id)
        warnings = user_data.get("warnings", [])
        warnings.append({
            "reason": self.reason.value,
            "moderator": interaction.user.display_name,
            "moderator_id": str(interaction.user.id),
            "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat()
        })
        update_user_data(member.id, "warnings", warnings)
        
        # Log
        await log_mod_action(interaction.guild, "Warn", member, self.reason.value, interaction.user)
        
        # DM user
        try:
            dm_embed = discord.Embed(
                title="⚠️ Warning Received",
                description=f"You have been warned in **{interaction.guild.name}**",
                color=0xf1c40f
            )
            dm_embed.add_field(name="Reason", value=self.reason.value, inline=False)
            dm_embed.add_field(name="Total Warnings", value=str(len(warnings)), inline=True)
            await member.send(embed=dm_embed)
        except:
            pass
        
        # Check thresholds
        await check_warning_thresholds(interaction.guild, member, len(warnings))
        
        await interaction.response.send_message(
            f"✅ Warned **{member.display_name}** (Warning #{len(warnings)})",
            ephemeral=True
        )


class QuickPromoteModal(discord.ui.Modal, title="📈 Quick Promote"):
    user_id = discord.ui.TextInput(
        label="User ID or username",
        placeholder="123456789 or username",
        style=discord.TextStyle.short,
        required=True
    )
    
    async def on_submit(self, interaction: discord.Interaction):
        user_input = self.user_id.value.strip()
        member = None
        
        if user_input.isdigit():
            member = interaction.guild.get_member(int(user_input))
        if not member:
            member = discord.utils.get(interaction.guild.members, name=user_input)
        if not member:
            member = discord.utils.get(interaction.guild.members, display_name=user_input)
        
        if not member:
            return await interaction.response.send_message("❌ User not found!", ephemeral=True)
        
        # Find current stage and promote
        current_stage = None
        for role in member.roles:
            for stage_num, stage_name in STAGE_ROLES.items():
                if stage_name in role.name:
                    current_stage = stage_num
                    break
        
        if current_stage is None:
            return await interaction.response.send_message("❌ User has no stage role!", ephemeral=True)
        
        if current_stage <= 0:
            return await interaction.response.send_message("❌ User is already at max stage!", ephemeral=True)
        
        new_stage = current_stage - 1
        old_role_name = STAGE_ROLES.get(current_stage)
        new_role_name = STAGE_ROLES.get(new_stage)
        
        old_role = discord.utils.get(interaction.guild.roles, name=old_role_name)
        new_role = discord.utils.get(interaction.guild.roles, name=new_role_name)
        
        if old_role:
            await member.remove_roles(old_role)
            await asyncio.sleep(0.5)
        if new_role:
            await member.add_roles(new_role)
        
        await log_mod_action(interaction.guild, "Promote", member, f"Stage {current_stage} → Stage {new_stage}", interaction.user)
        
        await interaction.response.send_message(
            f"✅ Promoted **{member.display_name}** to Stage {new_stage}!",
            ephemeral=True
        )


class QuickDemoteModal(discord.ui.Modal, title="📉 Quick Demote"):
    user_id = discord.ui.TextInput(
        label="User ID or username",
        placeholder="123456789 or username",
        style=discord.TextStyle.short,
        required=True
    )
    
    async def on_submit(self, interaction: discord.Interaction):
        user_input = self.user_id.value.strip()
        member = None
        
        if user_input.isdigit():
            member = interaction.guild.get_member(int(user_input))
        if not member:
            member = discord.utils.get(interaction.guild.members, name=user_input)
        if not member:
            member = discord.utils.get(interaction.guild.members, display_name=user_input)
        
        if not member:
            return await interaction.response.send_message("❌ User not found!", ephemeral=True)
        
        # Find current stage and demote
        current_stage = None
        for role in member.roles:
            for stage_num, stage_name in STAGE_ROLES.items():
                if stage_name in role.name:
                    current_stage = stage_num
                    break
        
        if current_stage is None:
            return await interaction.response.send_message("❌ User has no stage role!", ephemeral=True)
        
        if current_stage >= 5:
            return await interaction.response.send_message("❌ User is already at lowest stage!", ephemeral=True)
        
        new_stage = current_stage + 1
        old_role_name = STAGE_ROLES.get(current_stage)
        new_role_name = STAGE_ROLES.get(new_stage)
        
        old_role = discord.utils.get(interaction.guild.roles, name=old_role_name)
        new_role = discord.utils.get(interaction.guild.roles, name=new_role_name)
        
        if old_role:
            await member.remove_roles(old_role)
            await asyncio.sleep(0.5)
        if new_role:
            await member.add_roles(new_role)
        
        await log_mod_action(interaction.guild, "Demote", member, f"Stage {current_stage} → Stage {new_stage}", interaction.user)
        
        await interaction.response.send_message(
            f"✅ Demoted **{member.display_name}** to Stage {new_stage}",
            ephemeral=True
        )


class CheckStatsModal(discord.ui.Modal, title="📊 Check Member Stats"):
    user_id = discord.ui.TextInput(
        label="User ID or username",
        placeholder="123456789 or username",
        style=discord.TextStyle.short,
        required=True
    )
    
    async def on_submit(self, interaction: discord.Interaction):
        user_input = self.user_id.value.strip()
        member = None
        
        if user_input.isdigit():
            member = interaction.guild.get_member(int(user_input))
        if not member:
            member = discord.utils.get(interaction.guild.members, name=user_input)
        if not member:
            member = discord.utils.get(interaction.guild.members, display_name=user_input)
        
        if not member:
            return await interaction.response.send_message("❌ User not found!", ephemeral=True)
        
        user_data = get_user_data(member.id)
        
        embed = discord.Embed(
            title=f"📊 Stats: {member.display_name}",
            color=0x3498db
        )
        
        # Basic stats
        embed.add_field(name="💰 Coins", value=str(user_data.get("coins", 0)), inline=True)
        embed.add_field(name="⭐ Level", value=str(user_data.get("level", 1)), inline=True)
        embed.add_field(name="📊 XP", value=str(user_data.get("xp", 0)), inline=True)
        embed.add_field(name="🎖️ ELO", value=str(user_data.get("elo", 1000)), inline=True)
        
        # Activity
        embed.add_field(name="📚 Trainings", value=str(user_data.get("training_attendance", 0)), inline=True)
        embed.add_field(name="🎯 Tryouts", value=str(user_data.get("tryout_attendance", 0)), inline=True)
        
        # Warnings
        warnings = user_data.get("warnings", [])
        embed.add_field(name="⚠️ Warnings", value=str(len(warnings)), inline=True)
        
        # Inactivity
        strikes = user_data.get("inactivity_strikes", 0)
        embed.add_field(name="😴 Inactivity Strikes", value=f"{strikes}/5", inline=True)
        
        # Last active
        last_active = user_data.get("last_active")
        if last_active:
            try:
                last_dt = datetime.datetime.fromisoformat(last_active.replace('Z', '+00:00'))
                embed.add_field(name="🕐 Last Active", value=f"<t:{int(last_dt.timestamp())}:R>", inline=True)
            except:
                pass
        
        embed.set_thumbnail(url=member.display_avatar.url)
        
        await interaction.response.send_message(embed=embed, ephemeral=True)


@bot.command(name="setup_staffpanel")
@commands.has_permissions(administrator=True)
async def setup_staff_panel(ctx):
    """Setup the staff quick actions panel"""
    embed = discord.Embed(
        title="🛡️ Staff Quick Actions",
        description=(
            "Use these buttons for quick moderation!\n\n"
            "**Actions:**\n"
            "• ⚠️ **Quick Warn** - Warn a user\n"
            "• 📈 **Promote** - Promote user's stage\n"
            "• 📉 **Demote** - Demote user's stage\n"
            "• 📊 **Check Stats** - View user's full stats\n"
            "• 😴 **Inactivity List** - See inactive members\n"
            "• 📋 **Mod Log** - View mod log info"
        ),
        color=0x8B0000
    )
    embed.set_footer(text="✝ The Fallen Staff Tools ✝")
    
    await ctx.send(embed=embed, view=StaffQuickActionsView())
    await ctx.message.delete()


# ==========================================
# APPLICATION SYSTEM IMPROVEMENTS
# ==========================================

# Application templates
APPLICATION_TEMPLATES = {
    "staff": {
        "name": "Staff Application",
        "emoji": "🛡️",
        "color": 0x3498db,
        "questions": [
            "What is your Roblox username?",
            "How old are you?",
            "What timezone are you in?",
            "Why do you want to be staff?",
            "Do you have any previous staff experience?",
            "How active can you be per week?",
        ],
        "cooldown_days": 14,
        "required_level": 10,
    },
    "tryout_host": {
        "name": "Tryout Host Application",
        "emoji": "🎯",
        "color": 0x2ecc71,
        "questions": [
            "What is your Roblox username?",
            "What stage are you currently?",
            "How long have you been in The Fallen?",
            "Why do you want to host tryouts?",
            "What times are you available to host?",
        ],
        "cooldown_days": 7,
        "required_level": 5,
    },
    "event_host": {
        "name": "Event Host Application",
        "emoji": "📅",
        "color": 0xf1c40f,
        "questions": [
            "What is your Roblox username?",
            "What events would you like to host?",
            "Do you have experience hosting events?",
            "How often can you host events?",
        ],
        "cooldown_days": 7,
        "required_level": 5,
    }
}

# Store applications
APPLICATIONS_FILE = "applications_data.json"

def load_applications():
    try:
        with open(APPLICATIONS_FILE, "r") as f:
            return json.load(f)
    except:
        return {"applications": [], "cooldowns": {}, "archived": []}

def save_applications(data):
    with open(APPLICATIONS_FILE, "w") as f:
        json.dump(data, f, indent=2)

def check_application_cooldown(user_id, app_type):
    """Check if user is on cooldown for an application type"""
    data = load_applications()
    cooldowns = data.get("cooldowns", {})
    
    key = f"{user_id}_{app_type}"
    if key in cooldowns:
        try:
            cooldown_end = datetime.datetime.fromisoformat(cooldowns[key].replace('Z', '+00:00'))
            if datetime.datetime.now(datetime.timezone.utc) < cooldown_end:
                return True, cooldown_end
        except:
            pass
    
    return False, None

def set_application_cooldown(user_id, app_type, days):
    """Set cooldown for user's application type"""
    data = load_applications()
    if "cooldowns" not in data:
        data["cooldowns"] = {}
    
    key = f"{user_id}_{app_type}"
    cooldown_end = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(days=days)
    data["cooldowns"][key] = cooldown_end.isoformat()
    save_applications(data)


class ApplicationPanelView(discord.ui.View):
    """Panel for submitting applications"""
    def __init__(self):
        super().__init__(timeout=None)
    
    @discord.ui.button(label="🛡️ Staff", style=discord.ButtonStyle.primary, custom_id="app_staff", row=0)
    async def staff_app(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.start_application(interaction, "staff")
    
    @discord.ui.button(label="🎯 Tryout Host", style=discord.ButtonStyle.success, custom_id="app_tryout_host", row=0)
    async def tryout_host_app(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.start_application(interaction, "tryout_host")
    
    @discord.ui.button(label="📅 Event Host", style=discord.ButtonStyle.secondary, custom_id="app_event_host", row=0)
    async def event_host_app(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.start_application(interaction, "event_host")
    
    async def start_application(self, interaction: discord.Interaction, app_type: str):
        template = APPLICATION_TEMPLATES.get(app_type)
        if not template:
            return await interaction.response.send_message("❌ Invalid application type!", ephemeral=True)
        
        # Check cooldown
        on_cooldown, cooldown_end = check_application_cooldown(interaction.user.id, app_type)
        if on_cooldown:
            return await interaction.response.send_message(
                f"❌ You're on cooldown for this application!\n"
                f"You can apply again <t:{int(cooldown_end.timestamp())}:R>",
                ephemeral=True
            )
        
        # Check level requirement
        user_data = get_user_data(interaction.user.id)
        user_level = user_data.get("level", 1)
        if user_level < template.get("required_level", 1):
            return await interaction.response.send_message(
                f"❌ You need to be level {template['required_level']} to apply!\n"
                f"Your current level: {user_level}",
                ephemeral=True
            )
        
        # Start application modal
        await interaction.response.send_modal(ApplicationModal(app_type, template))


class ApplicationModal(discord.ui.Modal):
    def __init__(self, app_type: str, template: dict):
        super().__init__(title=template["name"][:45])
        self.app_type = app_type
        self.template = template
        
        # Add up to 5 questions (Discord limit)
        for i, question in enumerate(template["questions"][:5]):
            self.add_item(discord.ui.TextInput(
                label=question[:45],
                style=discord.TextStyle.paragraph if i >= 3 else discord.TextStyle.short,
                required=True,
                max_length=500
            ))
    
    async def on_submit(self, interaction: discord.Interaction):
        # Collect answers
        answers = {}
        for i, child in enumerate(self.children):
            if i < len(self.template["questions"]):
                answers[self.template["questions"][i]] = child.value
        
        # Save application
        data = load_applications()
        app_id = f"app_{int(datetime.datetime.now().timestamp())}"
        
        application = {
            "id": app_id,
            "type": self.app_type,
            "user_id": str(interaction.user.id),
            "user_name": interaction.user.display_name,
            "answers": answers,
            "status": "pending",
            "submitted_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            "reviewed_by": None,
            "review_notes": None
        }
        
        data["applications"].append(application)
        save_applications(data)
        
        # Set cooldown
        set_application_cooldown(interaction.user.id, self.app_type, self.template.get("cooldown_days", 7))
        
        # Create application channel/thread or send to staff
        await self.send_application_to_staff(interaction, application)
        
        await interaction.response.send_message(
            f"✅ Your **{self.template['name']}** has been submitted!\n"
            f"You will be notified when it's reviewed.",
            ephemeral=True
        )
    
    async def send_application_to_staff(self, interaction: discord.Interaction, application: dict):
        """Create application ticket channel"""
        try:
            # Get or create applications category
            cat = discord.utils.get(interaction.guild.categories, name="Applications")
            if not cat:
                cat = await interaction.guild.create_category("Applications", overwrites={
                    interaction.guild.default_role: discord.PermissionOverwrite(read_messages=False)
                })
                await asyncio.sleep(0.5)
            
            # Create overwrites
            overwrites = {
                interaction.guild.default_role: discord.PermissionOverwrite(read_messages=False),
                interaction.user: discord.PermissionOverwrite(read_messages=True, send_messages=True),
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
            
            await asyncio.sleep(0.5)
            
            # Create channel
            app_type_short = self.app_type[:10]
            channel = await interaction.guild.create_text_channel(
                name=f"app-{app_type_short}-{interaction.user.name[:10]}",
                category=cat,
                overwrites=overwrites
            )
            
            # Update application with channel ID
            data = load_applications()
            for app in data["applications"]:
                if app["id"] == application["id"]:
                    app["channel_id"] = channel.id
                    break
            save_applications(data)
            
            # Create embed
            embed = discord.Embed(
                title=f"{self.template['emoji']} {self.template['name']}",
                description=f"**Applicant:** {interaction.user.mention}",
                color=self.template["color"],
                timestamp=datetime.datetime.now(datetime.timezone.utc)
            )
            
            for question, answer in application["answers"].items():
                embed.add_field(name=question, value=answer[:1024], inline=False)
            
            embed.add_field(name="🆔 Application ID", value=f"`{application['id']}`", inline=True)
            
            user_data = get_user_data(interaction.user.id)
            embed.add_field(name="📊 Level", value=str(user_data.get("level", 1)), inline=True)
            embed.add_field(name="⏰ Account Age", value=f"<t:{int(interaction.user.created_at.timestamp())}:R>", inline=True)
            embed.add_field(name="📅 Joined Server", value=f"<t:{int(interaction.user.joined_at.timestamp())}:R>", inline=True)
            
            embed.set_thumbnail(url=interaction.user.display_avatar.url)
            embed.set_footer(text="Staff: Use the buttons below to review")
            
            # Mention staff
            staff_ping = staff_role.mention if staff_role else ""
            
            await channel.send(
                f"{staff_ping}\n**New Application from {interaction.user.mention}**",
                embed=embed,
                view=ApplicationReviewView(application["id"])
            )
            
            # Also send a message to the applicant
            await channel.send(
                f"{interaction.user.mention} Your application has been submitted!\n"
                f"Staff will review it and respond here. Feel free to add any additional information."
            )
            
        except Exception as e:
            print(f"Error creating application channel: {e}")
            # Fallback: try to send to applications channel
            app_channel = discord.utils.get(interaction.guild.text_channels, name="applications")
            if app_channel:
                embed = discord.Embed(
                    title=f"{self.template['emoji']} {self.template['name']}",
                    description=f"**Applicant:** {interaction.user.mention}",
                    color=self.template["color"]
                )
                for question, answer in application["answers"].items():
                    embed.add_field(name=question, value=answer[:1024], inline=False)
                await app_channel.send(embed=embed, view=ApplicationReviewView(application["id"]))


class ApplicationReviewView(discord.ui.View):
    def __init__(self, app_id: str):
        super().__init__(timeout=None)
        self.app_id = app_id
    
    @discord.ui.button(label="✅ Approve", style=discord.ButtonStyle.success, custom_id="app_approve")
    async def approve(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not is_staff(interaction.user):
            return await interaction.response.send_message("❌ Staff only!", ephemeral=True)
        
        await self.review_application(interaction, "approved")
    
    @discord.ui.button(label="❌ Deny", style=discord.ButtonStyle.danger, custom_id="app_deny")
    async def deny(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not is_staff(interaction.user):
            return await interaction.response.send_message("❌ Staff only!", ephemeral=True)
        
        await interaction.response.send_modal(ApplicationDenyModal(self.app_id))
    
    @discord.ui.button(label="📦 Archive", style=discord.ButtonStyle.secondary, custom_id="app_archive")
    async def archive(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not is_staff(interaction.user):
            return await interaction.response.send_message("❌ Staff only!", ephemeral=True)
        
        await self.archive_application(interaction)
    
    async def review_application(self, interaction: discord.Interaction, status: str, notes: str = None):
        data = load_applications()
        
        # Find and update application
        app = None
        for a in data["applications"]:
            if a["id"] == self.app_id:
                app = a
                a["status"] = status
                a["reviewed_by"] = str(interaction.user.id)
                a["reviewed_at"] = datetime.datetime.now(datetime.timezone.utc).isoformat()
                if notes:
                    a["review_notes"] = notes
                break
        
        if not app:
            return await interaction.response.send_message("❌ Application not found!", ephemeral=True)
        
        save_applications(data)
        
        # Notify applicant
        applicant = interaction.guild.get_member(int(app["user_id"]))
        if applicant:
            template = APPLICATION_TEMPLATES.get(app["type"], {})
            
            if status == "approved":
                dm_embed = discord.Embed(
                    title="✅ Application Approved!",
                    description=f"Your **{template.get('name', 'application')}** has been approved!",
                    color=0x2ecc71
                )
            else:
                dm_embed = discord.Embed(
                    title="❌ Application Denied",
                    description=f"Your **{template.get('name', 'application')}** has been denied.",
                    color=0xe74c3c
                )
                if notes:
                    dm_embed.add_field(name="Reason", value=notes, inline=False)
            
            dm_embed.set_footer(text=f"Reviewed by {interaction.user.display_name}")
            
            try:
                await applicant.send(embed=dm_embed)
            except:
                pass
        
        # Update the message
        embed = interaction.message.embeds[0] if interaction.message.embeds else None
        if embed:
            embed.color = 0x2ecc71 if status == "approved" else 0xe74c3c
            embed.add_field(
                name="📋 Status",
                value=f"**{status.upper()}** by {interaction.user.mention}",
                inline=False
            )
            await interaction.message.edit(embed=embed, view=None)
        
        await interaction.response.send_message(f"✅ Application {status}!", ephemeral=True)
    
    async def archive_application(self, interaction: discord.Interaction):
        data = load_applications()
        
        # Find and move to archive
        for i, app in enumerate(data["applications"]):
            if app["id"] == self.app_id:
                app["archived_at"] = datetime.datetime.now(datetime.timezone.utc).isoformat()
                app["archived_by"] = str(interaction.user.id)
                data["archived"].append(app)
                data["applications"].pop(i)
                break
        
        save_applications(data)
        
        # Update message
        embed = interaction.message.embeds[0] if interaction.message.embeds else None
        if embed:
            embed.color = 0x95a5a6
            embed.add_field(name="📦 Status", value=f"ARCHIVED by {interaction.user.mention}", inline=False)
            await interaction.message.edit(embed=embed, view=None)
        
        await interaction.response.send_message("📦 Application archived!", ephemeral=True)


class ApplicationDenyModal(discord.ui.Modal, title="Deny Application"):
    reason = discord.ui.TextInput(
        label="Reason for denial",
        placeholder="Why is this application being denied?",
        style=discord.TextStyle.paragraph,
        required=True,
        max_length=500
    )
    
    def __init__(self, app_id: str):
        super().__init__()
        self.app_id = app_id
    
    async def on_submit(self, interaction: discord.Interaction):
        data = load_applications()
        
        app = None
        for a in data["applications"]:
            if a["id"] == self.app_id:
                app = a
                a["status"] = "denied"
                a["reviewed_by"] = str(interaction.user.id)
                a["reviewed_at"] = datetime.datetime.now(datetime.timezone.utc).isoformat()
                a["review_notes"] = self.reason.value
                break
        
        if not app:
            return await interaction.response.send_message("❌ Application not found!", ephemeral=True)
        
        save_applications(data)
        
        # Notify applicant
        applicant = interaction.guild.get_member(int(app["user_id"]))
        if applicant:
            template = APPLICATION_TEMPLATES.get(app["type"], {})
            dm_embed = discord.Embed(
                title="❌ Application Denied",
                description=f"Your **{template.get('name', 'application')}** has been denied.",
                color=0xe74c3c
            )
            dm_embed.add_field(name="Reason", value=self.reason.value, inline=False)
            dm_embed.set_footer(text=f"Reviewed by {interaction.user.display_name}")
            
            try:
                await applicant.send(embed=dm_embed)
            except:
                pass
        
        # Update message
        embed = interaction.message.embeds[0] if interaction.message.embeds else None
        if embed:
            embed.color = 0xe74c3c
            embed.add_field(
                name="📋 Status",
                value=f"**DENIED** by {interaction.user.mention}\n**Reason:** {self.reason.value}",
                inline=False
            )
            await interaction.message.edit(embed=embed, view=None)
        
        await interaction.response.send_message("❌ Application denied!", ephemeral=True)


@bot.command(name="setup_applications")
@commands.has_permissions(administrator=True)
async def setup_applications_panel(ctx):
    """Setup the applications panel"""
    embed = discord.Embed(
        title="📋 Applications",
        description=(
            "Apply for positions in The Fallen!\n\n"
            "**Available Positions:**\n"
            "🛡️ **Staff** - Help moderate the server\n"
            "🎯 **Tryout Host** - Host tryouts for new members\n"
            "📅 **Event Host** - Host events and activities\n\n"
            "**Requirements:**\n"
            "• Must meet level requirements\n"
            "• Can only apply once per cooldown period\n"
            "• Honest and detailed answers\n\n"
            "Click a button below to apply!"
        ),
        color=0x8B0000
    )
    embed.set_footer(text="✝ The Fallen Applications ✝")
    
    await ctx.send(embed=embed, view=ApplicationPanelView())
    await ctx.message.delete()


@bot.command(name="archive_old_apps")
@commands.has_any_role(*HIGH_STAFF_ROLES)
async def archive_old_applications(ctx, days: int = 30):
    """Archive applications older than X days"""
    data = load_applications()
    now = datetime.datetime.now(datetime.timezone.utc)
    archived_count = 0
    
    apps_to_archive = []
    for app in data["applications"]:
        try:
            submitted = datetime.datetime.fromisoformat(app["submitted_at"].replace('Z', '+00:00'))
            if (now - submitted).days >= days:
                apps_to_archive.append(app)
        except:
            pass
    
    for app in apps_to_archive:
        app["archived_at"] = now.isoformat()
        app["archived_by"] = "auto"
        data["archived"].append(app)
        data["applications"].remove(app)
        archived_count += 1
    
    save_applications(data)
    
    await ctx.send(f"📦 Archived {archived_count} applications older than {days} days.")


# ==========================================
# SETUP MOD LOG CHANNEL
# ==========================================

@bot.command(name="setup_modlog")
@commands.has_permissions(administrator=True)
async def setup_mod_log(ctx):
    """Setup the mod log channel"""
    # Check if exists
    existing = discord.utils.get(ctx.guild.text_channels, name=MOD_LOG_CHANNEL_NAME)
    if existing:
        return await ctx.send(f"✅ Mod log channel already exists: {existing.mention}")
    
    # Create channel
    overwrites = {
        ctx.guild.default_role: discord.PermissionOverwrite(read_messages=False),
        ctx.guild.me: discord.PermissionOverwrite(read_messages=True, send_messages=True)
    }
    
    # Add staff access
    staff_role = discord.utils.get(ctx.guild.roles, name=STAFF_ROLE_NAME)
    if staff_role:
        overwrites[staff_role] = discord.PermissionOverwrite(read_messages=True, send_messages=False)
    
    for role_name in HIGH_STAFF_ROLES:
        role = discord.utils.get(ctx.guild.roles, name=role_name)
        if role:
            overwrites[role] = discord.PermissionOverwrite(read_messages=True, send_messages=True)
    
    channel = await ctx.guild.create_text_channel(
        name=MOD_LOG_CHANNEL_NAME,
        overwrites=overwrites,
        topic="📋 Automatic moderation log - All mod actions are recorded here"
    )
    
    embed = discord.Embed(
        title="📋 Mod Log Initialized",
        description=(
            "This channel logs all moderation actions.\n\n"
            "**Logged Actions:**\n"
            "• ⚠️ Warnings (manual & auto)\n"
            "• 🔇 Mutes\n"
            "• 👢 Kicks\n"
            "• 🔨 Bans\n"
            "• 📈 Promotions\n"
            "• 📉 Demotions\n\n"
            "**Auto-Mod:**\n"
            "• Link filter\n"
            "• Spam detection\n"
            "• Mention spam\n"
            "• Duplicate messages"
        ),
        color=0x3498db
    )
    
    await channel.send(embed=embed)
    await ctx.send(f"✅ Created mod log channel: {channel.mention}")


# ==========================================
# SERVER INFO COMMAND
# ==========================================

@bot.command(name="serverinfo")
@commands.has_any_role(*HIGH_STAFF_ROLES, STAFF_ROLE_NAME)
async def server_info_panel(ctx):
    """
    Post the server info panel with all interactive buttons.
    Usage: !serverinfo
    """
    await ctx.message.delete()
    
    # Main header embed
    embed1 = discord.Embed(
        title="༺✦ THE FALLEN — SERVER INFO ✦༻",
        description="*Order forged in shadow. Power earned through action.*",
        color=0x8B0000
    )
    await ctx.send(embed=embed1)
    
    # Role Information section
    embed2 = discord.Embed(
        title="༺✦ ROLE INFORMATION ✦༻",
        description=(
            "• Ranks and roles within The Fallen are **earned, not requested.**\n"
            "• Combat ranks are obtained through tryouts and performance, while staff and activity roles are granted based on trust, consistency, and contribution.\n"
            "• Staff Roles: Assigned by High Staff only\n"
            "• Activity / War / Raid Roles: Performance-based"
        ),
        color=0x8B0000
    )
    await ctx.send(embed=embed2, view=ServerInfoView())
    
    # Level Perks section
    embed3 = discord.Embed(
        title="༺✦ LEVEL PERKS ✦༻",
        description=(
            "**Activity fuels ascension.**\n\n"
            "As you level up, you unlock:\n"
            "• Cosmetic roles & titles\n"
            "• Access to special channels\n"
            "• Event priority\n"
            "• Increased recognition within the clan\n\n"
            "**Higher levels = greater presence within The Fallen.**"
        ),
        color=0x8B0000
    )
    await ctx.send(embed=embed3, view=ServerInfoLevelsView())
    
    # Booster Perks section
    embed4 = discord.Embed(
        title="༺✦ BOOSTER PERKS ✦༻",
        description=(
            "**Support the legion and be rewarded.**\n\n"
            "• Exclusive Booster role\n"
            "• Priority access to select events & trainings\n"
            "• Special chat access\n"
            "• Recognition within the server\n"
            "• Faster response on applications & support"
        ),
        color=0x8B0000
    )
    await ctx.send(embed=embed4, view=ServerInfoBoosterView())
    
    # Important Notes section
    embed5 = discord.Embed(
        title="༺✦ IMPORTANT NOTES ✦༻",
        description=(
            "• Respect the hierarchy — structure keeps us strong\n"
            "• Follow the Code of Conduct at all times\n"
            "• Participation matters — inactivity leads to replacement\n"
            "• Power is proven through consistency, not words"
        ),
        color=0x8B0000
    )
    await ctx.send(embed=embed5)
    
    # Bot Guide section
    embed6 = discord.Embed(
        title="༺✦ BOT GUIDE ✦༻",
        description="Press the button below for bot info.",
        color=0x8B0000
    )
    await ctx.send(embed=embed6, view=ServerInfoBotView())
    
    # Welcome section
    embed7 = discord.Embed(
        title="༺✦ WELCOME TO THE FALLEN ✦༻",
        description=(
            "**If you are here to grow, fight, and rise —**\n"
            "**you are in the right place.**\n\n"
            "*Strength is taken. Order is enforced.*"
        ),
        color=0x8B0000
    )
    await ctx.send(embed=embed7)


# ==========================================
# CUSTOM EMBEDS BUILDER
# ==========================================

def load_custom_embeds():
    """Load saved custom embeds"""
    try:
        with open(EMBEDS_FILE, "r") as f:
            return json.load(f)
    except:
        return {"embeds": {}}

def save_custom_embeds(data):
    """Save custom embeds"""
    with open(EMBEDS_FILE, "w") as f:
        json.dump(data, f, indent=2)

class EmbedBuilderView(discord.ui.View):
    def __init__(self, author_id, embed_data=None):
        super().__init__(timeout=300)
        self.author_id = author_id
        self.embed_data = embed_data or {
            "title": "",
            "description": "",
            "color": "8B0000",
            "fields": [],
            "footer": "",
            "thumbnail": "",
            "image": ""
        }
    
    @discord.ui.button(label="📝 Set Title", style=discord.ButtonStyle.primary, row=0)
    async def set_title(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user.id != self.author_id:
            return await interaction.response.send_message("❌ Not your embed builder.", ephemeral=True)
        await interaction.response.send_modal(EmbedTitleModal(self))
    
    @discord.ui.button(label="📄 Set Description", style=discord.ButtonStyle.primary, row=0)
    async def set_desc(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user.id != self.author_id:
            return await interaction.response.send_message("❌ Not your embed builder.", ephemeral=True)
        await interaction.response.send_modal(EmbedDescriptionModal(self))
    
    @discord.ui.button(label="🎨 Set Color", style=discord.ButtonStyle.primary, row=0)
    async def set_color(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user.id != self.author_id:
            return await interaction.response.send_message("❌ Not your embed builder.", ephemeral=True)
        await interaction.response.send_modal(EmbedColorModal(self))
    
    @discord.ui.button(label="➕ Add Field", style=discord.ButtonStyle.success, row=1)
    async def add_field(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user.id != self.author_id:
            return await interaction.response.send_message("❌ Not your embed builder.", ephemeral=True)
        if len(self.embed_data["fields"]) >= 25:
            return await interaction.response.send_message("❌ Max 25 fields.", ephemeral=True)
        await interaction.response.send_modal(EmbedFieldModal(self))
    
    @discord.ui.button(label="🖼️ Set Images", style=discord.ButtonStyle.secondary, row=1)
    async def set_images(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user.id != self.author_id:
            return await interaction.response.send_message("❌ Not your embed builder.", ephemeral=True)
        await interaction.response.send_modal(EmbedImageModal(self))
    
    @discord.ui.button(label="📋 Set Footer", style=discord.ButtonStyle.secondary, row=1)
    async def set_footer(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user.id != self.author_id:
            return await interaction.response.send_message("❌ Not your embed builder.", ephemeral=True)
        await interaction.response.send_modal(EmbedFooterModal(self))
    
    @discord.ui.button(label="👁️ Preview", style=discord.ButtonStyle.primary, row=2)
    async def preview(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user.id != self.author_id:
            return await interaction.response.send_message("❌ Not your embed builder.", ephemeral=True)
        embed = self.build_embed()
        await interaction.response.send_message("**Preview:**", embed=embed, ephemeral=True)
    
    @discord.ui.button(label="📤 Send to Channel", style=discord.ButtonStyle.success, row=2)
    async def send_embed(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user.id != self.author_id:
            return await interaction.response.send_message("❌ Not your embed builder.", ephemeral=True)
        await interaction.response.send_modal(EmbedSendModal(self))
    
    @discord.ui.button(label="💾 Save Template", style=discord.ButtonStyle.secondary, row=2)
    async def save_template(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user.id != self.author_id:
            return await interaction.response.send_message("❌ Not your embed builder.", ephemeral=True)
        await interaction.response.send_modal(EmbedSaveModal(self))
    
    @discord.ui.button(label="🗑️ Clear All", style=discord.ButtonStyle.danger, row=2)
    async def clear_all(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user.id != self.author_id:
            return await interaction.response.send_message("❌ Not your embed builder.", ephemeral=True)
        self.embed_data = {
            "title": "",
            "description": "",
            "color": "8B0000",
            "fields": [],
            "footer": "",
            "thumbnail": "",
            "image": ""
        }
        await interaction.response.send_message("🗑️ Embed cleared!", ephemeral=True)
    
    def build_embed(self):
        """Build discord.Embed from data"""
        try:
            color = int(self.embed_data["color"], 16)
        except:
            color = 0x8B0000
        
        embed = discord.Embed(
            title=self.embed_data["title"] or None,
            description=self.embed_data["description"] or None,
            color=color
        )
        
        for field in self.embed_data["fields"]:
            embed.add_field(
                name=field["name"],
                value=field["value"],
                inline=field.get("inline", True)
            )
        
        if self.embed_data["footer"]:
            embed.set_footer(text=self.embed_data["footer"])
        
        if self.embed_data["thumbnail"]:
            embed.set_thumbnail(url=self.embed_data["thumbnail"])
        
        if self.embed_data["image"]:
            embed.set_image(url=self.embed_data["image"])
        
        return embed

class EmbedTitleModal(discord.ui.Modal, title="Set Embed Title"):
    embed_title = discord.ui.TextInput(label="Title", max_length=256, required=False)
    
    def __init__(self, view):
        super().__init__()
        self.view = view
        self.embed_title.default = view.embed_data.get("title", "")
    
    async def on_submit(self, interaction: discord.Interaction):
        self.view.embed_data["title"] = self.embed_title.value
        await interaction.response.send_message("✅ Title updated!", ephemeral=True)

class EmbedDescriptionModal(discord.ui.Modal, title="Set Embed Description"):
    desc = discord.ui.TextInput(label="Description", style=discord.TextStyle.paragraph, max_length=4000, required=False)
    
    def __init__(self, view):
        super().__init__()
        self.view = view
        self.desc.default = view.embed_data.get("description", "")
    
    async def on_submit(self, interaction: discord.Interaction):
        self.view.embed_data["description"] = self.desc.value
        await interaction.response.send_message("✅ Description updated!", ephemeral=True)

class EmbedColorModal(discord.ui.Modal, title="Set Embed Color"):
    color = discord.ui.TextInput(label="Color (Hex)", placeholder="8B0000", max_length=6)
    
    def __init__(self, view):
        super().__init__()
        self.view = view
        self.color.default = view.embed_data.get("color", "8B0000")
    
    async def on_submit(self, interaction: discord.Interaction):
        try:
            int(self.color.value, 16)
            self.view.embed_data["color"] = self.color.value
            await interaction.response.send_message("✅ Color updated!", ephemeral=True)
        except:
            await interaction.response.send_message("❌ Invalid hex color. Use format like: 8B0000", ephemeral=True)

class EmbedFieldModal(discord.ui.Modal, title="Add Field"):
    name = discord.ui.TextInput(label="Field Name", max_length=256)
    value = discord.ui.TextInput(label="Field Value", style=discord.TextStyle.paragraph, max_length=1024)
    inline = discord.ui.TextInput(label="Inline? (yes/no)", default="yes", max_length=3)
    
    def __init__(self, view):
        super().__init__()
        self.view = view
    
    async def on_submit(self, interaction: discord.Interaction):
        self.view.embed_data["fields"].append({
            "name": self.name.value,
            "value": self.value.value,
            "inline": self.inline.value.lower() in ["yes", "y", "true"]
        })
        await interaction.response.send_message(f"✅ Field added! ({len(self.view.embed_data['fields'])} total)", ephemeral=True)

class EmbedImageModal(discord.ui.Modal, title="Set Images"):
    thumbnail = discord.ui.TextInput(label="Thumbnail URL", required=False, placeholder="https://...")
    image = discord.ui.TextInput(label="Image URL", required=False, placeholder="https://...")
    
    def __init__(self, view):
        super().__init__()
        self.view = view
        self.thumbnail.default = view.embed_data.get("thumbnail", "")
        self.image.default = view.embed_data.get("image", "")
    
    async def on_submit(self, interaction: discord.Interaction):
        self.view.embed_data["thumbnail"] = self.thumbnail.value
        self.view.embed_data["image"] = self.image.value
        await interaction.response.send_message("✅ Images updated!", ephemeral=True)

class EmbedFooterModal(discord.ui.Modal, title="Set Footer"):
    footer = discord.ui.TextInput(label="Footer Text", max_length=2048, required=False)
    
    def __init__(self, view):
        super().__init__()
        self.view = view
        self.footer.default = view.embed_data.get("footer", "")
    
    async def on_submit(self, interaction: discord.Interaction):
        self.view.embed_data["footer"] = self.footer.value
        await interaction.response.send_message("✅ Footer updated!", ephemeral=True)

class EmbedSendModal(discord.ui.Modal, title="Send Embed"):
    channel_id = discord.ui.TextInput(label="Channel ID or #channel", placeholder="#announcements or 123456789")
    
    def __init__(self, view):
        super().__init__()
        self.view = view
    
    async def on_submit(self, interaction: discord.Interaction):
        # Parse channel
        channel = None
        channel_input = self.channel_id.value.strip()
        
        # Try channel mention
        if channel_input.startswith("<#") and channel_input.endswith(">"):
            try:
                ch_id = int(channel_input[2:-1])
                channel = interaction.guild.get_channel(ch_id)
            except:
                pass
        # Try channel ID
        elif channel_input.isdigit():
            channel = interaction.guild.get_channel(int(channel_input))
        # Try channel name
        else:
            channel = discord.utils.get(interaction.guild.text_channels, name=channel_input.replace("#", ""))
        
        if not channel:
            return await interaction.response.send_message("❌ Channel not found.", ephemeral=True)
        
        embed = self.view.build_embed()
        await channel.send(embed=embed)
        await interaction.response.send_message(f"✅ Embed sent to {channel.mention}!", ephemeral=True)

class EmbedSaveModal(discord.ui.Modal, title="Save Template"):
    name = discord.ui.TextInput(label="Template Name", max_length=50, placeholder="my_template")
    
    def __init__(self, view):
        super().__init__()
        self.view = view
    
    async def on_submit(self, interaction: discord.Interaction):
        data = load_custom_embeds()
        data["embeds"][self.name.value] = self.view.embed_data
        save_custom_embeds(data)
        await interaction.response.send_message(f"✅ Template saved as `{self.name.value}`!", ephemeral=True)

@bot.command(name="embedbuilder")
@commands.has_any_role(*HIGH_STAFF_ROLES, STAFF_ROLE_NAME)
@commands.cooldown(1, 30, commands.BucketType.user)  # 30 second cooldown
async def embed_builder(ctx):
    """Open the custom embed builder"""
    embed = discord.Embed(
        title="🎨 Custom Embed Builder",
        description="Use the buttons below to build your custom embed!",
        color=0x8B0000
    )
    embed.add_field(name="📝 Current Status", value="Empty embed - click buttons to add content", inline=False)
    embed.set_footer(text="Embed builder will timeout after 5 minutes of inactivity")
    
    view = EmbedBuilderView(ctx.author.id)
    await ctx.send(embed=embed, view=view)

@bot.command(name="embedload")
@commands.has_any_role(*HIGH_STAFF_ROLES, STAFF_ROLE_NAME)
async def embed_load(ctx, template_name: str):
    """Load a saved embed template"""
    data = load_custom_embeds()
    
    if template_name not in data["embeds"]:
        return await ctx.send(f"❌ Template `{template_name}` not found.")
    
    embed_data = data["embeds"][template_name]
    view = EmbedBuilderView(ctx.author.id, embed_data.copy())
    
    embed = discord.Embed(
        title="🎨 Custom Embed Builder",
        description=f"Loaded template: **{template_name}**",
        color=0x8B0000
    )
    
    await ctx.send(embed=embed, view=view)

@bot.command(name="embedlist")
@commands.has_any_role(*HIGH_STAFF_ROLES, STAFF_ROLE_NAME)
async def embed_list(ctx):
    """List saved embed templates"""
    data = load_custom_embeds()
    
    if not data["embeds"]:
        return await ctx.send("📋 No saved templates.")
    
    embed = discord.Embed(
        title="📋 Saved Embed Templates",
        description="\n".join(f"• `{name}`" for name in data["embeds"].keys()),
        color=0x3498db
    )
    embed.set_footer(text="Use !embedload <name> to load a template")
    
    await ctx.send(embed=embed)

@bot.command(name="embeddelete")
@commands.has_any_role(*HIGH_STAFF_ROLES, STAFF_ROLE_NAME)
async def embed_delete(ctx, template_name: str):
    """Delete a saved embed template"""
    data = load_custom_embeds()
    
    if template_name not in data["embeds"]:
        return await ctx.send(f"❌ Template `{template_name}` not found.")
    
    del data["embeds"][template_name]
    save_custom_embeds(data)
    
    await ctx.send(f"✅ Template `{template_name}` deleted.")


# ==========================================
# ADD PERSISTENT VIEWS TO SETUP_HOOK
# ==========================================

# ==========================================
# THE FALLEN - TOURNAMENT SYSTEM V3
# TourneyBot-style with full features
# ==========================================

import discord
from discord import app_commands
from discord.ext import commands
import json
import os
import datetime
import asyncio
import random
import math
from io import BytesIO

# Try to import PIL for bracket images
try:
    from PIL import Image, ImageDraw, ImageFont
    PIL_AVAILABLE = True
except ImportError:
    PIL_AVAILABLE = False

# ==========================================
# CONFIGURATION
# ==========================================

TOURNAMENT_FILE = "tournament_data.json"

# Rate limit protection delays (in seconds)
CHANNEL_CREATE_DELAY = 1.0
THREAD_CREATE_DELAY = 0.5
MESSAGE_SEND_DELAY = 0.3

# ==========================================
# DATA MANAGEMENT
# ==========================================

def load_tournament_data():
    """Load tournament data from file"""
    try:
        with open(TOURNAMENT_FILE, "r") as f:
            return json.load(f)
    except:
        return {
            "active_tournament": None,
            "tournaments": {},
            "history": []
        }

def save_tournament_data(data):
    """Save tournament data to file"""
    with open(TOURNAMENT_FILE, "w") as f:
        json.dump(data, f, indent=2)

def get_active_tournament():
    """Get the currently active tournament"""
    data = load_tournament_data()
    if data["active_tournament"]:
        return data["tournaments"].get(data["active_tournament"])
    return None

def update_tournament(tournament):
    """Update tournament in data"""
    data = load_tournament_data()
    data["tournaments"][tournament["id"]] = tournament
    save_tournament_data(data)

# ==========================================
# BRACKET GENERATION
# ==========================================

def generate_bracket(participants, seeded=False):
    """
    Generate single elimination bracket with proper BYE distribution
    Handles any number of participants (not just powers of 2)
    """
    if not participants:
        return []
    
    # Make a copy to avoid modifying original
    players = [p.copy() for p in participants]
    
    # Always shuffle for random matchups
    random.shuffle(players)
    
    # Assign seeds after shuffle
    for i, p in enumerate(players):
        p["seed"] = i + 1
    
    num_participants = len(players)
    
    # Find nearest power of 2 that's >= num_participants
    bracket_size = 1
    while bracket_size < num_participants:
        bracket_size *= 2
    
    # Calculate number of BYEs needed
    num_byes = bracket_size - num_participants
    
    # Create BYE entries
    byes = [{"id": "BYE", "name": "BYE", "seed": 999} for _ in range(num_byes)]
    
    # Distribute BYEs evenly - top seeds get BYEs
    # This means players at the end of shuffled list get BYEs (random advantage)
    bracket_players = []
    bye_index = 0
    
    for i in range(bracket_size):
        if i < num_participants:
            bracket_players.append(players[i])
        else:
            bracket_players.append(byes[bye_index])
            bye_index += 1
    
    # Interleave BYEs with real players for fair distribution
    # Put BYEs in second position of each pair where possible
    final_order = []
    real_players = [p for p in bracket_players if p["id"] != "BYE"]
    bye_players = [p for p in bracket_players if p["id"] == "BYE"]
    
    # Pair real players first, then add BYEs to remaining slots
    random.shuffle(real_players)  # Shuffle again for extra randomness
    
    # Create pairs - give BYEs to random players
    pairs_needed = bracket_size // 2
    bye_recipients = random.sample(range(len(real_players)), min(len(bye_players), len(real_players)))
    
    used_players = set()
    matches = []
    match_id = 1
    
    # First, create matches for players who get BYEs (auto-advance)
    for idx in bye_recipients:
        if idx < len(real_players) and bye_players:
            player = real_players[idx]
            bye = bye_players.pop(0)
            used_players.add(idx)
            
            match = {
                "id": f"M{match_id}",
                "round": 1,
                "player1": player,
                "player2": bye,
                "player1_score": 0,
                "player2_score": 0,
                "winner": player["id"],  # Auto-advance
                "status": "completed",
                "thread_id": None
            }
            matches.append(match)
            match_id += 1
    
    # Then create matches for remaining players
    remaining = [p for i, p in enumerate(real_players) if i not in used_players]
    random.shuffle(remaining)
    
    for i in range(0, len(remaining), 2):
        if i + 1 < len(remaining):
            match = {
                "id": f"M{match_id}",
                "round": 1,
                "player1": remaining[i],
                "player2": remaining[i + 1],
                "player1_score": 0,
                "player2_score": 0,
                "winner": None,
                "status": "pending",
                "thread_id": None
            }
            matches.append(match)
            match_id += 1
        elif remaining:
            # Odd player left - give them a BYE
            match = {
                "id": f"M{match_id}",
                "round": 1,
                "player1": remaining[i],
                "player2": {"id": "BYE", "name": "BYE", "seed": 999},
                "player1_score": 0,
                "player2_score": 0,
                "winner": remaining[i]["id"],
                "status": "completed",
                "thread_id": None
            }
            matches.append(match)
            match_id += 1
    
    # Sort matches by ID to maintain order
    matches.sort(key=lambda m: int(m["id"][1:]))
    
    # Recalculate match IDs
    for i, m in enumerate(matches):
        m["id"] = f"M{i + 1}"
    match_id = len(matches) + 1
    
    # Calculate total rounds needed
    total_rounds = max(1, math.ceil(math.log2(bracket_size)))
    
    # Generate placeholder matches for future rounds
    current_round_matches = len(matches)
    
    for round_num in range(2, total_rounds + 1):
        next_round_matches = max(1, current_round_matches // 2)
        
        for i in range(next_round_matches):
            # Calculate which matches feed into this one
            feed_start = match_id - current_round_matches - (i * 2) - 2 + (next_round_matches * 2)
            
            match = {
                "id": f"M{match_id}",
                "round": round_num,
                "player1": None,
                "player2": None,
                "player1_score": 0,
                "player2_score": 0,
                "winner": None,
                "status": "waiting",
                "thread_id": None,
                "feeds_from": [f"M{match_id - next_round_matches * 2 + i * 2 + 1}", 
                               f"M{match_id - next_round_matches * 2 + i * 2 + 2}"]
            }
            matches.append(match)
            match_id += 1
        
        current_round_matches = next_round_matches
        
        if next_round_matches <= 1:
            break
    
    # Auto-advance BYE winners to next round
    matches = auto_advance_byes(matches)
    
    return matches


def auto_advance_byes(matches):
    """Auto-advance winners from BYE matches to next rounds"""
    changed = True
    while changed:
        changed = False
        for match in matches:
            if match["status"] == "waiting" and "feeds_from" in match:
                feed1_id, feed2_id = match["feeds_from"]
                
                feeder1 = next((m for m in matches if m["id"] == feed1_id), None)
                feeder2 = next((m for m in matches if m["id"] == feed2_id), None)
                
                if feeder1 and feeder2:
                    if feeder1["status"] == "completed" and feeder2["status"] == "completed":
                        # Both feeders complete, advance winners
                        winner1 = None
                        winner2 = None
                        
                        if feeder1["winner"]:
                            if feeder1["player1"] and feeder1["player1"]["id"] == feeder1["winner"]:
                                winner1 = feeder1["player1"]
                            elif feeder1["player2"] and feeder1["player2"]["id"] == feeder1["winner"]:
                                winner1 = feeder1["player2"]
                        
                        if feeder2["winner"]:
                            if feeder2["player1"] and feeder2["player1"]["id"] == feeder2["winner"]:
                                winner2 = feeder2["player1"]
                            elif feeder2["player2"] and feeder2["player2"]["id"] == feeder2["winner"]:
                                winner2 = feeder2["player2"]
                        
                        if winner1 and winner2:
                            match["player1"] = winner1
                            match["player2"] = winner2
                            match["status"] = "pending"
                            changed = True
    
    return matches

def advance_bracket(tournament):
    """
    Check if winners can advance to next round matches
    """
    matches = tournament.get("matches", [])
    
    for match in matches:
        if match["status"] == "waiting" and "feeds_from" in match:
            feed1, feed2 = match["feeds_from"]
            
            # Find feeder matches
            feeder1 = next((m for m in matches if m["id"] == feed1), None)
            feeder2 = next((m for m in matches if m["id"] == feed2), None)
            
            if feeder1 and feeder2:
                if feeder1["status"] == "completed" and feeder2["status"] == "completed":
                    # Get winners
                    winner1 = next((p for p in [feeder1["player1"], feeder1["player2"]] 
                                   if p and p["id"] == feeder1["winner"]), None)
                    winner2 = next((p for p in [feeder2["player1"], feeder2["player2"]] 
                                   if p and p["id"] == feeder2["winner"]), None)
                    
                    if winner1 and winner2:
                        match["player1"] = winner1
                        match["player2"] = winner2
                        match["status"] = "pending"
    
    return matches

# ==========================================
# BRACKET IMAGE GENERATION
# ==========================================

async def create_bracket_image(tournament):
    """Create visual bracket image"""
    if not PIL_AVAILABLE:
        return None
    
    matches = tournament.get("matches", [])
    if not matches:
        return None
    
    # Calculate dimensions
    total_rounds = max(m["round"] for m in matches)
    matches_per_round = {}
    for m in matches:
        r = m["round"]
        matches_per_round[r] = matches_per_round.get(r, 0) + 1
    
    first_round_matches = matches_per_round.get(1, 1)
    
    # Image dimensions
    match_width = 200
    match_height = 50
    h_spacing = 80
    v_spacing = 20
    margin = 40
    
    width = margin * 2 + total_rounds * (match_width + h_spacing)
    height = margin * 2 + first_round_matches * (match_height * 2 + v_spacing * 2)
    
    # Create image
    img = Image.new("RGBA", (width, max(height, 400)), (30, 30, 40, 255))
    draw = ImageDraw.Draw(img)
    
    # Load font
    try:
        font = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf", 14)
        small_font = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf", 12)
        title_font = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf", 24)
    except:
        font = small_font = title_font = ImageFont.load_default()
    
    # Draw title
    title = tournament.get("name", "Tournament")
    draw.text((width // 2, 15), title, font=title_font, fill=(255, 255, 255), anchor="mm")
    
    # Draw matches by round
    match_positions = {}
    
    for round_num in range(1, total_rounds + 1):
        round_matches = [m for m in matches if m["round"] == round_num]
        num_matches = len(round_matches)
        
        # Calculate vertical spacing for this round
        available_height = height - margin * 2 - 40
        if num_matches > 0:
            spacing = available_height / num_matches
        else:
            spacing = available_height
        
        x = margin + (round_num - 1) * (match_width + h_spacing)
        
        for i, match in enumerate(round_matches):
            y = margin + 40 + i * spacing + spacing / 2 - match_height / 2
            
            match_positions[match["id"]] = (x + match_width, y + match_height / 2)
            
            # Draw match box
            box_color = (50, 50, 60)
            if match["status"] == "completed":
                box_color = (40, 80, 40)
            elif match["status"] == "in_progress":
                box_color = (80, 80, 40)
            
            draw.rounded_rectangle(
                [(x, y), (x + match_width, y + match_height * 2)],
                radius=5,
                fill=box_color,
                outline=(100, 100, 120)
            )
            
            # Player 1
            p1_name = match["player1"]["name"][:18] if match["player1"] else "TBD"
            p1_score = match["player1_score"] if match["player1"] else "-"
            p1_color = (255, 255, 255)
            if match["winner"] and match["player1"] and match["winner"] == match["player1"]["id"]:
                p1_color = (100, 255, 100)
            
            # Player 1 seed
            if match["player1"] and match["player1"].get("seed"):
                seed = match["player1"]["seed"]
                if seed != 999:
                    draw.text((x + 5, y + match_height / 2), str(seed), font=small_font, fill=(255, 215, 0), anchor="lm")
            
            draw.text((x + 25, y + match_height / 2), p1_name, font=font, fill=p1_color, anchor="lm")
            draw.text((x + match_width - 10, y + match_height / 2), str(p1_score), font=font, fill=p1_color, anchor="rm")
            
            # Divider line
            draw.line([(x + 5, y + match_height), (x + match_width - 5, y + match_height)], fill=(80, 80, 100), width=1)
            
            # Player 2
            p2_name = match["player2"]["name"][:18] if match["player2"] else "TBD"
            p2_score = match["player2_score"] if match["player2"] else "-"
            p2_color = (255, 255, 255)
            if match["winner"] and match["player2"] and match["winner"] == match["player2"]["id"]:
                p2_color = (100, 255, 100)
            
            # Player 2 seed
            if match["player2"] and match["player2"].get("seed"):
                seed = match["player2"]["seed"]
                if seed != 999:
                    draw.text((x + 5, y + match_height + match_height / 2), str(seed), font=small_font, fill=(255, 215, 0), anchor="lm")
            
            draw.text((x + 25, y + match_height + match_height / 2), p2_name, font=font, fill=p2_color, anchor="lm")
            draw.text((x + match_width - 10, y + match_height + match_height / 2), str(p2_score), font=font, fill=p2_color, anchor="rm")
            
            # Draw round label
            if i == 0:
                round_label = f"Round {round_num}"
                if round_num == total_rounds:
                    round_label = "Finals"
                elif round_num == total_rounds - 1:
                    round_label = "Semi-Finals"
                draw.text((x + match_width / 2, y - 15), round_label, font=small_font, fill=(150, 150, 150), anchor="mm")
    
    # Draw connecting lines
    for match in matches:
        if "feeds_from" in match and match["id"] in match_positions:
            end_x, end_y = match_positions[match["id"]]
            end_x -= match_width  # Adjust to left side of match box
            
            for feeder_id in match["feeds_from"]:
                if feeder_id in match_positions:
                    start_x, start_y = match_positions[feeder_id]
                    
                    # Draw bracket line
                    mid_x = (start_x + end_x) / 2
                    draw.line([(start_x, start_y), (mid_x, start_y)], fill=(100, 100, 120), width=2)
                    draw.line([(mid_x, start_y), (mid_x, end_y)], fill=(100, 100, 120), width=2)
                    draw.line([(mid_x, end_y), (end_x, end_y)], fill=(100, 100, 120), width=2)
    
    # Footer
    draw.text((width // 2, height - 15), "✝ THE FALLEN ✝", font=small_font, fill=(139, 0, 0), anchor="mm")
    
    # Save to buffer
    buffer = BytesIO()
    img.save(buffer, format="PNG")
    buffer.seek(0)
    return buffer


# ==========================================
# TOURNAMENT MODALS
# ==========================================

class TournamentCreateModal(discord.ui.Modal, title="Create Tournament"):
    """Modal for creating a new tournament"""
    
    name = discord.ui.TextInput(
        label="Tournament Name",
        placeholder="Enter tournament name...",
        max_length=40,
        required=True
    )
    
    best_of = discord.ui.TextInput(
        label="Best Of",
        placeholder="1, 3, 5, or 7",
        default="1",
        max_length=1,
        required=True
    )
    
    max_participants = discord.ui.TextInput(
        label="Max Participants",
        placeholder="8, 16, 32, 64...",
        default="16",
        max_length=3,
        required=True
    )
    
    rules = discord.ui.TextInput(
        label="Rules",
        placeholder="Tournament rules...",
        style=discord.TextStyle.paragraph,
        max_length=900,
        required=False
    )
    
    info = discord.ui.TextInput(
        label="Additional Info",
        placeholder="Prize, schedule, etc...",
        style=discord.TextStyle.paragraph,
        max_length=900,
        required=False
    )
    
    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        
        # Validate best_of
        try:
            best_of = int(self.best_of.value)
            if best_of not in [1, 3, 5, 7]:
                best_of = 1
        except:
            best_of = 1
        
        # Validate max_participants
        try:
            max_p = int(self.max_participants.value)
            if max_p < 2:
                max_p = 8
            elif max_p > 128:
                max_p = 128
        except:
            max_p = 16
        
        # Create tournament data
        tournament_id = f"T{int(datetime.datetime.now().timestamp())}"
        tournament = {
            "id": tournament_id,
            "name": self.name.value,
            "creator_id": str(interaction.user.id),
            "guild_id": str(interaction.guild.id),
            "best_of": best_of,
            "max_participants": max_p,
            "rules": self.rules.value or "No rules specified.",
            "info": self.info.value or "No additional info.",
            "status": "setup",  # setup, registration, checkin, active, completed
            "bracket_type": "single_elimination",
            "participants": [],
            "checked_in": [],
            "spectators": [],
            "matches": [],
            "created_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            "started_at": None,
            "ended_at": None,
            "winner": None,
            "required_role_id": None,
            "required_role_name": None,
            "channels": {
                "category": None,
                "admin": None,
                "registration": None,
                "bracket": None,
                "chat": None,
                "results": None
            },
            "messages": {
                "admin_panel": None,
                "registration_portal": None,
                "bracket_display": None
            }
        }
        
        # Save to data
        data = load_tournament_data()
        data["tournaments"][tournament_id] = tournament
        data["active_tournament"] = tournament_id
        save_tournament_data(data)
        
        await interaction.followup.send(
            f"✅ Tournament **{self.name.value}** created!\n"
            f"Head to the admin channel to configure and publish.",
            ephemeral=True
        )
        
        # Create admin panel in current channel
        await create_admin_panel(interaction.channel, tournament)


class TournamentConfigModal(discord.ui.Modal, title="Edit Configuration"):
    """Modal for editing tournament config"""
    
    def __init__(self, tournament):
        super().__init__()
        self.tournament = tournament
        
        self.name = discord.ui.TextInput(
            label="Tournament Name",
            default=tournament.get("name", ""),
            max_length=40,
            required=True
        )
        self.add_item(self.name)
        
        self.best_of = discord.ui.TextInput(
            label="Best Of (1, 3, 5, or 7)",
            default=str(tournament.get("best_of", 1)),
            max_length=1,
            required=True
        )
        self.add_item(self.best_of)
        
        self.max_participants = discord.ui.TextInput(
            label="Max Participants",
            default=str(tournament.get("max_participants", 16)),
            max_length=3,
            required=True
        )
        self.add_item(self.max_participants)
        
        self.rules = discord.ui.TextInput(
            label="Rules",
            default=tournament.get("rules", ""),
            style=discord.TextStyle.paragraph,
            max_length=900,
            required=False
        )
        self.add_item(self.rules)
        
        self.info = discord.ui.TextInput(
            label="Additional Info",
            default=tournament.get("info", ""),
            style=discord.TextStyle.paragraph,
            max_length=900,
            required=False
        )
        self.add_item(self.info)
    
    async def on_submit(self, interaction: discord.Interaction):
        # Validate
        try:
            best_of = int(self.best_of.value)
            if best_of not in [1, 3, 5, 7]:
                best_of = 1
        except:
            best_of = 1
        
        try:
            max_p = int(self.max_participants.value)
            if max_p < 2:
                max_p = 8
        except:
            max_p = 16
        
        # Update tournament
        self.tournament["name"] = self.name.value
        self.tournament["best_of"] = best_of
        self.tournament["max_participants"] = max_p
        self.tournament["rules"] = self.rules.value or "No rules specified."
        self.tournament["info"] = self.info.value or "No additional info."
        
        update_tournament(self.tournament)
        
        await interaction.response.send_message(
            f"✅ Configuration updated!",
            ephemeral=True
        )
        
        # Update admin panel
        if self.tournament["messages"].get("admin_panel"):
            try:
                channel = interaction.channel
                msg = await channel.fetch_message(int(self.tournament["messages"]["admin_panel"]))
                embed = create_admin_embed(self.tournament)
                await msg.edit(embed=embed)
            except:
                pass


class TournamentPublishModal(discord.ui.Modal, title="Publish Tournament"):
    """Modal for publish text"""
    
    publish_text = discord.ui.TextInput(
        label="Announcement Message",
        placeholder="Hey! Join our tournament!",
        default="🏆 Tournament registration is now open!",
        style=discord.TextStyle.paragraph,
        max_length=500,
        required=True
    )
    
    def __init__(self, tournament, target_channel):
        super().__init__()
        self.tournament = tournament
        self.target_channel = target_channel
    
    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        
        # Create registration portal embed
        embed = create_registration_embed(self.tournament)
        
        # Send to target channel
        view = TournamentRegistrationView(self.tournament["id"])
        msg = await self.target_channel.send(
            content=self.publish_text.value,
            embed=embed,
            view=view
        )
        
        # Save message ID
        self.tournament["messages"]["registration_portal"] = str(msg.id)
        self.tournament["channels"]["registration"] = str(self.target_channel.id)
        self.tournament["status"] = "registration"
        update_tournament(self.tournament)
        
        await interaction.followup.send(
            f"✅ Registration portal published to {self.target_channel.mention}!",
            ephemeral=True
        )


class ScoreSubmitModal(discord.ui.Modal, title="Submit Match Score"):
    """Modal for submitting match scores"""
    
    def __init__(self, tournament, match, player1_name, player2_name):
        super().__init__()
        self.tournament = tournament
        self.match = match
        
        self.player1_score = discord.ui.TextInput(
            label=player1_name[:45],
            placeholder="Enter score (0-99)",
            max_length=2,
            required=True
        )
        self.add_item(self.player1_score)
        
        self.player2_score = discord.ui.TextInput(
            label=player2_name[:45],
            placeholder="Enter score (0-99)",
            max_length=2,
            required=True
        )
        self.add_item(self.player2_score)
    
    async def on_submit(self, interaction: discord.Interaction):
        try:
            p1_score = int(self.player1_score.value)
            p2_score = int(self.player2_score.value)
        except:
            return await interaction.response.send_message("❌ Invalid scores!", ephemeral=True)
        
        if p1_score < 0 or p2_score < 0:
            return await interaction.response.send_message("❌ Scores must be positive!", ephemeral=True)
        
        if p1_score == p2_score:
            return await interaction.response.send_message("❌ Scores cannot be tied!", ephemeral=True)
        
        # Determine winner
        best_of = self.tournament.get("best_of", 1)
        wins_needed = (best_of // 2) + 1
        
        # Update match
        self.match["player1_score"] = p1_score
        self.match["player2_score"] = p2_score
        
        if p1_score >= wins_needed:
            self.match["winner"] = self.match["player1"]["id"]
        elif p2_score >= wins_needed:
            self.match["winner"] = self.match["player2"]["id"]
        else:
            # Just record the score, match not complete yet
            update_tournament(self.tournament)
            return await interaction.response.send_message(
                f"📊 Score recorded: {p1_score} - {p2_score}\n"
                f"Need {wins_needed} wins to complete match.",
                ephemeral=True
            )
        
        self.match["status"] = "completed"
        
        # Advance bracket
        self.tournament["matches"] = advance_bracket(self.tournament)
        update_tournament(self.tournament)
        
        winner_name = self.match["player1"]["name"] if self.match["winner"] == self.match["player1"]["id"] else self.match["player2"]["name"]
        
        await interaction.response.send_message(
            f"✅ Match completed!\n"
            f"**Winner:** {winner_name} ({p1_score} - {p2_score})",
            ephemeral=True
        )
        
        # Check if tournament is complete
        await check_tournament_complete(interaction, self.tournament)
        
        # Update bracket image
        await update_bracket_display(interaction.guild, self.tournament)


# ==========================================
# EMBED BUILDERS
# ==========================================

def create_admin_embed(tournament):
    """Create the admin panel embed"""
    embed = discord.Embed(
        title="⚙️ Configuration",
        color=0x8B0000
    )
    
    # Config section
    config_text = (
        f"📝 **Name:** {tournament['name']}\n"
        f"🎯 **Best of:** {tournament['best_of']}\n"
        f"👥 **Player Cap:** {tournament['max_participants']}\n"
        f"📋 **Rules:** {'Custom rules entered.' if tournament['rules'] != 'No rules specified.' else 'None specified'}\n"
        f"ℹ️ **Info:** {'Custom info entered.' if tournament['info'] != 'No additional info.' else 'None specified'}\n"
        f"⚔️ **Bracket:** Single Elimination\n"
        f"👤 **Team Size:** 1\n"
        f"📊 **Status:** {tournament['status'].title()}\n"
        f"👥 **Participants:** {len(tournament['participants'])}/{tournament['max_participants']}"
    )
    embed.add_field(name="Settings", value=config_text, inline=False)
    
    # Role restriction
    if tournament.get("required_role_name"):
        embed.add_field(
            name="🔒 Role Restriction",
            value=f"Required: @{tournament['required_role_name']}",
            inline=False
        )
    
    # Buttons explanation
    buttons_text = (
        "▶️ **Start** — Starts the tournament\n"
        "📤 **Publish** — Opens registration\n"
        "➖ **Unpublish** — Closes registration\n"
        "✅ **Check-in** — Start check-in phase\n"
        "🗑️ **Delete** — Deletes tournament\n"
        "📝 **Config** — Edit settings\n"
        "🔒 **Role** — Set required role\n"
        "👥 **Players** — View participants"
    )
    embed.add_field(name="Controls", value=buttons_text, inline=False)
    
    embed.set_footer(text="✝ THE FALLEN ✝")
    
    return embed


def create_registration_embed(tournament):
    """Create the registration portal embed"""
    embed = discord.Embed(
        title=tournament["name"],
        color=0x8B0000
    )
    
    embed.add_field(name="⚔️ Bracket", value=tournament["bracket_type"].replace("_", " ").title(), inline=True)
    embed.add_field(name="🎯 Matches", value=f"Best {tournament['best_of']} of {tournament['best_of']}", inline=True)
    embed.add_field(name="👤 Team Size", value="1", inline=True)
    embed.add_field(
        name="👥 Participants", 
        value=f"{len(tournament['participants'])}/{tournament['max_participants']}", 
        inline=True
    )
    embed.add_field(name="📊 Status", value=tournament["status"].title(), inline=True)
    
    if tournament.get("required_role_name"):
        embed.add_field(name="🔒 Required Role", value=f"@{tournament['required_role_name']}", inline=True)
    
    if tournament["info"] and tournament["info"] != "No additional info.":
        embed.add_field(name="ℹ️ Information", value=tournament["info"], inline=False)
    
    if tournament["rules"] and tournament["rules"] != "No rules specified.":
        embed.add_field(name="📋 Rules", value=tournament["rules"], inline=False)
    
    embed.set_footer(text="✝ THE FALLEN ✝")
    
    return embed


def create_results_embed(tournament):
    """Create tournament results embed"""
    embed = discord.Embed(
        title=f"🏆 {tournament['name']} — Results",
        color=0xFFD700
    )
    
    # Get standings
    standings = get_standings(tournament)
    
    standings_text = ""
    medals = ["🥇", "🥈", "🥉", "4️⃣"]
    for i, player in enumerate(standings[:4]):
        medal = medals[i] if i < len(medals) else f"{i+1}."
        standings_text += f"{medal} **{player['name']}**\n"
    
    embed.add_field(name="Standings", value=standings_text or "No results yet", inline=False)
    embed.add_field(name="👥 Participants", value=f"{len(tournament['participants'])}/{tournament['max_participants']}", inline=True)
    
    embed.set_footer(text="✝ THE FALLEN ✝")
    
    return embed


def get_standings(tournament):
    """Get tournament standings based on match results"""
    matches = tournament.get("matches", [])
    if not matches:
        return []
    
    # Track eliminations by round
    player_rounds = {}  # player_id -> highest round reached
    
    for match in matches:
        if match["status"] == "completed" and match["winner"]:
            # Winner advances
            winner_id = match["winner"]
            if match["player1"] and match["player1"]["id"] == winner_id:
                winner = match["player1"]
                loser = match["player2"]
            else:
                winner = match["player2"]
                loser = match["player1"]
            
            if winner:
                player_rounds[winner["id"]] = max(player_rounds.get(winner["id"], 0), match["round"] + 1)
            if loser and loser["id"] != "BYE":
                player_rounds[loser["id"]] = max(player_rounds.get(loser["id"], 0), match["round"])
    
    # Sort by highest round reached
    participants = tournament.get("participants", [])
    sorted_players = sorted(participants, key=lambda p: player_rounds.get(p["id"], 0), reverse=True)
    
    return sorted_players


# ==========================================
# VIEWS
# ==========================================

class TournamentAdminView(discord.ui.View):
    """Admin control panel view"""
    
    def __init__(self, tournament_id):
        super().__init__(timeout=None)
        self.tournament_id = tournament_id
    
    def get_tournament(self):
        data = load_tournament_data()
        return data["tournaments"].get(self.tournament_id)
    
    @discord.ui.button(label="▶️", style=discord.ButtonStyle.success, custom_id="tourney_start", row=0)
    async def start_tournament(self, interaction: discord.Interaction, button: discord.ui.Button):
        tournament = self.get_tournament()
        if not tournament:
            return await interaction.response.send_message("❌ Tournament not found!", ephemeral=True)
        
        if len(tournament["participants"]) < 2:
            return await interaction.response.send_message("❌ Need at least 2 participants!", ephemeral=True)
        
        if tournament["status"] == "active":
            return await interaction.response.send_message("❌ Tournament already started!", ephemeral=True)
        
        # Confirmation
        await interaction.response.send_message(
            f"Are you sure you want to start the tournament with **{len(tournament['participants'])}** participants?",
            view=StartConfirmView(self.tournament_id),
            ephemeral=True
        )
    
    @discord.ui.button(label="📤", style=discord.ButtonStyle.primary, custom_id="tourney_publish", row=0)
    async def publish_tournament(self, interaction: discord.Interaction, button: discord.ui.Button):
        tournament = self.get_tournament()
        if not tournament:
            return await interaction.response.send_message("❌ Tournament not found!", ephemeral=True)
        
        # Ask for channel
        await interaction.response.send_message(
            "Select the channel to publish the registration portal:",
            view=ChannelSelectView(self.tournament_id),
            ephemeral=True
        )
    
    @discord.ui.button(label="➖", style=discord.ButtonStyle.danger, custom_id="tourney_unpublish", row=0)
    async def unpublish_tournament(self, interaction: discord.Interaction, button: discord.ui.Button):
        tournament = self.get_tournament()
        if not tournament:
            return await interaction.response.send_message("❌ Tournament not found!", ephemeral=True)
        
        tournament["status"] = "setup"
        update_tournament(tournament)
        
        await interaction.response.send_message("✅ Registration closed!", ephemeral=True)
    
    @discord.ui.button(label="✅", style=discord.ButtonStyle.success, custom_id="tourney_checkin", row=0)
    async def start_checkin(self, interaction: discord.Interaction, button: discord.ui.Button):
        tournament = self.get_tournament()
        if not tournament:
            return await interaction.response.send_message("❌ Tournament not found!", ephemeral=True)
        
        tournament["status"] = "checkin"
        update_tournament(tournament)
        
        await interaction.response.send_message("✅ Check-in phase started!", ephemeral=True)
    
    @discord.ui.button(label="🗑️", style=discord.ButtonStyle.danger, custom_id="tourney_delete", row=1)
    async def delete_tournament(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_message(
            "⚠️ Are you sure you want to delete this tournament?",
            view=DeleteConfirmView(self.tournament_id),
            ephemeral=True
        )
    
    @discord.ui.button(label="📝", style=discord.ButtonStyle.secondary, custom_id="tourney_config", row=1)
    async def edit_config(self, interaction: discord.Interaction, button: discord.ui.Button):
        tournament = self.get_tournament()
        if not tournament:
            return await interaction.response.send_message("❌ Tournament not found!", ephemeral=True)
        
        await interaction.response.send_modal(TournamentConfigModal(tournament))
    
    @discord.ui.button(label="🔒", style=discord.ButtonStyle.secondary, custom_id="tourney_role", row=1)
    async def set_role(self, interaction: discord.Interaction, button: discord.ui.Button):
        tournament = self.get_tournament()
        if not tournament:
            return await interaction.response.send_message("❌ Tournament not found!", ephemeral=True)
        
        await interaction.response.send_message(
            "Select a required role (only members with this role can join):",
            view=RoleSelectView(self.tournament_id),
            ephemeral=True
        )
    
    @discord.ui.button(label="👥", style=discord.ButtonStyle.secondary, custom_id="tourney_players", row=1)
    async def view_players(self, interaction: discord.Interaction, button: discord.ui.Button):
        tournament = self.get_tournament()
        if not tournament:
            return await interaction.response.send_message("❌ Tournament not found!", ephemeral=True)
        
        participants = tournament.get("participants", [])
        if not participants:
            return await interaction.response.send_message("No participants yet!", ephemeral=True)
        
        embed = discord.Embed(
            title=f"👥 Participants ({len(participants)}/{tournament['max_participants']})",
            color=0x8B0000
        )
        
        player_list = "\n".join([f"{i+1}. {p['name']}" for i, p in enumerate(participants[:25])])
        if len(participants) > 25:
            player_list += f"\n... and {len(participants) - 25} more"
        
        embed.description = player_list
        embed.set_footer(text="✝ THE FALLEN ✝")
        
        await interaction.response.send_message(embed=embed, ephemeral=True)


class TournamentRegistrationView(discord.ui.View):
    """Registration portal view"""
    
    def __init__(self, tournament_id):
        super().__init__(timeout=None)
        self.tournament_id = tournament_id
    
    def get_tournament(self):
        data = load_tournament_data()
        return data["tournaments"].get(self.tournament_id)
    
    @discord.ui.button(label="Register", style=discord.ButtonStyle.success, custom_id="tourney_register")
    async def register(self, interaction: discord.Interaction, button: discord.ui.Button):
        tournament = self.get_tournament()
        if not tournament:
            return await interaction.response.send_message("❌ Tournament not found!", ephemeral=True)
        
        if tournament["status"] not in ["registration", "checkin"]:
            return await interaction.response.send_message("❌ Registration is not open!", ephemeral=True)
        
        # Check role restriction
        if tournament.get("required_role_id"):
            role = interaction.guild.get_role(int(tournament["required_role_id"]))
            if role and role not in interaction.user.roles:
                return await interaction.response.send_message(
                    f"❌ You need the **@{tournament['required_role_name']}** role to join!",
                    ephemeral=True
                )
        
        # Check if already registered
        user_id = str(interaction.user.id)
        if any(p["id"] == user_id for p in tournament["participants"]):
            return await interaction.response.send_message("❌ You're already registered!", ephemeral=True)
        
        # Check capacity
        if len(tournament["participants"]) >= tournament["max_participants"]:
            return await interaction.response.send_message("❌ Tournament is full!", ephemeral=True)
        
        # Add participant
        tournament["participants"].append({
            "id": user_id,
            "name": interaction.user.display_name,
            "seed": len(tournament["participants"]) + 1
        })
        update_tournament(tournament)
        
        await interaction.response.send_message(
            f"✅ You have been registered for **{tournament['name']}**! 🎉",
            ephemeral=True
        )
        
        # Update registration embed
        await update_registration_embed(interaction, tournament)
    
    @discord.ui.button(label="Leave", style=discord.ButtonStyle.secondary, custom_id="tourney_leave")
    async def leave(self, interaction: discord.Interaction, button: discord.ui.Button):
        tournament = self.get_tournament()
        if not tournament:
            return await interaction.response.send_message("❌ Tournament not found!", ephemeral=True)
        
        if tournament["status"] == "active":
            return await interaction.response.send_message("❌ Cannot leave after tournament started!", ephemeral=True)
        
        user_id = str(interaction.user.id)
        tournament["participants"] = [p for p in tournament["participants"] if p["id"] != user_id]
        update_tournament(tournament)
        
        await interaction.response.send_message("✅ You have left the tournament.", ephemeral=True)
        await update_registration_embed(interaction, tournament)
    
    @discord.ui.button(label="Spectate", style=discord.ButtonStyle.primary, custom_id="tourney_spectate")
    async def spectate(self, interaction: discord.Interaction, button: discord.ui.Button):
        tournament = self.get_tournament()
        if not tournament:
            return await interaction.response.send_message("❌ Tournament not found!", ephemeral=True)
        
        user_id = str(interaction.user.id)
        if user_id not in tournament.get("spectators", []):
            if "spectators" not in tournament:
                tournament["spectators"] = []
            tournament["spectators"].append(user_id)
            update_tournament(tournament)
            await interaction.response.send_message("👀 You're now spectating this tournament!", ephemeral=True)
        else:
            tournament["spectators"].remove(user_id)
            update_tournament(tournament)
            await interaction.response.send_message("👀 You're no longer spectating.", ephemeral=True)


class MatchScoreView(discord.ui.View):
    """View for submitting match scores - STAFF ONLY"""
    
    def __init__(self, tournament_id, match_id):
        super().__init__(timeout=None)
        self.tournament_id = tournament_id
        self.match_id = match_id
    
    @discord.ui.button(label="⚔️ Report Score", style=discord.ButtonStyle.primary, custom_id="match_score")
    async def submit_score(self, interaction: discord.Interaction, button: discord.ui.Button):
        # STAFF ONLY CHECK
        if not is_staff(interaction.user):
            return await interaction.response.send_message("❌ Only staff can report match scores!", ephemeral=True)
        
        data = load_tournament_data()
        tournament = data["tournaments"].get(self.tournament_id)
        if not tournament:
            return await interaction.response.send_message("❌ Tournament not found!", ephemeral=True)
        
        match = next((m for m in tournament["matches"] if m["id"] == self.match_id), None)
        if not match:
            return await interaction.response.send_message("❌ Match not found!", ephemeral=True)
        
        if match["status"] == "completed":
            return await interaction.response.send_message("❌ Match already completed!", ephemeral=True)
        
        p1_name = match["player1"]["name"] if match["player1"] else "Player 1"
        p2_name = match["player2"]["name"] if match["player2"] else "Player 2"
        
        await interaction.response.send_modal(
            ScoreSubmitModal(tournament, match, p1_name, p2_name)
        )


class StartConfirmView(discord.ui.View):
    """Confirmation view for starting tournament"""
    
    def __init__(self, tournament_id):
        super().__init__(timeout=60)
        self.tournament_id = tournament_id
    
    @discord.ui.button(label="✅ Confirm", style=discord.ButtonStyle.success)
    async def confirm(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer(ephemeral=True)
        
        data = load_tournament_data()
        tournament = data["tournaments"].get(self.tournament_id)
        if not tournament:
            return await interaction.followup.send("❌ Tournament not found!", ephemeral=True)
        
        # Generate bracket
        tournament["matches"] = generate_bracket(tournament["participants"])
        tournament["status"] = "active"
        tournament["started_at"] = datetime.datetime.now(datetime.timezone.utc).isoformat()
        
        update_tournament(tournament)
        
        await interaction.followup.send("✅ Tournament started! Creating match threads...", ephemeral=True)
        
        # Create bracket display
        await create_bracket_display(interaction.guild, tournament)
        
        # Create match threads
        await create_match_threads(interaction.guild, tournament)
        
        self.stop()
    
    @discord.ui.button(label="❌ Cancel", style=discord.ButtonStyle.danger)
    async def cancel(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_message("Cancelled.", ephemeral=True)
        self.stop()


class DeleteConfirmView(discord.ui.View):
    """Confirmation view for deleting tournament"""
    
    def __init__(self, tournament_id):
        super().__init__(timeout=60)
        self.tournament_id = tournament_id
    
    @discord.ui.button(label="🗑️ Yes, Delete", style=discord.ButtonStyle.danger)
    async def confirm(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            data = load_tournament_data()
            
            tournament_name = "Unknown"
            if self.tournament_id in data["tournaments"]:
                tournament_name = data["tournaments"][self.tournament_id].get("name", "Unknown")
                del data["tournaments"][self.tournament_id]
            
            if data["active_tournament"] == self.tournament_id:
                data["active_tournament"] = None
            
            save_tournament_data(data)
            
            await interaction.response.send_message(
                f"✅ Tournament **{tournament_name}** has been deleted!", 
                ephemeral=True
            )
            
            # Try to delete the original admin panel message
            try:
                await interaction.message.delete()
            except:
                pass
                
        except Exception as e:
            await interaction.response.send_message(f"❌ Error deleting: {e}", ephemeral=True)
        
        self.stop()
    
    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.secondary)
    async def cancel(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_message("Cancelled.", ephemeral=True)
        self.stop()


class ChannelSelectView(discord.ui.View):
    """View for selecting publish channel"""
    
    def __init__(self, tournament_id):
        super().__init__(timeout=120)
        self.tournament_id = tournament_id
    
    @discord.ui.select(
        cls=discord.ui.ChannelSelect,
        channel_types=[discord.ChannelType.text],
        placeholder="Select a channel...",
        min_values=1,
        max_values=1
    )
    async def channel_select(self, interaction: discord.Interaction, select: discord.ui.ChannelSelect):
        try:
            selected_channel = select.values[0]
            
            # Resolve the channel object
            channel = interaction.guild.get_channel(selected_channel.id)
            if not channel:
                return await interaction.response.send_message("❌ Could not find that channel!", ephemeral=True)
            
            data = load_tournament_data()
            tournament = data["tournaments"].get(self.tournament_id)
            if not tournament:
                return await interaction.response.send_message("❌ Tournament not found!", ephemeral=True)
            
            # Instead of modal, just publish directly
            await interaction.response.defer(ephemeral=True)
            
            # Create registration portal embed
            embed = create_registration_embed(tournament)
            
            # Send to target channel
            view = TournamentRegistrationView(tournament["id"])
            msg = await channel.send(
                content="🏆 **Tournament registration is now open!**",
                embed=embed,
                view=view
            )
            
            # Save message ID
            tournament["messages"]["registration_portal"] = str(msg.id)
            tournament["channels"]["registration"] = str(channel.id)
            tournament["status"] = "registration"
            update_tournament(tournament)
            
            await interaction.followup.send(
                f"✅ Registration portal published to {channel.mention}!",
                ephemeral=True
            )
            self.stop()
            
        except Exception as e:
            print(f"Publish error: {e}")
            try:
                await interaction.response.send_message(f"❌ Error: {e}", ephemeral=True)
            except:
                await interaction.followup.send(f"❌ Error: {e}", ephemeral=True)


class RoleSelectView(discord.ui.View):
    """View for selecting required role"""
    
    def __init__(self, tournament_id):
        super().__init__(timeout=120)
        self.tournament_id = tournament_id
    
    @discord.ui.select(
        cls=discord.ui.RoleSelect,
        placeholder="Select a required role...",
        min_values=0,
        max_values=1
    )
    async def role_select(self, interaction: discord.Interaction, select: discord.ui.RoleSelect):
        data = load_tournament_data()
        tournament = data["tournaments"].get(self.tournament_id)
        if not tournament:
            return await interaction.response.send_message("❌ Tournament not found!", ephemeral=True)
        
        if select.values:
            role = select.values[0]
            tournament["required_role_id"] = str(role.id)
            tournament["required_role_name"] = role.name
            update_tournament(tournament)
            await interaction.response.send_message(f"✅ Required role set to **@{role.name}**", ephemeral=True)
        else:
            tournament["required_role_id"] = None
            tournament["required_role_name"] = None
            update_tournament(tournament)
            await interaction.response.send_message("✅ Role restriction removed.", ephemeral=True)
        
        self.stop()


class TournamentEndView(discord.ui.View):
    """View shown when tournament ends"""
    
    def __init__(self, tournament_id):
        super().__init__(timeout=None)
        self.tournament_id = tournament_id
    
    @discord.ui.button(label="🗑️ Delete", style=discord.ButtonStyle.danger, custom_id="tourney_end_delete")
    async def delete(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not is_staff(interaction.user):
            return await interaction.response.send_message("❌ Staff only!", ephemeral=True)
        await interaction.response.send_message(
            "⚠️ Are you sure you want to delete this tournament?",
            view=DeleteConfirmView(self.tournament_id),
            ephemeral=True
        )
    
    @discord.ui.button(label="🔄 Re-open", style=discord.ButtonStyle.secondary, custom_id="tourney_end_reopen")
    async def reopen(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not is_staff(interaction.user):
            return await interaction.response.send_message("❌ Staff only!", ephemeral=True)
        data = load_tournament_data()
        tournament = data["tournaments"].get(self.tournament_id)
        if tournament:
            tournament["status"] = "active"
            update_tournament(tournament)
            await interaction.response.send_message("✅ Tournament re-opened!", ephemeral=True)
    
    @discord.ui.button(label="📢 Publish Results", style=discord.ButtonStyle.primary, custom_id="tourney_end_publish")
    async def publish(self, interaction: discord.Interaction, button: discord.ui.Button):
        data = load_tournament_data()
        tournament = data["tournaments"].get(self.tournament_id)
        if not tournament:
            return await interaction.response.send_message("❌ Tournament not found!", ephemeral=True)
        
        await interaction.response.defer()
        
        # Create results embed
        embed = create_results_embed(tournament)
        
        # Add bracket image
        bracket_img = await create_bracket_image(tournament)
        
        if bracket_img:
            file = discord.File(bracket_img, filename="bracket.png")
            embed.set_image(url="attachment://bracket.png")
            await interaction.followup.send(embed=embed, file=file)
        else:
            await interaction.followup.send(embed=embed)
    
    @discord.ui.button(label="🏆 Update Top 10", style=discord.ButtonStyle.success, custom_id="tourney_end_top10")
    async def update_top10(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Update the Top 10 roster with tournament winner"""
        if not is_staff(interaction.user):
            return await interaction.response.send_message("❌ Staff only!", ephemeral=True)
        
        data = load_tournament_data()
        tournament = data["tournaments"].get(self.tournament_id)
        if not tournament:
            return await interaction.response.send_message("❌ Tournament not found!", ephemeral=True)
        
        if not tournament.get("winner"):
            return await interaction.response.send_message("❌ No winner determined yet!", ephemeral=True)
        
        # Show rank selection
        await interaction.response.send_message(
            "Select which rank to place the winner at:",
            view=Top10RankSelectView(self.tournament_id, tournament["winner"]),
            ephemeral=True
        )
    
    @discord.ui.button(label="🎁 Give Rewards", style=discord.ButtonStyle.success, custom_id="tourney_end_rewards")
    async def give_rewards(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Give coin/XP rewards to top 3"""
        if not is_staff(interaction.user):
            return await interaction.response.send_message("❌ Staff only!", ephemeral=True)
        
        data = load_tournament_data()
        tournament = data["tournaments"].get(self.tournament_id)
        if not tournament:
            return await interaction.response.send_message("❌ Tournament not found!", ephemeral=True)
        
        standings = get_standings(tournament)
        if not standings:
            return await interaction.response.send_message("❌ No standings to reward!", ephemeral=True)
        
        await interaction.response.defer(ephemeral=True)
        
        # Reward amounts
        rewards = [
            (5000, 500),   # 1st: 5000 coins, 500 XP
            (2500, 250),   # 2nd: 2500 coins, 250 XP
            (1000, 100),   # 3rd: 1000 coins, 100 XP
        ]
        
        reward_text = "**🎁 Rewards Given:**\n\n"
        
        for i, player in enumerate(standings[:3]):
            if i >= len(rewards):
                break
            
            coins, xp = rewards[i]
            user_id = player["id"]
            
            if user_id == "BYE":
                continue
            
            # Add coins and XP
            add_coins(user_id, coins)
            add_xp(user_id, xp)
            
            medal = ["🥇", "🥈", "🥉"][i]
            reward_text += f"{medal} **{player['name']}**: +{coins:,} coins, +{xp} XP\n"
        
        await interaction.followup.send(reward_text, ephemeral=True)


class Top10RankSelectView(discord.ui.View):
    """Select which rank to place winner at"""
    
    def __init__(self, tournament_id, winner_id):
        super().__init__(timeout=60)
        self.tournament_id = tournament_id
        self.winner_id = winner_id
    
    @discord.ui.select(
        placeholder="Select rank position...",
        options=[
            discord.SelectOption(label=f"Rank {i}", value=str(i), emoji="🏆" if i == 1 else "⚔️")
            for i in range(1, 11)
        ]
    )
    async def rank_select(self, interaction: discord.Interaction, select: discord.ui.Select):
        rank = int(select.values[0])
        
        try:
            # Load current roster
            roster = load_leaderboard()
            
            # Get winner info
            data = load_tournament_data()
            tournament = data["tournaments"].get(self.tournament_id)
            winner_name = "Unknown"
            if tournament:
                winner = next((p for p in tournament.get("participants", []) if p["id"] == self.winner_id), None)
                if winner:
                    winner_name = winner["name"]
            
            # Remove winner if already in roster
            if self.winner_id in roster:
                roster.remove(self.winner_id)
                roster.append(None)  # Keep 10 slots
            
            # Insert at new position (0-indexed)
            roster.insert(rank - 1, self.winner_id)
            
            # Keep only top 10
            roster = roster[:10]
            
            # Save
            save_leaderboard(roster)
            
            await interaction.response.send_message(
                f"✅ **{winner_name}** placed at **Rank {rank}** in Top 10!",
                ephemeral=True
            )
            
        except Exception as e:
            await interaction.response.send_message(f"❌ Error: {e}", ephemeral=True)
        
        self.stop()


# ==========================================
# HELPER FUNCTIONS
# ==========================================

async def create_admin_panel(channel, tournament):
    """Create and send admin panel"""
    embed = create_admin_embed(tournament)
    view = TournamentAdminView(tournament["id"])
    
    msg = await channel.send(embed=embed, view=view)
    
    tournament["messages"]["admin_panel"] = str(msg.id)
    update_tournament(tournament)
    
    return msg


async def update_registration_embed(interaction, tournament):
    """Update the registration portal embed"""
    if not tournament["messages"].get("registration_portal"):
        return
    
    try:
        channel_id = tournament["channels"].get("registration")
        if not channel_id:
            return
        
        channel = interaction.guild.get_channel(int(channel_id))
        if not channel:
            return
        
        msg = await channel.fetch_message(int(tournament["messages"]["registration_portal"]))
        embed = create_registration_embed(tournament)
        await msg.edit(embed=embed)
    except Exception as e:
        print(f"Failed to update registration embed: {e}")


async def create_bracket_display(guild, tournament):
    """Create bracket display in bracket channel or current channel"""
    bracket_img = await create_bracket_image(tournament)
    
    if not bracket_img:
        return
    
    # Find or create bracket channel
    channel_id = tournament["channels"].get("registration")
    if channel_id:
        channel = guild.get_channel(int(channel_id))
        if channel:
            file = discord.File(bracket_img, filename="bracket.png")
            embed = discord.Embed(
                title=f"🏆 {tournament['name']} — Bracket",
                color=0x8B0000
            )
            embed.set_image(url="attachment://bracket.png")
            embed.set_footer(text="✝ THE FALLEN ✝")
            
            msg = await channel.send(embed=embed, file=file)
            tournament["messages"]["bracket_display"] = str(msg.id)
            update_tournament(tournament)


async def update_bracket_display(guild, tournament):
    """Update the bracket display image"""
    if not tournament["messages"].get("bracket_display"):
        return
    
    try:
        channel_id = tournament["channels"].get("registration")
        if not channel_id:
            return
        
        channel = guild.get_channel(int(channel_id))
        if not channel:
            return
        
        msg = await channel.fetch_message(int(tournament["messages"]["bracket_display"]))
        
        bracket_img = await create_bracket_image(tournament)
        if bracket_img:
            file = discord.File(bracket_img, filename="bracket.png")
            embed = discord.Embed(
                title=f"🏆 {tournament['name']} — Bracket",
                color=0x8B0000
            )
            embed.set_image(url="attachment://bracket.png")
            embed.set_footer(text="✝ THE FALLEN ✝")
            
            await msg.edit(embed=embed, attachments=[file])
    except Exception as e:
        print(f"Failed to update bracket display: {e}")


async def create_match_threads(guild, tournament):
    """Create threads for each active match"""
    channel_id = tournament["channels"].get("registration")
    if not channel_id:
        return
    
    channel = guild.get_channel(int(channel_id))
    if not channel:
        return
    
    for match in tournament["matches"]:
        if match["status"] == "pending" and match["player1"] and match["player2"]:
            if match["player1"]["id"] == "BYE" or match["player2"]["id"] == "BYE":
                continue
            
            # Rate limit protection
            await asyncio.sleep(THREAD_CREATE_DELAY)
            
            try:
                thread_name = f"{match['player1']['name']} vs {match['player2']['name']}"
                thread = await channel.create_thread(
                    name=thread_name[:100],
                    type=discord.ChannelType.public_thread,
                    auto_archive_duration=1440  # 24 hours
                )
                
                match["thread_id"] = str(thread.id)
                match["status"] = "in_progress"
                
                # Send match info
                p1 = guild.get_member(int(match["player1"]["id"]))
                p2 = guild.get_member(int(match["player2"]["id"]))
                
                mentions = ""
                if p1:
                    mentions += p1.mention + " "
                if p2:
                    mentions += p2.mention
                
                embed = discord.Embed(
                    title=f"⚔️ {match['player1']['name']} vs {match['player2']['name']}",
                    description=(
                        f"**Match ID:** {match['id']}\n"
                        f"**Best of:** {tournament['best_of']}\n\n"
                        f"**Rules:**\n{tournament['rules']}\n\n"
                        f"**Info:**\n{tournament['info']}"
                    ),
                    color=0x8B0000
                )
                embed.set_footer(text="Click Score when match is complete")
                
                await thread.send(
                    content=f"{mentions}\n\nBegin your match! Click **Score** when complete.",
                    embed=embed,
                    view=MatchScoreView(tournament["id"], match["id"])
                )
                
            except Exception as e:
                print(f"Failed to create match thread: {e}")
    
    update_tournament(tournament)


async def check_tournament_complete(interaction, tournament):
    """Check if all matches are complete and auto-update Top 10"""
    matches = tournament.get("matches", [])
    if not matches:
        return
    
    # Find final match (highest round)
    max_round = max(m["round"] for m in matches)
    final_match = next((m for m in matches if m["round"] == max_round), None)
    
    if final_match and final_match["status"] == "completed":
        # Tournament complete!
        tournament["status"] = "completed"
        tournament["ended_at"] = datetime.datetime.now(datetime.timezone.utc).isoformat()
        tournament["winner"] = final_match["winner"]
        
        update_tournament(tournament)
        
        # Get standings
        standings = get_standings(tournament)
        
        # Get winner name
        winner = standings[0] if standings else None
        winner_name = winner["name"] if winner else "Unknown"
        
        # AUTO-UPDATE TOP 10 with tournament standings
        try:
            roster = load_leaderboard()
            
            # Get top 10 from tournament (excluding BYEs)
            top_10_players = [p for p in standings if p["id"] != "BYE"][:10]
            
            # Create new roster with tournament results
            new_roster = []
            for player in top_10_players:
                new_roster.append(player["id"])
            
            # Pad with None if less than 10 players
            while len(new_roster) < 10:
                new_roster.append(None)
            
            # Save the new roster
            save_leaderboard(new_roster)
            
            top10_updated = True
        except Exception as e:
            print(f"Failed to update Top 10: {e}")
            top10_updated = False
        
        # Give rewards to top 3
        rewards_given = []
        reward_amounts = [
            (5000, 500),   # 1st: 5000 coins, 500 XP
            (2500, 250),   # 2nd: 2500 coins, 250 XP
            (1000, 100),   # 3rd: 1000 coins, 100 XP
        ]
        
        for i, player in enumerate(standings[:3]):
            if player["id"] == "BYE":
                continue
            if i < len(reward_amounts):
                coins, xp = reward_amounts[i]
                try:
                    add_coins(player["id"], coins)
                    add_xp(player["id"], xp)
                    rewards_given.append(f"{['🥇', '🥈', '🥉'][i]} **{player['name']}**: +{coins:,} coins, +{xp} XP")
                except:
                    pass
        
        # Send completion message
        embed = discord.Embed(
            title="🏆 Tournament Complete!",
            description=f"**{tournament['name']}** has ended!\n\n🥇 **Champion:** {winner_name}",
            color=0xFFD700
        )
        
        # Add standings
        standings_text = ""
        medals = ["🥇", "🥈", "🥉", "4️⃣", "5️⃣", "6️⃣", "7️⃣", "8️⃣", "9️⃣", "🔟"]
        for i, player in enumerate(standings[:10]):
            if player["id"] == "BYE":
                continue
            medal = medals[i] if i < len(medals) else f"{i+1}."
            standings_text += f"{medal} {player['name']}\n"
        
        if standings_text:
            embed.add_field(name="📊 Final Standings", value=standings_text, inline=False)
        
        # Add rewards info
        if rewards_given:
            embed.add_field(name="🎁 Rewards", value="\n".join(rewards_given), inline=False)
        
        # Add Top 10 update status
        if top10_updated:
            embed.add_field(name="🏆 Top 10 Roster", value="✅ Updated with tournament results!", inline=False)
        
        embed.set_footer(text="✝ THE FALLEN ✝")
        
        channel_id = tournament["channels"].get("registration")
        if channel_id:
            channel = interaction.guild.get_channel(int(channel_id))
            if channel:
                await channel.send(embed=embed, view=TournamentEndView(tournament["id"]))
        
        # UPDATE VISUAL LEADERBOARD IMAGE
        if top10_updated:
            try:
                full_data = load_data()
                lb_info = full_data.get("leaderboard_message", {})
                
                if lb_info.get("channel_id") and lb_info.get("message_id"):
                    img_buffer = await create_top10_leaderboard_image(interaction.guild)
                    if img_buffer:
                        lb_channel = interaction.guild.get_channel(int(lb_info["channel_id"]))
                        if lb_channel:
                            lb_msg = await lb_channel.fetch_message(int(lb_info["message_id"]))
                            file = discord.File(img_buffer, filename="leaderboard.png")
                            await lb_msg.edit(attachments=[file])
                            print("✅ Visual leaderboard image updated!")
            except Exception as e:
                print(f"Failed to update visual leaderboard: {e}")



# ==========================================
# ==========================================
# TOURNAMENT PREFIX COMMANDS
# ==========================================

@bot.command(name="bracket")
async def bracket_prefix_cmd(ctx):
    """
    View the current tournament bracket
    Usage: !bracket
    """
    tournament = get_active_tournament()
    if not tournament:
        return await ctx.send("❌ No active tournament!")
    
    async with ctx.typing():
        bracket_img = await create_bracket_image(tournament)
        if bracket_img:
            file = discord.File(bracket_img, filename="bracket.png")
            embed = discord.Embed(
                title=f"🏆 {tournament['name']} — Bracket",
                color=0x8B0000
            )
            embed.set_image(url="attachment://bracket.png")
            embed.set_footer(text="✝ THE FALLEN ✝")
            await ctx.send(embed=embed, file=file)
        else:
            await ctx.send("❌ Could not generate bracket image.")


@bot.command(name="tparticipants", aliases=["tplayers"])
async def tournament_participants_cmd(ctx):
    """
    View tournament participants
    Usage: !tparticipants
    """
    tournament = get_active_tournament()
    if not tournament:
        return await ctx.send("❌ No active tournament!")
    
    participants = tournament.get("participants", [])
    
    embed = discord.Embed(
        title=f"👥 {tournament['name']} — Participants",
        description=f"**{len(participants)}/{tournament['max_participants']}** registered",
        color=0x8B0000
    )
    
    if participants:
        player_list = "\n".join([f"`{i+1}.` {p['name']}" for i, p in enumerate(participants[:25])])
        if len(participants) > 25:
            player_list += f"\n*... and {len(participants) - 25} more*"
        embed.add_field(name="Players", value=player_list, inline=False)
    else:
        embed.add_field(name="Players", value="No participants yet.", inline=False)
    
    embed.set_footer(text="✝ THE FALLEN ✝")
    await ctx.send(embed=embed)


@bot.command(name="tsetwinner")
async def tournament_set_winner_cmd(ctx, member: discord.Member):
    """
    Manually set tournament winner (staff only)
    Usage: !tsetwinner @user
    """
    if not is_staff(ctx.author):
        return await ctx.send("❌ Staff only!", delete_after=5)
    
    tournament = get_active_tournament()
    if not tournament:
        return await ctx.send("❌ No active tournament!")
    
    # Check if member is in tournament
    participant = next((p for p in tournament.get("participants", []) if p["id"] == str(member.id)), None)
    if not participant:
        return await ctx.send(f"❌ {member.display_name} is not in the tournament!")
    
    tournament["winner"] = str(member.id)
    tournament["status"] = "completed"
    tournament["ended_at"] = datetime.datetime.now(datetime.timezone.utc).isoformat()
    update_tournament(tournament)
    
    await ctx.send(f"✅ **{member.display_name}** set as tournament winner!")


@bot.command(name="tendtournament", aliases=["endt"])
async def tournament_end_cmd(ctx):
    """
    End current tournament (staff only)
    Usage: !tendtournament
    """
    if not is_staff(ctx.author):
        return await ctx.send("❌ Staff only!", delete_after=5)
    
    tournament = get_active_tournament()
    if not tournament:
        return await ctx.send("❌ No active tournament!")
    
    tournament["status"] = "completed"
    tournament["ended_at"] = datetime.datetime.now(datetime.timezone.utc).isoformat()
    
    # Get standings
    standings = get_standings(tournament)
    
    # Set winner if we have standings
    if standings and standings[0]["id"] != "BYE":
        tournament["winner"] = standings[0]["id"]
    
    update_tournament(tournament)
    
    # AUTO-UPDATE TOP 10
    top10_updated = False
    try:
        roster = load_leaderboard()
        top_10_players = [p for p in standings if p["id"] != "BYE"][:10]
        new_roster = [p["id"] for p in top_10_players]
        while len(new_roster) < 10:
            new_roster.append(None)
        save_leaderboard(new_roster)
        top10_updated = True
    except Exception as e:
        print(f"Failed to update Top 10: {e}")
    
    # GIVE REWARDS
    rewards_given = []
    reward_amounts = [(5000, 500), (2500, 250), (1000, 100)]
    
    for i, player in enumerate(standings[:3]):
        if player["id"] == "BYE":
            continue
        if i < len(reward_amounts):
            coins, xp = reward_amounts[i]
            try:
                add_coins(player["id"], coins)
                add_xp(player["id"], xp)
                rewards_given.append(f"{['🥇', '🥈', '🥉'][i]} **{player['name']}**: +{coins:,} coins, +{xp} XP")
            except:
                pass
    
    # Create end embed
    embed = discord.Embed(
        title=f"🏆 {tournament['name']} — Complete!",
        description="Tournament has been ended.",
        color=0xFFD700
    )
    
    if standings:
        standings_text = ""
        medals = ["🥇", "🥈", "🥉", "4️⃣", "5️⃣", "6️⃣", "7️⃣", "8️⃣", "9️⃣", "🔟"]
        for i, player in enumerate(standings[:10]):
            if player["id"] == "BYE":
                continue
            medal = medals[i] if i < len(medals) else f"{i+1}."
            standings_text += f"{medal} {player['name']}\n"
        embed.add_field(name="📊 Final Standings", value=standings_text or "No standings", inline=False)
    
    if rewards_given:
        embed.add_field(name="🎁 Rewards Given", value="\n".join(rewards_given), inline=False)
    
    if top10_updated:
        embed.add_field(name="🏆 Top 10 Roster", value="✅ Updated!", inline=False)
    
    embed.set_footer(text="✝ THE FALLEN ✝")
    
    await ctx.send(embed=embed, view=TournamentEndView(tournament["id"]))
    
    # UPDATE VISUAL LEADERBOARD IMAGE
    if top10_updated:
        try:
            full_data = load_data()
            lb_info = full_data.get("leaderboard_message", {})
            
            if lb_info.get("channel_id") and lb_info.get("message_id"):
                img_buffer = await create_top10_leaderboard_image(ctx.guild)
                if img_buffer:
                    lb_channel = ctx.guild.get_channel(int(lb_info["channel_id"]))
                    if lb_channel:
                        lb_msg = await lb_channel.fetch_message(int(lb_info["message_id"]))
                        file = discord.File(img_buffer, filename="leaderboard.png")
                        await lb_msg.edit(attachments=[file])
                        await ctx.send("✅ Visual leaderboard image also updated!", delete_after=5)
        except Exception as e:
            print(f"Failed to update visual leaderboard: {e}")


@bot.command(name="tdeletetournament", aliases=["tdel"])
async def tournament_delete_cmd(ctx):
    """
    Delete current tournament (staff only)
    Usage: !tdeletetournament
    """
    if not is_staff(ctx.author):
        return await ctx.send("❌ Staff only!", delete_after=5)
    
    tournament = get_active_tournament()
    if not tournament:
        return await ctx.send("❌ No active tournament!")
    
    data = load_tournament_data()
    tournament_name = tournament.get("name", "Unknown")
    
    if tournament["id"] in data["tournaments"]:
        del data["tournaments"][tournament["id"]]
    data["active_tournament"] = None
    save_tournament_data(data)
    
    await ctx.send(f"✅ Tournament **{tournament_name}** deleted!")


@bot.command(name="tstatus")
async def tournament_status_cmd(ctx):
    """
    View current tournament status
    Usage: !tstatus
    """
    tournament = get_active_tournament()
    if not tournament:
        return await ctx.send("❌ No active tournament!")
    
    embed = discord.Embed(
        title=f"📊 {tournament['name']} — Status",
        color=0x8B0000
    )
    
    embed.add_field(name="Status", value=tournament["status"].title(), inline=True)
    embed.add_field(name="Participants", value=f"{len(tournament['participants'])}/{tournament['max_participants']}", inline=True)
    embed.add_field(name="Best Of", value=str(tournament.get("best_of", 1)), inline=True)
    
    if tournament.get("required_role_name"):
        embed.add_field(name="Required Role", value=tournament["required_role_name"], inline=True)
    
    # Count matches
    matches = tournament.get("matches", [])
    completed = len([m for m in matches if m["status"] == "completed"])
    total = len([m for m in matches if m.get("player1") and m.get("player2") and m["player1"]["id"] != "BYE" and m["player2"]["id"] != "BYE"])
    
    if matches:
        embed.add_field(name="Matches", value=f"{completed}/{total} completed", inline=True)
    
    if tournament.get("winner"):
        winner = next((p for p in tournament["participants"] if p["id"] == tournament["winner"]), None)
        if winner:
            embed.add_field(name="🏆 Winner", value=winner["name"], inline=True)
    
    embed.set_footer(text="✝ THE FALLEN ✝")
    await ctx.send(embed=embed)


# TOURNAMENT PREFIX COMMAND
# ==========================================

@bot.command(name="tournament")
async def tournament_prefix_cmd(ctx):
    """
    Create a new tournament (opens modal via button)
    Usage: !tournament
    """
    if not is_staff(ctx.author):
        return await ctx.send("❌ Staff only!", delete_after=5)
    
    tournament = get_active_tournament()
    if tournament and tournament["status"] not in ["completed"]:
        return await ctx.send(
            f"❌ Active tournament exists: **{tournament['name']}**\n"
            f"Delete it first using the admin panel.",
            delete_after=10
        )
    
    embed = discord.Embed(
        title="🏆 Create Tournament",
        description="Click the button below to create a new tournament.",
        color=0x8B0000
    )
    embed.set_footer(text="✝ THE FALLEN ✝")
    
    view = TournamentCreateButtonView()
    await ctx.send(embed=embed, view=view)
    
    try:
        await ctx.message.delete()
    except:
        pass


class TournamentCreateButtonView(discord.ui.View):
    """Button to open tournament create modal"""
    def __init__(self):
        super().__init__(timeout=300)
    
    @discord.ui.button(label="➕ Create Tournament", style=discord.ButtonStyle.success)
    async def create_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not is_staff(interaction.user):
            return await interaction.response.send_message("❌ Staff only!", ephemeral=True)
        
        await interaction.response.send_modal(TournamentCreateModal())


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
                if e.status == 429:
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
    
    try:
        asyncio.run(run_bot())
    except KeyboardInterrupt:
        print("Bot stopped by user.")
