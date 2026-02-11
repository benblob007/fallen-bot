"""
✝ THE FALLEN ✝ - Centralized Configuration
All bot settings in one place. Edit this file instead of digging through main.py.
"""

import os

# --- CORE ---
TOKEN = os.getenv("DISCORD_TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL")
BOT_PREFIX = "!"

# --- ROLE NAMES ---
STAFF_ROLE_NAME = "Staff"
UNVERIFIED_ROLE_NAME = "Unverified"
VERIFIED_ROLE_NAME = "Verified"
MEMBER_ROLE_NAME = "Abyssbound"
BLOXLINK_VERIFIED_ROLE = "Bloxlink Verified"
FALLEN_VERIFIED_ROLE = "Fallen Verified"
BOOSTER_ROLE_NAME = "Fallen Ascendant"
COACHING_ROLE = "Coach"
TRYOUT_HOST_ROLE = "The Abyssal Overseer〢Tryout Host"

HIGH_STAFF_ROLES = [
    "The Fallen Sovereign〢Owner",
    "The Fallen Right Hand〢Co-Owner",
    "The Fallen Marshal〢Head of Staff"
]

REQUIRED_APP_ROLES = ["Stage 2〢FALLEN ASCENDANT", "High", "Stable"]

# --- CHANNEL NAMES ---
ANNOUNCEMENT_CHANNEL_NAME = "♰・set-annc"
LOG_CHANNEL_NAME = "fallen-logs"
SET_RESULTS_CHANNEL_NAME = "♰・set-score"
TOURNAMENT_RESULTS_CHANNEL_NAME = "╰・tournament-results"
LEVEL_UP_CHANNEL_NAME = "♰・level"
SHOP_CHANNEL_NAME = "♰・fallen-shop"
WELCOME_CHANNEL_NAME = "╰・welcome"

# NEW - Raid/War channels
RAID_LOG_CHANNEL_NAME = "♰・raid-log"
RAID_RESULTS_CHANNEL_NAME = "♰・raid-results"
WAR_ROOM_CHANNEL_NAME = "♰・war-room"
RECRUITMENT_CHANNEL_NAME = "♰・recruitment"

# --- DATA FILES ---
LEADERBOARD_FILE = "leaderboard.json"
RECURRING_EVENTS_FILE = "recurring_events.json"
TRANSCRIPTS_FILE = "ticket_transcripts.json"
PRACTICE_FILE = "practice_sessions.json"
LEGACY_FILE = "legacy_data.json"
EMBEDS_FILE = "custom_embeds.json"
POLLS_FILE = "polls_data.json"
DUELS_FILE = "duels_data.json"
EVENTS_FILE = "events_data.json"
TOURNAMENTS_FILE = "tournaments.json"
WARNINGS_FILE = "warnings_data.json"
INACTIVITY_FILE = "inactivity_data.json"
RAID_HISTORY_FILE = "raid_history.json"

# NEW data files
RAIDS_FILE = "raids_data.json"
RECRUITMENT_FILE = "recruitment_data.json"

# --- IMAGE FILES ---
LEVEL_CARD_FILE = "LevelFcard.png"
LEADERBOARD_BG_FILE = "leaderboard_bg.png"

LEVEL_CARD_PATHS = [
    "LevelFcard.png", "./LevelFcard.png", "levelcard.png", "./levelcard.png",
    "/opt/render/project/src/LevelFcard.png", "/opt/render/project/src/levelcard.png",
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "LevelFcard.png"),
]

WELCOME_CARD_PATHS = [
    "WelcomeCard.png", "./WelcomeCard.png",
    "/opt/render/project/src/WelcomeCard.png",
]

PROFILE_CARD_PATHS = [
    "ProfileCard.png", "./ProfileCard.png",
    "/opt/render/project/src/ProfileCard.png",
]

# --- XP & LEVELING ---
XP_TEXT_RANGE = (5, 15)
XP_VOICE_RANGE = (15, 30)
XP_REACTION_RANGE = (2, 8)
XP_MESSAGE_COOLDOWN = 60
XP_REACTION_COOLDOWN = 30

# --- ECONOMY ---
MAX_COINS = 1000000
BOOSTER_XP_MULTIPLIER = 1.25
BOOSTER_DAILY_MULTIPLIER = 2
BOOSTER_WEEKLY_COINS = 500
BOOSTER_WEEKLY_XP = 250

# --- RATE LIMITING ---
API_CALL_DELAY = 0.5
BULK_OPERATION_DELAY = 1.0
MAX_BULK_ACTIONS_PER_MINUTE = 30

# --- LEVELING MILESTONES ---
LEVEL_CONFIG = {
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

# --- RAID SYSTEM CONFIG ---
RAID_TYPES = {
    "standard": {"name": "Standard Raid", "emoji": "⚔️", "min_players": 4, "xp_reward": 150},
    "mega": {"name": "Mega Raid", "emoji": "💀", "min_players": 8, "xp_reward": 300},
    "war": {"name": "Clan War", "emoji": "🏴", "min_players": 6, "xp_reward": 500},
    "defense": {"name": "Defense Raid", "emoji": "🛡️", "min_players": 4, "xp_reward": 200},
    "scrimmage": {"name": "Scrimmage", "emoji": "🤝", "min_players": 3, "xp_reward": 100},
}

RAID_RANKS = {
    0: {"name": "Unranked Raider", "emoji": "⬛"},
    5: {"name": "Shadow Scout", "emoji": "🔰"},
    15: {"name": "Abyssal Striker", "emoji": "⚔️"},
    30: {"name": "Voidborne Vanguard", "emoji": "🛡️"},
    50: {"name": "Fallen Warmaster", "emoji": "💀"},
    75: {"name": "Dread Commander", "emoji": "👑"},
    100: {"name": "Eternal War Sovereign", "emoji": "🌌"},
}

# --- RECRUITMENT CONFIG ---
RECRUITMENT_POSITIONS = {
    "war_manager": {
        "name": "War Manager",
        "emoji": "🏴",
        "description": "Coordinate and lead clan wars, manage war strategies and rosters.",
        "requirements": ["Must be Stage 2+", "Active raid participation (30+ raids)", "Leadership experience"],
        "review_role": "The Fallen Marshal〢Head of Staff",
    },
    "tryout_host": {
        "name": "Tryout Host",
        "emoji": "⚔️",
        "description": "Host and evaluate tryouts for new members joining the clan.",
        "requirements": ["Must be Stage 2+", "Good communication skills", "Available 3+ days/week"],
        "review_role": "The Fallen Marshal〢Head of Staff",
    },
    "training_host": {
        "name": "Training Host",
        "emoji": "🏋️",
        "description": "Lead training sessions to improve member skills and coordination.",
        "requirements": ["Must be Stage 2+", "Strong game knowledge", "Patient and helpful"],
        "review_role": "The Fallen Marshal〢Head of Staff",
    },
    "raid_leader": {
        "name": "Raid Leader",
        "emoji": "💀",
        "description": "Lead raid parties, coordinate strategies, and manage raid schedules.",
        "requirements": ["50+ raid participations", "Proven raid performance", "Available for scheduled raids"],
        "review_role": "The Abyssal Raid Marshal",
    },
    "recruiter": {
        "name": "Recruiter",
        "emoji": "📢",
        "description": "Find and recruit talented players to join The Fallen.",
        "requirements": ["Good communication", "Active in gaming communities", "Knowledge of clan values"],
        "review_role": "Staff",
    },
    "content_creator": {
        "name": "Content Creator",
        "emoji": "🎬",
        "description": "Create content featuring The Fallen — clips, montages, streams.",
        "requirements": ["Portfolio/examples required", "Consistent output", "Represents clan values"],
        "review_role": "Staff",
    },
}

# Pipeline stages for recruitment
RECRUITMENT_STAGES = ["applied", "under_review", "interview", "trial", "accepted", "denied"]
RECRUITMENT_STAGE_EMOJIS = {
    "applied": "📋",
    "under_review": "🔍",
    "interview": "🎤",
    "trial": "⚔️",
    "accepted": "✅",
    "denied": "❌",
}

# --- DEFAULT THEME ---
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
