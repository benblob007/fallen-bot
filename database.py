"""
✝ THE FALLEN ✝ - Database Module
Full PostgreSQL database with proper tables, connection pooling, and JSON fallback.
Replaces the scattered JSON file approach with a unified data layer.

Usage:
    from database import db
    await db.init()
    user = await db.get_user(user_id)
    await db.update_user(user_id, xp=100, coins=50)
"""

import json
import os
import asyncio
import datetime
from typing import Optional, Dict, Any, List

try:
    import asyncpg
    POSTGRES_AVAILABLE = True
except ImportError:
    POSTGRES_AVAILABLE = False
    print("⚠️ asyncpg not installed - using JSON storage. Install with: pip install asyncpg")


class Database:
    """Unified database layer for The Fallen bot."""
    
    def __init__(self):
        self.pool: Optional[asyncpg.Pool] = None
        self.using_postgres = False
        self._cache = {}
        self._cache_times = {}
        self.CACHE_TTL = 5  # seconds
    
    # =========================================================
    # INITIALIZATION & CONNECTION
    # =========================================================
    
    async def init(self, database_url: str = None) -> bool:
        """Initialize database connection. Returns True if PostgreSQL connected."""
        url = database_url or os.getenv("DATABASE_URL")
        
        if not POSTGRES_AVAILABLE or not url:
            print("📁 Using JSON file storage (PostgreSQL not configured)")
            return False
        
        try:
            self.pool = await asyncpg.create_pool(
                url, min_size=2, max_size=15,
                command_timeout=30,
                server_settings={'application_name': 'TheFallenBot'}
            )
            
            # Run migrations
            await self._run_migrations()
            
            self.using_postgres = True
            print("✅ PostgreSQL database connected and initialized!")
            return True
            
        except Exception as e:
            print(f"❌ PostgreSQL connection failed: {e}")
            print("📁 Falling back to JSON file storage")
            self.pool = None
            return False
    
    async def close(self):
        """Close database pool."""
        if self.pool:
            await self.pool.close()
    
    # =========================================================
    # SCHEMA MIGRATIONS
    # =========================================================
    
    async def _run_migrations(self):
        """Run all database migrations in order."""
        async with self.pool.acquire() as conn:
            # Migration tracking table
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS _migrations (
                    id SERIAL PRIMARY KEY,
                    name TEXT UNIQUE NOT NULL,
                    applied_at TIMESTAMP DEFAULT NOW()
                )
            ''')
            
            migrations = [
                ("001_core_tables", self._migration_001_core),
                ("002_raid_system", self._migration_002_raids),
                ("003_recruitment_system", self._migration_003_recruitment),
                ("004_json_backup", self._migration_004_json_backup),
                ("005_indexes", self._migration_005_indexes),
            ]
            
            for name, func in migrations:
                applied = await conn.fetchval(
                    "SELECT 1 FROM _migrations WHERE name = $1", name
                )
                if not applied:
                    print(f"  Running migration: {name}...")
                    await func(conn)
                    await conn.execute(
                        "INSERT INTO _migrations (name) VALUES ($1)", name
                    )
                    print(f"  ✅ {name} applied!")
    
    async def _migration_001_core(self, conn):
        """Core tables - users, settings, etc."""
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
                elo_rating INTEGER DEFAULT 1000,
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
        
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY,
                value JSONB NOT NULL,
                updated_at TIMESTAMP DEFAULT NOW()
            )
        ''')
        
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS roster (
                position INTEGER PRIMARY KEY,
                user_id BIGINT,
                roblox_name TEXT,
                updated_at TIMESTAMP DEFAULT NOW()
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
                created_by BIGINT,
                created_at TIMESTAMP DEFAULT NOW(),
                completed_at TIMESTAMP
            )
        ''')
        
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS warnings (
                id SERIAL PRIMARY KEY,
                user_id BIGINT NOT NULL,
                moderator_id BIGINT NOT NULL,
                reason TEXT NOT NULL,
                category TEXT DEFAULT 'general',
                severity TEXT DEFAULT 'warning',
                created_at TIMESTAMP DEFAULT NOW(),
                expires_at TIMESTAMP,
                active BOOLEAN DEFAULT TRUE
            )
        ''')
        
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS tickets (
                id SERIAL PRIMARY KEY,
                channel_id BIGINT UNIQUE,
                user_id BIGINT NOT NULL,
                ticket_type TEXT DEFAULT 'support',
                status TEXT DEFAULT 'open',
                created_at TIMESTAMP DEFAULT NOW(),
                closed_at TIMESTAMP,
                closed_by BIGINT,
                transcript TEXT
            )
        ''')
        
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS duels (
                id SERIAL PRIMARY KEY,
                challenger_id BIGINT NOT NULL,
                opponent_id BIGINT NOT NULL,
                winner_id BIGINT,
                challenger_elo_change INTEGER DEFAULT 0,
                opponent_elo_change INTEGER DEFAULT 0,
                status TEXT DEFAULT 'pending',
                created_at TIMESTAMP DEFAULT NOW(),
                completed_at TIMESTAMP
            )
        ''')
        
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS events (
                id TEXT PRIMARY KEY,
                title TEXT NOT NULL,
                event_type TEXT NOT NULL,
                description TEXT,
                host_id BIGINT,
                channel_id BIGINT,
                message_id BIGINT,
                scheduled_time TIMESTAMP,
                attendees BIGINT[] DEFAULT ARRAY[]::BIGINT[],
                status TEXT DEFAULT 'scheduled',
                created_at TIMESTAMP DEFAULT NOW()
            )
        ''')
        
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS giveaways (
                id SERIAL PRIMARY KEY,
                channel_id BIGINT,
                message_id BIGINT,
                prize TEXT NOT NULL,
                host_id BIGINT NOT NULL,
                entries BIGINT[] DEFAULT ARRAY[]::BIGINT[],
                winner_ids BIGINT[] DEFAULT ARRAY[]::BIGINT[],
                max_winners INTEGER DEFAULT 1,
                ends_at TIMESTAMP NOT NULL,
                status TEXT DEFAULT 'active',
                created_at TIMESTAMP DEFAULT NOW()
            )
        ''')
    
    async def _migration_002_raids(self, conn):
        """Raid and War tracking system tables."""
        
        # Raid sessions - each raid event
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS raid_sessions (
                id SERIAL PRIMARY KEY,
                raid_type TEXT NOT NULL DEFAULT 'standard',
                target_clan TEXT,
                leader_id BIGINT NOT NULL,
                result TEXT,
                our_score INTEGER DEFAULT 0,
                their_score INTEGER DEFAULT 0,
                participants BIGINT[] DEFAULT ARRAY[]::BIGINT[],
                mvp_id BIGINT,
                xp_awarded INTEGER DEFAULT 0,
                coins_awarded INTEGER DEFAULT 0,
                notes TEXT,
                channel_id BIGINT,
                message_id BIGINT,
                started_at TIMESTAMP DEFAULT NOW(),
                completed_at TIMESTAMP,
                status TEXT DEFAULT 'active'
            )
        ''')
        
        # Individual raid performance per member per raid
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS raid_performance (
                id SERIAL PRIMARY KEY,
                raid_id INTEGER REFERENCES raid_sessions(id) ON DELETE CASCADE,
                user_id BIGINT NOT NULL,
                kills INTEGER DEFAULT 0,
                deaths INTEGER DEFAULT 0,
                damage_dealt INTEGER DEFAULT 0,
                objectives_completed INTEGER DEFAULT 0,
                rating TEXT DEFAULT 'average',
                xp_earned INTEGER DEFAULT 0,
                coins_earned INTEGER DEFAULT 0,
                UNIQUE(raid_id, user_id)
            )
        ''')
        
        # War declarations and tracking
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS wars (
                id SERIAL PRIMARY KEY,
                enemy_clan TEXT NOT NULL,
                status TEXT DEFAULT 'declared',
                our_wins INTEGER DEFAULT 0,
                our_losses INTEGER DEFAULT 0,
                best_of INTEGER DEFAULT 3,
                declared_by BIGINT,
                declared_at TIMESTAMP DEFAULT NOW(),
                completed_at TIMESTAMP,
                result TEXT,
                notes TEXT
            )
        ''')
        
        # War matches (individual battles within a war)
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS war_matches (
                id SERIAL PRIMARY KEY,
                war_id INTEGER REFERENCES wars(id) ON DELETE CASCADE,
                match_number INTEGER NOT NULL,
                raid_id INTEGER REFERENCES raid_sessions(id),
                our_score INTEGER DEFAULT 0,
                their_score INTEGER DEFAULT 0,
                result TEXT,
                played_at TIMESTAMP DEFAULT NOW()
            )
        ''')
        
        # Raid stats aggregation per user (cached for performance)
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS raid_stats (
                user_id BIGINT PRIMARY KEY,
                total_raids INTEGER DEFAULT 0,
                raids_won INTEGER DEFAULT 0,
                raids_lost INTEGER DEFAULT 0,
                total_kills INTEGER DEFAULT 0,
                total_deaths INTEGER DEFAULT 0,
                total_damage INTEGER DEFAULT 0,
                total_objectives INTEGER DEFAULT 0,
                mvp_count INTEGER DEFAULT 0,
                current_streak INTEGER DEFAULT 0,
                best_streak INTEGER DEFAULT 0,
                raid_xp_earned INTEGER DEFAULT 0,
                raid_rank TEXT DEFAULT 'Unranked Raider',
                last_raid_at TIMESTAMP,
                updated_at TIMESTAMP DEFAULT NOW()
            )
        ''')
    
    async def _migration_003_recruitment(self, conn):
        """Recruitment pipeline system tables."""
        
        # Open positions
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS recruitment_positions (
                id SERIAL PRIMARY KEY,
                position_key TEXT NOT NULL,
                title TEXT NOT NULL,
                description TEXT,
                requirements TEXT[],
                slots_available INTEGER DEFAULT 1,
                slots_filled INTEGER DEFAULT 0,
                posted_by BIGINT NOT NULL,
                review_role TEXT,
                channel_id BIGINT,
                message_id BIGINT,
                status TEXT DEFAULT 'open',
                created_at TIMESTAMP DEFAULT NOW(),
                closed_at TIMESTAMP
            )
        ''')
        
        # Applications for positions
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS recruitment_applications (
                id SERIAL PRIMARY KEY,
                position_id INTEGER REFERENCES recruitment_positions(id) ON DELETE CASCADE,
                user_id BIGINT NOT NULL,
                answers JSONB DEFAULT '{}',
                stage TEXT DEFAULT 'applied',
                reviewer_id BIGINT,
                review_notes TEXT,
                interview_time TIMESTAMP,
                trial_start TIMESTAMP,
                trial_end TIMESTAMP,
                trial_notes TEXT,
                decision_reason TEXT,
                channel_id BIGINT,
                created_at TIMESTAMP DEFAULT NOW(),
                updated_at TIMESTAMP DEFAULT NOW(),
                UNIQUE(position_id, user_id)
            )
        ''')
        
        # Recruitment activity log
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS recruitment_log (
                id SERIAL PRIMARY KEY,
                application_id INTEGER REFERENCES recruitment_applications(id) ON DELETE CASCADE,
                action TEXT NOT NULL,
                performed_by BIGINT NOT NULL,
                details TEXT,
                created_at TIMESTAMP DEFAULT NOW()
            )
        ''')
    
    async def _migration_004_json_backup(self, conn):
        """JSON backup tables for legacy data compatibility."""
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS json_data (
                key TEXT PRIMARY KEY,
                data JSONB NOT NULL,
                updated_at TIMESTAMP DEFAULT NOW()
            )
        ''')
    
    async def _migration_005_indexes(self, conn):
        """Performance indexes."""
        indexes = [
            "CREATE INDEX IF NOT EXISTS idx_users_xp ON users (xp DESC)",
            "CREATE INDEX IF NOT EXISTS idx_users_level ON users (level DESC)",
            "CREATE INDEX IF NOT EXISTS idx_users_coins ON users (coins DESC)",
            "CREATE INDEX IF NOT EXISTS idx_users_last_active ON users (last_active)",
            "CREATE INDEX IF NOT EXISTS idx_users_verified ON users (verified)",
            "CREATE INDEX IF NOT EXISTS idx_raid_sessions_status ON raid_sessions (status)",
            "CREATE INDEX IF NOT EXISTS idx_raid_sessions_leader ON raid_sessions (leader_id)",
            "CREATE INDEX IF NOT EXISTS idx_raid_sessions_started ON raid_sessions (started_at DESC)",
            "CREATE INDEX IF NOT EXISTS idx_raid_performance_user ON raid_performance (user_id)",
            "CREATE INDEX IF NOT EXISTS idx_raid_performance_raid ON raid_performance (raid_id)",
            "CREATE INDEX IF NOT EXISTS idx_raid_stats_raids ON raid_stats (total_raids DESC)",
            "CREATE INDEX IF NOT EXISTS idx_wars_status ON wars (status)",
            "CREATE INDEX IF NOT EXISTS idx_warnings_user ON warnings (user_id)",
            "CREATE INDEX IF NOT EXISTS idx_warnings_active ON warnings (active)",
            "CREATE INDEX IF NOT EXISTS idx_tickets_user ON tickets (user_id)",
            "CREATE INDEX IF NOT EXISTS idx_tickets_status ON tickets (status)",
            "CREATE INDEX IF NOT EXISTS idx_duels_challenger ON duels (challenger_id)",
            "CREATE INDEX IF NOT EXISTS idx_duels_opponent ON duels (opponent_id)",
            "CREATE INDEX IF NOT EXISTS idx_events_status ON events (status)",
            "CREATE INDEX IF NOT EXISTS idx_recruitment_positions_status ON recruitment_positions (status)",
            "CREATE INDEX IF NOT EXISTS idx_recruitment_apps_user ON recruitment_applications (user_id)",
            "CREATE INDEX IF NOT EXISTS idx_recruitment_apps_stage ON recruitment_applications (stage)",
        ]
        for idx in indexes:
            try:
                await conn.execute(idx)
            except Exception:
                pass
    
    # =========================================================
    # USER OPERATIONS
    # =========================================================
    
    async def get_user(self, user_id: int) -> Optional[Dict]:
        """Get a user's data. Returns None if not found."""
        if not self.pool:
            return self._json_get_user(user_id)
        
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow("SELECT * FROM users WHERE user_id = $1", user_id)
            return dict(row) if row else None
    
    async def ensure_user(self, user_id: int) -> Dict:
        """Get user data, creating with defaults if they don't exist."""
        if not self.pool:
            return self._json_ensure_user(user_id)
        
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow("SELECT * FROM users WHERE user_id = $1", user_id)
            if row:
                return dict(row)
            
            # Create with defaults
            await conn.execute(
                "INSERT INTO users (user_id) VALUES ($1) ON CONFLICT DO NOTHING", user_id
            )
            row = await conn.fetchrow("SELECT * FROM users WHERE user_id = $1", user_id)
            return dict(row)
    
    async def update_user(self, user_id: int, **kwargs):
        """Update specific fields on a user. Usage: await db.update_user(id, xp=100, coins=50)"""
        if not self.pool:
            return self._json_update_user(user_id, **kwargs)
        
        if not kwargs:
            return
        
        set_clauses = []
        values = [user_id]
        for i, (key, value) in enumerate(kwargs.items(), start=2):
            set_clauses.append(f"{key} = ${i}")
            values.append(value)
        
        query = f"UPDATE users SET {', '.join(set_clauses)} WHERE user_id = $1"
        
        async with self.pool.acquire() as conn:
            await conn.execute(query, *values)
    
    async def increment_user(self, user_id: int, **kwargs):
        """Increment numeric fields. Usage: await db.increment_user(id, xp=15, coins=5)"""
        if not self.pool:
            return self._json_increment_user(user_id, **kwargs)
        
        if not kwargs:
            return
        
        set_clauses = []
        values = [user_id]
        for i, (key, value) in enumerate(kwargs.items(), start=2):
            set_clauses.append(f"{key} = COALESCE({key}, 0) + ${i}")
            values.append(value)
        
        # Ensure user exists first
        async with self.pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO users (user_id) VALUES ($1) ON CONFLICT DO NOTHING", user_id
            )
            query = f"UPDATE users SET {', '.join(set_clauses)} WHERE user_id = $1"
            await conn.execute(query, *values)
    
    async def get_leaderboard(self, field: str = "xp", limit: int = 10) -> List[Dict]:
        """Get top users sorted by a field."""
        if not self.pool:
            return self._json_get_leaderboard(field, limit)
        
        allowed_fields = ["xp", "level", "coins", "raid_participation", "wins", "elo_rating", "messages", "voice_time"]
        if field not in allowed_fields:
            field = "xp"
        
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                f"SELECT * FROM users ORDER BY {field} DESC NULLS LAST LIMIT $1", limit
            )
            return [dict(r) for r in rows]
    
    async def get_all_users(self) -> Dict[str, Dict]:
        """Get all users as a dict keyed by user_id string."""
        if not self.pool:
            return self._json_get_all_users()
        
        async with self.pool.acquire() as conn:
            rows = await conn.fetch("SELECT * FROM users ORDER BY xp DESC")
            return {str(r['user_id']): dict(r) for r in rows}
    
    # =========================================================
    # RAID OPERATIONS
    # =========================================================
    
    async def create_raid(self, raid_type: str, leader_id: int, target_clan: str = None) -> int:
        """Create a new raid session. Returns raid ID."""
        if not self.pool:
            return self._json_create_raid(raid_type, leader_id, target_clan)
        
        async with self.pool.acquire() as conn:
            raid_id = await conn.fetchval('''
                INSERT INTO raid_sessions (raid_type, leader_id, target_clan)
                VALUES ($1, $2, $3) RETURNING id
            ''', raid_type, leader_id, target_clan)
            return raid_id
    
    async def get_raid(self, raid_id: int) -> Optional[Dict]:
        """Get a raid session by ID."""
        if not self.pool:
            return self._json_get_raid(raid_id)
        
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow("SELECT * FROM raid_sessions WHERE id = $1", raid_id)
            return dict(row) if row else None
    
    async def update_raid(self, raid_id: int, **kwargs):
        """Update raid session fields."""
        if not self.pool:
            return self._json_update_raid(raid_id, **kwargs)
        
        set_clauses = []
        values = [raid_id]
        for i, (key, value) in enumerate(kwargs.items(), start=2):
            set_clauses.append(f"{key} = ${i}")
            values.append(value)
        
        async with self.pool.acquire() as conn:
            await conn.execute(
                f"UPDATE raid_sessions SET {', '.join(set_clauses)} WHERE id = $1",
                *values
            )
    
    async def add_raid_participant(self, raid_id: int, user_id: int):
        """Add a participant to a raid."""
        if not self.pool:
            return self._json_add_raid_participant(raid_id, user_id)
        
        async with self.pool.acquire() as conn:
            await conn.execute('''
                UPDATE raid_sessions 
                SET participants = array_append(participants, $2)
                WHERE id = $1 AND NOT ($2 = ANY(participants))
            ''', raid_id, user_id)
    
    async def remove_raid_participant(self, raid_id: int, user_id: int):
        """Remove a participant from a raid."""
        if not self.pool:
            return
        
        async with self.pool.acquire() as conn:
            await conn.execute('''
                UPDATE raid_sessions 
                SET participants = array_remove(participants, $2)
                WHERE id = $1
            ''', raid_id, user_id)
    
    async def complete_raid(self, raid_id: int, result: str, our_score: int, 
                            their_score: int, mvp_id: int = None):
        """Complete a raid with results."""
        if not self.pool:
            return self._json_complete_raid(raid_id, result, our_score, their_score, mvp_id)
        
        async with self.pool.acquire() as conn:
            await conn.execute('''
                UPDATE raid_sessions SET
                    result = $2, our_score = $3, their_score = $4, 
                    mvp_id = $5, status = 'completed',
                    completed_at = NOW()
                WHERE id = $1
            ''', raid_id, result, our_score, their_score, mvp_id)
    
    async def log_raid_performance(self, raid_id: int, user_id: int, 
                                    kills: int = 0, deaths: int = 0,
                                    damage: int = 0, objectives: int = 0,
                                    rating: str = "average"):
        """Log individual performance for a raid."""
        if not self.pool:
            return
        
        async with self.pool.acquire() as conn:
            await conn.execute('''
                INSERT INTO raid_performance (raid_id, user_id, kills, deaths, 
                    damage_dealt, objectives_completed, rating)
                VALUES ($1, $2, $3, $4, $5, $6, $7)
                ON CONFLICT (raid_id, user_id) DO UPDATE SET
                    kills = $3, deaths = $4, damage_dealt = $5,
                    objectives_completed = $6, rating = $7
            ''', raid_id, user_id, kills, deaths, damage, objectives, rating)
    
    async def update_raid_stats(self, user_id: int, won: bool, kills: int = 0,
                                 deaths: int = 0, damage: int = 0, 
                                 objectives: int = 0, is_mvp: bool = False):
        """Update cached raid stats for a user."""
        if not self.pool:
            return
        
        async with self.pool.acquire() as conn:
            # Ensure row exists
            await conn.execute('''
                INSERT INTO raid_stats (user_id) VALUES ($1) ON CONFLICT DO NOTHING
            ''', user_id)
            
            streak_update = """
                current_streak = CASE WHEN $2 THEN COALESCE(current_streak, 0) + 1 ELSE 0 END,
                best_streak = GREATEST(COALESCE(best_streak, 0), 
                    CASE WHEN $2 THEN COALESCE(current_streak, 0) + 1 ELSE COALESCE(best_streak, 0) END)
            """
            
            await conn.execute(f'''
                UPDATE raid_stats SET
                    total_raids = COALESCE(total_raids, 0) + 1,
                    raids_won = COALESCE(raids_won, 0) + CASE WHEN $2 THEN 1 ELSE 0 END,
                    raids_lost = COALESCE(raids_lost, 0) + CASE WHEN NOT $2 THEN 1 ELSE 0 END,
                    total_kills = COALESCE(total_kills, 0) + $3,
                    total_deaths = COALESCE(total_deaths, 0) + $4,
                    total_damage = COALESCE(total_damage, 0) + $5,
                    total_objectives = COALESCE(total_objectives, 0) + $6,
                    mvp_count = COALESCE(mvp_count, 0) + CASE WHEN $7 THEN 1 ELSE 0 END,
                    {streak_update},
                    last_raid_at = NOW(),
                    updated_at = NOW()
                WHERE user_id = $1
            ''', user_id, won, kills, deaths, damage, objectives, is_mvp)
    
    async def get_raid_stats(self, user_id: int) -> Optional[Dict]:
        """Get raid stats for a user."""
        if not self.pool:
            return self._json_get_raid_stats(user_id)
        
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow("SELECT * FROM raid_stats WHERE user_id = $1", user_id)
            return dict(row) if row else None
    
    async def get_raid_leaderboard(self, field: str = "total_raids", limit: int = 10) -> List[Dict]:
        """Get raid leaderboard."""
        if not self.pool:
            return []
        
        allowed = ["total_raids", "raids_won", "total_kills", "total_damage", "mvp_count", "best_streak"]
        if field not in allowed:
            field = "total_raids"
        
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                f"SELECT * FROM raid_stats ORDER BY {field} DESC NULLS LAST LIMIT $1", limit
            )
            return [dict(r) for r in rows]
    
    async def get_raid_history(self, limit: int = 10, raid_type: str = None) -> List[Dict]:
        """Get recent raid history."""
        if not self.pool:
            return []
        
        async with self.pool.acquire() as conn:
            if raid_type:
                rows = await conn.fetch('''
                    SELECT * FROM raid_sessions 
                    WHERE status = 'completed' AND raid_type = $2
                    ORDER BY completed_at DESC LIMIT $1
                ''', limit, raid_type)
            else:
                rows = await conn.fetch('''
                    SELECT * FROM raid_sessions WHERE status = 'completed'
                    ORDER BY completed_at DESC LIMIT $1
                ''', limit)
            return [dict(r) for r in rows]
    
    async def get_active_raids(self) -> List[Dict]:
        """Get all currently active raids."""
        if not self.pool:
            return []
        
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT * FROM raid_sessions WHERE status = 'active' ORDER BY started_at DESC"
            )
            return [dict(r) for r in rows]
    
    # =========================================================
    # WAR OPERATIONS
    # =========================================================
    
    async def declare_war(self, enemy_clan: str, declared_by: int, best_of: int = 3) -> int:
        """Declare war on another clan. Returns war ID."""
        if not self.pool:
            return 0
        
        async with self.pool.acquire() as conn:
            war_id = await conn.fetchval('''
                INSERT INTO wars (enemy_clan, declared_by, best_of)
                VALUES ($1, $2, $3) RETURNING id
            ''', enemy_clan, declared_by, best_of)
            return war_id
    
    async def get_war(self, war_id: int) -> Optional[Dict]:
        """Get a war by ID."""
        if not self.pool:
            return None
        
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow("SELECT * FROM wars WHERE id = $1", war_id)
            return dict(row) if row else None
    
    async def get_active_wars(self) -> List[Dict]:
        """Get all active wars."""
        if not self.pool:
            return []
        
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT * FROM wars WHERE status IN ('declared', 'active') ORDER BY declared_at DESC"
            )
            return [dict(r) for r in rows]
    
    async def update_war(self, war_id: int, **kwargs):
        """Update war fields."""
        if not self.pool:
            return
        
        set_clauses = []
        values = [war_id]
        for i, (key, value) in enumerate(kwargs.items(), start=2):
            set_clauses.append(f"{key} = ${i}")
            values.append(value)
        
        async with self.pool.acquire() as conn:
            await conn.execute(
                f"UPDATE wars SET {', '.join(set_clauses)} WHERE id = $1", *values
            )
    
    async def log_war_match(self, war_id: int, match_number: int, our_score: int, 
                             their_score: int, raid_id: int = None) -> int:
        """Log a war match result."""
        if not self.pool:
            return 0
        
        result = "win" if our_score > their_score else ("loss" if their_score > our_score else "draw")
        
        async with self.pool.acquire() as conn:
            match_id = await conn.fetchval('''
                INSERT INTO war_matches (war_id, match_number, raid_id, our_score, their_score, result)
                VALUES ($1, $2, $3, $4, $5, $6) RETURNING id
            ''', war_id, match_number, raid_id, our_score, their_score, result)
            
            # Update war scores
            if result == "win":
                await conn.execute(
                    "UPDATE wars SET our_wins = our_wins + 1, status = 'active' WHERE id = $1", war_id
                )
            elif result == "loss":
                await conn.execute(
                    "UPDATE wars SET our_losses = our_losses + 1, status = 'active' WHERE id = $1", war_id
                )
            
            # Check if war is decided
            war = await conn.fetchrow("SELECT * FROM wars WHERE id = $1", war_id)
            wins_needed = (war['best_of'] // 2) + 1
            if war['our_wins'] >= wins_needed:
                await conn.execute(
                    "UPDATE wars SET status = 'completed', result = 'victory', completed_at = NOW() WHERE id = $1", war_id
                )
            elif war['our_losses'] >= wins_needed:
                await conn.execute(
                    "UPDATE wars SET status = 'completed', result = 'defeat', completed_at = NOW() WHERE id = $1", war_id
                )
            
            return match_id
    
    async def get_war_record(self, limit: int = 20) -> Dict:
        """Get overall war record stats."""
        if not self.pool:
            return {"total": 0, "wins": 0, "losses": 0}
        
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow('''
                SELECT 
                    COUNT(*) FILTER (WHERE status = 'completed') as total,
                    COUNT(*) FILTER (WHERE result = 'victory') as wins,
                    COUNT(*) FILTER (WHERE result = 'defeat') as losses
                FROM wars
            ''')
            return dict(row) if row else {"total": 0, "wins": 0, "losses": 0}
    
    # =========================================================
    # RECRUITMENT OPERATIONS
    # =========================================================
    
    async def create_position(self, position_key: str, title: str, description: str,
                               requirements: list, posted_by: int, 
                               review_role: str = None, slots: int = 1) -> int:
        """Create an open recruitment position. Returns position ID."""
        if not self.pool:
            return self._json_create_position(position_key, title, description, requirements, posted_by, slots)
        
        async with self.pool.acquire() as conn:
            pos_id = await conn.fetchval('''
                INSERT INTO recruitment_positions 
                    (position_key, title, description, requirements, posted_by, review_role, slots_available)
                VALUES ($1, $2, $3, $4, $5, $6, $7) RETURNING id
            ''', position_key, title, description, requirements, posted_by, review_role, slots)
            return pos_id
    
    async def get_open_positions(self) -> List[Dict]:
        """Get all open recruitment positions."""
        if not self.pool:
            return self._json_get_open_positions()
        
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT * FROM recruitment_positions WHERE status = 'open' ORDER BY created_at DESC"
            )
            return [dict(r) for r in rows]
    
    async def get_position(self, position_id: int) -> Optional[Dict]:
        """Get a position by ID."""
        if not self.pool:
            return self._json_get_position(position_id)
        
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM recruitment_positions WHERE id = $1", position_id
            )
            return dict(row) if row else None
    
    async def update_position(self, position_id: int, **kwargs):
        """Update position fields."""
        if not self.pool:
            return
        
        set_clauses = []
        values = [position_id]
        for i, (key, value) in enumerate(kwargs.items(), start=2):
            set_clauses.append(f"{key} = ${i}")
            values.append(value)
        
        async with self.pool.acquire() as conn:
            await conn.execute(
                f"UPDATE recruitment_positions SET {', '.join(set_clauses)} WHERE id = $1", *values
            )
    
    async def apply_for_position(self, position_id: int, user_id: int, 
                                  answers: dict = None) -> Optional[int]:
        """Apply for a position. Returns app ID or None if already applied."""
        if not self.pool:
            return self._json_apply_for_position(position_id, user_id, answers)
        
        async with self.pool.acquire() as conn:
            # Check if already applied
            existing = await conn.fetchval('''
                SELECT id FROM recruitment_applications 
                WHERE position_id = $1 AND user_id = $2
            ''', position_id, user_id)
            
            if existing:
                return None
            
            app_id = await conn.fetchval('''
                INSERT INTO recruitment_applications (position_id, user_id, answers)
                VALUES ($1, $2, $3) RETURNING id
            ''', position_id, user_id, json.dumps(answers or {}))
            
            # Log it
            await conn.execute('''
                INSERT INTO recruitment_log (application_id, action, performed_by, details)
                VALUES ($1, 'applied', $2, 'Application submitted')
            ''', app_id, user_id)
            
            return app_id
    
    async def advance_application(self, app_id: int, new_stage: str, 
                                   reviewer_id: int, notes: str = None):
        """Move an application to the next stage."""
        if not self.pool:
            return self._json_advance_application(app_id, new_stage, reviewer_id, notes)
        
        async with self.pool.acquire() as conn:
            await conn.execute('''
                UPDATE recruitment_applications SET
                    stage = $2, reviewer_id = $3, review_notes = COALESCE($4, review_notes),
                    updated_at = NOW()
                WHERE id = $1
            ''', app_id, new_stage, reviewer_id, notes)
            
            await conn.execute('''
                INSERT INTO recruitment_log (application_id, action, performed_by, details)
                VALUES ($1, $2, $3, $4)
            ''', app_id, f"stage_changed_to_{new_stage}", reviewer_id, notes or "")
    
    async def get_application(self, app_id: int) -> Optional[Dict]:
        """Get an application by ID."""
        if not self.pool:
            return self._json_get_application(app_id)
        
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM recruitment_applications WHERE id = $1", app_id
            )
            return dict(row) if row else None
    
    async def get_applications_for_position(self, position_id: int, 
                                             stage: str = None) -> List[Dict]:
        """Get all applications for a position, optionally filtered by stage."""
        if not self.pool:
            return self._json_get_applications_for_position(position_id, stage)
        
        async with self.pool.acquire() as conn:
            if stage:
                rows = await conn.fetch('''
                    SELECT * FROM recruitment_applications 
                    WHERE position_id = $1 AND stage = $2
                    ORDER BY created_at ASC
                ''', position_id, stage)
            else:
                rows = await conn.fetch('''
                    SELECT * FROM recruitment_applications 
                    WHERE position_id = $1
                    ORDER BY created_at ASC
                ''', position_id)
            return [dict(r) for r in rows]
    
    async def get_user_applications(self, user_id: int) -> List[Dict]:
        """Get all applications by a user."""
        if not self.pool:
            return []
        
        async with self.pool.acquire() as conn:
            rows = await conn.fetch('''
                SELECT a.*, p.title as position_title, p.position_key
                FROM recruitment_applications a
                JOIN recruitment_positions p ON a.position_id = p.id
                WHERE a.user_id = $1
                ORDER BY a.created_at DESC
            ''', user_id)
            return [dict(r) for r in rows]
    
    async def get_pipeline_overview(self) -> Dict:
        """Get counts of applications at each stage across all open positions."""
        if not self.pool:
            return {}
        
        async with self.pool.acquire() as conn:
            rows = await conn.fetch('''
                SELECT a.stage, COUNT(*) as count, p.title
                FROM recruitment_applications a
                JOIN recruitment_positions p ON a.position_id = p.id
                WHERE p.status = 'open'
                GROUP BY a.stage, p.title
                ORDER BY p.title, a.stage
            ''')
            
            overview = {}
            for row in rows:
                title = row['title']
                if title not in overview:
                    overview[title] = {}
                overview[title][row['stage']] = row['count']
            return overview
    
    async def get_application_log(self, app_id: int) -> List[Dict]:
        """Get activity log for an application."""
        if not self.pool:
            return []
        
        async with self.pool.acquire() as conn:
            rows = await conn.fetch('''
                SELECT * FROM recruitment_log 
                WHERE application_id = $1 ORDER BY created_at ASC
            ''', app_id)
            return [dict(r) for r in rows]
    
    # =========================================================
    # JSON DATA BACKUP (for legacy compatibility)
    # =========================================================
    
    async def save_json_blob(self, key: str, data: dict):
        """Save a JSON blob to PostgreSQL."""
        if not self.pool:
            return
        
        async with self.pool.acquire() as conn:
            await conn.execute('''
                INSERT INTO json_data (key, data, updated_at)
                VALUES ($1, $2, NOW())
                ON CONFLICT (key) DO UPDATE SET data = $2, updated_at = NOW()
            ''', key, json.dumps(data))
    
    async def load_json_blob(self, key: str) -> Optional[dict]:
        """Load a JSON blob from PostgreSQL."""
        if not self.pool:
            return None
        
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow("SELECT data FROM json_data WHERE key = $1", key)
            return json.loads(row['data']) if row else None
    
    # =========================================================
    # JSON FALLBACK METHODS (when PostgreSQL is unavailable)
    # =========================================================
    
    def _load_json_file(self, filepath: str, default: Any = None) -> Any:
        """Load data from a JSON file."""
        if not os.path.exists(filepath):
            return default if default is not None else {}
        try:
            with open(filepath, "r") as f:
                return json.load(f)
        except Exception:
            return default if default is not None else {}
    
    def _save_json_file(self, filepath: str, data: Any):
        """Save data to a JSON file."""
        with open(filepath, "w") as f:
            json.dump(data, f, indent=2, default=str)
    
    def _json_get_user(self, user_id: int) -> Optional[Dict]:
        data = self._load_json_file("leaderboard.json", {"users": {}})
        return data.get("users", {}).get(str(user_id))
    
    def _json_ensure_user(self, user_id: int) -> Dict:
        data = self._load_json_file("leaderboard.json", {"users": {}, "roster": [None]*10, "theme": {}})
        uid = str(user_id)
        if uid not in data.get("users", {}):
            data.setdefault("users", {})[uid] = {
                "xp": 0, "level": 0, "coins": 0, "wins": 0, "losses": 0,
                "raid_wins": 0, "raid_losses": 0, "raid_participation": 0,
                "daily_streak": 0, "weekly_xp": 0, "monthly_xp": 0,
                "voice_time": 0, "messages": 0, "warnings": [],
            }
            self._save_json_file("leaderboard.json", data)
        return data["users"][uid]
    
    def _json_update_user(self, user_id: int, **kwargs):
        data = self._load_json_file("leaderboard.json", {"users": {}})
        uid = str(user_id)
        if uid in data.get("users", {}):
            data["users"][uid].update(kwargs)
            self._save_json_file("leaderboard.json", data)
    
    def _json_increment_user(self, user_id: int, **kwargs):
        data = self._load_json_file("leaderboard.json", {"users": {}})
        uid = str(user_id)
        data.setdefault("users", {}).setdefault(uid, {})
        for key, value in kwargs.items():
            data["users"][uid][key] = data["users"][uid].get(key, 0) + value
        self._save_json_file("leaderboard.json", data)
    
    def _json_get_leaderboard(self, field: str, limit: int) -> List[Dict]:
        data = self._load_json_file("leaderboard.json", {"users": {}})
        users = data.get("users", {})
        sorted_users = sorted(users.items(), key=lambda x: x[1].get(field, 0), reverse=True)
        return [{"user_id": int(uid), **udata} for uid, udata in sorted_users[:limit]]
    
    def _json_get_all_users(self) -> Dict[str, Dict]:
        data = self._load_json_file("leaderboard.json", {"users": {}})
        return data.get("users", {})
    
    # JSON raid fallbacks
    def _json_create_raid(self, raid_type, leader_id, target_clan):
        data = self._load_json_file("raids_data.json", {"raids": [], "next_id": 1})
        raid_id = data["next_id"]
        data["raids"].append({
            "id": raid_id, "raid_type": raid_type, "leader_id": leader_id,
            "target_clan": target_clan, "participants": [], "status": "active",
            "result": None, "our_score": 0, "their_score": 0, "mvp_id": None,
            "started_at": datetime.datetime.now().isoformat()
        })
        data["next_id"] = raid_id + 1
        self._save_json_file("raids_data.json", data)
        return raid_id
    
    def _json_get_raid(self, raid_id):
        data = self._load_json_file("raids_data.json", {"raids": []})
        for raid in data.get("raids", []):
            if raid.get("id") == raid_id:
                return raid
        return None
    
    def _json_update_raid(self, raid_id, **kwargs):
        data = self._load_json_file("raids_data.json", {"raids": []})
        for raid in data.get("raids", []):
            if raid.get("id") == raid_id:
                raid.update(kwargs)
                break
        self._save_json_file("raids_data.json", data)
    
    def _json_add_raid_participant(self, raid_id, user_id):
        data = self._load_json_file("raids_data.json", {"raids": []})
        for raid in data.get("raids", []):
            if raid.get("id") == raid_id:
                if user_id not in raid.get("participants", []):
                    raid.setdefault("participants", []).append(user_id)
                break
        self._save_json_file("raids_data.json", data)
    
    def _json_complete_raid(self, raid_id, result, our_score, their_score, mvp_id):
        data = self._load_json_file("raids_data.json", {"raids": []})
        for raid in data.get("raids", []):
            if raid.get("id") == raid_id:
                raid["result"] = result
                raid["our_score"] = our_score
                raid["their_score"] = their_score
                raid["mvp_id"] = mvp_id
                raid["status"] = "completed"
                raid["completed_at"] = datetime.datetime.now().isoformat()
                break
        self._save_json_file("raids_data.json", data)
    
    def _json_get_raid_stats(self, user_id):
        data = self._load_json_file("raids_data.json", {"raids": [], "stats": {}})
        return data.get("stats", {}).get(str(user_id))
    
    # JSON recruitment fallbacks
    def _json_create_position(self, key, title, desc, reqs, posted_by, slots):
        data = self._load_json_file("recruitment_data.json", {"positions": [], "applications": [], "next_pos_id": 1, "next_app_id": 1})
        pos_id = data["next_pos_id"]
        data["positions"].append({
            "id": pos_id, "position_key": key, "title": title, "description": desc,
            "requirements": reqs, "posted_by": posted_by, "slots_available": slots,
            "slots_filled": 0, "status": "open",
            "created_at": datetime.datetime.now().isoformat()
        })
        data["next_pos_id"] = pos_id + 1
        self._save_json_file("recruitment_data.json", data)
        return pos_id
    
    def _json_get_open_positions(self):
        data = self._load_json_file("recruitment_data.json", {"positions": []})
        return [p for p in data.get("positions", []) if p.get("status") == "open"]
    
    def _json_get_position(self, pos_id):
        data = self._load_json_file("recruitment_data.json", {"positions": []})
        for p in data.get("positions", []):
            if p.get("id") == pos_id:
                return p
        return None
    
    def _json_apply_for_position(self, pos_id, user_id, answers):
        data = self._load_json_file("recruitment_data.json", {"positions": [], "applications": [], "next_app_id": 1})
        for app in data.get("applications", []):
            if app.get("position_id") == pos_id and app.get("user_id") == user_id:
                return None
        app_id = data.get("next_app_id", 1)
        data.setdefault("applications", []).append({
            "id": app_id, "position_id": pos_id, "user_id": user_id,
            "answers": answers or {}, "stage": "applied",
            "created_at": datetime.datetime.now().isoformat()
        })
        data["next_app_id"] = app_id + 1
        self._save_json_file("recruitment_data.json", data)
        return app_id
    
    def _json_advance_application(self, app_id, new_stage, reviewer_id, notes):
        data = self._load_json_file("recruitment_data.json", {"applications": []})
        for app in data.get("applications", []):
            if app.get("id") == app_id:
                app["stage"] = new_stage
                app["reviewer_id"] = reviewer_id
                if notes:
                    app["review_notes"] = notes
                break
        self._save_json_file("recruitment_data.json", data)
    
    def _json_get_application(self, app_id):
        data = self._load_json_file("recruitment_data.json", {"applications": []})
        for app in data.get("applications", []):
            if app.get("id") == app_id:
                return app
        return None
    
    def _json_get_applications_for_position(self, pos_id, stage=None):
        data = self._load_json_file("recruitment_data.json", {"applications": []})
        apps = [a for a in data.get("applications", []) if a.get("position_id") == pos_id]
        if stage:
            apps = [a for a in apps if a.get("stage") == stage]
        return apps


# Singleton instance
db = Database()
