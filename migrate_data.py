"""
✝ THE FALLEN ✝ - Data Migration Script
Migrates all existing JSON data into the new PostgreSQL database tables.

Run this ONCE after setting up the new database module.
Usage: python migrate_data.py

This script:
  1. Reads all existing JSON files (leaderboard.json, duels_data.json, etc.)
  2. Maps them into the new normalized PostgreSQL tables
  3. Preserves all existing data — nothing is deleted
  4. Can be safely re-run (uses INSERT ... ON CONFLICT)
"""

import asyncio
import json
import os
import sys
import datetime

# Add parent dir to path so we can import our modules
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from database import db

# --- JSON file paths (match your existing bot) ---
JSON_FILES = {
    "leaderboard": "leaderboard.json",
    "duels": "duels_data.json",
    "events": "events_data.json",
    "inactivity": "inactivity_data.json",
    "raids": "raid_history.json",
    "tournaments": "tournaments.json",
    "warnings": "warnings_data.json",
    "recurring": "recurring_events.json",
    "transcripts": "ticket_transcripts.json",
    "practice": "practice_sessions.json",
    "legacy": "legacy_data.json",
    "embeds": "custom_embeds.json",
    "polls": "polls_data.json",
}


def load_json(filepath):
    """Safely load a JSON file."""
    if not os.path.exists(filepath):
        print(f"  ⏭️  {filepath} not found, skipping.")
        return None
    try:
        with open(filepath, "r") as f:
            return json.load(f)
    except Exception as e:
        print(f"  ❌ Error loading {filepath}: {e}")
        return None


async def migrate_users(data):
    """Migrate user data from leaderboard.json → users table."""
    users = data.get("users", {})
    if not users:
        print("  ⏭️  No users to migrate.")
        return 0
    
    count = 0
    async with db.pool.acquire() as conn:
        for uid_str, udata in users.items():
            try:
                uid = int(uid_str)
            except ValueError:
                continue
            
            await conn.execute('''
                INSERT INTO users (
                    user_id, xp, level, coins, wins, losses,
                    raid_wins, raid_losses, raid_participation,
                    training_attendance, tryout_attendance, tryout_passes, tryout_fails,
                    events_hosted, daily_streak, weekly_xp, monthly_xp,
                    voice_time, messages, verified, roblox_username, roblox_id,
                    elo_shield_active, streak_saver_active, training_reserved,
                    custom_level_bg, warnings
                ) VALUES (
                    $1, $2, $3, $4, $5, $6,
                    $7, $8, $9,
                    $10, $11, $12, $13,
                    $14, $15, $16, $17,
                    $18, $19, $20, $21, $22,
                    $23, $24, $25,
                    $26, $27
                ) ON CONFLICT (user_id) DO UPDATE SET
                    xp = GREATEST(users.xp, $2),
                    level = GREATEST(users.level, $3),
                    coins = GREATEST(users.coins, $4),
                    wins = GREATEST(users.wins, $5),
                    losses = GREATEST(users.losses, $6),
                    raid_wins = GREATEST(users.raid_wins, $7),
                    raid_losses = GREATEST(users.raid_losses, $8),
                    raid_participation = GREATEST(users.raid_participation, $9)
            ''',
                uid,
                udata.get("xp", 0), udata.get("level", 0), udata.get("coins", 0),
                udata.get("wins", 0), udata.get("losses", 0),
                udata.get("raid_wins", 0), udata.get("raid_losses", 0), udata.get("raid_participation", 0),
                udata.get("training_attendance", 0), udata.get("tryout_attendance", 0),
                udata.get("tryout_passes", 0), udata.get("tryout_fails", 0),
                udata.get("events_hosted", 0), udata.get("daily_streak", 0),
                udata.get("weekly_xp", 0), udata.get("monthly_xp", 0),
                udata.get("voice_time", 0), udata.get("messages", 0),
                udata.get("verified", False),
                udata.get("roblox_username"), 
                udata.get("roblox_id"),
                udata.get("elo_shield_active", False),
                udata.get("streak_saver_active", False),
                udata.get("training_reserved", False),
                udata.get("custom_level_bg"),
                json.dumps(udata.get("warnings", []))
            )
            count += 1
    
    return count


async def migrate_roster(data):
    """Migrate roster from leaderboard.json → roster table."""
    roster = data.get("roster", [])
    if not roster:
        return 0
    
    count = 0
    async with db.pool.acquire() as conn:
        for position, user_id in enumerate(roster):
            if user_id is not None:
                await conn.execute('''
                    INSERT INTO roster (position, user_id)
                    VALUES ($1, $2)
                    ON CONFLICT (position) DO UPDATE SET user_id = $2
                ''', position, user_id)
                count += 1
    
    return count


async def migrate_raid_history(data):
    """Migrate raid history from raid_history.json → raid_sessions table."""
    if not data:
        return 0
    
    raids = data if isinstance(data, list) else data.get("raids", [])
    count = 0
    
    async with db.pool.acquire() as conn:
        for raid in raids:
            target = raid.get("target") or raid.get("target_clan", "Unknown")
            result = raid.get("result", "unknown")
            participants = raid.get("participants", [])
            
            # Convert participant list to bigint array
            participant_ids = []
            for p in participants:
                try:
                    participant_ids.append(int(p))
                except (ValueError, TypeError):
                    continue
            
            await conn.execute('''
                INSERT INTO raid_sessions (
                    raid_type, target_clan, leader_id, result, 
                    participants, xp_awarded, status, completed_at
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, NOW())
            ''',
                "standard",
                target,
                participant_ids[0] if participant_ids else 0,
                result,
                participant_ids,
                raid.get("xp_gained", 0),
                "completed"
            )
            count += 1
    
    return count


async def migrate_duels(data):
    """Migrate ELO/duel data from duels_data.json."""
    if not data:
        return 0
    
    # Also store as JSON blob for backward compatibility
    await db.save_json_blob("duels_data", data)
    
    # Migrate ELO ratings into users table if present
    elo_data = data.get("elo", {}) or data.get("ratings", {})
    count = 0
    
    if elo_data:
        async with db.pool.acquire() as conn:
            for uid_str, elo in elo_data.items():
                try:
                    uid = int(uid_str)
                    rating = int(elo) if isinstance(elo, (int, float)) else elo.get("rating", 1000)
                except (ValueError, TypeError):
                    continue
                
                await conn.execute('''
                    INSERT INTO users (user_id, elo_rating)
                    VALUES ($1, $2)
                    ON CONFLICT (user_id) DO UPDATE SET elo_rating = $2
                ''', uid, rating)
                count += 1
    
    return count


async def migrate_json_blobs():
    """Migrate remaining JSON files as blobs for backward compatibility."""
    blob_files = ["events", "inactivity", "recurring", "transcripts", 
                  "practice", "legacy", "embeds", "polls"]
    count = 0
    
    for key in blob_files:
        filepath = JSON_FILES.get(key)
        if not filepath:
            continue
        
        data = load_json(filepath)
        if data:
            await db.save_json_blob(key, data)
            count += 1
            print(f"  ✅ {key} → json_data blob")
    
    return count


async def build_raid_stats():
    """Build raid_stats aggregation table from migrated raid_sessions."""
    if not db.pool:
        return 0
    
    async with db.pool.acquire() as conn:
        # Get all completed raids
        raids = await conn.fetch(
            "SELECT * FROM raid_sessions WHERE status = 'completed'"
        )
        
        # Aggregate per user
        user_stats = {}
        for raid in raids:
            participants = raid.get("participants", [])
            won = raid.get("result") == "win"
            
            for pid in participants:
                if pid not in user_stats:
                    user_stats[pid] = {
                        "total": 0, "won": 0, "lost": 0,
                        "streak": 0, "best_streak": 0
                    }
                
                stats = user_stats[pid]
                stats["total"] += 1
                if won:
                    stats["won"] += 1
                    stats["streak"] += 1
                    stats["best_streak"] = max(stats["best_streak"], stats["streak"])
                else:
                    stats["lost"] += 1
                    stats["streak"] = 0
        
        # Insert aggregated stats
        count = 0
        for uid, stats in user_stats.items():
            await conn.execute('''
                INSERT INTO raid_stats (
                    user_id, total_raids, raids_won, raids_lost,
                    current_streak, best_streak, last_raid_at
                ) VALUES ($1, $2, $3, $4, $5, $6, NOW())
                ON CONFLICT (user_id) DO UPDATE SET
                    total_raids = $2, raids_won = $3, raids_lost = $4,
                    current_streak = $5, best_streak = $6, updated_at = NOW()
            ''', uid, stats["total"], stats["won"], stats["lost"],
                stats["streak"], stats["best_streak"])
            count += 1
        
        return count


async def main():
    """Run the full migration."""
    print("=" * 60)
    print("✝ THE FALLEN ✝ — Data Migration")
    print("=" * 60)
    
    # Connect to database
    print("\n📡 Connecting to PostgreSQL...")
    connected = await db.init()
    
    if not connected:
        print("❌ Cannot connect to PostgreSQL. Set DATABASE_URL environment variable.")
        print("   Example: export DATABASE_URL='postgresql://user:pass@host:5432/dbname'")
        return
    
    print("✅ Connected! Starting migration...\n")
    
    # 1. Migrate users + roster from leaderboard.json
    print("📦 Migrating leaderboard.json...")
    lb_data = load_json(JSON_FILES["leaderboard"])
    if lb_data:
        user_count = await migrate_users(lb_data)
        print(f"  ✅ {user_count} users migrated")
        
        roster_count = await migrate_roster(lb_data)
        print(f"  ✅ {roster_count} roster entries migrated")
        
        # Save theme as setting
        theme = lb_data.get("theme", {})
        if theme:
            async with db.pool.acquire() as conn:
                await conn.execute('''
                    INSERT INTO settings (key, value)
                    VALUES ('theme', $1)
                    ON CONFLICT (key) DO UPDATE SET value = $1, updated_at = NOW()
                ''', json.dumps(theme))
            print("  ✅ Theme settings migrated")
    
    # 2. Migrate raid history
    print("\n📦 Migrating raid history...")
    raid_data = load_json(JSON_FILES["raids"])
    if raid_data:
        raid_count = await migrate_raid_history(raid_data)
        print(f"  ✅ {raid_count} raids migrated")
        
        # Build aggregated stats
        stats_count = await build_raid_stats()
        print(f"  ✅ {stats_count} raid stat profiles built")
    
    # 3. Migrate duels/ELO
    print("\n📦 Migrating duels data...")
    duels_data = load_json(JSON_FILES["duels"])
    if duels_data:
        duel_count = await migrate_duels(duels_data)
        print(f"  ✅ {duel_count} ELO ratings migrated")
    
    # 4. Migrate remaining files as JSON blobs
    print("\n📦 Migrating remaining data as JSON blobs...")
    blob_count = await migrate_json_blobs()
    print(f"  ✅ {blob_count} JSON blobs stored")
    
    # 5. Also store the full leaderboard.json as a blob (backward compat)
    if lb_data:
        await db.save_json_blob("main_data", lb_data)
        print("  ✅ Full leaderboard.json backed up as blob")
    
    # Done
    print("\n" + "=" * 60)
    print("✅ Migration complete!")
    print("=" * 60)
    print("\nYour existing JSON files are untouched.")
    print("The bot will now use PostgreSQL as the primary store")
    print("with JSON as a fallback if the DB is unavailable.")
    
    await db.close()


if __name__ == "__main__":
    asyncio.run(main())
