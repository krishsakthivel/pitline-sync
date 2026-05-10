"""
worker/sync.py — Rolling team sync worker

Every hour, fetches 100 teams that were synced longest ago (or never)
from RobotEvents and upserts their data into Supabase.

Run locally:   python worker/sync.py
               python worker/sync.py --populate   (seed teams table first)
               python worker/sync.py --once        (one batch then exit)
Render:        Add as a second service: python worker/sync.py
"""

import sys
import os
import time
import logging
from datetime import datetime, timezone

import psycopg2
import psycopg2.extras
from pitlinev5 import RobotEvents, TeamAnalyzer, EventAnalyzer

class Config:
    DATABASE_URL      = os.environ["DATABASE_URL"]
    ROBOTEVENTS_TOKEN = os.environ["ROBOTEVENTS_TOKEN"]
# Inlined from app.services.stats to avoid importing the full app
def calc_season_stats(rankings):
    if not rankings:
        return None
    total_wins   = sum(r.get("wins",   0) or 0 for r in rankings)
    total_losses = sum(r.get("losses", 0) or 0 for r in rankings)
    total_games  = total_wins + total_losses
    winrate      = round((total_wins / total_games) * 100) if total_games else 0
    avg_pts      = [r.get("average_points") for r in rankings if r.get("average_points")]
    high_score   = max((r.get("high_score", 0) or 0 for r in rankings), default=None)
    return {
        "total_wins":   total_wins,
        "total_losses": total_losses,
        "winrate":      winrate,
        "avg_points":   round(sum(avg_pts) / len(avg_pts), 2) if avg_pts else None,
        "high_score":   high_score,
    }

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

CURRENT_SEASON = 197
BATCH_SIZE     = 100
REQUEST_DELAY  = 0.5

_re = RobotEvents(api_key=Config.ROBOTEVENTS_TOKEN)


# ── DB connection ─────────────────────────────────────────────────────────────

def get_conn():
    return psycopg2.connect(Config.DATABASE_URL, cursor_factory=psycopg2.extras.RealDictCursor)


# ── ELO map ───────────────────────────────────────────────────────────────────

# ── Fetch team data via pitlinev5 ─────────────────────────────────────────────

def fetch_team_data(team_id):
    """
    Fetch all season data for one team using pitlinev5.RobotEvents.
    Returns the standard fetch_team_data dict.
    """
    return _re.fetch_team_data(team_id)


def fetch_all_season_teams():
    log.info("Fetching all teams for season %d...", CURRENT_SEASON)
    teams = _re.fetch_all_season_teams()
    log.info("Found %d teams total", len(teams))
    return teams


# ── Compute OPR / ELO via pitlinev5 ──────────────────────────────────────────

def compute_opr(team_id, matches, past_events):
    """
    Compute OPR/DPR/TrueOPR for team_id using pitlinev5.

    Tries each past event in reverse chronological order until we get
    a result that includes this team.
    """
    for event in past_events:
        event_matches = _re.get_event_matches(event["id"])
        time.sleep(REQUEST_DELAY)
        if not event_matches:
            continue
        ea   = EventAnalyzer(event_matches)
        opr  = ea.opr()
        dpr  = ea.dpr()
        topr = ea.true_opr()
        if str(team_id) in opr or team_id in opr:
            key = str(team_id) if str(team_id) in opr else team_id
            return {
                "opr":      opr.get(key),
                "dpr":      dpr.get(key),
                "ccwm":     round((opr.get(key) or 0) - (dpr.get(key) or 0), 4),
                "true_opr": topr.get(key),
            }
    return {}


# ── DB upsert helpers ─────────────────────────────────────────────────────────

def upsert_event(cur, event):
    loc = event.get("location", {})
    cur.execute("""
        insert into events (id, name, code, season_id, start_date, end_date, city, region, country)
        values (%(id)s, %(name)s, %(code)s, %(season_id)s, %(start_date)s, %(end_date)s,
                %(city)s, %(region)s, %(country)s)
        on conflict (id) do update set
            name       = excluded.name,
            start_date = excluded.start_date,
            end_date   = excluded.end_date
    """, {
        "id":         event["id"],
        "name":       event.get("name"),
        "code":       event.get("code"),
        "season_id":  CURRENT_SEASON,
        "start_date": (event.get("start") or "")[:10] or None,
        "end_date":   (event.get("end")   or "")[:10] or None,
        "city":       loc.get("city"),
        "region":     loc.get("region"),
        "country":    loc.get("country"),
    })


def upsert_match(cur, match):
    alliances = match.get("alliances", [])
    if isinstance(alliances, list):
        red  = next((a for a in alliances if a.get("color") == "red"),  {})
        blue = next((a for a in alliances if a.get("color") == "blue"), {})
    else:
        red  = alliances.get("red",  {})
        blue = alliances.get("blue", {})

    def team_id(alliance, idx):
        teams = alliance.get("teams", [])
        if idx < len(teams):
            t = teams[idx]
            return t.get("team", t).get("id")
        return None

    division = match.get("division") or {}

    cur.execute("""
        insert into matches (
            id, event_id, season_id, division_id, division_name,
            round, matchnum, scheduled,
            red_team1_id, red_team2_id, red_score,
            blue_team1_id, blue_team2_id, blue_score
        ) values (
            %(id)s, %(event_id)s, %(season_id)s, %(division_id)s, %(division_name)s,
            %(round)s, %(matchnum)s, %(scheduled)s,
            %(red1)s, %(red2)s, %(red_score)s,
            %(blue1)s, %(blue2)s, %(blue_score)s
        )
        on conflict (id) do update set
            division_id   = excluded.division_id,
            division_name = excluded.division_name,
            red_score     = excluded.red_score,
            blue_score    = excluded.blue_score,
            scheduled     = excluded.scheduled
    """, {
        "id":            match["id"],
        "event_id":      (match.get("event") or {}).get("id"),
        "season_id":     CURRENT_SEASON,
        "division_id":   division.get("id"),
        "division_name": division.get("name"),
        "round":         match.get("round"),
        "matchnum":      match.get("matchnum"),
        "scheduled":     match.get("scheduled"),
        "red1":          team_id(red, 0),
        "red2":          team_id(red, 1),
        "red_score":     red.get("score"),
        "blue1":         team_id(blue, 0),
        "blue2":         team_id(blue, 1),
        "blue_score":    blue.get("score"),
    })


def upsert_ranking(cur, team_id, ranking):
    event_id = (ranking.get("event") or {}).get("id")
    if not event_id:
        return
    cur.execute("""
        insert into rankings (
            team_id, event_id, season_id, rank, wins, losses, ties,
            wp, ap, sp, avg_points, high_score, autonomous_win_point
        ) values (
            %(team_id)s, %(event_id)s, %(season_id)s, %(rank)s,
            %(wins)s, %(losses)s, %(ties)s,
            %(wp)s, %(ap)s, %(sp)s, %(avg_points)s, %(high_score)s,
            %(autonomous_win_point)s
        )
        on conflict (team_id, event_id) do update set
            rank                = excluded.rank,
            wins                = excluded.wins,
            losses              = excluded.losses,
            ties                = excluded.ties,
            wp                  = excluded.wp,
            ap                  = excluded.ap,
            sp                  = excluded.sp,
            avg_points          = excluded.avg_points,
            high_score          = excluded.high_score,
            autonomous_win_point = excluded.autonomous_win_point
    """, {
        "team_id":               team_id,
        "event_id":              event_id,
        "season_id":             CURRENT_SEASON,
        "rank":                  ranking.get("rank"),
        "wins":                  ranking.get("wins"),
        "losses":                ranking.get("losses"),
        "ties":                  ranking.get("ties"),
        "wp":                    ranking.get("wp"),
        "ap":                    ranking.get("ap"),
        "sp":                    ranking.get("sp"),
        "avg_points":            ranking.get("average_points"),
        "high_score":            ranking.get("high_score"),
        "autonomous_win_point":  ranking.get("autonomous_win_point"),
    })


def upsert_skill(cur, team_id, skill):
    event_id = (skill.get("event") or {}).get("id")
    if not event_id:
        return
    cur.execute("""
        insert into skills (id, team_id, event_id, season_id, type, score, rank, attempts)
        values (%(id)s, %(team_id)s, %(event_id)s, %(season_id)s,
                %(type)s, %(score)s, %(rank)s, %(attempts)s)
        on conflict (id) do update set
            score    = excluded.score,
            rank     = excluded.rank,
            attempts = excluded.attempts
    """, {
        "id":        skill.get("id"),
        "team_id":   team_id,
        "event_id":  event_id,
        "season_id": CURRENT_SEASON,
        "type":      skill.get("type"),
        "score":     skill.get("score"),
        "rank":      skill.get("rank"),
        "attempts":  skill.get("attempts"),
    })



def upsert_award(cur, team_id, award):
    event_id = (award.get("event") or {}).get("id")
    if not event_id:
        return
    cur.execute("""
        insert into awards (id, team_id, event_id, season_id, title, qualifications)
        values (%(id)s, %(team_id)s, %(event_id)s, %(season_id)s, %(title)s, %(qualifications)s)
        on conflict (id) do update set
            title          = excluded.title,
            qualifications = excluded.qualifications
    """, {
        "id":             award.get("id"),
        "team_id":        team_id,
        "event_id":       event_id,
        "season_id":      CURRENT_SEASON,
        "title":          award.get("title"),
        "qualifications": award.get("qualifications"),
    })


# ── Main sync for one team ────────────────────────────────────────────────────

def sync_team(conn, team_row, api_data):
    team_id  = team_row["id"]
    matches  = api_data.get("matches",  [])
    rankings = api_data.get("rankings", [])
    events   = api_data.get("events",   [])

    now = datetime.now(timezone.utc).isoformat()
    past_events = sorted(
        [e for e in events if e.get("start") and e["start"] <= now],
        key=lambda e: e["start"], reverse=True
    )

    season_stats = calc_season_stats(rankings)
    opr_entry    = compute_opr(team_id, matches, past_events)

    with conn.cursor() as cur:
        for event in events:
            try:
                upsert_event(cur, event)
            except Exception as e:
                log.warning("  event %s skipped: %s", event.get("id"), e)

        match_ok = match_fail = 0
        for match in matches:
            try:
                event_stub = match.get("event") or {}
                cur.execute("""
                    insert into events (id, name, season_id)
                    values (%(id)s, %(name)s, %(season_id)s)
                    on conflict (id) do nothing
                """, {
                    "id":        event_stub.get("id"),
                    "name":      event_stub.get("name"),
                    "season_id": CURRENT_SEASON,
                })
                upsert_match(cur, match)
                match_ok += 1
            except Exception as e:
                match_fail += 1
                log.warning("  match %s skipped: %s", match.get("id"), e)
        log.info("  matches: %d stored, %d skipped", match_ok, match_fail)

        for ranking in rankings:
            try:
                upsert_ranking(cur, team_id, ranking)
            except Exception as e:
                log.warning("  ranking skipped: %s", e)

        for skill in api_data.get("skills", []):
            try:
                upsert_skill(cur, team_id, skill)
            except Exception as e:
                log.warning("  skill %s skipped: %s", skill.get("id"), e)

        awards = api_data.get("awards", [])
        if not awards:
            try:
                time.sleep(REQUEST_DELAY)
                awards = _re.get_team_awards(team_id, season=CURRENT_SEASON)
            except Exception as e:
                log.warning("  awards fetch failed: %s", e)
                awards = []
        award_ok = award_fail = 0
        for award in awards:
            try:
                event_stub = award.get("event") or {}
                if event_stub.get("id"):
                    cur.execute("""
                        insert into events (id, name, season_id)
                        values (%(id)s, %(name)s, %(season_id)s)
                        on conflict (id) do nothing
                    """, {
                        "id":        event_stub.get("id"),
                        "name":      event_stub.get("name"),
                        "season_id": CURRENT_SEASON,
                    })
                upsert_award(cur, team_id, award)
                award_ok += 1
            except Exception as e:
                award_fail += 1
                log.warning("  award %s skipped: %s", award.get("id"), e)
        log.info("  awards: %d stored, %d skipped", award_ok, award_fail)

        cur.execute("""
            update teams set
                wins           = %(wins)s,
                losses         = %(losses)s,
                winrate        = %(winrate)s,
                avg_score      = %(avg_score)s,
                high_score     = %(high_score)s,
                opr            = %(opr)s,
                dpr            = %(dpr)s,
                ccwm           = %(ccwm)s,
                true_opr       = %(true_opr)s,
                last_synced_at = now()
            where id = %(team_id)s
        """, {
            "team_id":   team_id,
            "wins":      season_stats["total_wins"]   if season_stats else 0,
            "losses":    season_stats["total_losses"] if season_stats else 0,
            "winrate":   season_stats["winrate"]      if season_stats else 0,
            "avg_score": season_stats["avg_points"]   if season_stats else None,
            "high_score":season_stats["high_score"]   if season_stats else None,
            "opr":       opr_entry.get("opr"),
            "dpr":       opr_entry.get("dpr"),
            "ccwm":      opr_entry.get("ccwm"),
            "true_opr":  opr_entry.get("true_opr"),
            })

        conn.commit()


# ── Population pass ───────────────────────────────────────────────────────────

def populate_teams_table(conn):
    log.info("Starting population pass...")
    all_teams = fetch_all_season_teams()

    with conn.cursor() as cur:
        for t in all_teams:
            print(t)
            loc = t.get("location", {})
            cur.execute("""
                insert into teams (id, number, name, organization, city, region, country, grade, program, robot_name)
                values (%(id)s, %(number)s, %(name)s, %(org)s, %(city)s, %(region)s, %(country)s,
                        %(grade)s, %(program)s, %(robot)s)
                on conflict (id) do nothing
            """, {
                "id":      t["id"],
                "number":  t.get("number"),
                "name":    t.get("team_name"),
                "org":     t.get("organization"),
                "city":    loc.get("city"),
                "region":  loc.get("region"),
                "country": loc.get("country"),
                "grade":   t.get("grade"),
                "program": (t.get("program") or {}).get("code"),
                "robot":   t.get("robot_name"),
            })
        conn.commit()

    log.info("Population pass complete — %d teams seeded", len(all_teams))


# ── Sync loop helpers ─────────────────────────────────────────────────────────

def get_offset(conn):
    with conn.cursor() as cur:
        cur.execute("select value from sync_state where key = 'offset'")
        row = cur.fetchone()
        return int(row["value"]) if row else 0


def set_offset(conn, offset):
    with conn.cursor() as cur:
        cur.execute("""
            insert into sync_state (key, value) values ('offset', %s)
            on conflict (key) do update set value = excluded.value
        """, (str(offset),))
        conn.commit()


def get_total_teams(conn):
    with conn.cursor() as cur:
        cur.execute("select count(*) as n from teams")
        return cur.fetchone()["n"]


def run_sync_cycle(conn):
    """Returns True when all teams have been synced (full pass complete)."""
    total = get_total_teams(conn)
    if not total:
        log.info("No teams in DB yet — run with --populate first")
        return True

    offset = get_offset(conn)
    if offset >= total:
        log.info("Full pass complete.")
        set_offset(conn, 0)
        return True

    with conn.cursor() as cur:
        cur.execute("""
            select id, number, name
            from teams
            order by id
            limit %s offset %s
        """, (BATCH_SIZE, offset))
        batch = cur.fetchall()

    if not batch:
        set_offset(conn, 0)
        return True

    log.info("Syncing batch of %d teams (offset %d/%d)...", len(batch), offset, total)

    for i, team_row in enumerate(batch):
        team_id = team_row["id"]
        log.info("  [%d/%d] %s (%s)", i + 1, len(batch), team_row["number"], team_row["name"])

        try:
            api_data = fetch_team_data(team_id)
            sync_team(conn, team_row, api_data)
        except Exception as e:
            log.error("  Failed to sync team %s: %s", team_row["number"], e)
            try:
                conn.rollback()
            except Exception:
                pass
            continue

    new_offset = offset + len(batch)
    set_offset(conn, new_offset)
    log.info("Batch complete. Progress: %d/%d teams", new_offset, total)
    return new_offset >= total


# ── Entrypoint ────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Pitline sync worker")
    parser.add_argument("--populate", action="store_true",
                        help="Seed the teams table from RobotEvents (run once on first deploy)")
    parser.add_argument("--once", action="store_true",
                        help="Run one sync cycle then exit")
    args = parser.parse_args()

    conn = get_conn()
    log.info("Connected to Supabase")

    if args.populate:
        populate_teams_table(conn)
        if not args.once:
            sys.exit(0)

    if args.once:
        run_sync_cycle(conn)
        conn.close()
        sys.exit(0)

    log.info("Starting sync (batch_size=%d)...", BATCH_SIZE)
    while True:
        try:
            done = run_sync_cycle(conn)
        except Exception as e:
            log.error("Cycle failed: %s", e)
            try:
                conn.rollback()
            except Exception:
                pass
            done = False

        if done:
            log.info("All teams synced. Exiting.")
            break
