"""
elo_sync.py — Compute season-wide ELO for all synced teams.

Run this after sync.py has populated matches for all teams:
    python elo_sync.py

Pulls all qual matches from the DB, replays them chronologically in a single
shared ELO pool, then writes each team's final rating back to teams.elo.
"""

import logging
import sys
import os
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from config import Config
import psycopg2
import psycopg2.extras

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

QUAL_ROUND = 2  # RobotEvents round=2 = qualification matches


def get_conn():
    return psycopg2.connect(Config.DATABASE_URL, cursor_factory=psycopg2.extras.RealDictCursor)


def load_all_qual_matches(conn):
    """
    Load every qual match from the DB, sorted chronologically.
    Returns list of dicts shaped like RobotEvents match objects.
    """
    with conn.cursor() as cur:
        cur.execute("""
            select
                m.id, m.round, m.matchnum, m.scheduled,
                m.red_score, m.blue_score,
                t1.number as red1, t2.number as red2,
                t3.number as blue1, t4.number as blue2
            from matches m
            left join teams t1 on t1.id = m.red_team1_id
            left join teams t2 on t2.id = m.red_team2_id
            left join teams t3 on t3.id = m.blue_team1_id
            left join teams t4 on t4.id = m.blue_team2_id
            where m.round = %s
              and m.red_score  is not null
              and m.blue_score is not null
            order by m.scheduled asc nulls last, m.id asc
        """, (QUAL_ROUND,))
        return cur.fetchall()


def load_all_teams(conn):
    """Return list of (id, number) for all synced teams."""
    with conn.cursor() as cur:
        cur.execute("select id, number from teams where last_synced_at is not null")
        return cur.fetchall()


def compute_elo(matches, k=32, initial=1500.0):
    """
    Replay all matches chronologically in a single shared ELO pool.
    Returns dict: team_number -> final ELO float.
    """
    elo = defaultdict(lambda: initial)

    for m in matches:
        red  = [t for t in [m["red1"],  m["red2"]]  if t]
        blue = [t for t in [m["blue1"], m["blue2"]] if t]
        if not red or not blue:
            continue

        r_score = m["red_score"]  or 0
        b_score = m["blue_score"] or 0

        r_avg = sum(elo[t] for t in red)  / len(red)
        b_avg = sum(elo[t] for t in blue) / len(blue)

        exp_r = 1 / (1 + 10 ** ((b_avg - r_avg) / 400))
        exp_b = 1 - exp_r

        if   r_score > b_score: s_r, s_b = 1.0, 0.0
        elif r_score < b_score: s_r, s_b = 0.0, 1.0
        else:                   s_r, s_b = 0.5, 0.5

        for t in red:  elo[t] += k * (s_r - exp_r)
        for t in blue: elo[t] += k * (s_b - exp_b)

    return {t: round(v, 2) for t, v in elo.items()}


def write_elo(conn, team_id, elo_value):
    with conn.cursor() as cur:
        cur.execute(
            "update teams set elo = %s where id = %s",
            (elo_value, team_id)
        )


def main():
    log.info("Connecting to DB...")
    conn = get_conn()

    log.info("Loading all qual matches...")
    matches = load_all_qual_matches(conn)
    log.info("Loaded %d qual matches", len(matches))

    if not matches:
        log.error("No matches found — make sure teams are synced first.")
        conn.close()
        sys.exit(1)

    log.info("Computing ELO...")
    elo_map = compute_elo(matches)
    log.info("Computed ELO for %d teams", len(elo_map))

    log.info("Loading synced teams...")
    teams = load_all_teams(conn)
    log.info("Found %d synced teams", len(teams))

    updated = skipped = 0
    for team in teams:
        elo = elo_map.get(team["number"])
        if elo is not None:
            write_elo(conn, team["id"], elo)
            updated += 1
        else:
            skipped += 1

    conn.commit()
    log.info("Done — updated %d teams, skipped %d (no matches)", updated, skipped)

    # Print top 10 for sanity check
    top = sorted(
        [(t["number"], elo_map.get(t["number"], 0)) for t in teams],
        key=lambda x: x[1], reverse=True
    )[:10]
    log.info("Top 10 ELO:")
    for number, elo in top:
        log.info("  %s: %.1f", number, elo)

    conn.close()


if __name__ == "__main__":
    main()
