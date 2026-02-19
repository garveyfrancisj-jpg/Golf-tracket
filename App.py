import sqlite3
import time
from datetime import date, datetime
from typing import Dict, List, Optional, Tuple

import pandas as pd
import requests
import streamlit as st

# ----------------------------
# Configuration
# ----------------------------
DB_PATH = "golf_tracker.sqlite"

# Philadelphia City Hall-ish coordinates (good central point)
PHILLY_LAT = 39.9526
PHILLY_LON = -75.1652

# 30 miles in meters ≈ 48,280
DEFAULT_RADIUS_M = 48280

# Public Overpass endpoint (be nice; this app caches results)
OVERPASS_URL = "https://overpass-api.de/api/interpreter"

# Cache freshness (seconds). 7 days by default.
COURSE_CACHE_TTL_SEC = 7 * 24 * 3600


# ----------------------------
# Database helpers
# ----------------------------
def db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def init_db() -> None:
    conn = db()
    cur = conn.cursor()

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS courses (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            osm_type TEXT NOT NULL,      -- node / way / relation
            osm_id INTEGER NOT NULL,
            name TEXT NOT NULL,
            lat REAL,
            lon REAL,
            access TEXT,                 -- e.g., private / yes / permissive / unknown
            raw_tags_json TEXT,
            updated_at_utc TEXT NOT NULL,
            UNIQUE(osm_type, osm_id)
        );
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS rounds (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            played_on TEXT NOT NULL,     -- YYYY-MM-DD
            course_id INTEGER NOT NULL,
            tees TEXT,
            round_notes TEXT,
            created_at_utc TEXT NOT NULL,
            FOREIGN KEY(course_id) REFERENCES courses(id)
        );
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS holes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            round_id INTEGER NOT NULL,
            hole_number INTEGER NOT NULL CHECK(hole_number BETWEEN 1 AND 18),
            strokes INTEGER NOT NULL CHECK(strokes BETWEEN 1 AND 25),
            putts INTEGER,
            penalties INTEGER,
            hole_comment TEXT,
            FOREIGN KEY(round_id) REFERENCES rounds(id),
            UNIQUE(round_id, hole_number)
        );
        """
    )

    conn.commit()
    conn.close()


def utc_now_iso() -> str:
    return datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


# ----------------------------
# Overpass / OSM course fetching
# ----------------------------
def build_overpass_query(lat: float, lon: float, radius_m: int) -> str:
    # nwr = nodes/ways/relations
    # out center gives a centroid for ways/relations; nodes already have lat/lon
    return f"""
    [out:json][timeout:25];
    (
      nwr["leisure"="golf_course"](around:{radius_m},{lat},{lon});
    );
    out center tags;
    """


def fetch_courses_overpass(lat: float, lon: float, radius_m: int) -> List[dict]:
    query = build_overpass_query(lat, lon, radius_m)
    resp = requests.post(OVERPASS_URL, data={"data": query}, timeout=60)
    resp.raise_for_status()
    data = resp.json()
    return data.get("elements", [])


def element_center_lat_lon(el: dict) -> Tuple[Optional[float], Optional[float]]:
    # Nodes have lat/lon directly; ways/relations may have "center"
    if el.get("type") == "node":
        return el.get("lat"), el.get("lon")
    center = el.get("center") or {}
    return center.get("lat"), center.get("lon")


def normalize_access(tags: Dict[str, str]) -> str:
    # OSM uses access=* a lot; private clubs often have access=private.
    access = (tags.get("access") or "").strip().lower()
    if access:
        return access
    # Some mappers use other hints; keep it conservative.
    membership = (tags.get("membership") or "").strip().lower()
    if membership in {"yes", "required"}:
        return "private"
    return "unknown"


def upsert_courses(elements: List[dict]) -> int:
    conn = db()
    cur = conn.cursor()
    updated = 0

    for el in elements:
        tags = el.get("tags") or {}
        name = tags.get("name")
        if not name:
            continue

        lat, lon = element_center_lat_lon(el)
        access = normalize_access(tags)

        osm_type = el.get("type")
        osm_id = int(el.get("id"))

        # Store tags as a simple string (avoid requiring an extra dependency).
        raw_tags = str(tags)

        cur.execute(
            """
            INSERT INTO courses (osm_type, osm_id, name, lat, lon, access, raw_tags_json, updated_at_utc)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(osm_type, osm_id) DO UPDATE SET
                name=excluded.name,
                lat=excluded.lat,
                lon=excluded.lon,
                access=excluded.access,
                raw_tags_json=excluded.raw_tags_json,
                updated_at_utc=excluded.updated_at_utc
            ;
            """,
            (osm_type, osm_id, name, lat, lon, access, raw_tags, utc_now_iso()),
        )
        updated += 1

    conn.commit()
    conn.close()
    return updated


def courses_last_updated() -> Optional[datetime]:
    conn = db()
    cur = conn.cursor()
    cur.execute("SELECT MAX(updated_at_utc) AS max_ts FROM courses;")
    row = cur.fetchone()
    conn.close()
    if not row or not row["max_ts"]:
        return None
    # Parse Zulu time
    return datetime.fromisoformat(row["max_ts"].replace("Z", "+00:00"))


def maybe_refresh_courses(lat: float, lon: float, radius_m: int, force: bool = False) -> str:
    last = courses_last_updated()
    now = datetime.utcnow()

    if not force and last is not None:
        age_sec = (now - last.replace(tzinfo=None)).total_seconds()
        if age_sec < COURSE_CACHE_TTL_SEC:
            return f"Using cached courses (last refresh: {last.strftime('%Y-%m-%d %H:%M UTC')})."

    elements = fetch_courses_overpass(lat, lon, radius_m)
    count = upsert_courses(elements)
    return f"Refreshed courses from OpenStreetMap: stored/updated {count} items."


def load_courses_df() -> pd.DataFrame:
    conn = db()
    df = pd.read_sql_query(
        """
        SELECT
            c.id,
            c.name,
            c.access,
            c.lat,
            c.lon,
            c.updated_at_utc
        FROM courses c
        ORDER BY LOWER(c.name);
        """,
        conn,
    )
    conn.close()

    # Friendly label
    def access_label(a: str) -> str:
        a = (a or "unknown").lower()
        if a == "private":
            return "Private"
        if a in {"yes", "permissive", "customers"}:
            return "Public/Playable"
        return "Unknown"

    if not df.empty:
        df["access_label"] = df["access"].apply(access_label)
        df["dropdown_label"] = df["name"] + " — " + df["access_label"]

    return df


# ----------------------------
# Round / hole persistence
# ----------------------------
def create_round(course_id: int, played_on: str, tees: str, notes: str) -> int:
    conn = db()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO rounds (played_on, course_id, tees, round_notes, created_at_utc)
        VALUES (?, ?, ?, ?, ?);
        """,
        (played_on, course_id, tees or None, notes or None, utc_now_iso()),
    )
    round_id = cur.lastrowid
    conn.commit()
    conn.close()
    return int(round_id)


def upsert_hole(round_id: int, hole_number: int, strokes: int, putts: Optional[int],
                penalties: Optional[int], comment: str) -> None:
    conn = db()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO holes (round_id, hole_number, strokes, putts, penalties, hole_comment)
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(round_id, hole_number) DO UPDATE SET
            strokes=excluded.strokes,
            putts=excluded.putts,
            penalties=excluded.penalties,
            hole_comment=excluded.hole_comment
        ;
        """,
        (round_id, hole_number, strokes, putts, penalties, comment or None),
    )
    conn.commit()
    conn.close()


def load_rounds_df() -> pd.DataFrame:
    conn = db()
    df = pd.read_sql_query(
        """
        SELECT
            r.id AS round_id,
            r.played_on,
            c.name AS course,
            r.tees,
            r.round_notes,
            r.created_at_utc
        FROM rounds r
        JOIN courses c ON c.id = r.course_id
        ORDER BY r.played_on DESC, r.id DESC;
        """,
        conn,
    )
    conn.close()
    return df


def load_holes_df(round_id: int) -> pd.DataFrame:
    conn = db()
    df = pd.read_sql_query(
        """
        SELECT
            hole_number,
            strokes,
            putts,
            penalties,
            hole_comment
        FROM holes
        WHERE round_id = ?
        ORDER BY hole_number;
        """,
        conn,
        params=(round_id,),
    )
    conn.close()
    return df


# ----------------------------
# Streamlit UI
# ----------------------------
def main():
    st.set_page_config(page_title="Golf Course & Score Tracker", layout="wide")
    init_db()

    st.title("🏌️ Golf Course & Score Tracker")

    with st.expander("Settings (course search)"):
        colA, colB, colC = st.columns(3)
        with colA:
            lat = st.number_input("Center latitude", value=float(PHILLY_LAT), format="%.6f")
        with colB:
            lon = st.number_input("Center longitude", value=float(PHILLY_LON), format="%.6f")
        with colC:
            radius_m = st.number_input("Radius (meters)", value=int(DEFAULT_RADIUS_M), step=1000)

        col1, col2 = st.columns([1, 3])
        with col1:
            force_refresh = st.button("Refresh course list now")
        with col2:
            try:
                msg = maybe_refresh_courses(lat, lon, int(radius_m), force=force_refresh)
                st.caption(msg)
            except Exception as e:
                st.error(f"Could not refresh from Overpass API: {e}")

    courses_df = load_courses_df()
    if courses_df.empty:
        st.warning("No courses are cached yet. Expand Settings and click 'Refresh course list now'.")
        return

    st.subheader("Add a new round")

    left, right = st.columns([1, 2])

    with left:
        access_filter = st.multiselect(
            "Filter by access",
            options=sorted(courses_df["access_label"].unique().tolist()),
            default=sorted(courses_df["access_label"].unique().tolist()),
        )
        filtered = courses_df[courses_df["access_label"].isin(access_filter)].copy()

        course_label = st.selectbox(
            "Course",
            options=filtered["dropdown_label"].tolist(),
        )
        selected_row = filtered[filtered["dropdown_label"] == course_label].iloc[0]
        course_id = int(selected_row["id"])

        played_on = st.date_input("Date played", value=date.today()).isoformat()
        tees = st.text_input("Tees (optional)", placeholder="e.g., Blue / White / Tips")
        round_notes = st.text_area("Round notes (optional)", placeholder="Weather, swing thoughts, etc.", height=100)

    with right:
        st.caption("Enter hole-by-hole scores. Strokes are required; everything else is optional.")
        hole_cols = st.columns(6)
        hole_data = []

        # Header row
        hole_cols[0].markdown("**Hole**")
        hole_cols[1].markdown("**Strokes**")
        hole_cols[2].markdown("**Putts**")
        hole_cols[3].markdown("**Penalties**")
        hole_cols[4].markdown("**Comment**")
        hole_cols[5].markdown("**—**")

        for h in range(1, 19):
            cols = st.columns([0.6, 1, 1, 1, 3, 0.4])

            cols[0].write(f"{h}")
            strokes = cols[1].number_input(f"strokes_{h}", min_value=1, max_value=25, value=4, label_visibility="collapsed")
            putts = cols[2].number_input(f"putts_{h}", min_value=0, max_value=10, value=0, label_visibility="collapsed")
            penalties = cols[3].number_input(f"pen_{h}", min_value=0, max_value=10, value=0, label_visibility="collapsed")
            comment = cols[4].text_input(f"comment_{h}", value="", label_visibility="collapsed", placeholder="optional")
            hole_data.append((h, int(strokes), int(putts), int(penalties), comment))

        total_strokes = sum(x[1] for x in hole_data)
        st.metric("Total strokes", total_strokes)

        save = st.button("💾 Save round", type="primary")

        if save:
            new_round_id = create_round(course_id, played_on, tees, round_notes)
            for (h, strokes, putts, penalties, comment) in hole_data:
                # Save putts/penalties only if user set >0 to keep DB cleaner (optional)
                upsert_hole(
                    new_round_id,
                    h,
                    strokes,
                    putts if putts > 0 else None,
                    penalties if penalties > 0 else None,
                    comment,
                )
            st.success(f"Saved round #{new_round_id} — Total strokes: {total_strokes}")

    st.divider()
    st.subheader("History")

    rounds_df = load_rounds_df()
    if rounds_df.empty:
        st.info("No rounds saved yet.")
        return

    st.dataframe(rounds_df, use_container_width=True, hide_index=True)

    st.subheader("View a round’s hole-by-hole detail")
    round_choice = st.selectbox(
        "Select a round",
        options=rounds_df["round_id"].tolist(),
        format_func=lambda rid: f'Round {rid} — {rounds_df[rounds_df["round_id"] == rid].iloc[0]["played_on"]} — {rounds_df[rounds_df["round_id"] == rid].iloc[0]["course"]}',
    )

    holes_df = load_holes_df(int(round_choice))
    if holes_df.empty:
        st.info("No holes found for that round (unexpected).")
    else:
        holes_df = holes_df.copy()
        holes_df["strokes"] = holes_df["strokes"].astype(int)
        total = holes_df["strokes"].sum()
        st.write(f"**Total strokes:** {int(total)}")
        st.dataframe(holes_df, use_container_width=True, hide_index=True)


if __name__ == "__main__":
    main()
