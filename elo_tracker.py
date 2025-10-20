
"""
Old World League ELO Tracke
"""
from __future__ import annotations
from datetime import datetime, date, timedelta
from typing import Optional, Dict, Set, Tuple, List
import os, base64

import streamlit as st
from sqlmodel import SQLModel, Field, create_engine, Session, select

# =============== Config & State ===============
st.set_page_config(page_title="Old World League ELO Tracker", layout="wide")
if "is_admin" not in st.session_state:
    st.session_state.is_admin = False

DB_PATH = "elo_db.sqlite"
ADMIN_PASSWORD = st.secrets.get("ADMIN_PASSWORD", os.getenv("ADMIN_PASSWORD", "change-me"))
LOGO_URL = st.secrets.get("LOGO_URL", os.getenv("LOGO_URL", ""))
LOGO_WIDTH = int(st.secrets.get("LOGO_WIDTH", os.getenv("LOGO_WIDTH", 120)))

# Placeholder factions
PLACEHOLDER_FACTIONS: List[str] = ['Empire of Man', 'Dwarfen Mountain Holds', 'Kingdom of Bretonnia', 'Wood Elf Realms', 'High Elf Realms', 'Orc & Goblin Tribes', 'Warriors of Chaos', 'Beastmen Brayheards', 'Tomb Kings of Khemri', 'Skaven', 'Ogre Kingdoms', 'Lizardmen', 'Chaos Dwarfs', 'Dark Elves', 'Daemons of Chaos', 'Vampire Counts']
PLACEHOLDER_FACTIONS_WITH_BLANK: List[str] = ["— None —", *PLACEHOLDER_FACTIONS]

def _faction_index_or_blank(value: Optional[str]) -> int:
    if not value:
        return 0
    try:
        return 1 + PLACEHOLDER_FACTIONS.index(value)
    except ValueError:
        return 0

# =============== Models ===============
SQLModel.metadata.clear()

class Player(SQLModel, table=True):
    __tablename__ = "players"
    __table_args__ = {"extend_existing": True}
    id: Optional[int] = Field(default=None, primary_key=True)
    name: str
    rating: float = 1000.0
    active: bool = True
    faction: Optional[str] = None  # NEW: player’s default faction
    created_at: datetime = Field(default_factory=datetime.utcnow)

class Match(SQLModel, table=True):
    __tablename__ = "matches"
    __table_args__ = {"extend_existing": True}
    id: Optional[int] = Field(default=None, primary_key=True)
    week: str  # DD/MM/YYYY (Wednesday of that week)
    player_a_id: int
    player_b_id: Optional[int] = None  # None means BYE for A
    result: str = "pending"  # "pending" | "a_win" | "b_win" | "draw"
    a_rating_before: Optional[float] = None
    b_rating_before: Optional[float] = None
    a_rating_after: Optional[float] = None
    b_rating_after: Optional[float] = None
    reported_at: Optional[datetime] = None
    k_factor_used: Optional[int] = None
    a_faction: Optional[str] = None
    b_faction: Optional[str] = None

class Attendance(SQLModel, table=True):
    __tablename__ = "attendance"
    __table_args__ = {"extend_existing": True}
    id: Optional[int] = Field(default=None, primary_key=True)
    week: str  # DD/MM/YYYY (Wednesday id)
    player_id: int
    present: bool = True

class WeekKey(SQLModel, table=True):
    __tablename__ = "week_keys"
    __table_args__ = {"extend_existing": True}
    id: Optional[int] = Field(default=None, primary_key=True)
    week: str
    results_password: str

# =============== Database ===============

@st.cache_resource
def get_engine():
    import os
    DB_URL = st.secrets.get("DATABASE_URL", os.getenv("DATABASE_URL"))
    if DB_URL:
        # Postgres (or any SQLAlchemy URL) path
        return create_engine(DB_URL, echo=False)
    # Fallback to local SQLite (dev)
    from sqlalchemy import event
    eng = create_engine(f"sqlite:///{DB_PATH}", echo=False, connect_args={"check_same_thread": False})
    try:
        @event.listens_for(eng, "connect")
        def set_sqlite_pragma(dbapi_connection, connection_record):
            cur = dbapi_connection.cursor()
            cur.execute("PRAGMA journal_mode=WAL;")
            cur.execute("PRAGMA synchronous=NORMAL;")
            cur.close()
    except Exception:
        pass
    return eng

engine = get_engine()

SQLModel.metadata.create_all(engine)

# Run lightweight migrations only for SQLite (PRAGMA etc. are SQLite-specific)
try:
    if str(engine.url).startswith("sqlite:///"):
        with engine.connect() as _conn:
            existing = {r[0] for r in _conn.exec_driver_sql("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
            if "player" in existing and "players" not in existing:
                _conn.exec_driver_sql('ALTER TABLE "player" RENAME TO "players"'); existing.add("players")
            if "match" in existing and "matches" not in existing:
                _conn.exec_driver_sql('ALTER TABLE "match" RENAME TO "matches"'); existing.add("matches")
            if "weekkey" in existing and "week_keys" not in existing:
                _conn.exec_driver_sql('ALTER TABLE "weekkey" RENAME TO "week_keys"'); existing.add("week_keys")
            cols_players = [r[1] for r in _conn.exec_driver_sql('PRAGMA table_info("players")').fetchall()] if "players" in existing else []
            if "active" not in cols_players and "players" in existing:
                _conn.exec_driver_sql('ALTER TABLE "players" ADD COLUMN active BOOLEAN DEFAULT 1')
            if "faction" not in cols_players and "players" in existing:
                _conn.exec_driver_sql('ALTER TABLE "players" ADD COLUMN faction TEXT')
            cols_matches = [r[1] for r in _conn.exec_driver_sql('PRAGMA table_info("matches")').fetchall()] if "matches" in existing else []
            if "k_factor_used" not in cols_matches and "matches" in existing:
                _conn.exec_driver_sql('ALTER TABLE "matches" ADD COLUMN k_factor_used INTEGER')
            if "a_faction" not in cols_matches and "matches" in existing:
                _conn.exec_driver_sql('ALTER TABLE "matches" ADD COLUMN a_faction TEXT')
            if "b_faction" not in cols_matches and "matches" in existing:
                _conn.exec_driver_sql('ALTER TABLE "matches" ADD COLUMN b_faction TEXT')
            _conn.commit()
except Exception:
    pass

# =============== ELO helpers ===============
def expected_score(rp: float, ro: float) -> float:
    return 1.0 / (1.0 + 10 ** ((ro - rp) / 400.0))

def update_elo(r_a: float, r_b: float, score_a: float, k: int) -> Tuple[float, float]:
    e_a = expected_score(r_a, r_b); e_b = expected_score(r_b, r_a)
    return r_a + k * (score_a - e_a), r_b + k * ((1 - score_a) - e_b)

# =============== Utils ===============

def most_played_faction(session: Session, player_id: int) -> Optional[str]:
    """Return the faction this player has most often played in recorded matches.
    Counts any non-empty faction string (no dependency on global constants).
    If no recorded factions, returns None.
    """
    counts: Dict[str, int] = {}
    # Count appearances as A
    for m in session.exec(select(Match).where(Match.player_a_id == player_id)).all():
        if getattr(m, "a_faction", None):
            counts[m.a_faction] = counts.get(m.a_faction, 0) + 1
    # Count appearances as B
    for m in session.exec(select(Match).where(Match.player_b_id == player_id)).all():
        if getattr(m, "b_faction", None):
            counts[m.b_faction] = counts.get(m.b_faction, 0) + 1
    if not counts:
        return None
    # Deterministic tie-break: highest count, then alphabetical
    return sorted(counts.items(), key=lambda kv: (-kv[1], kv[0]))[0][0]


# =============== Week password utility ===============
def week_password(session: Session, week: str) -> Optional[str]:
    """Return the results password for a given week, or None if not set."""
    row = session.exec(select(WeekKey).where(WeekKey.week == week)).first()
    return row.results_password if row else None

def uk_date_str(d: date) -> str:
    return d.strftime("%d/%m/%Y")

def week_id_wed(d: date) -> str:
    """Return UK-formatted date string for the Wednesday of the week containing d."""
    # Monday=0 ... Sunday=6; Wednesday is 2
    offset = 2 - d.weekday()
    wednesday = d + timedelta(days=offset)
    return uk_date_str(wednesday)

def get_player_map(session: Session) -> Dict[int, Player]:
    return {p.id: p for p in session.exec(select(Player)).all()}

def player_record(session: Session, player_id: int):
    """Return (wins, draws, losses) for a player.
    BYE currently counts as a win for A (no rating change).
    """
    w = d = l = 0
    matches: List[Match] = session.exec(
        select(Match).where((Match.player_a_id == player_id) | (Match.player_b_id == player_id))
    ).all()
    for m in matches:
        # BYE handling
        if m.player_b_id is None:
            if m.player_a_id == player_id and m.result != "pending":
                w += 1  # policy: count BYE as W
            continue
        if m.result == "pending":
            continue
        if m.result == "draw":
            d += 1
        elif m.result == "a_win":
            if m.player_a_id == player_id: w += 1
            else: l += 1
        elif m.result == "b_win":
            if m.player_b_id == player_id: w += 1
            else: l += 1
    return w, d, l

def fetch_past_pairs(session: Session) -> Set[tuple[int, int]]:
    s: Set[tuple[int, int]] = set()
    for m in session.exec(select(Match)).all():
        if m.player_b_id is None: continue
        a, b = sorted([m.player_a_id, m.player_b_id]); s.add((a, b))
    return s

def generate_weekly_pairings(session: Session, week: str, restrict_to: Optional[Set[int]] = None):
    """Greedy pairing: sort by rating desc, avoid repeats when possible, fallback to next available.
    Creates Match rows (pending). Returns list of created Match objects.
    """
    q = select(Player).where(Player.active == True).order_by(Player.rating.desc())
    players = session.exec(q).all(); ids = [p.id for p in players]
    if restrict_to: ids = [i for i in ids if i in restrict_to]
    if not ids: return []
    past = fetch_past_pairs(session); used: Set[int] = set(); pairings = []
    for i, pid in enumerate(ids):
        if pid in used: continue
        opp = None
        # Prefer non-repeat vs nearest down the list
        for j in range(i + 1, len(ids)):
            cand = ids[j]
            if cand in used: continue
            if tuple(sorted([pid, cand])) not in past:
                opp = cand; break
        if opp is None:
            for j in range(i + 1, len(ids)):
                cand = ids[j]
                if cand in used: continue
                opp = cand; break
        if opp is None: pairings.append((pid, None)); used.add(pid)
        else: pairings.append((pid, opp)); used.update([pid, opp])
    created = []
    for a, b in pairings:
        m = Match(week=week, player_a_id=a, player_b_id=b, result="pending")
        session.add(m); session.commit(); session.refresh(m); created.append(m)
    return created

# =============== Theme (sticky header + spacing) ===============
_DEF_CSS = """
<style>
html, body, .stApp { background: #141414 !important; color: #f0e8d8 !important; }
.block-container { border: 1px solid rgba(200,163,95,.35); border-radius: 14px; padding: 2.8rem 1.25rem 1.25rem; }
.stTabs [aria-selected="true"] { border-bottom: 3px solid #c8a35f !important; }
.stTabs { margin-top: 0.6rem !important; }
.owl-header { position: sticky; top: 0; z-index: 20; background: #141414; padding: .5rem 0 .6rem; box-shadow: 0 2px 10px rgba(0,0,0,.25); }
.owl-spacer { height: .35rem; }
</style>
"""

def apply_theme(): st.markdown(_DEF_CSS, unsafe_allow_html=True)

# =============== Header (single render) ===============
def _find_logo_path() -> Optional[str]:
    for name in ["The-Old-World-Logo.png","The-Old-World-Logo.jpg","old_world_logo.png","old_world_logo.jpg"]:
        p = os.path.join(os.path.dirname(os.path.abspath(__file__)), name)
        if os.path.exists(p): return p
    return None

def render_header():
    logo_html = ""
    if LOGO_URL:
        logo_html = f"<img src='{LOGO_URL}' alt='Old World Logo' width='{LOGO_WIDTH}'/>"
    else:
        lp = _find_logo_path();
        if lp:
            with open(lp, "rb") as f:
                encoded = base64.b64encode(f.read()).decode()
            ext = lp.lower().split(".")[-1]
            mime = "image/png" if ext == "png" else "image/jpeg"
            logo_html = f"<img src='data:{mime};base64,{encoded}' alt='Old World Logo' width='{LOGO_WIDTH}'/>"
    header_html = f"""
<div class='owl-header' style='display:flex;flex-direction:column;align-items:center;gap:.35rem;margin:1.0rem 0 .6rem;'>
  {logo_html}
  <h1 style='margin:0;text-align:center'>OLD WORLD LEAGUE ELO TRACKER</h1>
  <div style='opacity:.75;text-align:center'>Imperial Registry of Victories &amp; Defeats</div>
</div>
<div class='owl-spacer'></div>
"""
    st.markdown(header_html, unsafe_allow_html=True)

_SPONSOR_NAMES = ["element-games-north-west-gaming-centre-logo-removebg-preview.png","element-games.png","element_games.png","elementgames.png"]

def _encode_local_image_if_exists(names) -> Optional[str]:
    base = os.path.dirname(os.path.abspath(__file__))
    for n in names:
        p = os.path.join(base, n)
        if os.path.exists(p):
            with open(p, "rb") as f: return base64.b64encode(f.read()).decode()
    return None

def sidebar_sponsor():
    enc = _encode_local_image_if_exists(_SPONSOR_NAMES)
    if enc:
        html = f"""
<div style='text-align:center; margin-top:1.5rem;'>
  <a href='https://elementgames.co.uk/north-west-gaming-centre' target='_blank'>
    <img src='data:image/png;base64,{enc}' width='200' alt='Element Games Logo'/>
  </a>
  <div style='font-size:0.8rem; opacity:0.8; margin-top:0.3rem;'>Venue Partner</div>
</div>
"""
        st.markdown(html, unsafe_allow_html=True)
    else:
        st.markdown("[Element Games: North West Gaming Centre](https://elementgames.co.uk/north-west-gaming-centre)")
    try:
        with open("discord.png", "rb") as f:
            discord_encoded = base64.b64encode(f.read()).decode()
        discord_html = f"""
<div style='text-align:center; margin-top:1.5rem;'>
  <a href='https://discord.gg/fJeE8NHeyg' target='_blank'>
    <img src='data:image/png;base64,{discord_encoded}' width='200' alt='Join us on Discord'/>
  </a>
</div>
"""
        st.markdown(discord_html, unsafe_allow_html=True)
    except FileNotFoundError:
        st.markdown("[Join us on Discord](https://discord.gg/fJeE8NHeyg)")

# =============== Boot ===============
apply_theme(); render_header()

# =============== Sidebar: Access & backup + sponsor ===============
with st.sidebar:
    st.header("Access")
    if not st.session_state.is_admin:
        with st.form("admin_unlock_form"):
            pw = st.text_input("Admin password", type="password", key="admin_pw")
            submitted = st.form_submit_button("Unlock admin", use_container_width=True)
        if submitted:
            if pw == ADMIN_PASSWORD:
                st.session_state.is_admin = True
                st.success("Admin mode unlocked.")
                st.rerun()
            else:
                st.error("Incorrect password.")
    else:
        st.success("Admin mode active")
        if st.button("Lock", use_container_width=True, key="btn_lock_admin"):
            st.session_state.is_admin = False
            st.rerun()
        try:
            if os.path.exists(DB_PATH):
                with open(DB_PATH, "rb") as f: data = f.read()
                st.download_button("Download DB", data=data, file_name=DB_PATH, mime="application/octet-stream", use_container_width=True)
        except Exception:
            pass
    sidebar_sponsor()

# =============== Tabs ===============
tabs_public = ["Leaderboard", "Data", "Enter Results"]
tabs_admin = ["Players", "Pairings", "Ad-Hoc Match"]
order = (tabs_public + tabs_admin) if st.session_state.get("is_admin", False) else tabs_public
idx = {name: i for i, name in enumerate(order)}
T = st.tabs(order)

# =============== Leaderboard ===============
with T[idx["Leaderboard"]]:
    st.subheader("Leaderboard")
    show_archived = st.checkbox("Include archived players", value=False, key="lb_arch")
    with Session(engine) as s:
        q = select(Player)
        if not show_archived: q = q.where(Player.active == True)
        players = s.exec(q.order_by(Player.rating.desc())).all()
        records = {p.id: (*player_record(s, p.id),) for p in players}
    if players:
        rows = [{"Rank": i+1, "Name": p.name, "Faction": p.faction, "Rating": round(p.rating, 1), "GP": sum(records[p.id]), "W": records[p.id][0], "D": records[p.id][1], "L": records[p.id][2]} for i, p in enumerate(players)]
        st.dataframe(rows, use_container_width=True, hide_index=True, column_config={"Rating": st.column_config.NumberColumn(format="%.1f"), "GP": st.column_config.NumberColumn(format="%d"), "W": st.column_config.NumberColumn(format="%d"), "D": st.column_config.NumberColumn(format="%d"), "L": st.column_config.NumberColumn(format="%d")})
    else: st.info("No players yet.")

# =============== Data (history) ===============
with T[idx["Data"]]:
    st.subheader("History")
    with Session(engine) as s:
        matches = s.exec(select(Match).order_by(Match.week.desc(), Match.id.desc())).all()
        pmap = get_player_map(s)
    if matches:
        rows = [{
            "Match ID": m.id,
            "Week": m.week,
            "A": pmap[m.player_a_id].name if m.player_a_id in pmap else f"A#{m.player_a_id}",
            "Faction A": m.a_faction,
            "B": (pmap[m.player_b_id].name if (m.player_b_id and m.player_b_id in pmap) else "BYE"),
            "Faction B": m.b_faction,
            "Result": m.result,
            "K Used": m.k_factor_used,
            "A Before": (round(m.a_rating_before,1) if m.a_rating_before is not None else None),
            "B Before": (round(m.b_rating_before,1) if m.b_rating_before is not None else None),
            "A After": (round(m.a_rating_after,1) if m.a_rating_after is not None else None),
            "B After": (round(m.b_rating_after,1) if m.b_rating_after is not None else None)
        } for m in matches]
        st.dataframe(rows, use_container_width=True, hide_index=True, column_config={"Rating": st.column_config.NumberColumn(format="%.1f"), "GP": st.column_config.NumberColumn(format="%d"), "W": st.column_config.NumberColumn(format="%d"), "D": st.column_config.NumberColumn(format="%d"), "L": st.column_config.NumberColumn(format="%d")})
    else: st.info("No matches recorded yet.")

# =============== Enter Results ===============


# =============== Week password helpers ===============
def set_week_password(session: Session, week: str, password: Optional[str]) -> None:
    """Create or update the results password for a given week. If password is empty, clears it."""
    if not password:
        wk = session.exec(select(WeekKey).where(WeekKey.week == week)).first()
        if wk:
            session.delete(wk)
            session.commit()
        return
    wk = session.exec(select(WeekKey).where(WeekKey.week == week)).first()
    if wk:
        wk.results_password = password
        session.add(wk)
        session.commit()
    else:
        session.add(WeekKey(week=week, results_password=password))
        session.commit()

def clear_week_password(session: Session, week: str) -> None:
    wk = session.exec(select(WeekKey).where(WeekKey.week == week)).first()
    if wk:
        session.delete(wk)
        session.commit()
with T[idx["Enter Results"]]:
    st.subheader("Submit Match Results")
    week_val = st.text_input("Week", value=week_id_wed(date.today()), key="wk_res")
    with Session(engine) as s: wk_pw = week_password(s, week_val)
    wk_key = f"wk_unlocked::{week_val}"
    if wk_key not in st.session_state: st.session_state[wk_key] = False
    if wk_pw and not st.session_state[wk_key]:
        pw_try = st.text_input(f"Password for {week_val}", type="password", key=f"wk_pw:::{week_val}")
        if st.button("Unlock week", key=f"btn_unlock:::{week_val}"): st.session_state[wk_key] = (pw_try == wk_pw); st.success("Unlocked." if st.session_state[wk_key] else "Incorrect password.")
    locked = wk_pw and (not st.session_state[wk_key])

    with Session(engine) as s:
        matches = s.exec(select(Match).where(Match.week == week_val).order_by(Match.id)).all()
        pmap = get_player_map(s)

    if locked: st.info("Week is locked. Enter the password to submit results.")
    elif not matches: st.info("No matches found for that week.")
    else:
        for m in matches:
            a = pmap.get(m.player_a_id); b = pmap.get(m.player_b_id) if m.player_b_id is not None else None
            with st.form(f"res_{m.id}"):
                ar = int(round(a.rating)) if a else None; br = int(round(b.rating)) if b else None
                st.markdown(f"**Match {m.id}** — {a.name if a else m.player_a_id} ({ar}) vs {(b.name if b else 'BYE')}{(' ('+str(br)+')' if b else '')} | Status: {m.result}")
                k_choice = st.radio("K-factor", ["Casual (10)", "Competitive (40)"], index=1, horizontal=True, key=f"k_{m.id}", disabled=(m.result != "pending" or b is None))
                result = st.selectbox("Result", ["a_win", "b_win", "draw"], key=f"sel_{m.id}", disabled=(m.result != "pending" or b is None))
                # Use placeholder factions, default to match faction if present, else player's saved faction, else blank
                with Session(engine) as _s_stats:

                    a_pref = most_played_faction(_s_stats, a.id) if a else None

                    b_pref = most_played_faction(_s_stats, b.id) if b else None
                a_default_idx = _faction_index_or_blank(getattr(m, 'a_faction', None) or a_pref or (a.faction if a else None))
                b_default_idx = 0 if b is None else _faction_index_or_blank(getattr(m, 'b_faction', None) or b_pref or (b.faction if b else None))
                a_faction = st.selectbox("Player A faction", PLACEHOLDER_FACTIONS_WITH_BLANK, index=a_default_idx, key=f"af_{m.id}", disabled=(m.result != "pending"))
                b_faction = None if b is None else st.selectbox("Player B faction", PLACEHOLDER_FACTIONS_WITH_BLANK, index=b_default_idx, key=f"bf_{m.id}", disabled=(m.result != "pending" or b is None))
                # Normalize '— None —' to None
                a_faction = None if a_faction == "— None —" else a_faction
                if b is not None:
                    b_faction = None if b_faction == "— None —" else b_faction
                if st.form_submit_button("Save result", disabled=(m.result != "pending")):
                    with Session(engine) as s2:
                        match = s2.get(Match, m.id)
                        if not match or match.result != "pending": st.error("Match not found or already recorded.")
                        else:
                            pa = s2.get(Player, match.player_a_id); pb = s2.get(Player, match.player_b_id) if match.player_b_id is not None else None
                            match.a_rating_before = pa.rating; match.b_rating_before = pb.rating if pb else None
                            if pb is None:
                                match.result = "a_win"; match.a_rating_after = pa.rating; match.k_factor_used = None; match.reported_at = datetime.utcnow()
                                match.a_faction = a_faction; match.b_faction = None
                                s2.add(match); s2.commit(); st.success("BYE recorded (no rating change).")
                            else:
                                k = 10 if k_choice.startswith("Casual") else 40; score_a = 1.0 if result == "a_win" else 0.0 if result == "b_win" else 0.5
                                new_a, new_b = update_elo(pa.rating, pb.rating, score_a, k)
                                match.result = result; match.a_rating_after = new_a; match.b_rating_after = new_b; match.k_factor_used = int(k); match.reported_at = datetime.utcnow(); match.a_faction = a_faction; match.b_faction = b_faction
                                pa.rating = new_a; pb.rating = new_b; s2.add(pa); s2.add(pb); s2.add(match); s2.commit(); st.success(f"Saved result (K={k}).")

# =============== Players (admin) ===============
if st.session_state.get("is_admin", False) and "Players" in idx:
    with T[idx["Players"]]:
        st.subheader("Add Players")
        with st.form("add_p", clear_on_submit=True):
            name = st.text_input("Player name", placeholder="e.g., Heinrich Kemmler")
            starting = st.number_input("Starting rating", 400.0, 3000.0, 1000.0, 10.0)
            # NEW: choose default faction at creation (optional)
            faction_choice = st.selectbox("Player faction (optional)", PLACEHOLDER_FACTIONS_WITH_BLANK, index=0)
            if st.form_submit_button("Add player"):
                if not name.strip(): st.error("Please enter a name.")
                else:
                    with Session(engine) as s:
                        faction_val = None if faction_choice == "— None —" else faction_choice
                        s.add(Player(name=name.strip(), rating=float(starting), active=True, faction=faction_val)); s.commit()
                    st.success(f"Added {name} with rating {starting:.0f}.")

        st.divider(); st.subheader("Current Players")
        with Session(engine) as s:
            show_arch = st.checkbox("Show archived players", value=False, key="pl_arch")
            q = select(Player)
            if not show_arch: q = q.where(Player.active == True)
            players = s.exec(q.order_by(Player.rating.desc())).all()
        if players:
            st.dataframe([{ "ID": p.id, "Name": p.name, "Rating": round(p.rating,1), "Faction": p.faction, "Active": p.active, "Joined": p.created_at.strftime("%Y-%m-%d") } for p in players],
                use_container_width=True, hide_index=True,
                column_config={"Rating": st.column_config.NumberColumn(format="%.1f"), "Active": st.column_config.CheckboxColumn(disabled=True)})
        else: st.info("No players yet. Add a few above.")

        # NEW: Collapsible editor to modify a player's faction
        with st.expander("Edit player faction", expanded=False):
            with Session(engine) as s:
                all_players = s.exec(select(Player).order_by(Player.name)).all()
            if not all_players:
                st.info("No players to edit.")
            else:
                labels = [f"{p.name} (ID {p.id})" for p in all_players]
                id_by_label = {labels[i]: all_players[i].id for i in range(len(all_players))}
                chosen = st.selectbox("Select a player", labels, key="edit_faction_player")
                pid = id_by_label[chosen]
                with Session(engine) as s:
                    pl = s.get(Player, pid)
                curr_idx = _faction_index_or_blank(pl.faction)
                new_faction = st.selectbox("Faction", PLACEHOLDER_FACTIONS_WITH_BLANK, index=curr_idx, key="edit_faction_choice")
                save = st.button("Save faction", key="btn_save_faction")
                if save:
                    with Session(engine) as s:
                        pl = s.get(Player, pid)
                        pl.faction = None if new_faction == "— None —" else new_faction
                        s.add(pl); s.commit()
                    st.success("Faction updated.")
        with st.expander("Archive / Restore Player", expanded=False):
                    with Session(engine) as s:
                        all_players = s.exec(select(Player).order_by(Player.name)).all()
                    if not all_players:
                        st.info("No players to manage.")
                    else:
                        options = {f"{p.name} (ID {p.id}, {round(p.rating,1)}, {'Active' if p.active else 'Archived'})": p.id for p in all_players}
                        label = st.selectbox("Choose a player", list(options.keys()), key="arch_sel")
                        pid = options[label]
                        with Session(engine) as s:
                            p = s.get(Player, pid)
                        c1, c2 = st.columns(2)
                        with c1:
                            if p and p.active and st.button("Archive", use_container_width=True, key="btn_archive_player"):
                                with Session(engine) as s:
                                    p = s.get(Player, pid); p.active = False; s.add(p); s.commit()
                                st.success("Archived."); st.rerun()
                        with c2:
                            if p and (not p.active) and st.button("Restore", use_container_width=True, key="btn_restore_player"):
                                with Session(engine) as s:
                                    p = s.get(Player, pid); p.active = True; s.add(p); s.commit()
                                st.success("Restored."); st.rerun()
        with st.expander("Delete Player (permanent)", expanded=False):
                    with Session(engine) as s:
                        del_players = s.exec(select(Player).order_by(Player.name)).all()
                    if not del_players:
                        st.info("No players to delete.")
                    else:
                        del_map = {f"{p.name} (ID {p.id})": p.id for p in del_players}
                        sel_label = st.selectbox("Select player to delete", list(del_map.keys()), key="del_player_sel")
                        sel_id = del_map[sel_label]
                        with Session(engine) as s:
                            m_count = s.exec(select(Match).where((Match.player_a_id == sel_id) | (Match.player_b_id == sel_id))).all()
                            a_count = s.exec(select(Attendance).where(Attendance.player_id == sel_id)).all()
                            matches_num = len(m_count)
                            attend_num = len(a_count)
                        st.write(f"Related records — Matches: **{matches_num}**, Attendance: **{attend_num}**.")
                        col_del1, col_del2 = st.columns([1,2])
                        with col_del1:
                            hard_delete = st.checkbox("Also delete all related matches & attendance", value=False, key="del_player_hard")
                        with col_del2:
                            confirm_text = st.text_input('Type **DELETE** to confirm', key="del_player_confirm")
                        if st.button("Delete player now", type="primary", key="btn_delete_player_now", disabled=(confirm_text.strip().upper() != "DELETE")):
                            with Session(engine) as s:
                                if hard_delete:
                                    for mm in s.exec(select(Match).where((Match.player_a_id == sel_id) | (Match.player_b_id == sel_id))).all():
                                        s.delete(mm)
                                    for aa in s.exec(select(Attendance).where(Attendance.player_id == sel_id)).all():
                                        s.delete(aa)
                                    s.commit()
                                else:
                                    m_exists = s.exec(select(Match).where((Match.player_a_id == sel_id) | (Match.player_b_id == sel_id))).first()
                                    a_exists = s.exec(select(Attendance).where(Attendance.player_id == sel_id)).first()
                                    if m_exists or a_exists:
                                        st.error("Player has related matches/attendance. Tick the checkbox to delete them too, or archive instead."); st.stop()
                                pl = s.get(Player, sel_id)
                                if pl:
                                    s.delete(pl); s.commit()
                                    st.success("Player deleted permanently."); st.rerun()
# =============== Pairings (admin) ===============
if st.session_state.get("is_admin", False) and "Pairings" in idx:
    with T[idx["Pairings"]]:
        st.subheader("Generate Pairings")
        c1, c2, c3 = st.columns(3)
        with c1:
            st.caption("Week ID uses the **Wednesday** of the week (DD/MM/YYYY).")
        with c2:
            week_str = st.text_input("Week id", value=week_id_wed(date.today()))
        with c3: 
            generate = st.button("Generate pairings", key="btn_generate_pairings")

        # Inline reset controls for the same week (tidier UI)
        with Session(engine) as s:
            _ms = s.exec(select(Match).where(Match.week == week_str)).all()
        _total = len(_ms); _pending = sum(1 for m in _ms if m.result == "pending"); _reported = _total - _pending
        st.caption(f"{week_str}: {_total} matches ({_pending} pending, {_reported} reported)")
        colr1, colr2 = st.columns([1,2])
        with colr1:
            include_reported = st.checkbox("Also delete reported (irrevocable)", value=False, key="reset_include_reported")
        with colr2:
            if st.button("Reset pairings for this week", key="btn_reset_pairings"):
                with Session(engine) as s:
                    ms = s.exec(select(Match).where(Match.week == week_str)).all()
                    for m in ms:
                        if include_reported or m.result == "pending":
                            s.delete(m)
                    s.commit()
                st.success("Deleted matches. Use 'Generate pairings' to recreate."); st.rerun()

        # Week results password (compact)
        with Session(engine) as s:
            existing_pw = week_password(s, week_str)
        with st.expander("Week results password", expanded=False):
            st.caption("Lock this week's result submissions behind a password.")
            pw_new = st.text_input("New password", type="password", key="wk_pw_new")
            cols_pw = st.columns([1,1,1])
            with cols_pw[0]:
                if st.button("Set / Update password", key="btn_set_wk_pw"):
                    with Session(engine) as s:
                        try:
                            _val = (pw_new or "").strip()
                            if _val:
                                set_week_password(s, week_str, _val)
                                st.success("Password set/updated.")
                            else:
                                clear_week_password(s, week_str)
                                st.success("Password cleared.")
                        except Exception as e:
                            st.error(f"Error updating password: {e}")
            with cols_pw[1]:
                if st.button("Clear password", key="btn_clear_wk_pw"):
                    with Session(engine) as s:
                        clear_week_password(s, week_str)
                    st.success("Password cleared.")
            with cols_pw[2]:
                st.metric("Current status", "Set" if existing_pw else "Not set")


        st.divider(); st.subheader("Weekly attendance")
        with Session(engine) as s:
            active_players = s.exec(select(Player).where(Player.active == True).order_by(Player.name)).all()
            already_present = {r.player_id for r in s.exec(select(Attendance).where(Attendance.week == week_str)).all() if r.present}
        labels = [f"{p.name} (ID {p.id}, {round(p.rating,1)})" for p in active_players]
        map_id = {labels[i]: active_players[i].id for i in range(len(active_players))}
        defaults = [lbl for lbl in labels if map_id[lbl] in already_present]
        sel = st.multiselect("Players present this week", labels, default=defaults); selected_ids = {map_id[lbl] for lbl in sel}
        a1, a2 = st.columns(2)
        with a1:
            if st.button("Save attendance", key="btn_save_attendance"):
                with Session(engine) as s:
                    existing = s.exec(select(Attendance).where(Attendance.week == week_str)).all()
                    for r in existing: s.delete(r)
                    for pid in selected_ids: s.add(Attendance(week=week_str, player_id=pid, present=True))
                    s.commit(); st.success("Attendance saved.")
        with a2:
            if st.button("Clear attendance", key="btn_clear_attendance"):
                with Session(engine) as s:
                    existing = s.exec(select(Attendance).where(Attendance.week == week_str)).all()
                    for r in existing: s.delete(r)
                    s.commit(); st.success("Attendance cleared.")

        if generate:
            with Session(engine) as s:
                existing = s.exec(select(Match).where(Match.week == week_str)).all()
                if existing: st.error(f"Pairings already exist for {week_str} ({len(existing)}). Reset above to redo.")
                else:
                    attendance_ids = {r.player_id for r in s.exec(select(Attendance).where(Attendance.week == week_str)).all() if r.present}
                    restrict = attendance_ids if attendance_ids else None
                    created = generate_weekly_pairings(s, week_str, restrict_to=restrict)
                    st.success(f"Created {len(created)} matches." if created else "No players to pair.")

        st.divider(); st.subheader("Weekly Pairings")
        lookup = st.text_input("Look up week (DD/MM/YYYY)", value=week_str, key="lookup_pair")
        with Session(engine) as s:
            matches = s.exec(select(Match).where(Match.week == lookup).order_by(Match.id)).all(); pmap = get_player_map(s)
        if matches:
            rows = [{"Match ID": m.id, "A": f"{pmap[m.player_a_id].name} (#{m.player_a_id})", "B": (f"{pmap[m.player_b_id].name} (#{m.player_b_id})" if m.player_b_id else "BYE"), "Result": m.result} for m in matches]
            st.dataframe(rows, use_container_width=True, hide_index=True)

            # Inline delete control for no-shows
            st.caption("Remove no-show pairings below. By default, only pending matches can be deleted.")
            allow_reported_delete = st.checkbox("Allow deleting reported results (dangerous)", value=False, key="del_allow_reported_inline")
            options = {}
            for mm in (matches if allow_reported_delete else [x for x in matches if x.result == "pending"]):
                a = pmap.get(mm.player_a_id); b = pmap.get(mm.player_b_id) if mm.player_b_id else None
                label = f"#{mm.id}: {a.name if a else mm.player_a_id} vs {b.name if b else 'BYE'} — result={mm.result}"
                options[label] = mm.id
            if options:
                sel_labels = st.multiselect("Select pairings to delete", list(options.keys()), key="delete_sel_inline")
                selected_ids = [options[l] for l in sel_labels]
                if st.button("Delete selected pairings", key="btn_delete_inline"):
                    with Session(engine) as s:
                        for mid in selected_ids:
                            m = s.get(Match, mid)
                            if m is None: continue
                            if (not allow_reported_delete) and m.result != "pending":
                                continue
                            s.delete(m)
                        s.commit()
                    st.success(f"Deleted {len(selected_ids)} pairing(s). ")
                    st.rerun()
            else:
                st.info("No matches eligible for deletion.")
        else:
            st.info("No matches for that week.")

        st.divider(); st.subheader("Manual pairing editor (admin)")
        st.caption("Enter a comma-separated list of player IDs to be paired in order: (1,2), (3,4), ... Use BYE token for an odd player.")
        with Session(engine) as s:
            eligible_ids = [p.id for p in s.exec(select(Player).where(Player.active == True).order_by(Player.name)).all()]
            attendance_ids = {r.player_id for r in s.exec(select(Attendance).where(Attendance.week == week_str)).all() if r.present}
            eligible_ids = [i for i in eligible_ids if i in (attendance_ids if attendance_ids else set(eligible_ids))]
            eligible_names = {}
            for pid in eligible_ids:
                pl = s.get(Player, pid)
                eligible_names[pid] = f"{pl.name} (#{pl.id}, {round(pl.rating,1)})" if pl else f"#{pid}"
        st.write("Eligible this week:")
        if eligible_ids:
            st.code(", ".join(f"{pid}:{eligible_names[pid]}" for pid in eligible_ids))
        else:
            st.info("No eligible players (check attendance).")
        manual_order = st.text_input("Manual pairing order (IDs)", placeholder="e.g. 7,12,3,9,18,21 or 7,12,3 (BYE)")
        clear_pending = st.checkbox("Clear existing pending matches for this week before applying", value=True)
        if st.button("Apply manual pairings", key="btn_apply_manual"):
            tokens = [t.strip().upper() for t in manual_order.split(",") if t.strip()]
            ids: List[int] = []
            has_bye_token = False
            for t in tokens:
                if t == "BYE":
                    has_bye_token = True
                    continue
                if not t.isdigit():
                    st.error(f"Invalid token: '{t}'. Use integers or 'BYE'."); break
                ids.append(int(t))
            else:
                if len(ids) == 0:
                    st.error("Please enter at least two player IDs.")
                elif len(ids) != len(set(ids)):
                    st.error("Duplicate IDs detected; each player can appear once.")
                else:
                    with Session(engine) as s:
                        if clear_pending:
                            pend = s.exec(select(Match).where((Match.week == week_str) & (Match.result == "pending"))).all()
                            for m in pend: s.delete(m)
                            s.commit()
                        created = 0
                        i = 0
                        while i < len(ids):
                            a = ids[i]
                            b = None
                            if i+1 < len(ids):
                                b = ids[i+1]
                                i += 2
                            else:
                                i += 1  # last one BYE
                            m = Match(week=week_str, player_a_id=a, player_b_id=b, result="pending")
                            s.add(m); s.commit(); created += 1
                        st.success(f"Manual pairings applied: {created} matches created.")

# =============== Ad-Hoc Match (admin) ===============
if st.session_state.get("is_admin", False) and "Ad-Hoc Match" in idx:
    with T[idx["Ad-Hoc Match"]]:
        st.subheader("Ad-Hoc Match")
        adhoc_week = st.text_input("Week for ad-hoc", value=week_id_wed(date.today()), key="adhoc_w")
        with Session(engine) as s:
            include_arch = st.checkbox("Include archived players", value=False, key="adhoc_arch")
            q = select(Player) 
            if not include_arch: q = q.where(Player.active == True)
            plist = s.exec(q.order_by(Player.name)).all()
        if not plist: st.info("No players available.")
        else:
            labels = [f"{p.name} (ID {p.id}, {int(round(p.rating))})" for p in plist]
            id_by_label = {labels[i]: plist[i].id for i in range(len(plist))}
            c1, c2 = st.columns(2)
            with c1: la = st.selectbox("Player A", options=labels, key="adhoc_a")
            with c2: lb = st.selectbox("Player B", options=labels, key="adhoc_b")
            k_choice2 = st.radio("K-factor", ["Casual (10)", "Competitive (40)"], index=1, horizontal=True, key="adhoc_k")
            result2 = st.selectbox("Result", ["a_win", "b_win", "draw"], index=0, key="adhoc_r")

            factions = PLACEHOLDER_FACTIONS
            # Default faction pickers to the players' saved faction (or blank)
            with Session(engine) as s:
                pa_tmp = s.get(Player, id_by_label[la]); pb_tmp = s.get(Player, id_by_label[lb])
            with Session(engine) as _s_stats2:

                a_most = most_played_faction(_s_stats2, pa_tmp.id) if pa_tmp else None

                b_most = most_played_faction(_s_stats2, pb_tmp.id) if pb_tmp else None

            a_def_idx = (factions.index(a_most) if (a_most in factions) else (factions.index(pa_tmp.faction) if (pa_tmp and pa_tmp.faction in factions) else 0))

            b_def_idx = (factions.index(b_most) if (b_most in factions) else (factions.index(pb_tmp.faction) if (pb_tmp and pb_tmp.faction in factions) else 0))
            a_faction2 = st.selectbox("Player A faction", PLACEHOLDER_FACTIONS_WITH_BLANK, index=a_def_idx, key="adhoc_af")
            b_faction2 = st.selectbox("Player B faction", PLACEHOLDER_FACTIONS_WITH_BLANK, index=b_def_idx, key="adhoc_bf")
            a_faction2 = None if a_faction2 == "— None —" else a_faction2
            b_faction2 = None if b_faction2 == "— None —" else b_faction2
            disabled = (la == lb)
            if disabled:
                st.caption("Players must differ.")
            if st.button("Save ad-hoc result", key="btn_save_adhoc", disabled=disabled):
                with Session(engine) as s3:
                    pid_a = id_by_label[la]; pid_b = id_by_label[lb]
                    pa = s3.get(Player, pid_a); pb = s3.get(Player, pid_b)
                    m = Match(week=adhoc_week, player_a_id=pid_a, player_b_id=pid_b, result="pending", a_rating_before=pa.rating, b_rating_before=pb.rating, a_faction=a_faction2, b_faction=b_faction2)
                    s3.add(m); s3.commit(); s3.refresh(m)
                    k = 10 if k_choice2.startswith("Casual") else 40; score_a = 1.0 if result2 == "a_win" else 0.0 if result2 == "b_win" else 0.5
                    new_a, new_b = update_elo(pa.rating, pb.rating, score_a, k)
                    m.result = result2; m.a_rating_after = new_a; m.b_rating_after = new_b; m.k_factor_used = int(k); m.reported_at = datetime.utcnow()
                    pa.rating = new_a; pb.rating = new_b; s3.add(pa); s3.add(pb); s3.add(m); s3.commit()
                    st.success(f"Ad-hoc match saved (K={k}). Match {m.id}.")
                    pa.rating = new_a; pb.rating = new_b; s3.add(pa); s3.add(pb); s3.add(m); s3.commit()
                    st.success(f"Ad-hoc match saved (K={k}). Match {m.id}.")



def clear_week_password(session: Session, week: str) -> None:
    wk = session.exec(select(WeekKey).where(WeekKey.week == week)).first()
    if wk:
        session.delete(wk); session.commit()
