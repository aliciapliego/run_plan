
# app.py
# Run: python -m streamlit run app.py

import io
import os
import re
import sqlite3
from datetime import date, datetime

import altair as alt
import numpy as np
import pandas as pd
import streamlit as st

# ---------------- CONFIG ----------------
TEMPLATE_XLSX_PATH = "/mnt/data/template.xlsx"
DB_PATH = "/mnt/data/marathon_app.db"

REQUIRED_SHEETS = ["Endurance", "Sessions", "Strength_exercises"]

ATHLETE_NAME = "Hans Wegen"
GOAL_NAME = "Marathon"
RACE_DATE = date(2026, 4, 12)
CLIENT_ID = "hans_wegen"

DEFAULT_RUN_PACE_EASY_MIN_KM = 5.25
DEFAULT_RUN_PACE_QUALITY_MIN_KM = 4.50

st.set_page_config(page_title="Hans Wegen Marathon Plan", layout="wide")

# ---------------- STYLE ----------------
st.markdown("""
<style>
.block-container{padding-top:1rem;}
.kpi-card{
  background:linear-gradient(180deg,rgba(255,255,255,.96),rgba(255,255,255,.86));
  border:1px solid rgba(120,120,120,.10);
  border-radius:16px;
  padding:14px 16px;
  box-shadow:0 8px 24px rgba(0,0,0,.05);
}
.kpi-title{font-size:.84rem;color:rgba(30,30,30,.65);margin-bottom:4px;}
.kpi-value{font-size:1.45rem;font-weight:800;margin:0;}
.kpi-sub{font-size:.8rem;color:rgba(30,30,30,.55);margin-top:4px;}
.small{font-size:.9rem;color:rgba(30,30,30,.68);}
.badge{
  display:inline-block;padding:4px 10px;border-radius:999px;font-size:.75rem;
  border:1px solid rgba(0,0,0,.08);background:rgba(0,0,0,.03);
}
</style>
""", unsafe_allow_html=True)

def kpi(title, value, sub=""):
    st.markdown(
        f"""
        <div class="kpi-card">
          <div class="kpi-title">{title}</div>
          <p class="kpi-value">{value}</p>
          <div class="kpi-sub">{sub}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

# ---------------- HELPERS ----------------
def ensure_dirs():
    os.makedirs("/mnt/data", exist_ok=True)

def to_num(x):
    try:
        if pd.isna(x):
            return np.nan
        if isinstance(x, (int, float, np.number)):
            return float(x)
        s = str(x).strip().replace(",", ".")
        if s == "" or s.lower() == "nan":
            return np.nan
        return float(s)
    except Exception:
        return np.nan

def txtcol(df, col, default=""):
    if col not in df.columns:
        return pd.Series([default] * len(df))
    return df[col].astype(str).fillna(default).str.strip()

def numcol(df, col, default=np.nan):
    if col not in df.columns:
        return pd.Series([default] * len(df))
    return df[col].apply(to_num)

def parse_any_date(x):
    if pd.isna(x):
        return None
    if isinstance(x, (pd.Timestamp, datetime)):
        return x.date()
    if isinstance(x, date):
        return x
    s = str(x).strip()
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d.%m.%Y", "%d-%m-%Y", "%m/%d/%Y"):
        try:
            return datetime.strptime(s, fmt).date()
        except Exception:
            pass
    return None

def range_to_num(s):
    if s is None:
        return np.nan
    t = str(s).strip().replace(",", ".")
    if t == "" or t.lower() == "nan":
        return np.nan
    m = re.match(r"^\s*(\d+(\.\d+)?)\s*-\s*(\d+(\.\d+)?)\s*$", t)
    if m:
        return (float(m.group(1)) + float(m.group(3))) / 2.0
    try:
        return float(t)
    except Exception:
        return np.nan

def arrow_safe(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for c in out.columns:
        if out[c].dtype == "object":
            out[c] = out[c].astype(str)
    return out

def endurance_key(row) -> str:
    d = row.get("_date")
    d_txt = d.isoformat() if isinstance(d, date) else str(row.get("Date", ""))
    return f"W{int(row.get('Week', 0))}_{d_txt}_{row.get('Discipline','')}_{row.get('Session','')}"

def read_template_sheets():
    if not os.path.exists(TEMPLATE_XLSX_PATH):
        raise FileNotFoundError(f"Missing template: {TEMPLATE_XLSX_PATH}")
    xls = pd.ExcelFile(TEMPLATE_XLSX_PATH)
    missing = [s for s in REQUIRED_SHEETS if s not in xls.sheet_names]
    if missing:
        raise ValueError(f"Template is missing sheets: {missing}")
    return {s: pd.read_excel(xls, sheet_name=s) for s in REQUIRED_SHEETS}

def to_excel_bytes(sheets: dict) -> bytes:
    out = io.BytesIO()
    with pd.ExcelWriter(out, engine="openpyxl") as writer:
        for name, df in sheets.items():
            df.to_excel(writer, index=False, sheet_name=name)
    return out.getvalue()

# ---------------- DB ----------------
def db():
    ensure_dirs()
    con = sqlite3.connect(DB_PATH, check_same_thread=False)

    con.execute("""
        CREATE TABLE IF NOT EXISTS client_data (
            client_id TEXT PRIMARY KEY,
            updated_at TEXT NOT NULL,
            endurance_csv TEXT NOT NULL,
            sessions_csv TEXT NOT NULL,
            strength_exercises_csv TEXT NOT NULL
        )
    """)

    con.execute("""
        CREATE TABLE IF NOT EXISTS daily_checkin (
            client_id TEXT NOT NULL,
            log_date TEXT NOT NULL,
            done INTEGER DEFAULT 0,
            sleep_hours REAL,
            sleep_quality INTEGER,
            PRIMARY KEY (client_id, log_date)
        )
    """)

    con.execute("""
        CREATE TABLE IF NOT EXISTS endurance_completion (
            client_id TEXT NOT NULL,
            endurance_key TEXT NOT NULL,
            plan_week INTEGER,
            actual_date TEXT NOT NULL,
            done INTEGER DEFAULT 1,
            actual_minutes REAL,
            PRIMARY KEY (client_id, endurance_key)
        )
    """)

    con.execute("""
        CREATE TABLE IF NOT EXISTS strength_completion (
            client_id TEXT NOT NULL,
            session_id TEXT NOT NULL,
            plan_week INTEGER,
            actual_date TEXT NOT NULL,
            done INTEGER DEFAULT 1,
            PRIMARY KEY (client_id, session_id)
        )
    """)

    con.execute("""
        CREATE TABLE IF NOT EXISTS strength_sets_log (
            client_id TEXT NOT NULL,
            log_date TEXT NOT NULL,
            session_id TEXT NOT NULL,
            exercise TEXT NOT NULL,
            set_number INTEGER NOT NULL,
            reps_done REAL,
            weight_kg REAL,
            rir_real REAL,
            note TEXT,
            PRIMARY KEY (client_id, log_date, session_id, exercise, set_number)
        )
    """)
    con.commit()
    return con

def df_to_csv_text(df: pd.DataFrame) -> str:
    return df.to_csv(index=False)

def csv_text_to_df(txt: str) -> pd.DataFrame:
    return pd.read_csv(io.StringIO(txt))

def save_client(client_id: str, endurance: pd.DataFrame, sessions: pd.DataFrame, exercises: pd.DataFrame):
    con = db()
    now = datetime.utcnow().isoformat()
    con.execute(
        """
        INSERT INTO client_data(client_id, updated_at, endurance_csv, sessions_csv, strength_exercises_csv)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(client_id) DO UPDATE SET
          updated_at=excluded.updated_at,
          endurance_csv=excluded.endurance_csv,
          sessions_csv=excluded.sessions_csv,
          strength_exercises_csv=excluded.strength_exercises_csv
        """,
        (client_id, now, df_to_csv_text(endurance), df_to_csv_text(sessions), df_to_csv_text(exercises)),
    )
    con.commit()

def load_or_init_client(client_id: str):
    con = db()
    cur = con.cursor()
    cur.execute(
        "SELECT endurance_csv, sessions_csv, strength_exercises_csv FROM client_data WHERE client_id=?",
        (client_id,),
    )
    row = cur.fetchone()
    if row:
        return {
            "Endurance": csv_text_to_df(row[0]),
            "Sessions": csv_text_to_df(row[1]),
            "Strength_exercises": csv_text_to_df(row[2]),
        }

    tpl = read_template_sheets()
    save_client(client_id, tpl["Endurance"], tpl["Sessions"], tpl["Strength_exercises"])
    return tpl

# ---------------- SIMPLE DAILY CHECK-IN ----------------
def get_daily_checkin(client_id: str, d: date):
    con = db()
    cur = con.cursor()
    cur.execute(
        """
        SELECT done, sleep_hours, sleep_quality
        FROM daily_checkin
        WHERE client_id=? AND log_date=?
        """,
        (client_id, d.isoformat()),
    )
    row = cur.fetchone()
    if not row:
        return {"done": 0, "sleep_hours": None, "sleep_quality": 3}
    return {
        "done": row[0] or 0,
        "sleep_hours": row[1],
        "sleep_quality": row[2] if row[2] is not None else 3,
    }

def upsert_daily_checkin(client_id: str, d: date, done: int, sleep_hours, sleep_quality):
    con = db()
    con.execute(
        """
        INSERT INTO daily_checkin(client_id, log_date, done, sleep_hours, sleep_quality)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(client_id, log_date) DO UPDATE SET
          done=excluded.done,
          sleep_hours=excluded.sleep_hours,
          sleep_quality=excluded.sleep_quality
        """,
        (client_id, d.isoformat(), int(done), to_num(sleep_hours), int(sleep_quality)),
    )
    con.commit()

# ---------------- FLEXIBLE SESSION COMPLETION ----------------
def upsert_endurance_completion(client_id: str, e_key: str, plan_week: int, actual_date: date, actual_minutes=None):
    con = db()
    con.execute(
        """
        INSERT INTO endurance_completion(client_id, endurance_key, plan_week, actual_date, done, actual_minutes)
        VALUES (?, ?, ?, ?, 1, ?)
        ON CONFLICT(client_id, endurance_key) DO UPDATE SET
          plan_week=excluded.plan_week,
          actual_date=excluded.actual_date,
          done=1,
          actual_minutes=excluded.actual_minutes
        """,
        (client_id, e_key, int(plan_week), actual_date.isoformat(), to_num(actual_minutes)),
    )
    con.commit()

def upsert_strength_completion(client_id: str, session_id: str, plan_week: int, actual_date: date):
    con = db()
    con.execute(
        """
        INSERT INTO strength_completion(client_id, session_id, plan_week, actual_date, done)
        VALUES (?, ?, ?, ?, 1)
        ON CONFLICT(client_id, session_id) DO UPDATE SET
          plan_week=excluded.plan_week,
          actual_date=excluded.actual_date,
          done=1
        """,
        (client_id, session_id, int(plan_week), actual_date.isoformat()),
    )
    con.commit()

def get_endurance_completion_df(client_id: str):
    con = db()
    return pd.read_sql_query(
        "SELECT * FROM endurance_completion WHERE client_id=?",
        con,
        params=(client_id,),
    )

def get_strength_completion_df(client_id: str):
    con = db()
    return pd.read_sql_query(
        "SELECT * FROM strength_completion WHERE client_id=?",
        con,
        params=(client_id,),
    )

# ---------------- SETS LOG ----------------
def get_sets_log(client_id: str, d: date, session_id: str):
    con = db()
    return pd.read_sql_query(
        """
        SELECT exercise, set_number, reps_done, weight_kg, rir_real, note
        FROM strength_sets_log
        WHERE client_id=? AND log_date=? AND session_id=?
        ORDER BY exercise, set_number
        """,
        con,
        params=(client_id, d.isoformat(), session_id),
    )

def save_sets_log(client_id: str, d: date, session_id: str, df_sets: pd.DataFrame):
    con = db()
    con.execute(
        "DELETE FROM strength_sets_log WHERE client_id=? AND log_date=? AND session_id=?",
        (client_id, d.isoformat(), session_id),
    )
    for _, r in df_sets.iterrows():
        con.execute(
            """
            INSERT INTO strength_sets_log
            (client_id, log_date, session_id, exercise, set_number, reps_done, weight_kg, rir_real, note)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                client_id,
                d.isoformat(),
                session_id,
                str(r.get("exercise", "")).strip(),
                int(r.get("set_number", 1)),
                to_num(r.get("reps_done")),
                to_num(r.get("weight_kg")),
                to_num(r.get("rir_real")),
                str(r.get("note", "")).strip(),
            ),
        )
    con.commit()

# ---------------- PLAN PREP ----------------
def classify_quality(session_name: str) -> bool:
    s = (session_name or "").lower()
    keys = ["threshold", "tempo", "interval", "pace", "marathon pace", "mp", "key", "steady"]
    return any(k in s for k in keys)

def build_data(endurance: pd.DataFrame, sessions: pd.DataFrame, exercises: pd.DataFrame):
    e = endurance.copy()
    s = sessions.copy()
    x = exercises.copy()

    # ---- ENDURANCE ----
    required_e = ["Week", "Date", "Day", "Discipline", "Session", "Minutes", "sRPE_num"]
    for c in required_e:
        if c not in e.columns:
            raise ValueError(f"Endurance is missing column '{c}'")

    e["Week"] = numcol(e, "Week").astype("Int64")
    e["Minutes"] = numcol(e, "Minutes").fillna(0)
    e["sRPE_num"] = numcol(e, "sRPE_num").fillna(0)
    e["Load_session"] = e["Minutes"] * e["sRPE_num"]
    e["_date"] = e["Date"].apply(parse_any_date)
    e["Discipline"] = txtcol(e, "Discipline")
    e["Session"] = txtcol(e, "Session")
    e["Day"] = txtcol(e, "Day")
    e["Interval_details_and_objective"] = txtcol(e, "Interval_details_and_objective")
    e["RPE_blocks"] = txtcol(e, "RPE_blocks")
    e["Cadence_obj"] = txtcol(e, "Cadence_obj")
    e["Run_min(blocks)"] = numcol(e, "Run_min(blocks)").fillna(e["Minutes"])

    quality_mask = e["Session"].apply(classify_quality)
    e["Run_km_est"] = np.where(
        txtcol(e, "Discipline").str.lower().str.contains("run"),
        np.where(quality_mask, e["Run_min(blocks)"] / DEFAULT_RUN_PACE_QUALITY_MIN_KM,
                 e["Run_min(blocks)"] / DEFAULT_RUN_PACE_EASY_MIN_KM),
        0.0
    )
    e["Run_km_est"] = np.round(e["Run_km_est"], 2)
    e["endurance_key"] = e.apply(endurance_key, axis=1)

    end_week = e.groupby("Week", dropna=False).agg(
        Endurance_load_week=("Load_session", "sum"),
        Run_km_week_est=("Run_km_est", "sum"),
        Endurance_sessions_planned=("endurance_key", "count"),
    ).reset_index()

    # ---- STRENGTH SESSIONS ----
    required_s = ["SesionID", "Semana", "Fecha", "Dia", "Sesion", "Minutos_sesion", "sRPE_sesion"]
    for c in required_s:
        if c not in s.columns:
            raise ValueError(f"Sessions is missing column '{c}'")

    s["SesionID"] = txtcol(s, "SesionID")
    s["Semana"] = numcol(s, "Semana").astype("Int64")
    s["Fecha"] = s["Fecha"].apply(parse_any_date)
    s["Minutos_sesion"] = numcol(s, "Minutos_sesion").fillna(0)
    s["sRPE_sesion"] = numcol(s, "sRPE_sesion").fillna(0)
    s["planned_strength_load"] = s["Minutos_sesion"] * s["sRPE_sesion"]

    str_week = s.groupby("Semana", dropna=False).agg(
        Strength_load_week=("planned_strength_load", "sum"),
        Strength_sessions_planned=("SesionID", "count"),
    ).reset_index().rename(columns={"Semana": "Week"})

    # ---- STRENGTH EXERCISES ----
    required_x = ["SesionID", "Week", "Exercise", "Musculo", "Series", "Reps", "Kg", "RIR_objetive"]
    for c in required_x:
        if c not in x.columns:
            raise ValueError(f"Strength_exercises is missing column '{c}'")

    x["SesionID"] = txtcol(x, "SesionID")
    x["Week"] = numcol(x, "Week").astype("Int64")
    x["Exercise"] = txtcol(x, "Exercise")
    x["Musculo"] = txtcol(x, "Musculo")
    x["Series"] = numcol(x, "Series").fillna(0)
    x["Reps_text"] = x["Reps"].astype(str).str.strip()
    x["Reps_num"] = x["Reps_text"].apply(range_to_num).fillna(0)
    x["Kg"] = numcol(x, "Kg")
    x["RIR_target_text"] = x["RIR_objetive"].astype(str).str.strip()
    x["RIR_target_num"] = x["RIR_target_text"].apply(range_to_num)
    x["Tonnage"] = x["Series"] * x["Reps_num"] * x["Kg"].fillna(0)
    x["Effective_Series_calc"] = np.where(x["RIR_target_num"] <= 4, x["Series"], 0)

    tonnage_week = x.groupby("Week", dropna=False).agg(
        Strength_tonnage_week=("Tonnage", "sum"),
        Effective_series_week=("Effective_Series_calc", "sum"),
    ).reset_index()

    dash = end_week.merge(str_week, on="Week", how="outer").merge(tonnage_week, on="Week", how="left").fillna(0)
    dash["Total_load_week"] = dash["Endurance_load_week"] + dash["Strength_load_week"]
    dash = dash.sort_values("Week")

    return e, s, x, dash

def add_completion_metrics(dash: pd.DataFrame, e_done: pd.DataFrame, s_done: pd.DataFrame):
    out = dash.copy()

    if e_done.empty:
        e_week = pd.DataFrame(columns=["Week", "Endurance_sessions_done"])
    else:
        e_week = e_done.groupby("plan_week", dropna=False).size().reset_index(name="Endurance_sessions_done").rename(columns={"plan_week": "Week"})

    if s_done.empty:
        s_week = pd.DataFrame(columns=["Week", "Strength_sessions_done"])
    else:
        s_week = s_done.groupby("plan_week", dropna=False).size().reset_index(name="Strength_sessions_done").rename(columns={"plan_week": "Week"})

    out = out.merge(e_week, on="Week", how="left").merge(s_week, on="Week", how="left").fillna(0)
    out["Endurance_completion_%"] = np.where(out["Endurance_sessions_planned"] > 0, 100 * out["Endurance_sessions_done"] / out["Endurance_sessions_planned"], 0)
    out["Strength_completion_%"] = np.where(out["Strength_sessions_planned"] > 0, 100 * out["Strength_sessions_done"] / out["Strength_sessions_planned"], 0)
    return out

# ---------------- LOAD DATA ----------------
state = load_or_init_client(CLIENT_ID)
endurance_calc, sessions_calc, exercises_calc, dash = build_data(
    state["Endurance"], state["Sessions"], state["Strength_exercises"]
)

endurance_done_df = get_endurance_completion_df(CLIENT_ID)
strength_done_df = get_strength_completion_df(CLIENT_ID)
dash2 = add_completion_metrics(dash, endurance_done_df, strength_done_df)

# ---------------- HEADER ----------------
st.title("Hans Wegen Marathon Plan")
st.markdown("<span class='small'>A simpler daily check-in, English labels, and flexible workout logging for busy days.</span>", unsafe_allow_html=True)

# ---------------- SIDEBAR ----------------
week_list = sorted([int(w) for w in dash2["Week"].dropna().unique()])
if not week_list:
    week_list = [0]

if "selected_plan_week" not in st.session_state:
    st.session_state["selected_plan_week"] = week_list[0]

week_ranges = {}
for w in week_list:
    dts = [d for d in endurance_calc.loc[endurance_calc["Week"] == w, "_date"].tolist() if d is not None]
    if dts:
        week_ranges[w] = (min(dts), max(dts))
    else:
        sd = [d for d in sessions_calc.loc[sessions_calc["Semana"] == w, "Fecha"].tolist() if d is not None]
        week_ranges[w] = (min(sd), max(sd)) if sd else (None, None)

with st.sidebar:
    st.markdown(f"## {ATHLETE_NAME}")
    st.markdown(f"**Goal:** {GOAL_NAME}")
    st.markdown(f"**Race date:** {RACE_DATE.strftime('%d %b %Y')}")
    countdown_days = (RACE_DATE - date.today()).days
    st.metric("Countdown", f"{countdown_days} days" if countdown_days >= 0 else f"+{abs(countdown_days)} days")

    st.divider()
    st.subheader("Plan week")
    selected_week = st.selectbox("Week", week_list, index=week_list.index(st.session_state["selected_plan_week"]))
    st.session_state["selected_plan_week"] = selected_week

    start_d, end_d = week_ranges.get(selected_week, (None, None))
    if start_d and end_d:
        st.caption(f"{start_d.strftime('%d %b')} → {end_d.strftime('%d %b %Y')}")
    else:
        st.caption("No dates available")

    row = dash2[dash2["Week"] == selected_week]
    if not row.empty:
        weekly_km = float(row["Run_km_week_est"].iloc[0])
        st.metric("Weekly total km (est)", f"{weekly_km:.1f}")
        st.metric("Endurance done", f"{float(row['Endurance_sessions_done'].iloc[0]):.0f} / {float(row['Endurance_sessions_planned'].iloc[0]):.0f}")
        st.metric("Strength done", f"{float(row['Strength_sessions_done'].iloc[0]):.0f} / {float(row['Strength_sessions_planned'].iloc[0]):.0f}")

    if st.button("Reset from template"):
        tpl = read_template_sheets()
        save_client(CLIENT_ID, tpl["Endurance"], tpl["Sessions"], tpl["Strength_exercises"])
        st.success("Template restored. Refresh the page.")

# ---------------- TABS ----------------
tab_today, tab_week, tab_plan, tab_export = st.tabs(["Today", "Week", "Plan", "Export"])

# =============== TAB: TODAY ===============
with tab_today:
    st.subheader("Daily check-in")
    selected_date = st.date_input("Date", value=date.today())

    checkin = get_daily_checkin(CLIENT_ID, selected_date)
    c1, c2, c3 = st.columns(3)
    with c1:
        done_today = st.checkbox("Done", value=bool(checkin["done"]))
    with c2:
        sleep_hours = st.number_input("Sleep (hours)", min_value=0.0, max_value=14.0, value=float(checkin["sleep_hours"] or 0.0), step=0.25)
    with c3:
        sleep_quality = st.slider("Sleep quality", min_value=1, max_value=5, value=int(checkin["sleep_quality"] or 3))

    if st.button("Save daily check-in"):
        upsert_daily_checkin(
            CLIENT_ID,
            selected_date,
            int(done_today),
            sleep_hours if sleep_hours > 0 else None,
            sleep_quality,
        )
        st.success("Daily check-in saved.")

    st.markdown("---")
    st.subheader("Flexible workout logging")
    st.caption("You can move workouts to a different day. Pick the week, then choose the run session and/or strength session you actually did today.")

    flex_week = st.selectbox("Week to log from", week_list, index=week_list.index(st.session_state["selected_plan_week"]), key="flex_week")

    e_week = endurance_calc[endurance_calc["Week"] == flex_week].copy()
    s_week = sessions_calc[sessions_calc["Semana"] == flex_week].copy()

    suggested_e = endurance_calc[endurance_calc["_date"] == selected_date].copy()
    suggested_s = sessions_calc[sessions_calc["Fecha"] == selected_date].copy()

    col_a, col_b = st.columns(2)

    with col_a:
        st.markdown("### Run / endurance")
        if not suggested_e.empty:
            st.markdown("**Planned on this calendar date**")
            for _, r in suggested_e.iterrows():
                st.markdown(f"- {r['Day']} · **{r['Session']}** · {r['Minutes']:.0f} min")
        options_e = ["-- none --"] + [
            f"{r['Day']} | {r['Session']} | {r['Minutes']:.0f} min"
            for _, r in e_week.iterrows()
        ]
        choice_e = st.selectbox("Select endurance session completed today", options_e, key="endurance_choice")
        actual_endurance_minutes = st.number_input("Actual endurance minutes", min_value=0.0, max_value=600.0, value=0.0, step=5.0, key="actual_endurance_minutes")

        if st.button("Save endurance completion"):
            if choice_e == "-- none --":
                st.warning("Select a session first.")
            else:
                idx = options_e.index(choice_e) - 1
                chosen_row = e_week.iloc[idx]
                upsert_endurance_completion(
                    CLIENT_ID,
                    chosen_row["endurance_key"],
                    int(chosen_row["Week"]),
                    selected_date,
                    actual_endurance_minutes if actual_endurance_minutes > 0 else chosen_row["Minutes"],
                )
                st.success("Endurance session logged.")

        if choice_e != "-- none --":
            idx = options_e.index(choice_e) - 1
            chosen_row = e_week.iloc[idx]
            st.markdown(
                f"**Selected:** {chosen_row['Session']}  \n"
                f"- Discipline: {chosen_row['Discipline']}  \n"
                f"- Planned day: {chosen_row['Day']} ({chosen_row['_date']})  \n"
                f"- Planned minutes: {chosen_row['Minutes']:.0f}  \n"
                f"- Est. km: {chosen_row['Run_km_est']:.1f}"
            )
            st.write(chosen_row["Interval_details_and_objective"])

    with col_b:
        st.markdown("### Strength")
        if not suggested_s.empty:
            st.markdown("**Planned on this calendar date**")
            for _, r in suggested_s.iterrows():
                st.markdown(f"- {r['Dia']} · **Session {r['Sesion']}** · {r['Minutos_sesion']:.0f} min")
        options_s = ["-- none --"] + [
            f"{r['Dia']} | Session {r['Sesion']} | {r['Minutos_sesion']:.0f} min"
            for _, r in s_week.iterrows()
        ]
        choice_s = st.selectbox("Select strength session completed today", options_s, key="strength_choice")

        if st.button("Save strength completion"):
            if choice_s == "-- none --":
                st.warning("Select a session first.")
            else:
                idx = options_s.index(choice_s) - 1
                chosen_row = s_week.iloc[idx]
                upsert_strength_completion(
                    CLIENT_ID,
                    chosen_row["SesionID"],
                    int(chosen_row["Semana"]),
                    selected_date,
                )
                st.success("Strength session logged.")

        if choice_s != "-- none --":
            idx = options_s.index(choice_s) - 1
            chosen_row = s_week.iloc[idx]
            sid = chosen_row["SesionID"]
            ex = exercises_calc[exercises_calc["SesionID"] == sid].copy()

            st.markdown(
                f"**Selected:** Session {chosen_row['Sesion']}  \n"
                f"- Planned day: {chosen_row['Dia']} ({chosen_row['Fecha']})  \n"
                f"- Planned minutes: {chosen_row['Minutos_sesion']:.0f}"
            )

            if not ex.empty:
                preview = ex[["Exercise", "Series", "Reps_text", "Kg", "RIR_target_text", "Tempo", "Rest"]].copy()
                preview = preview.rename(columns={"Reps_text": "Reps", "RIR_target_text": "RIR target"})
                st.dataframe(arrow_safe(preview), use_container_width=True)

                st.markdown("**Optional set log**")
                existing_sets = get_sets_log(CLIENT_ID, selected_date, sid)

                if existing_sets.empty:
                    rows = []
                    for _, er in ex.iterrows():
                        for i in range(1, int(er.get("Series", 0) or 0) + 1):
                            rows.append({
                                "exercise": er.get("Exercise", ""),
                                "set_number": i,
                                "reps_done": np.nan,
                                "weight_kg": er.get("Kg", np.nan),
                                "rir_real": np.nan,
                                "note": "",
                            })
                    existing_sets = pd.DataFrame(rows)

                edited_sets = st.data_editor(
                    arrow_safe(existing_sets),
                    use_container_width=True,
                    num_rows="dynamic",
                    column_config={
                        "set_number": st.column_config.NumberColumn("set_number", step=1),
                        "reps_done": st.column_config.NumberColumn("reps_done", step=1),
                        "weight_kg": st.column_config.NumberColumn("weight_kg", step=0.5),
                        "rir_real": st.column_config.NumberColumn("rir_real", step=1),
                    },
                )

                if st.button(f"Save set log — {sid}"):
                    save_sets_log(CLIENT_ID, selected_date, sid, edited_sets)
                    st.success("Set log saved.")

# =============== TAB: WEEK ===============
with tab_week:
    st.subheader("Weekly dashboard")

    c1, c2, c3, c4 = st.columns(4)
    with c1:
        kpi("Peak weekly km", f"{dash2['Run_km_week_est'].max():.1f}", "estimated from planned run minutes")
    with c2:
        kpi("Peak endurance load", f"{dash2['Endurance_load_week'].max():.0f}", "minutes × sRPE")
    with c3:
        kpi("Endurance completion", f"{dash2['Endurance_completion_%'].mean():.0f}%", "average across plan weeks")
    with c4:
        kpi("Strength completion", f"{dash2['Strength_completion_%'].mean():.0f}%", "average across plan weeks")

    st.markdown("### Weekly load")
    load_long = dash2[["Week", "Endurance_load_week", "Strength_load_week"]].melt("Week", var_name="Type", value_name="Load")
    load_chart = alt.Chart(load_long).mark_bar().encode(
        x=alt.X("Week:O"),
        y=alt.Y("Load:Q", title="Load"),
        color=alt.Color("Type:N"),
        tooltip=["Week", "Type", "Load"]
    ).properties(height=280)
    st.altair_chart(load_chart, use_container_width=True)

    st.markdown("### Weekly km")
    km_chart = alt.Chart(dash2).mark_line(point=True).encode(
        x=alt.X("Week:O"),
        y=alt.Y("Run_km_week_est:Q", title="Estimated weekly km"),
        tooltip=["Week", "Run_km_week_est"]
    ).properties(height=260)
    st.altair_chart(km_chart, use_container_width=True)

    st.markdown("### Session completion")
    comp_long = dash2[["Week", "Endurance_completion_%", "Strength_completion_%"]].melt("Week", var_name="Type", value_name="Completion")
    comp_chart = alt.Chart(comp_long).mark_line(point=True).encode(
        x=alt.X("Week:O"),
        y=alt.Y("Completion:Q", title="Completion %", scale=alt.Scale(domain=[0, 100])),
        color="Type:N",
        tooltip=["Week", "Type", "Completion"]
    ).properties(height=260)
    st.altair_chart(comp_chart, use_container_width=True)

    st.dataframe(arrow_safe(dash2), use_container_width=True)

# =============== TAB: PLAN ===============
with tab_plan:
    st.subheader("Plan preview")

    st.markdown("### Endurance")
    end_preview = endurance_calc[[
        "Week", "Date", "Day", "Discipline", "Session", "Minutes",
        "Interval_details_and_objective", "sRPE_num", "Run_km_est"
    ]].copy()
    st.dataframe(arrow_safe(end_preview.head(50)), use_container_width=True)

    st.markdown("### Strength sessions")
    ses_preview = sessions_calc[[
        "Semana", "Fecha", "Dia", "Sesion", "Fase", "Minutos_sesion", "sRPE_sesion", "SesionID"
    ]].copy()
    st.dataframe(arrow_safe(ses_preview.head(50)), use_container_width=True)

    st.markdown("### Strength exercises")
    ex_preview = exercises_calc[[
        "Week", "SesionID", "Exercise", "Musculo", "Series", "Reps_text", "Kg", "RIR_target_text", "Tempo", "Rest"
    ]].copy()
    ex_preview = ex_preview.rename(columns={"Reps_text": "Reps", "RIR_target_text": "RIR target"})
    st.dataframe(arrow_safe(ex_preview.head(80)), use_container_width=True)

# =============== TAB: EXPORT ===============
with tab_export:
    st.subheader("Export workbook and logs")

    con = db()
    daily_df = pd.read_sql_query("SELECT * FROM daily_checkin WHERE client_id=? ORDER BY log_date", con, params=(CLIENT_ID,))
    endurance_done_export = pd.read_sql_query("SELECT * FROM endurance_completion WHERE client_id=? ORDER BY actual_date", con, params=(CLIENT_ID,))
    strength_done_export = pd.read_sql_query("SELECT * FROM strength_completion WHERE client_id=? ORDER BY actual_date", con, params=(CLIENT_ID,))
    sets_df = pd.read_sql_query("SELECT * FROM strength_sets_log WHERE client_id=? ORDER BY log_date, session_id, exercise, set_number", con, params=(CLIENT_ID,))

    xlsx_bytes = to_excel_bytes({
        "Endurance": endurance_calc.drop(columns=["_date"], errors="ignore"),
        "Sessions": sessions_calc,
        "Strength_exercises": exercises_calc,
        "Weekly_summary": dash2,
        "Daily_checkin": daily_df,
        "Endurance_done": endurance_done_export,
        "Strength_done": strength_done_export,
        "Strength_sets_log": sets_df,
    })

    st.download_button(
        "Download updated Excel",
        data=xlsx_bytes,
        file_name=f"{CLIENT_ID}_marathon_dashboard.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )

st.caption("Flexible logging: workouts can be completed on a different day than planned, while still counting toward the original plan week.")
