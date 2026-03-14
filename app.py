# app.py
# Run: python -m streamlit run app.py

import io
import os
import re
import sqlite3
from datetime import datetime, date, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
import streamlit as st
import altair as alt

# ---------------- CONFIG ----------------
BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
TEMPLATE_XLSX_PATH = str(DATA_DIR / "template.xlsx")
DB_PATH = str(DATA_DIR / "fitapp.db")

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

def force_text(x):
    if pd.isna(x):
        return ""
    if isinstance(x, (pd.Timestamp, datetime)):
        return x.strftime("%d.%m.%Y")
    return str(x).strip()
    

def safe_date_list(values):
    out = []
    for v in values:
        if pd.isna(v):
            continue
        try:
            d = pd.to_datetime(v, dayfirst=True, errors="coerce")
            if pd.isna(d):
                continue
            out.append(d.date())
        except Exception:
            continue
    return out
    
def get_week_dates_from_plan(week_num, endurance_df, sessions_df):
    e_raw = endurance_df.loc[endurance_df["Week"] == week_num, "_date"].tolist()
    s_raw = sessions_df.loc[sessions_df["Week"] == week_num, "_date"].tolist()

    e_dates = safe_date_list(e_raw)
    s_dates = safe_date_list(s_raw)

    all_dates = sorted(set(e_dates + s_dates))

    if not all_dates:
        return []

    start_d = min(all_dates)
    end_d = max(all_dates)

    out = []
    cur = start_d
    while cur <= end_d:
        out.append(cur)
        cur += timedelta(days=1)

    return out


def get_plan_week_for_date(selected_date, endurance_df, sessions_df):
    e_match = endurance_df.loc[endurance_df["_date"] == selected_date, "Week"]
    if not e_match.empty:
        val = to_num(e_match.iloc[0])
        if pd.notna(val):
            return int(val)

    s_match = sessions_df.loc[sessions_df["_date"] == selected_date, "Week"]
    if not s_match.empty:
        val = to_num(s_match.iloc[0])
        if pd.notna(val):
            return int(val)

    return None
    
def ensure_dirs():
    DATA_DIR.mkdir(parents=True, exist_ok=True)

def clean_columns(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out.columns = [str(c).strip() for c in out.columns]
    return out

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
    return df[col].fillna(default).astype(str).str.strip()

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

def endurance_key(row):
    week_val = to_num(row.get("Week"))
    week_num = int(week_val) if pd.notna(week_val) else 0
    d = parse_any_date(row.get("Date"))
    d_txt = d.isoformat() if d else "no_date"
    discipline = str(row.get("Discipline", "") or "").strip()
    session_name = str(row.get("Session", "") or "").strip()
    return f"W{week_num}_{d_txt}_{discipline}_{session_name}"

def read_template_sheets():
    if not os.path.exists(TEMPLATE_XLSX_PATH):
        raise FileNotFoundError(f"Missing template: {TEMPLATE_XLSX_PATH}")
    xls = pd.ExcelFile(TEMPLATE_XLSX_PATH)
    missing = [s for s in REQUIRED_SHEETS if s not in xls.sheet_names]
    if missing:
        raise ValueError(f"Template is missing sheets: {missing}")
    return {s: clean_columns(pd.read_excel(xls, sheet_name=s)) for s in REQUIRED_SHEETS}

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

    con.execute("""
        CREATE TABLE IF NOT EXISTS body_metrics (
            client_id TEXT NOT NULL,
            log_date TEXT NOT NULL,
            weight_kg REAL,
            waist_cm REAL,
            hip_cm REAL,
            biceps_cm REAL,
            gut_status TEXT,
            note TEXT,
            PRIMARY KEY (client_id, log_date)
        )
    """)
    con.commit()
    return con

def df_to_csv_text(df: pd.DataFrame) -> str:
    return df.to_csv(index=False)

def csv_text_to_df(txt: str) -> pd.DataFrame:
    return clean_columns(pd.read_csv(io.StringIO(txt)))

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

def load_or_init_client(client_id: str, force_template: bool = False):
    if force_template:
        tpl = read_template_sheets()
        save_client(client_id, tpl["Endurance"], tpl["Sessions"], tpl["Strength_exercises"])
        return tpl

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

# ---------------- DAILY CHECK-IN ----------------
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

# ---------------- FLEXIBLE COMPLETION ----------------
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

# ---------------- SET LOG ----------------
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
        set_number_num = to_num(r.get("set_number", 1))
        set_number = int(set_number_num) if pd.notna(set_number_num) else 1

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
                set_number,
                to_num(r.get("reps_done")),
                to_num(r.get("weight_kg")),
                to_num(r.get("rir_real")),
                str(r.get("note", "")).strip(),
            ),
        )
    con.commit()

# ---------------- BODY METRICS ----------------
def get_body_metrics(client_id: str, d: date):
    con = db()
    cur = con.cursor()
    cur.execute(
        """
        SELECT weight_kg, waist_cm, hip_cm, biceps_cm, gut_status, note
        FROM body_metrics
        WHERE client_id=? AND log_date=?
        """,
        (client_id, d.isoformat()),
    )
    row = cur.fetchone()
    if not row:
        return {
            "weight_kg": None,
            "waist_cm": None,
            "hip_cm": None,
            "biceps_cm": None,
            "gut_status": "",
            "note": "",
        }

    return {
        "weight_kg": row[0],
        "waist_cm": row[1],
        "hip_cm": row[2],
        "biceps_cm": row[3],
        "gut_status": row[4] or "",
        "note": row[5] or "",
    }

def upsert_body_metrics(client_id: str, d: date, weight_kg, waist_cm, hip_cm, biceps_cm, gut_status, note=""):
    con = db()
    con.execute(
        """
        INSERT INTO body_metrics
        (client_id, log_date, weight_kg, waist_cm, hip_cm, biceps_cm, gut_status, note)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(client_id, log_date) DO UPDATE SET
          weight_kg=excluded.weight_kg,
          waist_cm=excluded.waist_cm,
          hip_cm=excluded.hip_cm,
          biceps_cm=excluded.biceps_cm,
          gut_status=excluded.gut_status,
          note=excluded.note
        """,
        (
            client_id,
            d.isoformat(),
            to_num(weight_kg),
            to_num(waist_cm),
            to_num(hip_cm),
            to_num(biceps_cm),
            str(gut_status or "").strip(),
            str(note or "").strip(),
        ),
    )
    con.commit()

def get_body_metrics_df(client_id: str):
    con = db()
    df = pd.read_sql_query(
        """
        SELECT log_date, weight_kg, waist_cm, hip_cm, biceps_cm, gut_status, note
        FROM body_metrics
        WHERE client_id=?
        ORDER BY log_date
        """,
        con,
        params=(client_id,),
    )
    if not df.empty:
        df["log_date"] = pd.to_datetime(df["log_date"], errors="coerce")
    return df
# ---------------- PLAN PREP ----------------
def classify_quality(session_name: str) -> bool:
    s = (session_name or "").lower()
    keys = ["threshold", "tempo", "interval", "pace", "marathon pace", "mp", "key", "steady", "hills", "hill"]
    return any(k in s for k in keys)

def build_data(endurance: pd.DataFrame, sessions: pd.DataFrame, exercises: pd.DataFrame):
    e = clean_columns(endurance.copy())
    s = clean_columns(sessions.copy())
    x = clean_columns(exercises.copy())

    # ---- ENDURANCE ----
       # ---- ENDURANCE ----
    required_e = [
        "Week", "Date", "Day", "Discipline", "Session", "Minutes",
        "Interval_details_and_objective", "RPE_blocks", "sRPE_num",
        "Cadence_target", "Run_min_blocks"
    ]
    for c in required_e:
        if c not in e.columns:
            raise ValueError(f"Endurance is missing column '{c}'")

    e["Week"] = numcol(e, "Week").astype("Int64")
    e["Date_raw"] = e["Date"]
    e["Date"] = pd.to_datetime(e["Date"], dayfirst=True, errors="coerce").dt.date
    e["_date"] = e["Date"]

    e["Day"] = txtcol(e, "Day")
    e["Discipline"] = txtcol(e, "Discipline")
    e["Session"] = txtcol(e, "Session")
    e["Minutes"] = numcol(e, "Minutes").fillna(0)
    e["Interval_details_and_objective"] = txtcol(e, "Interval_details_and_objective")
    e["RPE_blocks"] = txtcol(e, "RPE_blocks")
    e["sRPE_num"] = numcol(e, "sRPE_num").fillna(0)
    e["Cadence_target"] = txtcol(e, "Cadence_target")
    e["Run_min_blocks"] = numcol(e, "Run_min_blocks").fillna(e["Minutes"])

    if "Load_session" not in e.columns:
        e["Load_session"] = e["Minutes"] * e["sRPE_num"]
    else:
        e["Load_session"] = numcol(e, "Load_session").fillna(e["Minutes"] * e["sRPE_num"])

    quality_mask = e["Session"].apply(classify_quality)
    is_run = e["Discipline"].str.lower().str.contains("run", na=False)

    e["Run_km_est"] = np.where(
        is_run,
        np.where(
            quality_mask,
            e["Run_min_blocks"] / DEFAULT_RUN_PACE_QUALITY_MIN_KM,
            e["Run_min_blocks"] / DEFAULT_RUN_PACE_EASY_MIN_KM,
        ),
        0.0,
    )
    e["Run_km_est"] = np.round(e["Run_km_est"], 2)
    e["endurance_key"] = e.apply(endurance_key, axis=1)

    end_week = e.groupby("Week", dropna=False).agg(
        Endurance_load_week=("Load_session", "sum"),
        Run_km_week_est=("Run_km_est", "sum"),
        Endurance_sessions_planned=("endurance_key", "count"),
    ).reset_index()

    # ---- STRENGTH SESSIONS ----
    required_s = [
        "SesionID", "Week", "Date", "Day", "Session", "Phase",
        "Minutes_session", "sRPE_session", "Done(1/0)"
    ]
    for c in required_s:
        if c not in s.columns:
            raise ValueError(f"Sessions is missing column '{c}'")

    s["SesionID"] = txtcol(s, "SesionID")
    s["Week"] = numcol(s, "Week").astype("Int64")
    s["Date_raw"] = s["Date"]
    s["Date"] = pd.to_datetime(s["Date"], dayfirst=True, errors="coerce").dt.date
    s["_date"] = s["Date"]

    s["Day"] = txtcol(s, "Day")
    s["Session"] = txtcol(s, "Session")
    s["Phase"] = txtcol(s, "Phase")
    s["Minutes_session"] = numcol(s, "Minutes_session").fillna(0)
    s["sRPE_session"] = numcol(s, "sRPE_session").fillna(0)
    s["Done(1/0)"] = numcol(s, "Done(1/0)").fillna(0)
    s["planned_strength_load"] = s["Minutes_session"] * s["sRPE_session"]

    str_week = s.groupby("Week", dropna=False).agg(
        Strength_load_week=("planned_strength_load", "sum"),
        Strength_sessions_planned=("SesionID", "count"),
    ).reset_index()

    # ---- STRENGTH EXERCISES ----
    required_x = [
        "SesionID", "Week", "Exercise", "Muscle", "Series", "Reps",
        "Kg", "RIR_objetive", "Tempo", "Rest"
    ]
    for c in required_x:
        if c not in x.columns:
            raise ValueError(f"Strength_exercises is missing column '{c}'")

    x["SesionID"] = txtcol(x, "SesionID")
    x["Week"] = numcol(x, "Week").astype("Int64")
    x["Exercise"] = txtcol(x, "Exercise")
    x["Muscle"] = txtcol(x, "Muscle")
    x["Series"] = numcol(x, "Series").fillna(0)
    x["Kg"] = numcol(x, "Kg")

    x["Reps_text"] = x["Reps"].apply(force_text)
    x["RIR_target_text"] = x["RIR_objetive"].apply(force_text)
    x["Tempo"] = x["Tempo"].apply(force_text)
    x["Rest"] = x["Rest"].apply(force_text)

    x["Reps_num"] = x["Reps_text"].apply(range_to_num).fillna(0)
    x["RIR_target_num"] = x["RIR_target_text"].apply(range_to_num)

    if "Effective_Series" in x.columns:
        x["Effective_Series"] = numcol(x, "Effective_Series").fillna(0)
    else:
        x["Effective_Series"] = np.where(x["RIR_target_num"] <= 4, x["Series"], 0)

    if "Load_session_sRPE" in x.columns:
        x["Load_session_sRPE"] = numcol(x, "Load_session_sRPE").fillna(0)
    else:
        x["Load_session_sRPE"] = 0

    x["Tonnage"] = x["Series"] * x["Reps_num"] * x["Kg"].fillna(0)

    ton_week = x.groupby("Week", dropna=False).agg(
        Strength_tonnage_week=("Tonnage", "sum"),
        Effective_series_week=("Effective_Series", "sum"),
    ).reset_index()

    dash = (
        end_week
        .merge(str_week, on="Week", how="outer")
        .merge(ton_week, on="Week", how="left")
        .fillna(0)
        .sort_values("Week")
    )
    dash["Total_load_week"] = dash["Endurance_load_week"] + dash["Strength_load_week"]

    return e, s, x, dash

def add_completion_metrics(dash: pd.DataFrame, e_done: pd.DataFrame, s_done: pd.DataFrame):
    out = dash.copy()

    if e_done.empty:
        e_week = pd.DataFrame(columns=["Week", "Endurance_sessions_done"])
    else:
        e_done["plan_week"] = pd.to_numeric(e_done["plan_week"], errors="coerce")
        e_week = (
            e_done.groupby("plan_week", dropna=False)
            .size()
            .reset_index(name="Endurance_sessions_done")
            .rename(columns={"plan_week": "Week"})
        )

    if s_done.empty:
        s_week = pd.DataFrame(columns=["Week", "Strength_sessions_done"])
    else:
        s_done["plan_week"] = pd.to_numeric(s_done["plan_week"], errors="coerce")
        s_week = (
            s_done.groupby("plan_week", dropna=False)
            .size()
            .reset_index(name="Strength_sessions_done")
            .rename(columns={"plan_week": "Week"})
        )

    out = out.merge(e_week, on="Week", how="left").merge(s_week, on="Week", how="left").fillna(0)
    out["Endurance_completion_%"] = np.where(
        out["Endurance_sessions_planned"] > 0,
        100 * out["Endurance_sessions_done"] / out["Endurance_sessions_planned"],
        0,
    )
    out["Strength_completion_%"] = np.where(
        out["Strength_sessions_planned"] > 0,
        100 * out["Strength_sessions_done"] / out["Strength_sessions_planned"],
        0,
    )
    return out

def get_strength_progress_df(client_id: str):
    con = db()
    df = pd.read_sql_query(
        """
        SELECT log_date, session_id, exercise, set_number, reps_done, weight_kg, rir_real
        FROM strength_sets_log
        WHERE client_id=?
        ORDER BY log_date, session_id, exercise, set_number
        """,
        con,
        params=(client_id,),
    )
    if df.empty:
        return df

    df["log_date"] = pd.to_datetime(df["log_date"], errors="coerce")
    df["weight_kg"] = pd.to_numeric(df["weight_kg"], errors="coerce")
    df["reps_done"] = pd.to_numeric(df["reps_done"], errors="coerce")
    df["rir_real"] = pd.to_numeric(df["rir_real"], errors="coerce")

    summary = (
        df.groupby(["log_date", "exercise"], dropna=False)
        .agg(
            sets_logged=("set_number", "count"),
            avg_weight_kg=("weight_kg", "mean"),
            max_weight_kg=("weight_kg", "max"),
            avg_reps_done=("reps_done", "mean"),
            avg_rir_real=("rir_real", "mean"),
        )
        .reset_index()
    )
    return summary

# ---------------- LOAD DATA ----------------
state = load_or_init_client(CLIENT_ID, force_template=False)
endurance_calc, sessions_calc, exercises_calc, dash = build_data(
    state["Endurance"], state["Sessions"], state["Strength_exercises"]
)

endurance_done_df = get_endurance_completion_df(CLIENT_ID)
strength_done_df = get_strength_completion_df(CLIENT_ID)
dash2 = add_completion_metrics(dash, endurance_done_df, strength_done_df)

# ---------------- HEADER ----------------
st.title("Hans Wegen Marathon Plan")
st.markdown(
    "<span class='small'>Simple daily check-in, flexible workout logging, and strength progression tracking.</span>",
    unsafe_allow_html=True,
)

# ---------------- SIDEBAR ----------------
week_list = sorted([int(w) for w in dash2["Week"].dropna().unique()])
if not week_list:
    week_list = [0]

if "selected_plan_week" not in st.session_state:
    st.session_state["selected_plan_week"] = week_list[0]

week_ranges = {}
for w in week_list:
    dts = safe_date_list(
        endurance_calc.loc[endurance_calc["Week"] == w, "_date"].tolist()
    )

    if dts:
        week_ranges[w] = (min(dts), max(dts))
    else:
        sd = safe_date_list(
            sessions_calc.loc[sessions_calc["Week"] == w, "_date"].tolist()
        )
        week_ranges[w] = (min(sd), max(sd)) if sd else (None, None)


with st.sidebar:
    st.markdown(f"## {ATHLETE_NAME}")
    st.markdown(f"**Goal:** {GOAL_NAME}")
    st.markdown(f"**Race date:** {RACE_DATE.strftime('%d %b %Y')}")
    countdown_days = (RACE_DATE - date.today()).days
    st.metric("Countdown", f"{countdown_days} days" if countdown_days >= 0 else f"+{abs(countdown_days)} days")

    st.divider()
    st.subheader("Week Plan")
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
        st.metric(
            "Endurance done",
            f"{float(row['Endurance_sessions_done'].iloc[0]):.0f} / {float(row['Endurance_sessions_planned'].iloc[0]):.0f}"
        )
        st.metric(
            "Strength done",
            f"{float(row['Strength_sessions_done'].iloc[0]):.0f} / {float(row['Strength_sessions_planned'].iloc[0]):.0f}"
        )

    if st.button("Reset from template"):
        tpl = read_template_sheets()
        save_client(CLIENT_ID, tpl["Endurance"], tpl["Sessions"], tpl["Strength_exercises"])
        st.success("Template restored. Refresh the page.")

# ---------------- TABS ----------------
tab_today, tab_week, tab_plan, tab_strength_progress, tab_body_metrics, tab_export = st.tabs(
    ["Today", "Week", "Plan", "Strength Progress", "Body Metrics", "Export"]
)
# =============== TAB: TODAY ===============
with tab_today:
    st.subheader("Daily check-in")

    default_date = date.today()
    all_plan_dates = sorted(set(
    safe_date_list(endurance_calc["_date"].tolist()) +
    safe_date_list(sessions_calc["_date"].tolist())
))

    selected_date = st.date_input("Date", value=default_date, key="calendar_date")

    derived_week = get_plan_week_for_date(selected_date, endurance_calc, sessions_calc)
    if derived_week is not None:
        st.session_state["selected_plan_week"] = derived_week

    current_week = st.session_state.get("selected_plan_week", week_list[0])
    week_dates = get_week_dates_from_plan(current_week, endurance_calc, sessions_calc)

    if week_dates:
        st.markdown("#### Week view")
        week_cols = st.columns(len(week_dates))

        for i, d in enumerate(week_dates):
            day_runs = endurance_calc[endurance_calc["_date"] == d].copy()
            day_strength = sessions_calc[sessions_calc["_date"] == d].copy()
    
            run_txt = ""
            strength_txt = ""
    
            if not day_runs.empty:
                run_names = day_runs["Session"].dropna().astype(str).tolist()
                run_txt = "<br>".join([f"🏃 {x}" for x in run_names[:2]])
    
            if not day_strength.empty:
                strength_names = day_strength["Session"].dropna().astype(str).tolist()
                strength_txt = "<br>".join([f"🏋️ {x}" for x in strength_names[:2]])
    
            if run_txt == "" and strength_txt == "":
                detail_txt = "Rest / free day"
            else:
                detail_txt = f"{run_txt}<br>{strength_txt}".strip("<br>")
    
            selected_marker = "✅ " if d == selected_date else ""
    
            with week_cols[i]:
                st.markdown(
                    f"""
                    <div class="kpi-card" style="padding:10px; min-height:140px;">
                        <div class="kpi-title">{selected_marker}{d.strftime('%A')}</div>
                        <p class="kpi-value" style="font-size:1rem;">{d.strftime('%d %b')}</p>
                        <div class="kpi-sub">{detail_txt}</div>
                    </div>
                    """,
                    unsafe_allow_html=True,
                )

    st.markdown("<br>", unsafe_allow_html=True)
    checkin = get_daily_checkin(CLIENT_ID, selected_date)

    c1, c2, c3 = st.columns(3)
    with c1:
        done_today = st.checkbox("Done", value=bool(checkin["done"]))
    with c2:
        sleep_hours = st.number_input(
            "Sleep (hours)",
            min_value=0.0,
            max_value=14.0,
            value=float(checkin["sleep_hours"] or 0.0),
            step=0.25,
        )
    with c3:
        sleep_quality = st.slider(
            "Sleep quality",
            min_value=1,
            max_value=5,
            value=int(checkin["sleep_quality"] or 3)
        )

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
    st.subheader("Sessions for selected day")

    suggested_e = endurance_calc[endurance_calc["_date"] == selected_date].copy()
    suggested_s = sessions_calc[sessions_calc["_date"] == selected_date].copy()

    col_day1, col_day2 = st.columns(2)

    with col_day1:
        st.markdown("### Planned endurance")
        if suggested_e.empty:
            st.info("No endurance session planned for this day.")
        else:
            for _, r in suggested_e.iterrows():
                st.markdown(
                    f"""
                    **{r['Session']}**  
                    - Discipline: {r['Discipline']}  
                    - Minutes: {r['Minutes']:.0f}  
                    - sRPE: {r['sRPE_num']:.1f}  
                    - Est. km: {r['Run_km_est']:.1f}
                    """
                )
                if str(r.get("Cadence_target", "")).strip():
                    st.markdown(f"<span class='badge'>Cadence: {r['Cadence_target']}</span>", unsafe_allow_html=True)
                if str(r.get("RPE_blocks", "")).strip():
                    st.markdown(f"<span class='badge'>RPE blocks: {r['RPE_blocks']}</span>", unsafe_allow_html=True)
                st.write(r["Interval_details_and_objective"])

    with col_day2:
        st.markdown("### Planned strength")
        if suggested_s.empty:
            st.info("No strength session planned for this day.")
        else:
            for _, r in suggested_s.iterrows():
                st.markdown(
                    f"""
                    **Workout {r['Session']}**  
                    - Phase: {r['Phase']}  
                    - Minutes: {r['Minutes_session']:.0f}  
                    - sRPE: {r['sRPE_session']:.1f}
                    """
                )

    st.markdown("---")
    st.subheader("Flexible workout logging")
    st.caption("Pick the plan week, then log the endurance and/or strength session you actually did today.")

    flex_week = st.selectbox(
        "Week to log from",
        week_list,
        index=week_list.index(current_week),
        key="flex_week"
    )

    e_week = endurance_calc[endurance_calc["Week"] == flex_week].copy()
    s_week = sessions_calc[sessions_calc["Week"] == flex_week].copy()

    col_a, col_b = st.columns(2)

    with col_a:
        st.markdown("### Run / endurance")

        options_e = ["-- none --"] + [
            f"{r['Day']} | {r['Session']} | {r['Minutes']:.0f} min"
            for _, r in e_week.iterrows()
        ]

        choice_e = st.selectbox(
            "Select endurance session completed today",
            options_e,
            key="endurance_choice"
        )

        actual_endurance_minutes = st.number_input(
            "Actual endurance minutes",
            min_value=0.0,
            max_value=600.0,
            value=0.0,
            step=5.0,
            key="actual_endurance_minutes",
        )

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
                f"""
                **Selected:** {chosen_row['Session']}  
                - Discipline: {chosen_row['Discipline']}  
                - Planned day: {chosen_row['Day']} ({chosen_row['_date']})  
                - Planned minutes: {chosen_row['Minutes']:.0f}  
                - Est. km: {chosen_row['Run_km_est']:.1f}
                """
            )
            st.write(chosen_row["Interval_details_and_objective"])

    with col_b:
        st.markdown("### Strength")

        options_s = ["-- none --"] + [
            f"{r['Day']} | Workout {r['Session']} | {r['Minutes_session']:.0f} min | {r['SesionID']}"
            for _, r in s_week.iterrows()
        ]

        choice_s = st.selectbox(
            "Select strength workout completed today",
            options_s,
            key="strength_choice"
        )

        if choice_s != "-- none --":
            idx = options_s.index(choice_s) - 1
            chosen_row = s_week.iloc[idx]
            sid = chosen_row["SesionID"]

            st.markdown(
                f"""
                **Selected:** Workout {chosen_row['Session']}  
                - Planned day: {chosen_row['Day']} ({chosen_row['Date']})  
                - Planned minutes: {chosen_row['Minutes_session']:.0f}  
                - Phase: {chosen_row['Phase']}
                """
            )

            if st.button(f"Save strength completion — {sid}"):
                upsert_strength_completion(
                    CLIENT_ID,
                    sid,
                    int(chosen_row["Week"]),
                    selected_date,
                )
                st.success("Strength workout logged.")

            ex = exercises_calc[exercises_calc["SesionID"] == sid].copy()

            if ex.empty:
                st.warning("No exercises found for this workout.")
            else:
                st.markdown("#### Workout exercises")
                preview = ex[["Exercise", "Series", "Reps_text", "Kg", "RIR_target_text", "Tempo", "Rest"]].copy()
                preview = preview.rename(columns={
                    "Reps_text": "Reps target",
                    "Kg": "Planned kg",
                    "RIR_target_text": "RIR target"
                })
                st.dataframe(arrow_safe(preview), use_container_width=True)

                st.markdown("#### Log sets")
                st.caption("Enter weight used, reps done, and real RIR for each set. Saved for this date and workout.")

                existing_sets = get_sets_log(CLIENT_ID, selected_date, sid)

                if existing_sets.empty:
                    rows = []
                    for _, er in ex.iterrows():
                        nsets_val = to_num(er.get("Series"))
                        nsets = int(nsets_val) if pd.notna(nsets_val) else 0
                        planned_weight = to_num(er.get("Kg"))

                        for i in range(1, nsets + 1):
                            rows.append({
                                "exercise": er.get("Exercise", ""),
                                "set_number": i,
                                "reps_done": np.nan,
                                "weight_kg": planned_weight if pd.notna(planned_weight) else np.nan,
                                "rir_real": np.nan,
                                "note": "",
                            })

                    existing_sets = pd.DataFrame(rows)

                edited_sets = st.data_editor(
                    arrow_safe(existing_sets),
                    use_container_width=True,
                    hide_index=True,
                    num_rows="fixed",
                    column_config={
                        "exercise": st.column_config.TextColumn("Exercise", disabled=True),
                        "set_number": st.column_config.NumberColumn("Set", step=1, disabled=True),
                        "reps_done": st.column_config.NumberColumn("Reps done", step=1),
                        "weight_kg": st.column_config.NumberColumn("Weight (kg)", step=0.5),
                        "rir_real": st.column_config.NumberColumn("Real RIR", step=0.5),
                        "note": st.column_config.TextColumn("Note"),
                    },
                    key=f"sets_editor_{sid}_{selected_date.isoformat()}",
                )

                csave1, csave2 = st.columns(2)
                with csave1:
                    if st.button(f"Save set log — {sid}"):
                        save_sets_log(CLIENT_ID, selected_date, sid, edited_sets)
                        st.success("Set log saved for this date.")
                with csave2:
                    if st.button(f"Save workout + set log — {sid}"):
                        upsert_strength_completion(
                            CLIENT_ID,
                            sid,
                            int(chosen_row["Week"]),
                            selected_date,
                        )
                        save_sets_log(CLIENT_ID, selected_date, sid, edited_sets)
                        st.success("Workout and set log saved.")
        else:
            st.info("Select a strength workout to open its exercises and log each set.")
            
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
    st.dataframe(arrow_safe(end_preview.head(80)), use_container_width=True)

    st.markdown("### Strength sessions")
    ses_preview = sessions_calc[[
        "Week", "Date", "Day", "Session", "Phase", "Minutes_session", "sRPE_session", "SesionID"
    ]].copy()
    st.dataframe(arrow_safe(ses_preview.head(80)), use_container_width=True)

    st.markdown("### Strength exercises")
    ex_preview = exercises_calc[[
        "Week", "SesionID", "Exercise", "Muscle", "Series", "Reps_text", "Kg", "RIR_target_text", "Tempo", "Rest"
    ]].copy()
    ex_preview = ex_preview.rename(columns={"Reps_text": "Reps", "RIR_target_text": "RIR target"})
    st.dataframe(arrow_safe(ex_preview.head(120)), use_container_width=True)

# =============== TAB: STRENGTH PROGRESS ===============
with tab_strength_progress:
    st.subheader("Strength Progress Tracker")

    progress_df = get_strength_progress_df(CLIENT_ID)

    if progress_df.empty:
        st.info("No strength set logs yet. Log some strength workouts first.")
    else:
        exercise_list = sorted(progress_df["exercise"].dropna().unique().tolist())
        selected_exercise = st.selectbox("Exercise", exercise_list)

        ex_prog = progress_df[progress_df["exercise"] == selected_exercise].copy().sort_values("log_date")

        c1, c2, c3 = st.columns(3)
        with c1:
            last_max = ex_prog["max_weight_kg"].dropna().iloc[-1] if ex_prog["max_weight_kg"].dropna().shape[0] > 0 else np.nan
            kpi("Last max weight", f"{last_max:.1f} kg" if pd.notna(last_max) else "—", "highest set on latest logged date")
        with c2:
            last_avg = ex_prog["avg_weight_kg"].dropna().iloc[-1] if ex_prog["avg_weight_kg"].dropna().shape[0] > 0 else np.nan
            kpi("Last avg weight", f"{last_avg:.1f} kg" if pd.notna(last_avg) else "—", "average load on latest logged date")
        with c3:
            last_rir = ex_prog["avg_rir_real"].dropna().iloc[-1] if ex_prog["avg_rir_real"].dropna().shape[0] > 0 else np.nan
            kpi("Last avg RIR", f"{last_rir:.1f}" if pd.notna(last_rir) else "—", "average real RIR on latest logged date")

        st.markdown("### Max weight over time")
        chart1 = alt.Chart(ex_prog).mark_line(point=True).encode(
            x=alt.X("log_date:T", title="Date"),
            y=alt.Y("max_weight_kg:Q", title="Max weight (kg)"),
            tooltip=["log_date:T", "exercise:N", "max_weight_kg:Q", "avg_rir_real:Q"]
        ).properties(height=280)
        st.altair_chart(chart1, use_container_width=True)

        st.markdown("### Average weight over time")
        chart2 = alt.Chart(ex_prog).mark_line(point=True).encode(
            x=alt.X("log_date:T", title="Date"),
            y=alt.Y("avg_weight_kg:Q", title="Average weight (kg)"),
            tooltip=["log_date:T", "exercise:N", "avg_weight_kg:Q", "avg_reps_done:Q", "avg_rir_real:Q"]
        ).properties(height=260)
        st.altair_chart(chart2, use_container_width=True)

        st.markdown("### Logged sessions table")
        show_df = ex_prog.copy()
        show_df["log_date"] = show_df["log_date"].dt.date.astype(str)
        st.dataframe(arrow_safe(show_df), use_container_width=True)

# =============== TAB: BODY METRICS ===============
with tab_body_metrics:
    st.subheader("Body Metrics")

    metrics_date = st.date_input("Measurement date", value=date.today(), key="body_metrics_date")
    bm = get_body_metrics(CLIENT_ID, metrics_date)

    c1, c2, c3 = st.columns(3)
    with c1:
        weight_kg = st.number_input(
            "Weight (kg)",
            min_value=0.0,
            max_value=300.0,
            value=float(bm["weight_kg"] or 0.0),
            step=0.1,
        )
        waist_cm = st.number_input(
            "Waist (cm)",
            min_value=0.0,
            max_value=200.0,
            value=float(bm["waist_cm"] or 0.0),
            step=0.1,
        )

    with c2:
        hip_cm = st.number_input(
            "Hip (cm)",
            min_value=0.0,
            max_value=250.0,
            value=float(bm["hip_cm"] or 0.0),
            step=0.1,
        )
        biceps_cm = st.number_input(
            "Biceps (cm)",
            min_value=0.0,
            max_value=100.0,
            value=float(bm["biceps_cm"] or 0.0),
            step=0.1,
        )

    with c3:
        gut_status = st.selectbox(
            "Gut status",
            [
                "",
                "Good",
                "Normal",
                "Bloated",
                "Sensitive",
                "Cramps",
                "Acid / reflux",
                "Loose stool",
                "Constipated",
                "Other",
            ],
            index=(
                ["", "Good", "Normal", "Bloated", "Sensitive", "Cramps", "Acid / reflux", "Loose stool", "Constipated", "Other"].index(bm["gut_status"])
                if bm["gut_status"] in ["", "Good", "Normal", "Bloated", "Sensitive", "Cramps", "Acid / reflux", "Loose stool", "Constipated", "Other"]
                else 0
            ),
        )
        body_note = st.text_area("Note", value=bm["note"], height=100)

    if st.button("Save body metrics"):
        upsert_body_metrics(
            CLIENT_ID,
            metrics_date,
            weight_kg if weight_kg > 0 else None,
            waist_cm if waist_cm > 0 else None,
            hip_cm if hip_cm > 0 else None,
            biceps_cm if biceps_cm > 0 else None,
            gut_status,
            body_note,
        )
        st.success("Body metrics saved.")

    st.markdown("---")
    st.markdown("### History")

    body_df = get_body_metrics_df(CLIENT_ID)
    if body_df.empty:
        st.info("No body metrics logged yet.")
    else:
        st.dataframe(arrow_safe(body_df), use_container_width=True)

        chart_df = body_df.copy()

        metrics_to_plot = ["weight_kg", "waist_cm", "hip_cm", "biceps_cm"]
        plot_df = chart_df.melt(
            id_vars=["log_date", "gut_status", "note"],
            value_vars=metrics_to_plot,
            var_name="metric",
            value_name="value"
        ).dropna(subset=["value"])

        if not plot_df.empty:
            st.markdown("### Trends")
            chart = alt.Chart(plot_df).mark_line(point=True).encode(
                x=alt.X("log_date:T", title="Date"),
                y=alt.Y("value:Q", title="Value"),
                color=alt.Color("metric:N", title="Metric"),
                tooltip=["log_date:T", "metric:N", "value:Q"]
            ).properties(height=320)
            st.altair_chart(chart, use_container_width=True)
            
# =============== TAB: EXPORT ===============
with tab_export:
    st.subheader("Export workbook and logs")

    con = db()
    daily_df = pd.read_sql_query("SELECT * FROM daily_checkin WHERE client_id=? ORDER BY log_date", con, params=(CLIENT_ID,))
    endurance_done_export = pd.read_sql_query("SELECT * FROM endurance_completion WHERE client_id=? ORDER BY actual_date", con, params=(CLIENT_ID,))
    strength_done_export = pd.read_sql_query("SELECT * FROM strength_completion WHERE client_id=? ORDER BY actual_date", con, params=(CLIENT_ID,))
    sets_df = pd.read_sql_query(
        "SELECT * FROM strength_sets_log WHERE client_id=? ORDER BY log_date, session_id, exercise, set_number",
        con,
        params=(CLIENT_ID,)
    )
    body_metrics_df = pd.read_sql_query(
        "SELECT * FROM body_metrics WHERE client_id=? ORDER BY log_date",
        con,
        params=(CLIENT_ID,)
    )

    xlsx_bytes = to_excel_bytes({
        "Endurance": endurance_calc.drop(columns=["_date"], errors="ignore"),
        "Sessions": sessions_calc.drop(columns=["_date"], errors="ignore"),
        "Strength_exercises": exercises_calc,
        "Weekly_summary": dash2,
        "Daily_checkin": daily_df,
        "Endurance_done": endurance_done_export,
        "Strength_done": strength_done_export,
        "Strength_sets_log": sets_df, "Body_metrics": body_metrics_df,
    })

    st.download_button(
        "Download updated Excel",
        data=xlsx_bytes,
        file_name=f"{CLIENT_ID}_marathon_dashboard.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )

st.caption("Flexible logging: workouts can be completed on a different day than planned, while still counting toward the original plan week.")