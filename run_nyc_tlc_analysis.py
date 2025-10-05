#!/usr/bin/env python3
"""
NYC TLC Yellow vs (Strict/Proxy) HVFHV Crash Rate Analysis — 2022–2023
with proportional-allocation fallback when sub-industry counts are zero.

Fallback logic:
  If Yellow/HVFHV crash counts are all zero for 2022–2023, allocate
  All-Industries crashes to Yellow & HVFHV using exposure shares
  (by trips or by miles). Estimated columns are clearly labeled.

Inputs (./data):
  - tlc_crashes_monthly_2022_2023.csv  (long schema: year_month, industry, total_crashes, ...)
  - yellow_tripdata_2022-*.parquet, yellow_tripdata_2023-*.parquet
  - fhvhv_tripdata_2022-*.parquet,  fhvhv_tripdata_2023-*.parquet  (legacy hvfhv_* accepted)

Outputs (./out):
  - monthly_crash_rates_2022_2023.csv
  - crashes_per_million_trips.png
  - crashes_per_million_miles.png
  - avg_trips_bar.png
  - avg_miles_bar.png
  - NYC_Taxi_vs_HVFHV_Crash_Report_2022_2023.pdf
"""

from __future__ import annotations
from pathlib import Path
import glob
import duckdb
import pandas as pd
import matplotlib.pyplot as plt
from reportlab.lib.pagesizes import letter
from reportlab.pdfgen import canvas

# ---------------------------- Config ----------------------------
DATA_DIR = Path("data")
OUT_DIR = Path("out")
OUT_DIR.mkdir(exist_ok=True)

CRASH_CSV = DATA_DIR / "tlc_crashes_monthly_2022_2023.csv"

YELLOW_GLOBS = [
    str(DATA_DIR / "yellow_tripdata_2022-*.parquet"),
    str(DATA_DIR / "yellow_tripdata_2023-*.parquet"),
]
FHVHV_GLOBS = [
    str(DATA_DIR / "fhvhv_tripdata_2022-*.parquet"),
    str(DATA_DIR / "fhvhv_tripdata_2023-*.parquet"),
    str(DATA_DIR / "hvfhv_tripdata_2022-*.parquet"),  # legacy spelling guard
    str(DATA_DIR / "hvfhv_tripdata_2023-*.parquet"),
]

# Candidate column names (schemas vary)
YELLOW_DATETIME_CANDS = ["tpep_pickup_datetime", "pickup_datetime"]
YELLOW_MILES_CANDS    = ["trip_distance", "trip_miles"]
FHVHV_DATETIME_CANDS  = ["pickup_datetime", "tpep_pickup_datetime"]
FHVHV_MILES_CANDS     = ["trip_miles", "trip_distance"]

# Analysis window
WINDOW_START = "2022-01"
WINDOW_END   = "2023-12"

# Whether to include SHL (green cabs) in the Yellow bucket
INCLUDE_SHL_IN_YELLOW = True  # set True if you want Medallion+SHL treated as "taxi"

# Fallback allocation basis if sub-industry crash counts are all zero: "trips" or "miles"
ALLOCATION_BASIS = "miles"  # or "miles" or "trips"

# ---------------------------- Helpers ----------------------------
def existing_paths(globs):
    paths = []
    for g in globs:
        paths.extend(glob.glob(g))
    return sorted(set(paths))

def detect_columns(sample_path, datetime_candidates, miles_candidates):
    con = duckdb.connect()
    cols = con.execute(f"SELECT * FROM read_parquet('{sample_path}') LIMIT 0;").fetchdf().columns
    con.close()
    dt = next((c for c in datetime_candidates if c in cols), None)
    mi = next((c for c in miles_candidates if c in cols), None)
    if not dt or not mi:
        raise KeyError(
            f"Could not find required columns in {sample_path}. "
            f"Have: {list(cols)}; need one of {datetime_candidates} and {miles_candidates}"
        )
    return dt, mi

def duckdb_monthly_aggregate(parquet_paths, datetime_col, miles_col, label):
    """
    Fast monthly aggregation from Parquet using DuckDB.
    Returns: DataFrame [month, trips_<label>, miles_<label>]
    """
    if not parquet_paths:
        raise FileNotFoundError("No parquet paths provided for aggregation.")
    file_array_sql = "[" + ", ".join(f"'{p}'" for p in parquet_paths) + "]"
    sql = f"""
        WITH src AS (
            SELECT {datetime_col} AS dt, {miles_col} AS miles
            FROM read_parquet({file_array_sql}, union_by_name := TRUE)
        ),
        trips AS (
            SELECT
                strftime(dt, '%Y-%m') AS month,
                CAST(miles AS DOUBLE) AS trip_miles
            FROM src
            WHERE dt IS NOT NULL
        )
        SELECT
            month,
            COUNT(*)                     AS trips_{label},
            COALESCE(SUM(trip_miles),0)  AS miles_{label}
        FROM trips
        GROUP BY 1
        ORDER BY 1;
    """
    con = duckdb.connect()
    try:
        return con.execute(sql).fetchdf()
    finally:
        con.close()

# ---------------------------- Crash aggregation with strict+proxy HVFHV ----------------------------
def build_crash_counts(crash_csv_path: Path) -> pd.DataFrame:
    """
    Returns DataFrame:
      [month, yellow_taxi_crashes, hvfhv_crashes, all_industries_crashes]

    Mapping:
      Yellow  := 'medallion' (+ 'shl' if INCLUDE_SHL_IN_YELLOW)
      HVFHV   STRICT := 'high volume', 'high volume fhv (hvfhv)'
              PROXY  := 'black car', 'livery', 'lux limo', 'paratransit van', 'commuter van', 'unaffiliated'
      All     := 'all industries'
    """
    if not crash_csv_path.exists():
        raise FileNotFoundError(f"Crash CSV not found: {crash_csv_path}")

    df = pd.read_csv(crash_csv_path)
    df.columns = [c.strip() for c in df.columns]
    if "year_month" not in df.columns or "industry" not in df.columns:
        raise ValueError(f"Crash CSV missing 'year_month' or 'industry'. Found: {df.columns.tolist()}")

    # choose a crash measure
    measure_col = "total_crashes" if "total_crashes" in df.columns else None
    if not measure_col and "total_crashed_vehicles" in df.columns:
        measure_col = "total_crashed_vehicles"
    if not measure_col:
        raise ValueError("Crash CSV has neither 'total_crashes' nor 'total_crashed_vehicles'.")

    # normalize and restrict window
    df["industry_norm"] = df["industry"].astype(str).str.strip().str.lower()
    df["month"] = pd.to_datetime(df["year_month"], errors="coerce").dt.to_period("M").astype(str)
    df = df[df["month"].between(WINDOW_START, WINDOW_END)].copy()
    df[measure_col] = pd.to_numeric(df[measure_col], errors="coerce").fillna(0)

    industries = sorted(df["industry"].dropna().unique().tolist())
    print(f"[info] Crash industries present {WINDOW_START}..{WINDOW_END}: {industries}")
    print(f"[info] Using crash measure: {measure_col}")

    # buckets
    yellow_bucket = {"medallion"}
    if INCLUDE_SHL_IN_YELLOW:
        yellow_bucket |= {"shl"}

    hvfhv_strict_bucket = {"high volume", "high volume fhv (hvfhv)"}
    hvfhv_proxy_bucket  = {"black car", "livery", "lux limo", "paratransit van", "commuter van", "unaffiliated"}

    # aggregates
    all_monthly = (df[df["industry_norm"].eq("all industries")]
                   .groupby("month")[measure_col].sum().rename("all_industries_crashes"))

    yellow_monthly = (df[df["industry_norm"].isin(yellow_bucket)]
                      .groupby("month")[measure_col].sum().rename("yellow_taxi_crashes"))

    hvfhv_strict = (df[df["industry_norm"].isin(hvfhv_strict_bucket)]
                    .groupby("month")[measure_col].sum())

    hvfhv_proxy  = (df[df["industry_norm"].isin(hvfhv_proxy_bucket)]
                    .groupby("month")[measure_col].sum())

    # choose hvfhv source
    if (not hvfhv_strict.empty) and (hvfhv_strict.sum() > 0):
        hvfhv_monthly = hvfhv_strict.rename("hvfhv_crashes")
        print("[info] HVFHV source: STRICT (High Volume)")
    else:
        hvfhv_monthly = hvfhv_proxy.rename("hvfhv_crashes")
        print("[warn] No 'High Volume' rows with non-zero counts. Using PROXY HVFHV "
              "(Black Car + Livery + Lux Limo + Paratransit Van + Commuter Van + Unaffiliated).")

    out = (pd.concat([yellow_monthly, hvfhv_monthly, all_monthly], axis=1)
             .fillna(0).reset_index()
             .sort_values("month").reset_index(drop=True))

    # ints
    for c in ["yellow_taxi_crashes","hvfhv_crashes","all_industries_crashes"]:
        out[c] = pd.to_numeric(out[c], errors="coerce").fillna(0).astype(int)

    # sanity
    print("[info] Sample crash counts:\n", out.head())
    return out

# ---------------------------- Plotting & PDF ----------------------------
def plot_timeseries(df, cols, title, fname, ylabel):
    plot_df = df[["month"] + cols].set_index("month")
    plot_df.plot(figsize=(10, 5))
    plt.title(title)
    plt.ylabel(ylabel)
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig(OUT_DIR / fname, dpi=200)
    plt.close()

def plot_bar_avg(df, cols, title, fname, ylabel):
    avg = df[cols].mean()
    ax = avg.plot(kind="bar", figsize=(6, 4))
    ax.set_title(title)
    ax.set_ylabel(ylabel)
    plt.tight_layout()
    plt.savefig(OUT_DIR / fname, dpi=200)
    plt.close()

def make_pdf_report():
    pdf_path = OUT_DIR / "NYC_Taxi_vs_HVFHV_Crash_Report_2022_2023.pdf"
    c = canvas.Canvas(str(pdf_path), pagesize=letter)
    width, height = letter

    c.setFont("Helvetica-Bold", 16)
    c.drawString(72, height - 72, "NYC Taxi (Medallion) vs HVFHV (strict/proxy) — Crash Rates 2022–2023")
    c.setFont("Helvetica", 10)
    c.drawString(72, height - 88, "If sub-industry counts are unavailable, crashes are allocated to Yellow/HVFHV by exposure share (see CSV notes).")

    y = height - 120
    for name in [
        "crashes_per_million_trips.png",
        "crashes_per_million_miles.png",
        "avg_trips_bar.png",
        "avg_miles_bar.png",
    ]:
        img_path = OUT_DIR / name
        if img_path.exists():
            c.drawImage(str(img_path), 72, y - 250, width=450, height=250, preserveAspectRatio=True)
            y -= 270
            if y < 160:
                c.showPage()
                y = height - 72

    c.showPage()
    c.save()
    print(f"[pdf] Saved -> {pdf_path}")

# ---------------------------- Main ----------------------------
def main():
    # Validate and list inputs
    if not CRASH_CSV.exists():
        raise FileNotFoundError(f"Missing crash CSV at {CRASH_CSV}.")
    yellow_paths = existing_paths(YELLOW_GLOBS)
    fhvhv_paths  = existing_paths(FHVHV_GLOBS)
    if not yellow_paths:
        raise FileNotFoundError("No Yellow Taxi parquet files found under ./data for 2022–2023.")
    if not fhvhv_paths:
        raise FileNotFoundError("No FHVHV parquet files found under ./data for 2022–2023.")
    print(f"[info] Yellow files: {len(yellow_paths)} | FHVHV files: {len(fhvhv_paths)}")

    # Detect column names
    y_dt, y_mi = detect_columns(yellow_paths[0], YELLOW_DATETIME_CANDS, YELLOW_MILES_CANDS)
    f_dt, f_mi = detect_columns(fhvhv_paths[0], FHVHV_DATETIME_CANDS, FHVHV_MILES_CANDS)
    print(f"[info] Yellow uses columns: datetime='{y_dt}', miles='{y_mi}'")
    print(f"[info] FHVHV uses columns:  datetime='{f_dt}', miles='{f_mi}'")

    # Aggregate exposure via DuckDB
    print("[run ] Aggregating Yellow monthly exposure via DuckDB…")
    monthly_yellow = duckdb_monthly_aggregate(yellow_paths, y_dt, y_mi, label="yellow")
    print("[ok  ] Yellow monthly done.")
    print("[run ] Aggregating FHVHV monthly exposure via DuckDB…")
    monthly_fhvhv  = duckdb_monthly_aggregate(fhvhv_paths,  f_dt, f_mi, label="hvfhv")
    print("[ok  ] FHVHV monthly done.")

    # Build crash counts (yellow / hvfhv / all industries)
    crashes = build_crash_counts(CRASH_CSV)

    # Merge everything
    df = (
        crashes
        .merge(monthly_yellow, on="month", how="outer")
        .merge(monthly_fhvhv,  on="month", how="outer")
        .fillna(0)
        .sort_values("month")
        .reset_index(drop=True)
    )
    # Focus window
    df = df[df["month"].between(WINDOW_START, WINDOW_END)].copy()

    # Coerce numerics
    for col in ["yellow_taxi_crashes","hvfhv_crashes","all_industries_crashes",
                "trips_yellow","trips_hvfhv","miles_yellow","miles_hvfhv"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    # Combined exposure (useful context & to sanity-check)
    df["trips_all"] = df["trips_yellow"] + df["trips_hvfhv"]
    df["miles_all"] = df["miles_yellow"] + df["miles_hvfhv"]

    # -------- Proportional allocation fallback (if sub-industry counts are zero) --------
    total_yellow = df["yellow_taxi_crashes"].sum()
    total_hvfhv  = df["hvfhv_crashes"].sum()

    if total_yellow == 0 and total_hvfhv == 0:
        print(f"[warn] Yellow/HVFHV crash counts are all zero in {WINDOW_START}..{WINDOW_END}.")
        print(f"[info] Applying proportional allocation by {ALLOCATION_BASIS} from All-Industries crashes.")
        if ALLOCATION_BASIS == "miles":
            denom = df["miles_all"].clip(lower=1)
            y_share = (df["miles_yellow"].clip(lower=0)) / denom
        else:  # default to trips
            denom = df["trips_all"].clip(lower=1)
            y_share = (df["trips_yellow"].clip(lower=0)) / denom

        # estimated crashes (integers)
        df["yellow_taxi_crashes_est"] = (df["all_industries_crashes"] * y_share).round().astype(int)
        df["hvfhv_crashes_est"]       = df["all_industries_crashes"] - df["yellow_taxi_crashes_est"]

        # Use estimates for rate calculations and for CSV export (with clear labels)
        use_est = True
    else:
        use_est = False

    # Crash rates (per million)
    if use_est:
        df["taxi_crash_rate_per_million_trips"]  = df["yellow_taxi_crashes_est"] / (df["trips_yellow"].clip(lower=1) / 1e6)
        df["hvfhv_crash_rate_per_million_trips"] = df["hvfhv_crashes_est"]       / (df["trips_hvfhv"].clip(lower=1) / 1e6)
        df["taxi_crash_rate_per_million_miles"]  = df["yellow_taxi_crashes_est"] / (df["miles_yellow"].clip(lower=1) / 1e6)
        df["hvfhv_crash_rate_per_million_miles"] = df["hvfhv_crashes_est"]       / (df["miles_hvfhv"].clip(lower=1) / 1e6)
    else:
        df["taxi_crash_rate_per_million_trips"]  = df["yellow_taxi_crashes"] / (df["trips_yellow"].clip(lower=1) / 1e6)
        df["hvfhv_crash_rate_per_million_trips"] = df["hvfhv_crashes"]       / (df["trips_hvfhv"].clip(lower=1) / 1e6)
        df["taxi_crash_rate_per_million_miles"]  = df["yellow_taxi_crashes"] / (df["miles_yellow"].clip(lower=1) / 1e6)
        df["hvfhv_crash_rate_per_million_miles"] = df["hvfhv_crashes"]       / (df["miles_hvfhv"].clip(lower=1) / 1e6)

    # All-Industries baseline (against combined exposure)
    df["all_industries_crash_rate_per_million_trips"] = df["all_industries_crashes"] / (df["trips_all"].clip(lower=1) / 1e6)
    df["all_industries_crash_rate_per_million_miles"] = df["all_industries_crashes"] / (df["miles_all"].clip(lower=1) / 1e6)

    # Save table
    out_csv = OUT_DIR / "monthly_crash_rates_2022_2023.csv"
    df.to_csv(out_csv, index=False)
    print(f"[csv ] Saved -> {out_csv}")
    if use_est:
        print("[note] Yellow/HVFHV crash columns used for rates are *_est (proportional allocation). See CSV.")

    # Plots — Yellow vs HVFHV (strict/proxy or estimated)
    plot_timeseries(
        df,
        ["taxi_crash_rate_per_million_trips", "hvfhv_crash_rate_per_million_trips"],
        "Crashes per Million Trips — Yellow vs HVFHV (observed/estimated) — 2022–2023",
        "crashes_per_million_trips.png",
        "Crashes / million trips",
    )
    plot_timeseries(
        df,
        ["taxi_crash_rate_per_million_miles", "hvfhv_crash_rate_per_million_miles"],
        "Crashes per Million Miles — Yellow vs HVFHV (observed/estimated) — 2022–2023",
        "crashes_per_million_miles.png",
        "Crashes / million miles",
    )
    plot_bar_avg(
        df,
        ["taxi_crash_rate_per_million_trips", "hvfhv_crash_rate_per_million_trips"],
        "Average Crash Rate per Million Trips — Yellow vs HVFHV (2022–2023)",
        "avg_trips_bar.png",
        "Crashes / million trips",
    )
    plot_bar_avg(
        df,
        ["taxi_crash_rate_per_million_miles", "hvfhv_crash_rate_per_million_miles"],
        "Average Crash Rate per Million Miles — Yellow vs HVFHV (2022–2023)",
        "avg_miles_bar.png",
        "Crashes / million miles",
    )

    # PDF
    make_pdf_report()
    print("[done] Analysis complete.")

if __name__ == "__main__":
    main()
