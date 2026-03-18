import argparse
from pathlib import Path
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt


CORE_NUMERIC_COLS = [
    "avg_spread_bps",
    "clearing_time_hat_min",
    "tick_day_moves",
    "hit_cnt",
    "n_hit_bid",
    "n_hit_ask",
    "Q_bid",
    "Q_ask",
    "V_bid",
    "V_ask",
    "r_bid",
    "r_ask",
    "nRecords",
    "totalVol",
    "hi",
    "lo",
    "tick_size_ref",
]

REQUIRED_MIN_COLS = ["sym", "date", "region", "bkt"]


def safe_div(a, b):
    a = np.asarray(a, dtype="float64")
    b = np.asarray(b, dtype="float64")
    out = np.full_like(a, np.nan, dtype="float64")
    mask = np.isfinite(a) & np.isfinite(b) & (b != 0)
    out[mask] = a[mask] / b[mask]
    return out


def cv_series(s: pd.Series) -> float:
    s = pd.to_numeric(s, errors="coerce").dropna()
    if len(s) < 2:
        return np.nan
    mean = s.mean()
    if mean == 0 or not np.isfinite(mean):
        return np.nan
    return float(s.std(ddof=1) / mean)


def load_all_metrics(root: Path) -> pd.DataFrame:
    files = sorted(root.glob("*/bucket_metrics.csv"))
    if not files:
        raise FileNotFoundError(f"No bucket_metrics.csv found under {root}")

    frames = []
    for fp in files:
        try:
            df = pd.read_csv(fp)
        except Exception as e:
            print(f"[WARN] failed reading {fp}: {e}")
            continue

        miss = [c for c in REQUIRED_MIN_COLS if c not in df.columns]
        if miss:
            print(f"[WARN] skip {fp} because missing columns: {miss}")
            continue

        df["source_dir"] = fp.parent.name
        frames.append(df)

    if not frames:
        raise RuntimeError("No valid bucket_metrics.csv files were loaded")

    out = pd.concat(frames, ignore_index=True)
    out["date"] = pd.to_datetime(out["date"], errors="coerce")
    out["region"] = out["region"].astype("string")
    out["sym"] = out["sym"].astype("string")
    out["bkt"] = out["bkt"].astype("string")

    for c in CORE_NUMERIC_COLS:
        if c in out.columns:
            out[c] = pd.to_numeric(out[c], errors="coerce")

    if "hit_cnt" not in out.columns and {"n_hit_bid", "n_hit_ask"}.issubset(out.columns):
        out["hit_cnt"] = out["n_hit_bid"].fillna(0) + out["n_hit_ask"].fillna(0)

    out = out.dropna(subset=["date", "sym", "region"])
    out["date_str"] = out["date"].dt.strftime("%Y-%m-%d")
    out["month"] = out["date"].dt.to_period("M").astype(str)
    out["weekday"] = out["date"].dt.day_name()
    out["bucket_minutes"] = np.nan
    return out


def add_derived_columns(df: pd.DataFrame, bucket_minutes: float) -> pd.DataFrame:
    out = df.copy()
    out["bucket_minutes"] = float(bucket_minutes)

    out["gate_hit1"] = (out["hit_cnt"] >= 1).astype("int8")
    out["gate_hit2"] = (out["hit_cnt"] >= 2).astype("int8")
    out["gate_hit3"] = (out["hit_cnt"] >= 3).astype("int8")
    out["gate_hit5"] = (out["hit_cnt"] >= 5).astype("int8")

    out["log_ct"] = np.log1p(out["clearing_time_hat_min"])
    out["ct_bucket_ratio"] = safe_div(out["clearing_time_hat_min"], out["bucket_minutes"])
    out["ratio_lt_025"] = (out["ct_bucket_ratio"] < 0.25).astype("int8")
    out["ratio_025_2"] = ((out["ct_bucket_ratio"] >= 0.25) & (out["ct_bucket_ratio"] <= 2.0)).astype("int8")
    out["ratio_gt_2"] = (out["ct_bucket_ratio"] > 2.0).astype("int8")
    return out


def summarize_series(s: pd.Series, prefix: str) -> dict:
    s = pd.to_numeric(s, errors="coerce").dropna()
    if s.empty:
        return {
            f"{prefix}_count": 0,
            f"{prefix}_mean": np.nan,
            f"{prefix}_std": np.nan,
            f"{prefix}_min": np.nan,
            f"{prefix}_p10": np.nan,
            f"{prefix}_p25": np.nan,
            f"{prefix}_median": np.nan,
            f"{prefix}_p75": np.nan,
            f"{prefix}_p90": np.nan,
            f"{prefix}_max": np.nan,
        }
    return {
        f"{prefix}_count": int(s.shape[0]),
        f"{prefix}_mean": float(s.mean()),
        f"{prefix}_std": float(s.std(ddof=1)) if s.shape[0] > 1 else np.nan,
        f"{prefix}_min": float(s.min()),
        f"{prefix}_p10": float(s.quantile(0.10)),
        f"{prefix}_p25": float(s.quantile(0.25)),
        f"{prefix}_median": float(s.quantile(0.50)),
        f"{prefix}_p75": float(s.quantile(0.75)),
        f"{prefix}_p90": float(s.quantile(0.90)),
        f"{prefix}_max": float(s.max()),
    }


def build_region_distribution_summary(df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    metrics = [
        "clearing_time_hat_min",
        "avg_spread_bps",
        "tick_day_moves",
        "hit_cnt",
        "ct_bucket_ratio",
    ]

    for region, g in df.groupby("region", dropna=False):
        row = {
            "region": region,
            "n_rows": int(len(g)),
            "n_days": int(g["date"].nunique()),
            "n_syms": int(g["sym"].nunique()),
            "gate_rate_hit1": float(g["gate_hit1"].mean()) if len(g) else np.nan,
            "gate_rate_hit2": float(g["gate_hit2"].mean()) if len(g) else np.nan,
            "gate_rate_hit3": float(g["gate_hit3"].mean()) if len(g) else np.nan,
            "gate_rate_hit5": float(g["gate_hit5"].mean()) if len(g) else np.nan,
            "pct_ratio_lt_025": float(g["ratio_lt_025"].mean()) if len(g) else np.nan,
            "pct_ratio_025_2": float(g["ratio_025_2"].mean()) if len(g) else np.nan,
            "pct_ratio_gt_2": float(g["ratio_gt_2"].mean()) if len(g) else np.nan,
        }
        for m in metrics:
            if m in g.columns:
                row.update(summarize_series(g[m], m))
        rows.append(row)

    return pd.DataFrame(rows).sort_values("region").reset_index(drop=True)


def build_region_day_summary(df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for (region, date_str), g in df.groupby(["region", "date_str"], dropna=False):
        row = {
            "region": region,
            "date": date_str,
            "n_rows": int(len(g)),
            "n_syms": int(g["sym"].nunique()),
            "gate_rate_hit1": float(g["gate_hit1"].mean()) if len(g) else np.nan,
            "gate_rate_hit2": float(g["gate_hit2"].mean()) if len(g) else np.nan,
            "gate_rate_hit3": float(g["gate_hit3"].mean()) if len(g) else np.nan,
            "gate_rate_hit5": float(g["gate_hit5"].mean()) if len(g) else np.nan,
            "pct_ratio_lt_025": float(g["ratio_lt_025"].mean()) if len(g) else np.nan,
            "pct_ratio_025_2": float(g["ratio_025_2"].mean()) if len(g) else np.nan,
            "pct_ratio_gt_2": float(g["ratio_gt_2"].mean()) if len(g) else np.nan,
        }
        for m in ["clearing_time_hat_min", "avg_spread_bps", "tick_day_moves", "hit_cnt", "ct_bucket_ratio"]:
            row.update(summarize_series(g[m], m))
        rows.append(row)

    return pd.DataFrame(rows).sort_values(["region", "date"]).reset_index(drop=True)


def build_symbol_stability_summary(df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for (region, sym), g in df.groupby(["region", "sym"], dropna=False):
        per_day = (
            g.groupby("date_str", dropna=False)
             .agg(
                 n_rows=("sym", "size"),
                 ct_median=("clearing_time_hat_min", "median"),
                 spread_median=("avg_spread_bps", "median"),
                 tick_median=("tick_day_moves", "median"),
                 hit_cnt_median=("hit_cnt", "median"),
                 gate_hit2_rate=("gate_hit2", "mean"),
                 ratio_mid_rate=("ratio_025_2", "mean"),
             )
             .reset_index()
        )

        rows.append({
            "region": region,
            "sym": sym,
            "n_days": int(per_day["date_str"].nunique()),
            "n_rows_total": int(len(g)),
            "avg_rows_per_day": float(per_day["n_rows"].mean()) if len(per_day) else np.nan,
            "median_ct": float(g["clearing_time_hat_min"].median()) if len(g) else np.nan,
            "median_spread_bps": float(g["avg_spread_bps"].median()) if len(g) else np.nan,
            "median_tick_day_moves": float(g["tick_day_moves"].median()) if len(g) else np.nan,
            "avg_hit_cnt": float(g["hit_cnt"].mean()) if len(g) else np.nan,
            "median_hit_cnt": float(g["hit_cnt"].median()) if len(g) else np.nan,
            "gate_hit2_rate": float(g["gate_hit2"].mean()) if len(g) else np.nan,
            "ratio_mid_rate": float(g["ratio_025_2"].mean()) if len(g) else np.nan,
            "ct_cv_by_day": cv_series(per_day["ct_median"]),
            "spread_cv_by_day": cv_series(per_day["spread_median"]),
            "tick_cv_by_day": cv_series(per_day["tick_median"]),
            "hit_cnt_cv_by_day": cv_series(per_day["hit_cnt_median"]),
        })

    return (
        pd.DataFrame(rows)
        .sort_values(["region", "n_days", "avg_hit_cnt"], ascending=[True, False, False])
        .reset_index(drop=True)
    )


def build_top_tickers_by_region(symbol_summary: pd.DataFrame, top_n: int) -> pd.DataFrame:
    df = symbol_summary.copy()

    for c in ["ct_cv_by_day", "spread_cv_by_day", "tick_cv_by_day", "hit_cnt_cv_by_day"]:
        if c in df.columns:
            med = df[c].median()
            df[c] = df[c].fillna(med if np.isfinite(med) else 1.0)

    stability_penalty = (
        1.0
        + df["ct_cv_by_day"].fillna(1.0)
        + df["spread_cv_by_day"].fillna(1.0)
        + df["tick_cv_by_day"].fillna(1.0)
    )

    df["research_score"] = (
        df["gate_hit2_rate"].fillna(0.0)
        * np.log1p(df["avg_hit_cnt"].clip(lower=0).fillna(0.0))
        * np.log1p(df["n_days"].clip(lower=0).fillna(0.0))
        * df["ratio_mid_rate"].fillna(0.0)
        / stability_penalty.replace(0, np.nan)
    )

    df = df.sort_values(
        ["region", "research_score", "n_days", "avg_hit_cnt"],
        ascending=[True, False, False, False]
    )
    top = df.groupby("region", group_keys=False).head(top_n).reset_index(drop=True)
    top["rank_in_region"] = top.groupby("region").cumcount() + 1

    cols = [
        "region", "rank_in_region", "sym", "research_score", "n_days", "n_rows_total",
        "avg_rows_per_day", "avg_hit_cnt", "median_hit_cnt", "gate_hit2_rate",
        "ratio_mid_rate", "median_ct", "median_spread_bps", "median_tick_day_moves",
        "ct_cv_by_day", "spread_cv_by_day", "tick_cv_by_day"
    ]
    cols = [c for c in cols if c in top.columns]
    return top.loc[:, cols]


def build_region_threshold_candidates(df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for region, g in df.groupby("region", dropna=False):
        ct = pd.to_numeric(g["clearing_time_hat_min"], errors="coerce").dropna()
        sp = pd.to_numeric(g["avg_spread_bps"], errors="coerce").dropna()
        tk = pd.to_numeric(g["tick_day_moves"], errors="coerce").dropna()
        hc = pd.to_numeric(g["hit_cnt"], errors="coerce").dropna()

        rows.append({
            "region": region,
            "n_rows": int(len(g)),
            "n_days": int(g["date_str"].nunique()),
            "X_min_q20_ct": float(ct.quantile(0.20)) if len(ct) else np.nan,
            "X_min_q25_ct": float(ct.quantile(0.25)) if len(ct) else np.nan,
            "X_min_q30_ct": float(ct.quantile(0.30)) if len(ct) else np.nan,
            "Y_bps_q20_spread": float(sp.quantile(0.20)) if len(sp) else np.nan,
            "Y_bps_q25_spread": float(sp.quantile(0.25)) if len(sp) else np.nan,
            "Y_bps_q30_spread": float(sp.quantile(0.30)) if len(sp) else np.nan,
            "Z_ticks_q70_tick": float(tk.quantile(0.70)) if len(tk) else np.nan,
            "Z_ticks_q75_tick": float(tk.quantile(0.75)) if len(tk) else np.nan,
            "Z_ticks_q80_tick": float(tk.quantile(0.80)) if len(tk) else np.nan,
            "hit_cnt_q50": float(hc.quantile(0.50)) if len(hc) else np.nan,
            "hit_cnt_q60": float(hc.quantile(0.60)) if len(hc) else np.nan,
            "hit_cnt_q75": float(hc.quantile(0.75)) if len(hc) else np.nan,
            "suggest_min_hit_cnt": (
                3 if len(hc) and hc.quantile(0.60) >= 3
                else 2 if len(hc) and hc.quantile(0.50) >= 2
                else 1
            ),
        })

    return pd.DataFrame(rows).sort_values("region").reset_index(drop=True)


def make_boxplot(df: pd.DataFrame, metric: str, out_path: Path, title: str, clip_q: float = 0.99):
    tmp = df[["region", metric]].copy()
    tmp[metric] = pd.to_numeric(tmp[metric], errors="coerce")
    tmp = tmp.dropna()
    if tmp.empty:
        print(f"[WARN] no data for {metric} boxplot")
        return

    upper = tmp[metric].quantile(clip_q)
    tmp = tmp[tmp[metric] <= upper] if np.isfinite(upper) else tmp
    regions = sorted(tmp["region"].dropna().unique().tolist())
    data = [tmp.loc[tmp["region"] == r, metric].values for r in regions]
    data = [x for x in data if len(x) > 0]
    labels = [r for r in regions if len(tmp.loc[tmp["region"] == r, metric].values) > 0]

    if not data:
        print(f"[WARN] empty grouped data for {metric}")
        return

    plt.figure(figsize=(10, 6))
    plt.boxplot(data, labels=labels, vert=True, patch_artist=False)
    plt.title(title)
    plt.xlabel("Region")
    plt.ylabel(metric)
    plt.xticks(rotation=0)
    plt.tight_layout()
    plt.savefig(out_path, dpi=160)
    plt.close()
    print(f"[OK] wrote plot {out_path}")


def make_hist_by_region(df: pd.DataFrame, metric: str, plots_dir: Path, clip_q: float = 0.99):
    tmp = df[["region", metric]].copy()
    tmp[metric] = pd.to_numeric(tmp[metric], errors="coerce")
    tmp = tmp.dropna()
    if tmp.empty:
        print(f"[WARN] no data for {metric} histograms")
        return

    upper = tmp[metric].quantile(clip_q)
    tmp = tmp[tmp[metric] <= upper] if np.isfinite(upper) else tmp

    for region, g in tmp.groupby("region"):
        vals = g[metric].dropna().values
        if len(vals) == 0:
            continue
        plt.figure(figsize=(8, 5))
        plt.hist(vals, bins=40)
        plt.title(f"{metric} histogram - {region}")
        plt.xlabel(metric)
        plt.ylabel("Count")
        plt.tight_layout()
        out_path = plots_dir / f"hist_{metric}_{region}.png"
        plt.savefig(out_path, dpi=160)
        plt.close()
        print(f"[OK] wrote plot {out_path}")


def make_region_day_lineplots(region_day: pd.DataFrame, plots_dir: Path):
    metric_map = {
        "clearing_time_hat_min_median": "median_ct",
        "avg_spread_bps_median": "median_spread_bps",
        "tick_day_moves_median": "median_tick_day_moves",
        "gate_rate_hit2": "gate_rate_hit2",
        "ct_bucket_ratio_median": "median_ct_bucket_ratio",
    }

    tmp = region_day.copy()
    tmp["date"] = pd.to_datetime(tmp["date"], errors="coerce")
    tmp = tmp.dropna(subset=["date"])

    for region, g in tmp.groupby("region"):
        g = g.sort_values("date")
        for col, short_name in metric_map.items():
            if col not in g.columns:
                continue
            plt.figure(figsize=(10, 5))
            plt.plot(g["date"], g[col])
            plt.title(f"{short_name} over time - {region}")
            plt.xlabel("Date")
            plt.ylabel(short_name)
            plt.xticks(rotation=45)
            plt.tight_layout()
            out_path = plots_dir / f"region_day_{short_name}_{region}.png"
            plt.savefig(out_path, dpi=160)
            plt.close()
            print(f"[OK] wrote plot {out_path}")


def make_top_ticker_timeseries(df: pd.DataFrame, top_tickers: pd.DataFrame, plots_dir: Path):
    top_pairs = top_tickers[["region", "sym"]].drop_duplicates()

    metric_map = {
        "clearing_time_hat_min": "daily_median_ct",
        "avg_spread_bps": "daily_median_spread_bps",
        "gate_hit2": "daily_gate_hit2_rate",
    }

    base = df.merge(top_pairs, on=["region", "sym"], how="inner")
    if base.empty:
        print("[WARN] no rows for top ticker timeseries plots")
        return

    daily = (
        base.groupby(["region", "sym", "date_str"], dropna=False)
            .agg(
                clearing_time_hat_min=("clearing_time_hat_min", "median"),
                avg_spread_bps=("avg_spread_bps", "median"),
                gate_hit2=("gate_hit2", "mean"),
            )
            .reset_index()
    )
    daily["date"] = pd.to_datetime(daily["date_str"], errors="coerce")
    daily = daily.dropna(subset=["date"])

    for region, rg in daily.groupby("region"):
        syms = top_pairs.loc[top_pairs["region"] == region, "sym"].tolist()
        if not syms:
            continue

        for metric, short_name in metric_map.items():
            plt.figure(figsize=(11, 6))
            plotted = False
            for sym in syms:
                sg = rg.loc[rg["sym"] == sym].sort_values("date")
                if sg.empty:
                    continue
                plt.plot(sg["date"], sg[metric], label=str(sym))
                plotted = True
            if not plotted:
                plt.close()
                continue
            plt.title(f"{short_name} for top tickers - {region}")
            plt.xlabel("Date")
            plt.ylabel(short_name)
            plt.xticks(rotation=45)
            plt.legend(fontsize=8)
            plt.tight_layout()
            out_path = plots_dir / f"top_tickers_{short_name}_{region}.png"
            plt.savefig(out_path, dpi=160)
            plt.close()
            print(f"[OK] wrote plot {out_path}")


def main():
    parser = argparse.ArgumentParser(description="Distribution-first analysis for bucket_metrics outputs.")
    parser.add_argument("--root", default="out", help="Root output directory containing YYYY-MM-DD subfolders")
    parser.add_argument("--analysis_out", default="analysis_out", help="Directory to write analysis CSVs")
    parser.add_argument("--bucket_minutes", type=float, default=10.0, help="Bucket length in minutes used to generate metrics")
    parser.add_argument("--top_n", type=int, default=10, help="Top N tickers per region")
    parser.add_argument("--write_plots", action="store_true", default=True, help="Write PNG plots")
    args = parser.parse_args()

    root = Path(args.root)
    analysis_out = Path(args.analysis_out)
    analysis_out.mkdir(parents=True, exist_ok=True)
    plots_dir = analysis_out / "plots"
    plots_dir.mkdir(parents=True, exist_ok=True)

    print(f"[INFO] loading metrics from {root}")
    all_df = load_all_metrics(root)
    all_df = add_derived_columns(all_df, bucket_minutes=args.bucket_minutes)

    all_metrics_path = analysis_out / "all_bucket_metrics.csv"
    all_df.to_csv(all_metrics_path, index=False)
    print(f"[OK] wrote {all_metrics_path}")

    region_dist = build_region_distribution_summary(all_df)
    region_dist_path = analysis_out / "region_distribution_summary.csv"
    region_dist.to_csv(region_dist_path, index=False)
    print(f"[OK] wrote {region_dist_path}")

    region_day = build_region_day_summary(all_df)
    region_day_path = analysis_out / "region_day_summary.csv"
    region_day.to_csv(region_day_path, index=False)
    print(f"[OK] wrote {region_day_path}")

    symbol_summary = build_symbol_stability_summary(all_df)
    symbol_summary_path = analysis_out / "symbol_stability_summary.csv"
    symbol_summary.to_csv(symbol_summary_path, index=False)
    print(f"[OK] wrote {symbol_summary_path}")

    top_tickers = build_top_tickers_by_region(symbol_summary, top_n=args.top_n)
    top_tickers_path = analysis_out / "top_tickers_by_region.csv"
    top_tickers.to_csv(top_tickers_path, index=False)
    print(f"[OK] wrote {top_tickers_path}")

    threshold_candidates = build_region_threshold_candidates(all_df)
    threshold_candidates_path = analysis_out / "region_threshold_candidates.csv"
    threshold_candidates.to_csv(threshold_candidates_path, index=False)
    print(f"[OK] wrote {threshold_candidates_path}")

    manifest = pd.DataFrame([
        {"file": "all_bucket_metrics.csv", "description": "Merged bucket-level research table across all days"},
        {"file": "region_distribution_summary.csv", "description": "Distribution summary by region"},
        {"file": "region_day_summary.csv", "description": "Region x day stability summary"},
        {"file": "symbol_stability_summary.csv", "description": "Symbol-level stability summary by region"},
        {"file": "top_tickers_by_region.csv", "description": "Top research tickers in each region"},
        {"file": "region_threshold_candidates.csv", "description": "Quantile-based candidate thresholds by region"},
        {"file": "plots/", "description": "PNG plots for distributions and time series"},
    ])
    manifest_path = analysis_out / "analysis_manifest.csv"
    manifest.to_csv(manifest_path, index=False)
    print(f"[OK] wrote {manifest_path}")

    if args.write_plots:
        make_boxplot(all_df, "clearing_time_hat_min", plots_dir / "region_boxplot_clearing_time_hat_min.png",
                     "Clearing time by region")
        make_boxplot(all_df, "avg_spread_bps", plots_dir / "region_boxplot_avg_spread_bps.png",
                     "Spread bps by region")
        make_boxplot(all_df, "tick_day_moves", plots_dir / "region_boxplot_tick_day_moves.png",
                     "Daily tick moves by region")
        make_boxplot(all_df, "hit_cnt", plots_dir / "region_boxplot_hit_cnt.png",
                     "Hit count by region")
        make_boxplot(all_df, "ct_bucket_ratio", plots_dir / "region_boxplot_ct_bucket_ratio.png",
                     "Clearing-time / bucket ratio by region")

        for metric in ["clearing_time_hat_min", "avg_spread_bps", "tick_day_moves", "hit_cnt", "ct_bucket_ratio"]:
            make_hist_by_region(all_df, metric, plots_dir)

        make_region_day_lineplots(region_day, plots_dir)
        make_top_ticker_timeseries(all_df, top_tickers, plots_dir)

    print("[DONE] analysis complete")


if __name__ == "__main__":
    main()
