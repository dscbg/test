import argparse
from pathlib import Path
import itertools
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt


def ensure_tick_col(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    if "tick_day_moves" not in out.columns and "daily_range_ticks" in out.columns:
        out["tick_day_moves"] = pd.to_numeric(out["daily_range_ticks"], errors="coerce")
    return out


def load_inputs(analysis_out: Path, prior_dir_name: str):
    metrics_path = analysis_out / "all_bucket_metrics.csv"
    best_path = analysis_out / prior_dir_name / "best_threshold_combo_by_region.csv"

    if not metrics_path.exists():
        raise FileNotFoundError(f"Missing {metrics_path}")
    if not best_path.exists():
        raise FileNotFoundError(f"Missing {best_path}")

    df = pd.read_csv(metrics_path)
    best = pd.read_csv(best_path)

    df = ensure_tick_col(df)
    df["region"] = df["region"].astype(str)
    df["sym"] = df["sym"].astype(str)
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    if "date_str" not in df.columns:
        df["date_str"] = df["date"].dt.strftime("%Y-%m-%d")

    for c in ["clearing_time_hat_min", "avg_spread_bps", "tick_day_moves", "hit_cnt", "n_hit_bid", "n_hit_ask"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    if "hit_cnt" not in df.columns and {"n_hit_bid", "n_hit_ask"}.issubset(df.columns):
        df["hit_cnt"] = df["n_hit_bid"].fillna(0) + df["n_hit_ask"].fillna(0)

    best["region"] = best["region"].astype(str)
    for c in ["X_min", "Y_bps", "Z_ticks"]:
        best[c] = pd.to_numeric(best[c], errors="coerce")
    return df, best


def qvals(s: pd.Series, qs):
    s = pd.to_numeric(s, errors="coerce").dropna()
    vals = []
    for q in qs:
        try:
            v = float(s.quantile(q))
        except Exception:
            continue
        if np.isfinite(v) and v > 0:
            vals.append(round(v, 6))
    return sorted(set(vals))


def refined_candidate_grid(region: str, region_df: pd.DataFrame, base_row: pd.Series):
    ct = region_df["clearing_time_hat_min"]
    sp = region_df["avg_spread_bps"]
    tk = region_df["tick_day_moves"]

    if region in {"HK", "JP"}:
        x_vals = qvals(ct, [0.25, 0.35, 0.45])
        y_vals = qvals(sp, [0.25, 0.35, 0.45])
        z_vals = [round(float(base_row["Z_ticks"]), 6)] if pd.notna(base_row["Z_ticks"]) else qvals(tk, [0.70, 0.75])
    elif region in {"AU", "KS"}:
        x_vals = qvals(ct, [0.25, 0.35, 0.45])
        y_vals = qvals(sp, [0.25, 0.35, 0.45])
        z_vals = qvals(tk, [0.60, 0.70])
    elif region == "IN":
        x_vals = qvals(ct, [0.40, 0.50, 0.60])
        y_vals = qvals(sp, [0.25, 0.35, 0.45])
        z_vals = qvals(tk, [0.50, 0.60, 0.70])
    else:
        x_vals = qvals(ct, [0.25, 0.35, 0.45])
        y_vals = qvals(sp, [0.25, 0.35, 0.45])
        z_vals = qvals(tk, [0.60, 0.70, 0.80])

    if not x_vals and pd.notna(base_row.get("X_min")):
        x_vals = [round(float(base_row["X_min"]), 6)]
    if not y_vals and pd.notna(base_row.get("Y_bps")):
        y_vals = [round(float(base_row["Y_bps"]), 6)]
    if not z_vals and pd.notna(base_row.get("Z_ticks")):
        z_vals = [round(float(base_row["Z_ticks"]), 6)]
    return x_vals, y_vals, z_vals


def normalize_series(s: pd.Series) -> pd.Series:
    s = pd.to_numeric(s, errors="coerce")
    mn = s.min()
    mx = s.max()
    if not np.isfinite(mn) or not np.isfinite(mx) or mx <= mn:
        return pd.Series(np.full(len(s), 0.5), index=s.index)
    return (s - mn) / (mx - mn)


def evaluate_combo(region_df, region, X_min, Y_bps, Z_ticks, min_hit_cnt,
                   coverage_target_low, coverage_target_high, min_days_per_symbol,
                   eligible_ticker_passrate):
    x = region_df.copy()
    combo_id = f"X{X_min:.6f}|Y{Y_bps:.6f}|Z{Z_ticks:.6f}|H{int(min_hit_cnt)}"

    x["gate_pass"] = (x["hit_cnt"] >= int(min_hit_cnt))
    x["fail_ct"] = x["gate_pass"] & (x["clearing_time_hat_min"] < float(X_min))
    x["fail_spread"] = x["gate_pass"] & (x["avg_spread_bps"] < float(Y_bps))
    x["fail_tick"] = x["gate_pass"] & (x["tick_day_moves"] > float(Z_ticks))
    x["is_queue_bucket"] = x["gate_pass"] & (~x["fail_ct"]) & (~x["fail_spread"]) & (~x["fail_tick"])

    # success rates within gated population
    gated = x.loc[x["gate_pass"]].copy()
    ct_success_rate = float((~gated["fail_ct"]).mean()) if len(gated) else np.nan
    spread_success_rate = float((~gated["fail_spread"]).mean()) if len(gated) else np.nan
    tick_success_rate = float((~gated["fail_tick"]).mean()) if len(gated) else np.nan

    day = (
        x.groupby("date_str", dropna=False)
         .agg(
             gate_pass_rate=("gate_pass", "mean"),
             bucket_coverage=("is_queue_bucket", "mean"),
             ct_success_rate=("fail_ct", lambda s: float((~s).mean()) if len(s) else np.nan),
             spread_success_rate=("fail_spread", lambda s: float((~s).mean()) if len(s) else np.nan),
             tick_success_rate=("fail_tick", lambda s: float((~s).mean()) if len(s) else np.nan),
         )
         .reset_index()
    )
    day["region"] = region
    day["combo_id"] = combo_id
    day["X_min"] = float(X_min)
    day["Y_bps"] = float(Y_bps)
    day["Z_ticks"] = float(Z_ticks)
    day["min_hit_cnt"] = int(min_hit_cnt)

    coverage_mean = float(day["bucket_coverage"].mean()) if len(day) else np.nan
    coverage_std_by_day = float(day["bucket_coverage"].std(ddof=1)) if len(day) > 1 else 0.0
    coverage_target = 0.5 * (coverage_target_low + coverage_target_high)
    coverage_penalty = abs(coverage_mean - coverage_target) if pd.notna(coverage_mean) else 1.0
    coverage_in_range = int(pd.notna(coverage_mean) and coverage_target_low <= coverage_mean <= coverage_target_high)

    ticker_day = (
        x.groupby(["sym", "date_str"], dropna=False)
         .agg(
             daily_pass_rate=("is_queue_bucket", "mean"),
             n_buckets=("sym", "size"),
         )
         .reset_index()
    )
    ticker_day["region"] = region
    ticker_day["combo_id"] = combo_id

    ticker_summary = (
        ticker_day.groupby("sym", dropna=False)
        .agg(
            n_days=("date_str", "nunique"),
            avg_pass_rate=("daily_pass_rate", "mean"),
            std_daily_pass_rate=("daily_pass_rate", "std"),
        )
        .reset_index()
    )
    ticker_summary["std_daily_pass_rate"] = ticker_summary["std_daily_pass_rate"].fillna(0.0)
    ticker_summary["region"] = region
    ticker_summary["combo_id"] = combo_id

    eligible = ticker_summary.loc[
        (ticker_summary["n_days"] >= min_days_per_symbol) &
        (ticker_summary["avg_pass_rate"] >= eligible_ticker_passrate)
    ].copy()

    ticker_day["is_eligible_ticker_day"] = (ticker_day["daily_pass_rate"] >= eligible_ticker_passrate).astype(int)
    ticker_ratio_day = (
        ticker_day.groupby("date_str", dropna=False)
        .agg(
            pct_tickers_ge_thr=("is_eligible_ticker_day", "mean"),
            median_ticker_daily_pass_rate=("daily_pass_rate", "median"),
        )
        .reset_index()
    )
    ticker_ratio_day["region"] = region
    ticker_ratio_day["combo_id"] = combo_id

    combo_row = {
        "region": region,
        "combo_id": combo_id,
        "X_min": float(X_min),
        "Y_bps": float(Y_bps),
        "Z_ticks": float(Z_ticks),
        "min_hit_cnt": int(min_hit_cnt),
        "coverage_mean": coverage_mean,
        "coverage_std_by_day": coverage_std_by_day,
        "coverage_in_target_range": coverage_in_range,
        "coverage_penalty": coverage_penalty,
        "ct_success_rate": ct_success_rate,
        "spread_success_rate": spread_success_rate,
        "tick_success_rate": tick_success_rate,
        "ticker_median_pass_rate": float(ticker_summary["avg_pass_rate"].median()) if len(ticker_summary) else np.nan,
        "pct_tickers_ge_20pct": float((ticker_summary["avg_pass_rate"] >= 0.20).mean()) if len(ticker_summary) else np.nan,
        "n_symbols_eligible": int(len(eligible)),
        "eligible_ticker_stability": float((1.0 / (1.0 + eligible["std_daily_pass_rate"])).mean()) if len(eligible) else 0.0,
        "ticker_ratio_mean_by_day": float(ticker_ratio_day["pct_tickers_ge_thr"].mean()) if len(ticker_ratio_day) else np.nan,
    }
    return combo_row, day, ticker_ratio_day


def score_region_grid(df_region_eval):
    out = df_region_eval.copy()
    out["coverage_target_score"] = 1.0 - normalize_series(out["coverage_penalty"])
    out["coverage_stability_score"] = 1.0 - normalize_series(out["coverage_std_by_day"].fillna(out["coverage_std_by_day"].max()))
    out["ticker_breadth_score"] = normalize_series(out["n_symbols_eligible"])
    out["ticker_passrate_score"] = normalize_series(out["ticker_median_pass_rate"])
    out["ticker_20pct_score"] = normalize_series(out["pct_tickers_ge_20pct"])
    out["ticker_stability_score"] = normalize_series(out["eligible_ticker_stability"])
    out["ticker_ratio_score"] = normalize_series(out["ticker_ratio_mean_by_day"])
    out["final_score"] = (
        1.0 * out["coverage_target_score"]
        + 0.75 * out["coverage_stability_score"]
        + 2.0 * out["ticker_breadth_score"]
        + 1.25 * out["ticker_passrate_score"]
        + 1.25 * out["ticker_20pct_score"]
        + 1.0 * out["ticker_stability_score"]
        + 1.0 * out["ticker_ratio_score"]
        + 0.5 * out["coverage_in_target_range"].astype(float)
    )
    out = out.sort_values(
        ["final_score", "ticker_breadth_score", "ticker_20pct_score", "ticker_ratio_score"],
        ascending=[False, False, False, False]
    ).reset_index(drop=True)
    out["rank_in_region"] = np.arange(1, len(out) + 1)
    return out


def add_bar_labels(ax, fmt="{:.2f}"):
    for p in ax.patches:
        h = p.get_height()
        ax.annotate(fmt.format(h),
                    (p.get_x() + p.get_width() / 2, h),
                    ha="center", va="bottom", fontsize=8,
                    xytext=(0, 3), textcoords="offset points")


def make_overview_plot(best_df, coverage_low, coverage_high, eligible_ticker_passrate, out_path: Path):
    fig, axes = plt.subplots(1, 3, figsize=(15, 5))

    axes[0].bar(best_df["region"], best_df["coverage_mean"])
    axes[0].axhline(coverage_low, linestyle="--")
    axes[0].axhline(coverage_high, linestyle="--")
    axes[0].set_title("Best-combo coverage mean")
    axes[0].set_ylabel("coverage_mean")
    add_bar_labels(axes[0])

    axes[1].bar(best_df["region"], best_df["n_symbols_eligible"])
    axes[1].set_title("Best-combo eligible tickers")
    axes[1].set_ylabel("n_symbols_eligible")
    add_bar_labels(axes[1], "{:.0f}")

    axes[2].bar(best_df["region"], best_df["ticker_ratio_mean_by_day"])
    axes[2].axhline(eligible_ticker_passrate, linestyle="--")
    axes[2].set_title("Best-combo ticker ratio mean")
    axes[2].set_ylabel("pct_tickers_ge_thr")
    add_bar_labels(axes[2])

    plt.tight_layout()
    plt.savefig(out_path, dpi=160)
    plt.close()


def make_region_dashboard(region, best_row, day_eval, ticker_ratio_eval,
                          coverage_low, coverage_high, eligible_ticker_passrate, out_path: Path):
    fig, axes = plt.subplots(3, 1, figsize=(11, 12))

    combo_label = f"Best combo: X={best_row['X_min']:.2f}, Y={best_row['Y_bps']:.2f}, Z={best_row['Z_ticks']:.1f}"

    # Panel 1: best combo coverage
    d1 = day_eval.copy()
    d1["date"] = pd.to_datetime(d1["date_str"], errors="coerce")
    d1 = d1.sort_values("date")
    axes[0].plot(d1["date"], d1["bucket_coverage"], label="coverage")
    axes[0].axhline(coverage_low, linestyle="--")
    axes[0].axhline(coverage_high, linestyle="--")
    axes[0].set_title(f"{region}: coverage over time\n{combo_label}")
    axes[0].legend()

    # Panel 2: success rates (positive direction)
    axes[1].plot(d1["date"], d1["ct_success_rate"], label="ct success")
    axes[1].plot(d1["date"], d1["spread_success_rate"], label="spread success")
    axes[1].plot(d1["date"], d1["tick_success_rate"], label="tick success")
    axes[1].set_title(f"{region}: success rates over time")
    axes[1].legend()

    # Panel 3: ticker ratio
    tr = ticker_ratio_eval.copy()
    tr["date"] = pd.to_datetime(tr["date_str"], errors="coerce")
    tr = tr.sort_values("date")
    tr["roll5"] = tr["pct_tickers_ge_thr"].rolling(5, min_periods=1).mean()
    axes[2].plot(tr["date"], tr["pct_tickers_ge_thr"], label="daily")
    axes[2].plot(tr["date"], tr["roll5"], label="roll5")
    axes[2].axhline(eligible_ticker_passrate, linestyle="--")
    axes[2].set_title(f"{region}: ticker ratio over time")
    axes[2].legend()

    for ax in axes:
        ax.tick_params(axis="x", rotation=45)

    plt.tight_layout()
    plt.savefig(out_path, dpi=160)
    plt.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--analysis_out", default="analysis_out")
    parser.add_argument("--coverage_target_low", type=float, default=0.10)
    parser.add_argument("--coverage_target_high", type=float, default=0.30)
    parser.add_argument("--eligible_ticker_passrate", type=float, default=0.10)
    parser.add_argument("--min_days_per_symbol", type=int, default=5)
    parser.add_argument("--min_hit_cnt", type=int, default=2)
    args = parser.parse_args()

    analysis_out = Path(args.analysis_out)
    out_dir = analysis_out / "refined_v4_best_only"
    plots_dir = out_dir / "plots"
    out_dir.mkdir(parents=True, exist_ok=True)
    plots_dir.mkdir(parents=True, exist_ok=True)

    df, prior_best = load_inputs(analysis_out, "threshold_search_ticker_focus")

    combo_rows, day_rows, ticker_ratio_rows = [], [], []
    for _, b in prior_best.iterrows():
        region = b["region"]
        region_df = df[df["region"] == region].copy()
        x_vals, y_vals, z_vals = refined_candidate_grid(region, region_df, b)

        for X, Y, Z in itertools.product(x_vals, y_vals, z_vals):
            combo_row, day_eval, ticker_ratio_day = evaluate_combo(
                region_df, region, X, Y, Z,
                args.min_hit_cnt,
                args.coverage_target_low, args.coverage_target_high,
                args.min_days_per_symbol, args.eligible_ticker_passrate
            )
            combo_rows.append(combo_row)
            day_rows.append(day_eval)
            ticker_ratio_rows.append(ticker_ratio_day)

    combo_eval = pd.DataFrame(combo_rows)
    day_eval = pd.concat(day_rows, ignore_index=True)
    ticker_ratio_eval = pd.concat(ticker_ratio_rows, ignore_index=True)

    scored = pd.concat([score_region_grid(g) for _, g in combo_eval.groupby("region")], ignore_index=True)
    scored = scored.sort_values(["region", "rank_in_region"]).reset_index(drop=True)
    best = scored.groupby("region", group_keys=False).head(1).reset_index(drop=True)

    scored.to_csv(out_dir / "refined_region_combo_grid_eval.csv", index=False)
    best.to_csv(out_dir / "refined_best_threshold_combo_by_region.csv", index=False)
    day_eval.to_csv(out_dir / "refined_region_day_combo_eval.csv", index=False)
    ticker_ratio_eval.to_csv(out_dir / "refined_region_ticker_ratio_day_combo_eval.csv", index=False)

    make_overview_plot(best, args.coverage_target_low, args.coverage_target_high,
                       args.eligible_ticker_passrate, plots_dir / "overview.png")

    for _, row in best.iterrows():
        region = row["region"]
        combo_id = row["combo_id"]
        make_region_dashboard(
            region,
            row,
            day_eval[(day_eval["region"] == region) & (day_eval["combo_id"] == combo_id)],
            ticker_ratio_eval[(ticker_ratio_eval["region"] == region) & (ticker_ratio_eval["combo_id"] == combo_id)],
            args.coverage_target_low,
            args.coverage_target_high,
            args.eligible_ticker_passrate,
            plots_dir / f"{region}.png"
        )

    manifest = pd.DataFrame([
        {"file": "refined_region_combo_grid_eval.csv", "description": "Round-2 refined combo evaluation"},
        {"file": "refined_best_threshold_combo_by_region.csv", "description": "Best refined threshold combo by region"},
        {"file": "refined_region_day_combo_eval.csv", "description": "Best-combo region-day evaluation base table"},
        {"file": "refined_region_ticker_ratio_day_combo_eval.csv", "description": "Best-combo ticker-ratio day table"},
        {"file": "plots/overview.png", "description": "Overview bars with numeric labels"},
        {"file": "plots/<REGION>.png", "description": "Best-only 3-panel dashboard per region"},
    ])
    manifest.to_csv(out_dir / "manifest.csv", index=False)
    print(f"[OK] wrote {out_dir}")


if __name__ == "__main__":
    main()
