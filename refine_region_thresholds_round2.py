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

    gated = x.loc[x["gate_pass"]]
    fail_ct_rate = float(gated["fail_ct"].mean()) if len(gated) else np.nan
    fail_spread_rate = float(gated["fail_spread"].mean()) if len(gated) else np.nan
    fail_tick_rate = float(gated["fail_tick"].mean()) if len(gated) else np.nan

    day = (
        x.groupby("date_str", dropna=False)
         .agg(
             gate_pass_rate=("gate_pass", "mean"),
             bucket_coverage=("is_queue_bucket", "mean"),
             fail_ct_rate=("fail_ct", "mean"),
             fail_spread_rate=("fail_spread", "mean"),
             fail_tick_rate=("fail_tick", "mean"),
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
    coverage_iqr_by_day = float(day["bucket_coverage"].quantile(0.75) - day["bucket_coverage"].quantile(0.25)) if len(day) else np.nan

    ticker_day = (
        x.groupby(["sym", "date_str"], dropna=False)
         .agg(
             daily_pass_rate=("is_queue_bucket", "mean"),
             daily_gate_rate=("gate_pass", "mean"),
             n_buckets=("sym", "size"),
         )
         .reset_index()
    )
    ticker_day["region"] = region
    ticker_day["combo_id"] = combo_id
    ticker_day["X_min"] = float(X_min)
    ticker_day["Y_bps"] = float(Y_bps)
    ticker_day["Z_ticks"] = float(Z_ticks)
    ticker_day["min_hit_cnt"] = int(min_hit_cnt)

    ticker_summary = (
        ticker_day.groupby("sym", dropna=False)
        .agg(
            n_days=("date_str", "nunique"),
            avg_pass_rate=("daily_pass_rate", "mean"),
            median_daily_pass_rate=("daily_pass_rate", "median"),
            std_daily_pass_rate=("daily_pass_rate", "std"),
            avg_daily_gate_rate=("daily_gate_rate", "mean"),
            avg_buckets_per_day=("n_buckets", "mean"),
        )
        .reset_index()
    )
    ticker_summary["std_daily_pass_rate"] = ticker_summary["std_daily_pass_rate"].fillna(0.0)
    ticker_summary["region"] = region
    ticker_summary["combo_id"] = combo_id
    ticker_summary["X_min"] = float(X_min)
    ticker_summary["Y_bps"] = float(Y_bps)
    ticker_summary["Z_ticks"] = float(Z_ticks)
    ticker_summary["min_hit_cnt"] = int(min_hit_cnt)

    eligible = ticker_summary.loc[
        (ticker_summary["n_days"] >= min_days_per_symbol) &
        (ticker_summary["avg_pass_rate"] >= eligible_ticker_passrate)
    ].copy()
    ticker_summary["is_eligible_ticker"] = (
        (ticker_summary["n_days"] >= min_days_per_symbol) &
        (ticker_summary["avg_pass_rate"] >= eligible_ticker_passrate)
    ).astype(int)

    fail_rates = [v for v in [fail_ct_rate, fail_spread_rate, fail_tick_rate] if pd.notna(v)]
    fail_imbalance = float(max(fail_rates) - min(fail_rates)) if fail_rates else np.nan
    coverage_target = 0.5 * (coverage_target_low + coverage_target_high)
    coverage_penalty = abs(coverage_mean - coverage_target) if pd.notna(coverage_mean) else 1.0
    coverage_in_range = int(pd.notna(coverage_mean) and coverage_target_low <= coverage_mean <= coverage_target_high)

    combo_row = {
        "region": region,
        "combo_id": combo_id,
        "X_min": float(X_min),
        "Y_bps": float(Y_bps),
        "Z_ticks": float(Z_ticks),
        "min_hit_cnt": int(min_hit_cnt),
        "n_rows": int(len(x)),
        "n_days": int(x["date_str"].nunique()),
        "n_syms": int(x["sym"].nunique()),
        "gate_pass_rate": float(x["gate_pass"].mean()) if len(x) else np.nan,
        "bucket_coverage": float(x["is_queue_bucket"].mean()) if len(x) else np.nan,
        "coverage_mean": coverage_mean,
        "coverage_std_by_day": coverage_std_by_day,
        "coverage_iqr_by_day": coverage_iqr_by_day,
        "coverage_in_target_range": coverage_in_range,
        "coverage_penalty": coverage_penalty,
        "fail_ct_rate": fail_ct_rate,
        "fail_spread_rate": fail_spread_rate,
        "fail_tick_rate": fail_tick_rate,
        "fail_imbalance": fail_imbalance,
        "ticker_median_pass_rate": float(ticker_summary["avg_pass_rate"].median()) if len(ticker_summary) else np.nan,
        "pct_tickers_ge_10pct": float((ticker_summary["avg_pass_rate"] >= 0.10).mean()) if len(ticker_summary) else np.nan,
        "pct_tickers_ge_20pct": float((ticker_summary["avg_pass_rate"] >= 0.20).mean()) if len(ticker_summary) else np.nan,
        "pct_tickers_ge_30pct": float((ticker_summary["avg_pass_rate"] >= 0.30).mean()) if len(ticker_summary) else np.nan,
        "n_symbols_eligible": int(len(eligible)),
        "eligible_ticker_stability": float((1.0 / (1.0 + eligible["std_daily_pass_rate"])).mean()) if len(eligible) else 0.0,
    }
    return combo_row, ticker_summary, ticker_day, day


def score_region_grid(df_region_eval):
    out = df_region_eval.copy()
    out["coverage_target_score"] = 1.0 - normalize_series(out["coverage_penalty"])
    out["coverage_stability_score"] = 1.0 - normalize_series(out["coverage_std_by_day"].fillna(out["coverage_std_by_day"].max()))
    out["ticker_breadth_score"] = normalize_series(out["n_symbols_eligible"])
    out["ticker_passrate_score"] = normalize_series(out["ticker_median_pass_rate"])
    out["ticker_20pct_score"] = normalize_series(out["pct_tickers_ge_20pct"])
    out["ticker_stability_score"] = normalize_series(out["eligible_ticker_stability"])
    out["fail_balance_score"] = 1.0 - normalize_series(out["fail_imbalance"].fillna(out["fail_imbalance"].max()))
    out["final_score"] = (
        1.0 * out["coverage_target_score"]
        + 1.0 * out["coverage_stability_score"]
        + 2.0 * out["ticker_breadth_score"]
        + 1.5 * out["ticker_passrate_score"]
        + 1.5 * out["ticker_20pct_score"]
        + 1.25 * out["ticker_stability_score"]
        + 0.5 * out["fail_balance_score"]
        + 0.5 * out["coverage_in_target_range"].astype(float)
    )
    out = out.sort_values(
        ["final_score", "ticker_breadth_score", "ticker_20pct_score", "coverage_stability_score"],
        ascending=[False, False, False, False]
    ).reset_index(drop=True)
    out["rank_in_region"] = np.arange(1, len(out) + 1)
    return out


def make_overview_plot(best_df, coverage_low, coverage_high, out_path: Path):
    fig, axes = plt.subplots(1, 2, figsize=(12, 5))
    axes[0].bar(best_df["region"], best_df["coverage_mean"])
    axes[0].axhline(coverage_low, linestyle="--")
    axes[0].axhline(coverage_high, linestyle="--")
    axes[0].set_title("Best-combo coverage mean by region")
    axes[0].set_xlabel("Region")
    axes[0].set_ylabel("coverage_mean")
    axes[1].bar(best_df["region"], best_df["n_symbols_eligible"])
    axes[1].set_title("Best-combo eligible tickers by region")
    axes[1].set_xlabel("Region")
    axes[1].set_ylabel("n_symbols_eligible")
    plt.tight_layout()
    plt.savefig(out_path, dpi=160)
    plt.close()


def make_region_dashboard(region, top3_eval, best_combo_id, day_eval, ticker_day_eval, ticker_summary_eval,
                          coverage_low, coverage_high, eligible_ticker_passrate, top_n_tickers, out_path: Path):
    fig, axes = plt.subplots(3, 1, figsize=(11, 12))

    d1 = day_eval.loc[day_eval["combo_id"].isin(top3_eval["combo_id"].tolist())].copy()
    d1["date"] = pd.to_datetime(d1["date_str"], errors="coerce")
    for combo_id, g in d1.groupby("combo_id"):
        rank = int(top3_eval.loc[top3_eval["combo_id"] == combo_id, "rank_in_region"].iloc[0])
        g = g.sort_values("date")
        axes[0].plot(g["date"], g["bucket_coverage"], label=f"#{rank}")
    axes[0].axhline(coverage_low, linestyle="--")
    axes[0].axhline(coverage_high, linestyle="--")
    axes[0].set_title(f"{region}: top-3 combos daily bucket coverage")
    axes[0].set_ylabel("bucket_coverage")
    axes[0].legend()

    d2 = day_eval.loc[day_eval["combo_id"] == best_combo_id].copy()
    d2["date"] = pd.to_datetime(d2["date_str"], errors="coerce")
    d2 = d2.sort_values("date")
    axes[1].plot(d2["date"], d2["fail_ct_rate"], label="fail_ct_rate")
    axes[1].plot(d2["date"], d2["fail_spread_rate"], label="fail_spread_rate")
    axes[1].plot(d2["date"], d2["fail_tick_rate"], label="fail_tick_rate")
    axes[1].set_title(f"{region}: best-combo fail rates over time")
    axes[1].set_ylabel("fail rate")
    axes[1].legend()

    ts = ticker_summary_eval.loc[ticker_summary_eval["combo_id"] == best_combo_id].copy()
    ts = ts.sort_values(["avg_pass_rate", "n_days"], ascending=[False, False]).head(top_n_tickers)
    keep_syms = ts["sym"].astype(str).tolist()

    td = ticker_day_eval.loc[(ticker_day_eval["combo_id"] == best_combo_id) &
                             (ticker_day_eval["sym"].astype(str).isin(keep_syms))].copy()
    td["date"] = pd.to_datetime(td["date_str"], errors="coerce")
    for sym, g in td.groupby("sym"):
        g = g.sort_values("date")
        axes[2].plot(g["date"], g["daily_pass_rate"], label=str(sym))
    axes[2].axhline(eligible_ticker_passrate, linestyle="--")
    axes[2].set_title(f"{region}: top-{top_n_tickers} ticker daily pass rates (best combo)")
    axes[2].set_ylabel("daily_pass_rate")
    axes[2].legend(fontsize=8)

    for ax in axes:
        ax.tick_params(axis="x", rotation=45)

    plt.tight_layout()
    plt.savefig(out_path, dpi=160)
    plt.close()


def main():
    parser = argparse.ArgumentParser(description="Second-round region-specific threshold refinement with compact stability plots.")
    parser.add_argument("--analysis_out", default="analysis_out")
    parser.add_argument("--prior_dir_name", default="threshold_search_ticker_focus")
    parser.add_argument("--coverage_target_low", type=float, default=0.10)
    parser.add_argument("--coverage_target_high", type=float, default=0.30)
    parser.add_argument("--min_days_per_symbol", type=int, default=5)
    parser.add_argument("--eligible_ticker_passrate", type=float, default=0.10)
    parser.add_argument("--min_hit_cnt", type=int, default=2)
    parser.add_argument("--top_n_tickers", type=int, default=5)
    args = parser.parse_args()

    analysis_out = Path(args.analysis_out)
    out_dir = analysis_out / "threshold_refine_round2"
    plots_dir = out_dir / "plots"
    out_dir.mkdir(parents=True, exist_ok=True)
    plots_dir.mkdir(parents=True, exist_ok=True)

    df, prior_best = load_inputs(analysis_out, args.prior_dir_name)

    combo_rows, ticker_summary_rows, ticker_day_rows, day_rows = [], [], [], []
    for _, b in prior_best.iterrows():
        region = str(b["region"])
        region_df = df.loc[df["region"] == region].copy()
        if region_df.empty:
            continue

        x_vals, y_vals, z_vals = refined_candidate_grid(region, region_df, b)
        if not x_vals or not y_vals or not z_vals:
            print(f"[WARN] skip region {region}: empty refined grid")
            continue

        for X_min, Y_bps, Z_ticks in itertools.product(x_vals, y_vals, z_vals):
            combo_row, ticker_summary, ticker_day, day_eval = evaluate_combo(
                region_df=region_df,
                region=region,
                X_min=X_min,
                Y_bps=Y_bps,
                Z_ticks=Z_ticks,
                min_hit_cnt=args.min_hit_cnt,
                coverage_target_low=args.coverage_target_low,
                coverage_target_high=args.coverage_target_high,
                min_days_per_symbol=args.min_days_per_symbol,
                eligible_ticker_passrate=args.eligible_ticker_passrate,
            )
            combo_rows.append(combo_row)
            ticker_summary_rows.append(ticker_summary)
            ticker_day_rows.append(ticker_day)
            day_rows.append(day_eval)

    if not combo_rows:
        raise RuntimeError("No refined combo evaluations were produced")

    combo_eval = pd.DataFrame(combo_rows)
    ticker_summary_eval = pd.concat(ticker_summary_rows, ignore_index=True)
    ticker_day_eval = pd.concat(ticker_day_rows, ignore_index=True)
    day_eval = pd.concat(day_rows, ignore_index=True)

    scored = pd.concat([score_region_grid(g) for _, g in combo_eval.groupby("region")], ignore_index=True)
    scored = scored.sort_values(["region", "rank_in_region"]).reset_index(drop=True)
    best = scored.groupby("region", group_keys=False).head(1).reset_index(drop=True)

    scored.to_csv(out_dir / "refined_region_combo_grid_eval.csv", index=False)
    best.to_csv(out_dir / "refined_best_threshold_combo_by_region.csv", index=False)
    ticker_summary_eval.to_csv(out_dir / "refined_region_ticker_combo_eval.csv", index=False)
    ticker_day_eval.to_csv(out_dir / "refined_region_ticker_day_combo_eval.csv", index=False)
    day_eval.to_csv(out_dir / "refined_region_day_combo_eval.csv", index=False)

    manifest = pd.DataFrame([
        {"file": "refined_region_combo_grid_eval.csv", "description": "Round-2 refined combo evaluation per region"},
        {"file": "refined_best_threshold_combo_by_region.csv", "description": "Best refined threshold combo per region"},
        {"file": "refined_region_ticker_combo_eval.csv", "description": "Ticker-level pass-rate summary for every refined combo"},
        {"file": "refined_region_ticker_day_combo_eval.csv", "description": "Ticker-day pass-rate table for every refined combo"},
        {"file": "refined_region_day_combo_eval.csv", "description": "Region-day coverage/fail-rate table for every refined combo"},
        {"file": "plots/refined_best_combo_overview.png", "description": "Coverage + eligible ticker overview for best refined combos"},
        {"file": "plots/<REGION>_stability_dashboard.png", "description": "Compact 3-panel stability dashboard per region"},
    ])
    manifest.to_csv(out_dir / "refine_round2_manifest.csv", index=False)

    make_overview_plot(best, args.coverage_target_low, args.coverage_target_high, plots_dir / "refined_best_combo_overview.png")

    for region, region_best in best.groupby("region"):
        best_combo_id = region_best["combo_id"].iloc[0]
        top3 = scored.loc[scored["region"] == region].sort_values("rank_in_region").head(3)
        make_region_dashboard(
            region=region,
            top3_eval=top3,
            best_combo_id=best_combo_id,
            day_eval=day_eval.loc[day_eval["region"] == region],
            ticker_day_eval=ticker_day_eval.loc[ticker_day_eval["region"] == region],
            ticker_summary_eval=ticker_summary_eval.loc[ticker_summary_eval["region"] == region],
            coverage_low=args.coverage_target_low,
            coverage_high=args.coverage_target_high,
            eligible_ticker_passrate=args.eligible_ticker_passrate,
            top_n_tickers=args.top_n_tickers,
            out_path=plots_dir / f"{region}_stability_dashboard.png",
        )

    print(f"[OK] wrote {out_dir / 'refined_region_combo_grid_eval.csv'}")
    print(f"[OK] wrote {out_dir / 'refined_best_threshold_combo_by_region.csv'}")
    print(f"[OK] wrote {out_dir / 'refined_region_ticker_combo_eval.csv'}")
    print(f"[OK] wrote {out_dir / 'refined_region_ticker_day_combo_eval.csv'}")
    print(f"[OK] wrote {out_dir / 'refined_region_day_combo_eval.csv'}")
    print(f"[OK] wrote {out_dir / 'refine_round2_manifest.csv'}")
    print("[DONE] round-2 threshold refinement complete")


if __name__ == "__main__":
    main()
