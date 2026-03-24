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


def load_inputs(analysis_out: Path):
    metrics_path = analysis_out / "all_bucket_metrics.csv"
    threshold_path = analysis_out / "region_threshold_candidates.csv"

    if not metrics_path.exists():
        raise FileNotFoundError(f"Missing {metrics_path}")
    if not threshold_path.exists():
        raise FileNotFoundError(f"Missing {threshold_path}")

    df = pd.read_csv(metrics_path)
    cand = pd.read_csv(threshold_path)

    df = ensure_tick_col(df)
    df["region"] = df["region"].astype(str)
    df["sym"] = df["sym"].astype(str)
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    if "date_str" not in df.columns:
        df["date_str"] = df["date"].dt.strftime("%Y-%m-%d")

    for c in [
        "clearing_time_hat_min", "avg_spread_bps", "tick_day_moves", "hit_cnt",
        "n_hit_bid", "n_hit_ask"
    ]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    if "hit_cnt" not in df.columns and {"n_hit_bid", "n_hit_ask"}.issubset(df.columns):
        df["hit_cnt"] = df["n_hit_bid"].fillna(0) + df["n_hit_ask"].fillna(0)

    cand["region"] = cand["region"].astype(str)
    return df, cand


def clamp_positive(vals):
    out = []
    for v in vals:
        if pd.isna(v):
            continue
        try:
            fv = float(v)
        except Exception:
            continue
        if np.isfinite(fv) and fv > 0:
            out.append(fv)
    return sorted(set(out))


def candidate_grid_for_region(row: pd.Series):
    x_vals = clamp_positive([
        row.get("X_min_q20_ct"),
        row.get("X_min_q25_ct"),
        row.get("X_min_q30_ct"),
    ])
    y_vals = clamp_positive([
        row.get("Y_bps_q20_spread"),
        row.get("Y_bps_q25_spread"),
        row.get("Y_bps_q30_spread"),
    ])
    z_vals = clamp_positive([
        row.get("Z_ticks_q70_tick"),
        row.get("Z_ticks_q75_tick"),
        row.get("Z_ticks_q80_tick"),
    ])

    base_hit = row.get("suggest_min_hit_cnt", 2)
    try:
        base_hit = int(base_hit)
    except Exception:
        base_hit = 2
    hit_vals = sorted(set([max(1, base_hit - 1), max(1, base_hit), max(1, base_hit + 1)]))

    return x_vals, y_vals, z_vals, hit_vals


def normalize_series(s: pd.Series) -> pd.Series:
    s = pd.to_numeric(s, errors="coerce")
    mn = s.min()
    mx = s.max()
    if not np.isfinite(mn) or not np.isfinite(mx) or mx <= mn:
        return pd.Series(np.full(len(s), 0.5), index=s.index)
    return (s - mn) / (mx - mn)


def evaluate_combo(region_df: pd.DataFrame, region: str, X_min: float, Y_bps: float, Z_ticks: float, min_hit_cnt: int,
                   coverage_target_low: float, coverage_target_high: float, min_days_per_symbol: int) -> dict:
    x = region_df.copy()

    x["gate_pass"] = (x["hit_cnt"] >= int(min_hit_cnt))
    x["fail_ct"] = x["gate_pass"] & (x["clearing_time_hat_min"] < float(X_min))
    x["fail_spread"] = x["gate_pass"] & (x["avg_spread_bps"] < float(Y_bps))
    x["fail_tick"] = x["gate_pass"] & (x["tick_day_moves"] > float(Z_ticks))
    x["is_queue_bucket"] = x["gate_pass"] & (~x["fail_ct"]) & (~x["fail_spread"]) & (~x["fail_tick"])

    gate_pass_rate = float(x["gate_pass"].mean()) if len(x) else np.nan
    bucket_coverage = float(x["is_queue_bucket"].mean()) if len(x) else np.nan

    gated = x.loc[x["gate_pass"]]
    fail_ct_rate = float(gated["fail_ct"].mean()) if len(gated) else np.nan
    fail_spread_rate = float(gated["fail_spread"].mean()) if len(gated) else np.nan
    fail_tick_rate = float(gated["fail_tick"].mean()) if len(gated) else np.nan

    day = (
        x.groupby("date_str", dropna=False)
         .agg(
             n_rows=("sym", "size"),
             gate_pass_rate=("gate_pass", "mean"),
             bucket_coverage=("is_queue_bucket", "mean"),
             fail_ct_rate=("fail_ct", "mean"),
             fail_spread_rate=("fail_spread", "mean"),
             fail_tick_rate=("fail_tick", "mean"),
         )
         .reset_index()
    )
    coverage_mean = float(day["bucket_coverage"].mean()) if len(day) else np.nan
    coverage_std_by_day = float(day["bucket_coverage"].std(ddof=1)) if len(day) > 1 else 0.0
    coverage_iqr_by_day = (
        float(day["bucket_coverage"].quantile(0.75) - day["bucket_coverage"].quantile(0.25))
        if len(day) else np.nan
    )

    sym = (
        x.groupby(["sym", "date_str"], dropna=False)
         .agg(sym_day_coverage=("is_queue_bucket", "mean"))
         .reset_index()
    )
    sym_summary = (
        sym.groupby("sym", dropna=False)
           .agg(
               n_days=("date_str", "nunique"),
               avg_coverage=("sym_day_coverage", "mean"),
               std_coverage=("sym_day_coverage", "std"),
           )
           .reset_index()
    )
    sym_summary["std_coverage"] = sym_summary["std_coverage"].fillna(0.0)

    eligible_syms = sym_summary.loc[sym_summary["n_days"] >= min_days_per_symbol].copy()
    n_symbols_eligible = int(len(eligible_syms))
    top_ticker_stability = (
        float((1.0 / (1.0 + eligible_syms["std_coverage"])).mean())
        if len(eligible_syms) else 0.0
    )
    avg_symbol_coverage = float(eligible_syms["avg_coverage"].mean()) if len(eligible_syms) else 0.0

    fail_rates = [v for v in [fail_ct_rate, fail_spread_rate, fail_tick_rate] if pd.notna(v)]
    fail_imbalance = float(max(fail_rates) - min(fail_rates)) if fail_rates else np.nan

    coverage_in_range = (
        (coverage_mean >= coverage_target_low) and (coverage_mean <= coverage_target_high)
        if pd.notna(coverage_mean) else False
    )
    coverage_target = 0.5 * (coverage_target_low + coverage_target_high)
    coverage_penalty = abs(coverage_mean - coverage_target) if pd.notna(coverage_mean) else 1.0

    return {
        "region": region,
        "X_min": float(X_min),
        "Y_bps": float(Y_bps),
        "Z_ticks": float(Z_ticks),
        "min_hit_cnt": int(min_hit_cnt),
        "n_rows": int(len(x)),
        "n_days": int(x["date_str"].nunique()),
        "n_syms": int(x["sym"].nunique()),
        "gate_pass_rate": gate_pass_rate,
        "bucket_coverage": bucket_coverage,
        "coverage_mean": coverage_mean,
        "coverage_std_by_day": coverage_std_by_day,
        "coverage_iqr_by_day": coverage_iqr_by_day,
        "fail_ct_rate": fail_ct_rate,
        "fail_spread_rate": fail_spread_rate,
        "fail_tick_rate": fail_tick_rate,
        "fail_imbalance": fail_imbalance,
        "n_symbols_eligible": n_symbols_eligible,
        "avg_symbol_coverage": avg_symbol_coverage,
        "top_ticker_stability": top_ticker_stability,
        "coverage_in_target_range": int(bool(coverage_in_range)),
        "coverage_penalty": coverage_penalty,
    }


def score_region_grid(df_region_eval: pd.DataFrame) -> pd.DataFrame:
    out = df_region_eval.copy()

    out["coverage_target_score"] = 1.0 - normalize_series(out["coverage_penalty"])
    out["coverage_stability_score"] = 1.0 - normalize_series(out["coverage_std_by_day"].fillna(out["coverage_std_by_day"].max()))
    out["symbol_depth_score"] = normalize_series(out["n_symbols_eligible"])
    out["top_ticker_stability_score"] = normalize_series(out["top_ticker_stability"])
    out["fail_balance_score"] = 1.0 - normalize_series(out["fail_imbalance"].fillna(out["fail_imbalance"].max()))

    out["final_score"] = (
        2.0 * out["coverage_target_score"]
        + 1.5 * out["coverage_stability_score"]
        + 1.25 * out["symbol_depth_score"]
        + 1.0 * out["top_ticker_stability_score"]
        + 0.75 * out["fail_balance_score"]
        + 0.75 * out["coverage_in_target_range"].astype(float)
    )

    out = out.sort_values(
        ["coverage_in_target_range", "final_score", "symbol_depth_score", "coverage_stability_score"],
        ascending=[False, False, False, False]
    ).reset_index(drop=True)
    out["rank_in_region"] = np.arange(1, len(out) + 1)
    return out


def write_plots(region_eval: pd.DataFrame, best_df: pd.DataFrame, plots_dir: Path):
    plots_dir.mkdir(parents=True, exist_ok=True)

    for region, g in region_eval.groupby("region"):
        top = g.sort_values("rank_in_region").head(10)

        plt.figure(figsize=(10, 5))
        labels = [
            f"X{r.X_min:.2f}\nY{r.Y_bps:.2f}\nZ{r.Z_ticks:.2f}\nH{int(r.min_hit_cnt)}"
            for _, r in top.iterrows()
        ]
        plt.bar(labels, top["final_score"])
        plt.title(f"Top threshold combos by score - {region}")
        plt.xlabel("Combo")
        plt.ylabel("final_score")
        plt.xticks(rotation=45, ha="right")
        plt.tight_layout()
        out = plots_dir / f"top_combos_score_{region}.png"
        plt.savefig(out, dpi=160)
        plt.close()

        plt.figure(figsize=(8, 6))
        size_norm = normalize_series(top["n_symbols_eligible"]).fillna(0.5)
        sizes = 40 + 120 * size_norm
        plt.scatter(top["coverage_mean"], top["coverage_std_by_day"], s=sizes)
        for _, r in top.iterrows():
            plt.annotate(
                f"#{int(r.rank_in_region)}",
                (r.coverage_mean, r.coverage_std_by_day),
                fontsize=8,
                xytext=(3, 3),
                textcoords="offset points",
            )
        plt.title(f"Coverage mean vs std (top combos) - {region}")
        plt.xlabel("coverage_mean")
        plt.ylabel("coverage_std_by_day")
        plt.tight_layout()
        out = plots_dir / f"coverage_mean_vs_std_{region}.png"
        plt.savefig(out, dpi=160)
        plt.close()

    if not best_df.empty:
        plt.figure(figsize=(9, 5))
        plt.bar(best_df["region"], best_df["bucket_coverage"])
        plt.title("Best-combo bucket coverage by region")
        plt.xlabel("Region")
        plt.ylabel("bucket_coverage")
        plt.tight_layout()
        plt.savefig(plots_dir / "best_combo_bucket_coverage_by_region.png", dpi=160)
        plt.close()

        plt.figure(figsize=(10, 6))
        x = np.arange(len(best_df))
        width = 0.22
        cols = ["fail_ct_rate", "fail_spread_rate", "fail_tick_rate"]
        for i, c in enumerate(cols):
            plt.bar(x + (i - 1) * width, best_df[c], width=width, label=c)
        plt.xticks(x, best_df["region"])
        plt.title("Best-combo fail reason rates by region")
        plt.xlabel("Region")
        plt.ylabel("Rate")
        plt.legend()
        plt.tight_layout()
        plt.savefig(plots_dir / "best_combo_fail_reason_rates_by_region.png", dpi=160)
        plt.close()


def main():
    parser = argparse.ArgumentParser(description="Search best threshold combo by region from existing analysis outputs.")
    parser.add_argument("--analysis_out", default="analysis_out", help="Directory containing all_bucket_metrics.csv and region_threshold_candidates.csv")
    parser.add_argument("--coverage_target_low", type=float, default=0.10, help="Lower bound of preferred bucket coverage")
    parser.add_argument("--coverage_target_high", type=float, default=0.40, help="Upper bound of preferred bucket coverage")
    parser.add_argument("--min_days_per_symbol", type=int, default=5, help="Minimum days for symbol to count as eligible in scoring")
    args = parser.parse_args()

    analysis_out = Path(args.analysis_out)
    out_dir = analysis_out / "threshold_search"
    plots_dir = out_dir / "plots"
    out_dir.mkdir(parents=True, exist_ok=True)
    plots_dir.mkdir(parents=True, exist_ok=True)

    df, candidates = load_inputs(analysis_out)

    all_rows = []
    for _, row in candidates.iterrows():
        region = str(row["region"])
        region_df = df.loc[df["region"] == region].copy()
        if region_df.empty:
            continue

        x_vals, y_vals, z_vals, hit_vals = candidate_grid_for_region(row)
        if not x_vals or not y_vals or not z_vals or not hit_vals:
            print(f"[WARN] skip region {region}: empty candidate grid")
            continue

        for X_min, Y_bps, Z_ticks, min_hit_cnt in itertools.product(x_vals, y_vals, z_vals, hit_vals):
            res = evaluate_combo(
                region_df=region_df,
                region=region,
                X_min=X_min,
                Y_bps=Y_bps,
                Z_ticks=Z_ticks,
                min_hit_cnt=min_hit_cnt,
                coverage_target_low=args.coverage_target_low,
                coverage_target_high=args.coverage_target_high,
                min_days_per_symbol=args.min_days_per_symbol,
            )
            all_rows.append(res)

    if not all_rows:
        raise RuntimeError("No combo evaluations were produced")

    region_combo_grid_eval = pd.DataFrame(all_rows)

    scored_frames = []
    for region, g in region_combo_grid_eval.groupby("region"):
        scored_frames.append(score_region_grid(g))

    scored = pd.concat(scored_frames, ignore_index=True)
    scored = scored.sort_values(["region", "rank_in_region"]).reset_index(drop=True)

    best = scored.groupby("region", group_keys=False).head(1).reset_index(drop=True)

    grid_path = out_dir / "region_combo_grid_eval.csv"
    best_path = out_dir / "best_threshold_combo_by_region.csv"

    scored.to_csv(grid_path, index=False)
    best.to_csv(best_path, index=False)

    manifest = pd.DataFrame([
        {"file": "region_combo_grid_eval.csv", "description": "All evaluated threshold combos for each region with scores"},
        {"file": "best_threshold_combo_by_region.csv", "description": "Top-ranked threshold combo for each region"},
        {"file": "plots/top_combos_score_<REGION>.png", "description": "Top 10 combo scores by region"},
        {"file": "plots/coverage_mean_vs_std_<REGION>.png", "description": "Coverage mean vs day-to-day std for top combos"},
        {"file": "plots/best_combo_bucket_coverage_by_region.png", "description": "Best-combo bucket coverage by region"},
        {"file": "plots/best_combo_fail_reason_rates_by_region.png", "description": "Best-combo fail reason rates by region"},
    ])
    manifest.to_csv(out_dir / "threshold_search_manifest.csv", index=False)

    write_plots(scored, best, plots_dir)

    print(f"[OK] wrote {grid_path}")
    print(f"[OK] wrote {best_path}")
    print(f"[OK] wrote {out_dir / 'threshold_search_manifest.csv'}")
    print("[DONE] threshold combo search complete")


if __name__ == "__main__":
    main()
