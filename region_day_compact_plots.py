import argparse
from pathlib import Path
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt


def load_inputs(analysis_out: Path):
    region_day_path = analysis_out / "region_day_summary.csv"
    threshold_path = analysis_out / "region_threshold_candidates.csv"

    if not region_day_path.exists():
        raise FileNotFoundError(f"Missing {region_day_path}")
    if not threshold_path.exists():
        raise FileNotFoundError(f"Missing {threshold_path}")

    region_day = pd.read_csv(region_day_path)
    thresholds = pd.read_csv(threshold_path)

    region_day["date"] = pd.to_datetime(region_day["date"], errors="coerce")
    region_day["region"] = region_day["region"].astype(str)
    thresholds["region"] = thresholds["region"].astype(str)

    region_day = region_day.dropna(subset=["date", "region"])
    return region_day, thresholds


def add_threshold_ratios(region_day: pd.DataFrame, thresholds: pd.DataFrame) -> pd.DataFrame:
    df = region_day.merge(thresholds, on="region", how="left")

    def safe_ratio(num, den):
        num = pd.to_numeric(num, errors="coerce")
        den = pd.to_numeric(den, errors="coerce")
        out = num / den
        out[(~np.isfinite(num)) | (~np.isfinite(den)) | (den == 0)] = np.nan
        return out

    if {"clearing_time_hat_min_median", "X_min_q25_ct"}.issubset(df.columns):
        df["ct_ratio_to_threshold"] = safe_ratio(df["clearing_time_hat_min_median"], df["X_min_q25_ct"])

    if {"avg_spread_bps_median", "Y_bps_q25_spread"}.issubset(df.columns):
        df["spread_ratio_to_threshold"] = safe_ratio(df["avg_spread_bps_median"], df["Y_bps_q25_spread"])

    if {"tick_day_moves_median", "Z_ticks_q75_tick"}.issubset(df.columns):
        df["tick_ratio_to_threshold"] = safe_ratio(df["Z_ticks_q75_tick"], df["tick_day_moves_median"])

    if {"hit_cnt_median", "suggest_min_hit_cnt"}.issubset(df.columns):
        df["hit_ratio_to_threshold"] = safe_ratio(df["hit_cnt_median"], df["suggest_min_hit_cnt"])

    return df


def add_rolling(df: pd.DataFrame, cols, window: int) -> pd.DataFrame:
    out = df.sort_values(["region", "date"]).copy()
    for c in cols:
        if c in out.columns:
            out[f"{c}_roll{window}"] = (
                out.groupby("region", group_keys=False)[c]
                .apply(lambda s: s.rolling(window=window, min_periods=1).mean())
            )
    return out


def plot_all_regions_line(
    df: pd.DataFrame,
    value_col: str,
    out_path: Path,
    title: str,
    y_label: str,
    threshold_line=None,
):
    if value_col not in df.columns:
        print(f"[WARN] missing column {value_col}, skip {out_path.name}")
        return

    tmp = df[["date", "region", value_col]].copy()
    tmp[value_col] = pd.to_numeric(tmp[value_col], errors="coerce")
    tmp = tmp.dropna(subset=["date", "region", value_col])

    if tmp.empty:
        print(f"[WARN] no data for {value_col}")
        return

    plt.figure(figsize=(11, 6))
    for region, g in tmp.groupby("region"):
        g = g.sort_values("date")
        plt.plot(g["date"], g[value_col], label=region)

    if threshold_line is not None:
        plt.axhline(threshold_line, linestyle="--")

    plt.title(title)
    plt.xlabel("Date")
    plt.ylabel(y_label)
    plt.xticks(rotation=45)
    plt.legend()
    plt.tight_layout()
    plt.savefig(out_path, dpi=160)
    plt.close()
    print(f"[OK] wrote plot {out_path}")


def plot_heatmap(df: pd.DataFrame, value_col: str, out_path: Path, title: str):
    if value_col not in df.columns:
        print(f"[WARN] missing column {value_col}, skip {out_path.name}")
        return

    tmp = df[["date", "region", value_col]].copy()
    tmp[value_col] = pd.to_numeric(tmp[value_col], errors="coerce")
    tmp = tmp.dropna(subset=["date", "region", value_col])

    if tmp.empty:
        print(f"[WARN] no data for heatmap {value_col}")
        return

    piv = tmp.pivot(index="region", columns="date", values=value_col)
    piv = piv.sort_index()

    if piv.empty:
        print(f"[WARN] empty pivot for heatmap {value_col}")
        return

    plt.figure(figsize=(13, max(3, 0.8 * len(piv.index))))
    plt.imshow(piv.values, aspect="auto")
    plt.yticks(range(len(piv.index)), piv.index)
    ncols = len(piv.columns)
    step = max(1, ncols // 10)
    xticks = list(range(0, ncols, step))
    xlabels = [pd.Timestamp(piv.columns[i]).strftime("%Y-%m-%d") for i in xticks]
    plt.xticks(xticks, xlabels, rotation=45, ha="right")
    plt.colorbar()
    plt.title(title)
    plt.tight_layout()
    plt.savefig(out_path, dpi=160)
    plt.close()
    print(f"[OK] wrote plot {out_path}")


def main():
    parser = argparse.ArgumentParser(description="Compact region-day plots from existing analysis CSVs.")
    parser.add_argument("--analysis_out", default="analysis_out", help="Directory containing analysis CSVs")
    parser.add_argument("--rolling_window", type=int, default=5, help="Rolling window for smoothed plots")
    args = parser.parse_args()

    analysis_out = Path(args.analysis_out)
    out_dir = analysis_out / "plots" / "from_region_day_compact"
    out_dir.mkdir(parents=True, exist_ok=True)

    region_day, thresholds = load_inputs(analysis_out)
    df = add_threshold_ratios(region_day, thresholds)

    raw_cols = [
        "clearing_time_hat_min_median",
        "avg_spread_bps_median",
        "tick_day_moves_median",
        "hit_cnt_median",
        "gate_rate_hit2",
    ]
    ratio_cols = [
        "ct_ratio_to_threshold",
        "spread_ratio_to_threshold",
        "tick_ratio_to_threshold",
        "hit_ratio_to_threshold",
    ]
    df = add_rolling(df, raw_cols + ratio_cols, args.rolling_window)
    rw = args.rolling_window

    # Raw combined plots
    plot_all_regions_line(
        df, "clearing_time_hat_min_median",
        out_dir / "all_regions_median_ct.png",
        "All regions: median clearing time by day",
        "median_ct",
    )
    plot_all_regions_line(
        df, "avg_spread_bps_median",
        out_dir / "all_regions_median_spread_bps.png",
        "All regions: median spread bps by day",
        "median_spread_bps",
    )
    plot_all_regions_line(
        df, "tick_day_moves_median",
        out_dir / "all_regions_median_tick_day_moves.png",
        "All regions: median daily tick moves by day",
        "median_tick_day_moves",
    )
    plot_all_regions_line(
        df, "hit_cnt_median",
        out_dir / "all_regions_median_hit_cnt.png",
        "All regions: median hit count by day",
        "median_hit_cnt",
    )
    plot_all_regions_line(
        df, "gate_rate_hit2",
        out_dir / "all_regions_gate_rate_hit2.png",
        "All regions: gate_rate_hit2 by day",
        "gate_rate_hit2",
    )

    # Rolling raw combined plots
    plot_all_regions_line(
        df, f"clearing_time_hat_min_median_roll{rw}",
        out_dir / f"all_regions_median_ct_roll{rw}.png",
        f"All regions: median clearing time {rw}-day rolling mean",
        f"median_ct_roll{rw}",
    )
    plot_all_regions_line(
        df, f"avg_spread_bps_median_roll{rw}",
        out_dir / f"all_regions_median_spread_bps_roll{rw}.png",
        f"All regions: median spread bps {rw}-day rolling mean",
        f"median_spread_bps_roll{rw}",
    )
    plot_all_regions_line(
        df, f"tick_day_moves_median_roll{rw}",
        out_dir / f"all_regions_median_tick_day_moves_roll{rw}.png",
        f"All regions: median daily tick moves {rw}-day rolling mean",
        f"median_tick_day_moves_roll{rw}",
    )
    plot_all_regions_line(
        df, f"hit_cnt_median_roll{rw}",
        out_dir / f"all_regions_median_hit_cnt_roll{rw}.png",
        f"All regions: median hit count {rw}-day rolling mean",
        f"median_hit_cnt_roll{rw}",
    )
    plot_all_regions_line(
        df, f"gate_rate_hit2_roll{rw}",
        out_dir / f"all_regions_gate_rate_hit2_roll{rw}.png",
        f"All regions: gate_rate_hit2 {rw}-day rolling mean",
        f"gate_rate_hit2_roll{rw}",
    )

    # Ratio-to-threshold plots
    plot_all_regions_line(
        df, "ct_ratio_to_threshold",
        out_dir / "all_regions_ct_ratio_to_threshold.png",
        "All regions: median ct / X_min baseline",
        "ct_ratio_to_threshold",
        threshold_line=1.0,
    )
    plot_all_regions_line(
        df, "spread_ratio_to_threshold",
        out_dir / "all_regions_spread_ratio_to_threshold.png",
        "All regions: median spread / Y_bps baseline",
        "spread_ratio_to_threshold",
        threshold_line=1.0,
    )
    plot_all_regions_line(
        df, "tick_ratio_to_threshold",
        out_dir / "all_regions_tick_ratio_to_threshold.png",
        "All regions: Z_ticks baseline / median tick moves",
        "tick_ratio_to_threshold",
        threshold_line=1.0,
    )
    plot_all_regions_line(
        df, "hit_ratio_to_threshold",
        out_dir / "all_regions_hit_ratio_to_threshold.png",
        "All regions: median hit count / min_hit_cnt baseline",
        "hit_ratio_to_threshold",
        threshold_line=1.0,
    )

    # Rolling ratio plots
    plot_all_regions_line(
        df, f"ct_ratio_to_threshold_roll{rw}",
        out_dir / f"all_regions_ct_ratio_to_threshold_roll{rw}.png",
        f"All regions: ct ratio to threshold {rw}-day rolling mean",
        f"ct_ratio_to_threshold_roll{rw}",
        threshold_line=1.0,
    )
    plot_all_regions_line(
        df, f"spread_ratio_to_threshold_roll{rw}",
        out_dir / f"all_regions_spread_ratio_to_threshold_roll{rw}.png",
        f"All regions: spread ratio to threshold {rw}-day rolling mean",
        f"spread_ratio_to_threshold_roll{rw}",
        threshold_line=1.0,
    )
    plot_all_regions_line(
        df, f"tick_ratio_to_threshold_roll{rw}",
        out_dir / f"all_regions_tick_ratio_to_threshold_roll{rw}.png",
        f"All regions: tick ratio to threshold {rw}-day rolling mean",
        f"tick_ratio_to_threshold_roll{rw}",
        threshold_line=1.0,
    )
    plot_all_regions_line(
        df, f"hit_ratio_to_threshold_roll{rw}",
        out_dir / f"all_regions_hit_ratio_to_threshold_roll{rw}.png",
        f"All regions: hit ratio to threshold {rw}-day rolling mean",
        f"hit_ratio_to_threshold_roll{rw}",
        threshold_line=1.0,
    )

    # Compact heatmaps
    plot_heatmap(
        df, "gate_rate_hit2",
        out_dir / "heatmap_gate_rate_hit2.png",
        "Heatmap: gate_rate_hit2 by region and day",
    )
    if "ct_ratio_to_threshold" in df.columns:
        plot_heatmap(
            df, "ct_ratio_to_threshold",
            out_dir / "heatmap_ct_ratio_to_threshold.png",
            "Heatmap: ct ratio to threshold by region and day",
        )

    manifest = pd.DataFrame([
        {"file": "all_regions_median_ct.png", "description": "All-region daily median clearing time"},
        {"file": f"all_regions_median_ct_roll{rw}.png", "description": "All-region rolling median clearing time"},
        {"file": "all_regions_ct_ratio_to_threshold.png", "description": "All-region ct ratio to threshold, baseline y=1"},
        {"file": f"all_regions_ct_ratio_to_threshold_roll{rw}.png", "description": "All-region rolling ct ratio to threshold"},
        {"file": "heatmap_gate_rate_hit2.png", "description": "Region-day heatmap for gate_rate_hit2"},
        {"file": "heatmap_ct_ratio_to_threshold.png", "description": "Region-day heatmap for ct ratio to threshold"},
    ])
    manifest.to_csv(out_dir / "region_day_compact_manifest.csv", index=False)
    print(f"[OK] wrote manifest {out_dir / 'region_day_compact_manifest.csv'}")
    print("[DONE] compact region-day plots complete")


if __name__ == "__main__":
    main()
