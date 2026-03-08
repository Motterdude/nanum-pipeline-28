from __future__ import annotations

import argparse
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd


DEFAULT_INPUT = Path(
    r"D:\Drive\Faculdade\PUC\Mestrado\Dados_Ensaios\KIBOX\Convertidos\TESTE_50KW_E100-2026-01-17--17-12-46-081.csv"
)
DEFAULT_OUTPUT_DIR = Path(r"F:\temporario")
DEFAULT_CYCLE_BLOCK_SIZE = 30


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Standalone utility to average KIBOX PCYL_1 and Q_1 by crank angle across cycles."
    )
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT, help="Input KIBOX CSV file.")
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR, help="Output directory for plots.")
    parser.add_argument(
        "--cycle-block-size",
        type=int,
        default=DEFAULT_CYCLE_BLOCK_SIZE,
        help="Number of cycles to average per block.",
    )
    return parser.parse_args()


def load_cycle_dataframe(csv_path: Path) -> pd.DataFrame:
    df = pd.read_csv(
        csv_path,
        sep="\t",
        decimal=",",
        skiprows=[1],
        usecols=["Cycle number", "Crank angle", "PCYL_1", "Q_1"],
    )
    df = df.rename(columns={"Cycle number": "CycleNumber", "Crank angle": "CrankAngle_deg"})
    df["CycleNumber"] = pd.to_numeric(df["CycleNumber"], errors="coerce").ffill()
    df["CrankAngle_deg"] = pd.to_numeric(df["CrankAngle_deg"], errors="coerce").round(1)
    df["PCYL_1"] = pd.to_numeric(df["PCYL_1"], errors="coerce")
    df["Q_1"] = pd.to_numeric(df["Q_1"], errors="coerce")
    df = df.dropna(subset=["CycleNumber", "CrankAngle_deg"])
    df["CycleNumber"] = df["CycleNumber"].astype("int64")
    return df


def mean_curve_by_cycle_block(df: pd.DataFrame, value_col: str, cycle_block_size: int) -> pd.DataFrame:
    if cycle_block_size <= 0:
        raise ValueError("cycle_block_size must be > 0")

    per_cycle = (
        df.dropna(subset=[value_col])
        .groupby(["CycleNumber", "CrankAngle_deg"], as_index=False)[value_col]
        .mean()
    )
    if per_cycle.empty:
        return pd.DataFrame(
            columns=[
                "CycleBlockIndex",
                "CycleBlockStart",
                "CycleBlockEnd",
                "CycleBlockLabel",
                "CrankAngle_deg",
                "mean_value",
                "std_value",
                "n_cycles",
            ]
        )

    per_cycle["CycleBlockIndex"] = ((per_cycle["CycleNumber"] - 1) // cycle_block_size) + 1

    curve = (
        per_cycle.groupby(
            ["CycleBlockIndex", "CrankAngle_deg"],
            as_index=False,
        )
        .agg(
            mean_value=(value_col, "mean"),
            std_value=(value_col, "std"),
            n_cycles=("CycleNumber", "nunique"),
        )
    )
    max_cycle = int(per_cycle["CycleNumber"].max())
    curve["CycleBlockStart"] = ((curve["CycleBlockIndex"] - 1) * cycle_block_size) + 1
    curve["CycleBlockEnd"] = (curve["CycleBlockStart"] + cycle_block_size - 1).clip(upper=max_cycle)
    curve["CycleBlockLabel"] = curve["CycleBlockStart"].astype(str) + "-" + curve["CycleBlockEnd"].astype(str)
    curve = curve[
        [
            "CycleBlockIndex",
            "CycleBlockStart",
            "CycleBlockEnd",
            "CycleBlockLabel",
            "CrankAngle_deg",
            "mean_value",
            "std_value",
            "n_cycles",
        ]
    ].sort_values(["CycleBlockIndex", "CrankAngle_deg"])
    return curve


def save_plot(
    curve: pd.DataFrame,
    *,
    value_label: str,
    x_min: float,
    x_max: float,
    title: str,
    out_path: Path,
) -> None:
    window = curve[(curve["CrankAngle_deg"] >= x_min) & (curve["CrankAngle_deg"] <= x_max)].copy()
    if window.empty:
        raise ValueError(f"No data available for range {x_min} to {x_max} deg CA.")

    fig, ax = plt.subplots(figsize=(10, 5.5))
    for block_label, d in window.groupby("CycleBlockLabel", sort=False):
        d = d.sort_values("CrankAngle_deg")
        ax.plot(d["CrankAngle_deg"], d["mean_value"], linewidth=1.2, label=f"Cycles {block_label}")
    ax.set_xlim(x_min, x_max)
    ax.set_xlabel("Crank angle (deg CA)")
    ax.set_ylabel(value_label)
    ax.set_title(title)
    ax.grid(True, which="both", linestyle="--", linewidth=0.5)
    ax.legend(loc="center left", bbox_to_anchor=(1.02, 0.5), fontsize=8, ncol=1)
    fig.tight_layout()
    fig.savefig(out_path, dpi=200)
    plt.close(fig)


def main() -> None:
    args = parse_args()
    csv_path = args.input.resolve()
    output_dir = args.output_dir.resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    if not csv_path.exists():
        raise SystemExit(f"Input file not found: {csv_path}")

    df = load_cycle_dataframe(csv_path)
    cycle_count = int(df["CycleNumber"].nunique())
    block_count = ((cycle_count - 1) // args.cycle_block_size) + 1

    pcyl_curve = mean_curve_by_cycle_block(df, "PCYL_1", args.cycle_block_size)
    q1_curve = mean_curve_by_cycle_block(df, "Q_1", args.cycle_block_size)

    pcyl_plot = output_dir / f"{csv_path.stem}_pcyl_mean_vs_crank_angle.png"
    q1_plot = output_dir / f"{csv_path.stem}_q1_mean_vs_crank_angle.png"
    summary_csv = output_dir / f"{csv_path.stem}_cycle_block_mean_curves.csv"

    save_plot(
        pcyl_curve,
        value_label="P_CYL (bar)",
        x_min=-40.0,
        x_max=80.0,
        title=f"Mean P_CYL vs Crank angle ({cycle_count} cycles, blocks of {args.cycle_block_size})",
        out_path=pcyl_plot,
    )
    save_plot(
        q1_curve,
        value_label="Q_1 (J/deg CA)",
        x_min=-30.0,
        x_max=90.0,
        title=f"Mean Q_1 vs Crank angle ({cycle_count} cycles, blocks of {args.cycle_block_size})",
        out_path=q1_plot,
    )

    merged = pcyl_curve.rename(
        columns={
            "mean_value": "PCYL_1_mean",
            "std_value": "PCYL_1_std",
            "n_cycles": "PCYL_1_n_cycles",
        }
    )
    merged = merged.merge(
        q1_curve.rename(
            columns={
                "mean_value": "Q_1_mean",
                "std_value": "Q_1_std",
                "n_cycles": "Q_1_n_cycles",
            }
        ),
        on=["CycleBlockIndex", "CycleBlockStart", "CycleBlockEnd", "CycleBlockLabel", "CrankAngle_deg"],
        how="outer",
    ).sort_values(["CycleBlockIndex", "CrankAngle_deg"])
    merged.to_csv(summary_csv, index=False)

    print(f"[OK] Input: {csv_path}")
    print(f"[OK] Cycles found: {cycle_count}")
    print(f"[OK] Cycle blocks: {block_count} (size={args.cycle_block_size})")
    print(f"[OK] Saved: {pcyl_plot}")
    print(f"[OK] Saved: {q1_plot}")
    print(f"[OK] Saved: {summary_csv}")


if __name__ == "__main__":
    main()
