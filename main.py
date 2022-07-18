import math
import os
from shutil import rmtree
import sys
from typing import Literal
import polars as pl
from merge_csv import merge_csv

from spilt_csv import RUNS_DIR, no_suffix, split_csv


Velocity: type = tuple[float, float, float]


def relative_velocity(vi: Velocity, vj: Velocity) -> float:
    squares: float = 0
    for i in range(3):
        squares += (vi[i] - vj[i]) ** 2
    return math.sqrt(squares)


def average_relative_velocity(vi: Velocity, nv: list[Velocity]) -> float:
    return sum(relative_velocity(vi, vj) for vj in nv) / (len(nv) - 1)


VX: Literal = ' "Trans. Velocity Ch. 1 (m/sec)"'
VY: Literal = ' "Trans. Velocity Ch. 2 (m/sec)"'
VZ: Literal = ' "Trans. Velocity Ch. 3 (m/sec)"'
V: Literal = (VX, VY, VZ)


def velocity(series: pl.Series) -> Velocity:
    return (series[VX], series[VY], series[VZ])


def n_velocity(df: pl.DataFrame) -> list[Velocity]:
    return list(
        zip(
            df[VX].to_list(),
            df[VY].to_list(),
            df[VZ].to_list(),
        )
    )


COL_ARV: Literal = "ARV"
COL_AARV: Literal = "Average ARV"
COL_COUNT: Literal = "Count"

RESOLVED_DIR: Literal = "resolved"


class Run:
    def __init__(self, file: str, parents) -> None:
        self.run = file
        self.parents: str = parents
        path = f"{RUNS_DIR}/{self.parents}"
        self.df: pl.DataFrame = pl.read_csv(f"{path}/{self.run}.csv")
        self.nv: pl.DataFrame = n_velocity(self.df)
        self.avg_rv = lambda series: average_relative_velocity(
            velocity(series), self.nv
        )

    def resolve(self) -> None:
        path = f"{RESOLVED_DIR}/{self.parents}"
        if not os.path.exists(path):
            os.makedirs(path)
        resolved_file = f"{path}/{self.run}.csv"

        if len(self.nv) < 2:
            print(f"\t{self.run}: Invaild record")
            pl.DataFrame(
                {
                    VX: self.df[VX],
                    VY: self.df[VY],
                    VZ: self.df[VZ],
                    COL_ARV: ["Invaild record"],
                    COL_AARV: ["Invaild record"],
                    COL_COUNT: [len(self.nv)],
                }
            ).write_csv(resolved_file)
            return

        self.df.select([*V, pl.struct(V).apply(self.avg_rv).alias(COL_ARV)]).select(
            [
                pl.col("*"),
                pl.mean(COL_ARV).alias(COL_AARV),
                pl.col(COL_ARV).count().alias(COL_COUNT),
            ]
        ).write_csv(resolved_file)


if __name__ == "__main__":
    file = sys.argv[1]
    parents_path = no_suffix(file)
    runs = sorted(split_csv(file))

    for i in runs:
        run = no_suffix(i)
        print(f"Resolving {run}")
        Run(run, parents_path).resolve()

    runs = map(lambda i: f"{RESOLVED_DIR}/{parents_path}/{no_suffix(i)}.csv", runs)
    merge_csv(f"[OK]{parents_path}.csv", runs)

    print("Cleaning temporary files")
    rmtree("runs")
    rmtree("resolved")
