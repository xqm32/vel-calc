import os
from typing import Literal


def no_suffix(file: str) -> str:
    return os.path.basename(file).removesuffix(".csv").strip(",")


def run_no(line: str) -> str:
    return os.path.basename(line).strip("\n")


def is_run(line: str) -> bool:
    return run_no(line).startswith("Run")

RUNS_DIR: Literal = "runs"

def split_csv(file: str):
    print(f"Spliting {no_suffix(file)}.csv")
    
    runs: dict[str, list[str]] = {}
    this_run: str = "here is no run"

    with open(file, "r") as f:
        lines: list[str] = f.readlines()

        for i, line in enumerate(lines):
            if is_run(line):
                this_run = run_no(line)
                runs[this_run] = []
            elif this_run == "here is no run":
                raise Exception(this_run)
            else:
                runs[this_run].append(line)

    dir = no_suffix(file)
    path = f"{RUNS_DIR}/{dir}"

    if not os.path.exists(path):
        os.makedirs(path)

    for run, lines in runs.items():
        with open(f"{path}/{no_suffix(run)}.csv", "w") as f:
            f.writelines(lines)

    return runs.keys()
