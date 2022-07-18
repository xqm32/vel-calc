from spilt_csv import no_suffix


def merge_csv(target: str, files: list[str]):
    print(f"Merging into {target}")
    with open(target, "w") as f:
        for i in files:
            run = no_suffix(i)
            f.write(f"{run}\n")
            with open(i, "r") as rf:
                f.write(rf.read())
