#!/usr/bin/env python
import sys
import typer
from rich import print
from rich.columns import Columns
from rich.console import Console
from typing import Optional

# fmt: off
# Mapping from topics to colors
TOPICS = {
    "TIMR": "#9a9a99",
    "VOTE": "#67a0b2",
    "APET": "#26d970",


    "CMIT": "#98719f",
    "APET": "#d08341",
    "SNAP": "#FD971F",
    "KILL": "#ff615c",
    "CLNT": "#00813c",
    "TEST": "#fe2c79",

    "WARN": "#d08341",
    "ERRO": "#fe2626",
    "TRCE": "#fe2626",
}


# fmt: on


def list_topics(value: Optional[str]):
    if value is None:
        return value
    topics = value.split(",")
    for topic in topics:
        if topic not in TOPICS:
            raise typer.BadParameter(f"topic {topic} not recognized")
    return topics


def isDigit(x):
    try:
        x = int(x)
        return isinstance(x, int)
    except ValueError:
        return False


def main(
        file: typer.FileText = typer.Argument(None, help="File to read, stdin otherwise"),
        colorize: bool = typer.Option(True, "--no-color"),
        n_columns: Optional[int] = typer.Option(None, "--columns", "-c"),
        ignore: Optional[str] = typer.Option(None, "--ignore", "-i", callback=list_topics),
        just: Optional[str] = typer.Option(None, "--just", "-j", callback=list_topics),
):
    topics = list(TOPICS)

    # We can take input from a stdin (pipes) or from a file
    input_ = file if file else sys.stdin
    # Print just some topics or exclude some topics (good for avoiding verbose ones)
    if just:
        topics = just
    if ignore:
        topics = [lvl for lvl in topics if lvl not in set(ignore)]

    topics = set(topics)
    console = Console()
    width = console.size.width

    panic = False
    for line in input_:
        try:
            if "FAIL    6.824/raft" in line:
                print(line)
                return 1
            time, topic, *msg = line.strip().split(" ")
            if  not isDigit(time):
                print(line,end="")
            # To ignore some topics
            if topic not in topics:
                continue

            i = int(msg[1][1:])

            msg = " ".join(msg)


            # Colorize output by using rich syntax when needed
            if colorize and topic in TOPICS:
                color = TOPICS[topic]
                msg = f"[{color}]{time} {topic} {msg}[/{color}]"

            # Single column printing. Always the case for debug stmts in tests
            if n_columns is None:
                print(time, msg)
            # Multi column printing, timing is dropped to maximize horizontal
            # space. Heavylifting is done through rich.column.Columns object
            else:
                cols = ["" for _ in range(n_columns)]
                msg = "" + msg
                cols[i] = msg
                col_width = int(width / n_columns)
                cols = Columns(cols, width=col_width - 1, equal=True, expand=True)
                print(cols)
        except:
            print(line, end="")


if __name__ == "__main__":
    typer.run(main)
