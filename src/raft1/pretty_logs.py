#!/usr/bin/env python3
import sys
import re
from collections import defaultdict
from rich.console import Console
from rich.table import Table

console = Console()

lines = sys.stdin.readlines()

entries = []

pattern = re.compile(r"(\d+)\s+\[(\d+)\]\s+(.*)")

for line in lines:
    line = line.strip()
    m = pattern.match(line)
    if m:
        ts, sid, msg = m.groups()
        entries.append({
            "server": int(sid),
            "time": int(ts),
            "msg": msg
        })

# Group by time
by_time = defaultdict(lambda: defaultdict(list))
for e in entries:
    by_time[e["time"]][e["server"]].append(e["msg"])


times = sorted(by_time.keys())
servers = sorted(set(e["server"] for e in entries))

# Build table
table = Table(show_header=True, header_style="bold magenta")
table.add_column("Time", style="dim")
for s in servers:
    table.add_column(f"S{s}")

colors = ["green", "yellow", "cyan", "magenta", "red"]
for t in times:
    short_time = str(t)[-6:]
    row = [short_time]
    for s in servers:
        msgs = by_time[t].get(s, [])
        color = colors[s % len(colors)]
        if msgs:
            joined = " \n ".join(msgs)
            row.append(f"[{color}]{joined}[/]")
        else:
            row.append("")
    table.add_row(*row)

console.print(table)