# pi-async-bash

> Fork of [isthatyousaf/pi-async-bash](https://github.com/isthatyousaf/pi-async-bash) at commit `373e4e3`. Maintained at [Fatih0234/pi-async-bash](https://github.com/Fatih0234/pi-async-bash).

**Long commands. Short waits.**

Claude-style Bash jobs for [Pi](https://pi.dev). Quick commands return normally. After ten seconds, long commands move to the background and notify the agent when they finish.

![Shell jobs panel with live output and command status](qa/captures/06-jobs-panel.png)

*Real Pi terminal capture, rendered to PNG.*

## Install

```sh
pi install git:github.com/Fatih0234/pi-async-bash
```

Restart Pi or run `/reload`. Requires **Pi 1.1.0+**. Tested on Linux.

## What you get

- **Live command rows** — compact output, elapsed time, and clear outcomes.
- **Background jobs** — automatic handoff, completion notices, and private output logs.
- **A jobs panel** — inspect commands, follow output, and stop work you no longer need.
- **Real deadlines** — `timeout` stays in seconds and still applies after handoff.

| Key | Action |
| --- | --- |
| `Alt+J` or `/bash-jobs` | Open jobs panel |
| `Ctrl+Alt+B` | Background foreground commands |
| `Ctrl+O` | Expand command output and details |
| `↑` / `↓` | Select a job in the panel |
| `PgUp` / `PgDn` | Scroll its output |
| `x`, then `x` | Stop the selected job |
| `Esc` | Close the panel |

The agent keeps using `bash`. It can also set `run_in_background: true` and use `bash_job` to list, inspect, wait for, or stop jobs. No polling is needed for completion notices.

## Configure

```sh
pi --bash-foreground-ms 5000   # Wait five seconds before handoff
pi --bash-foreground-ms off    # Only background when explicitly requested
```

Or set `PI_ASYNC_BASH_FOREGROUND_MS`. The flag takes precedence.

Set `PI_ASYNC_BASH_EVENT_LOG=/path/to/events.jsonl` to record every job as
one JSON object per line:
`{"event":"started"|"settled","job_id","command","status?","exitCode?","ts"}`.
Each event is a single append; parent directories are created as needed, and
log I/O failures never break jobs. Consumer contract: read whole lines,
parse each as JSON; `started` carries no `status` and no `exitCode`,
`settled` carries the final status plus the process `exitCode`
(`0` for a clean exit, `null` when no exit code exists e.g. signal kill),
`ts` is epoch milliseconds.

Jobs belong to the session: **quitting, switching sessions, or reloading stops them**. In print/JSON mode, handoff is opt-in with `--bash-foreground-ms 2000`; pending job results are delivered before the run exits. Stop unneeded servers and watchers before finishing. Process cleanup covers the process group, not escaped daemons.

This extension replaces ordinary Pi `bash`, not `!` commands.

[Behavior, limits & development](docs/reference.md) · [MIT license](LICENSE)
