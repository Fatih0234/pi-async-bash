/**
 * Session-owned shell job manager.
 *
 * Each job is one detached shell process (its own process group) whose stdout and stderr
 * share a private log file. The manager owns the process independently of any tool call:
 * a caller can wait for it in the foreground, hand it off to the background, or stop it.
 *
 * Invariants:
 * - A job finishes exactly once (`finish()` is idempotent).
 * - Ownership moves foreground -> background only while the job is still running, so the
 *   foreground caller and the background completion callback never both report a result.
 * - The execution deadline (`timeoutMs`) is armed at launch and survives handoff.
 * - Disk use is bounded per job and per manager; finished-job bookkeeping is bounded.
 *
 * No Pi imports: this module is tested directly with real subprocesses.
 */
import { type ChildProcess, spawn } from "node:child_process";
import { randomBytes } from "node:crypto";
import * as fs from "node:fs";
import { constants as osConstants } from "node:os";
import { dirname, join } from "node:path";
import { StringDecoder } from "node:string_decoder";

export const MAX_TIMER_MS = 2_147_483_647;

export type JobStatus =
	| "running"
	| "exited"
	| "stopped"
	| "aborted"
	| "timed_out"
	| "output_limit"
	| "shutdown"
	| "spawn_error";

export type StopReason = "stopped" | "aborted" | "timed_out" | "output_limit" | "shutdown";

export type JobOwner = "foreground" | "background";

export type JobChange = "start" | "handoff" | "stop" | "finish";

/**
 * One line of the `PI_ASYNC_BASH_EVENT_LOG` JSONL contract. `started` carries no
 * `status` and no `exitCode`; `settled` carries the final first-reason-wins status
 * plus the process `exitCode`. `ts` is epoch ms.
 */
export interface JobEvent {
	event: "started" | "settled";
	job_id: string;
	command: string;
	status?: JobStatus;
	exitCode?: number | null;
	ts: number;
}

export interface JobSpec {
	/** Text handed to the shell. */
	command: string;
	/** Text shown to the model and user (defaults to `command`). */
	displayCommand?: string;
	/** Optional short human-readable label, for display only. */
	description?: string;
	cwd: string;
	env?: NodeJS.ProcessEnv;
	/** Hard execution deadline in milliseconds, measured from launch. */
	timeoutMs?: number;
	/** Start owned by the background (explicit `run_in_background`). */
	background?: boolean;
	/**
	 * Readiness watch: JS regex source matched against each new complete output line.
	 * On the first match the manager fires `onReady` once; the job keeps running.
	 */
	readyPattern?: string;
	/** How long after launch the readiness watch stays armed. The job keeps running after expiry. */
	readyTimeoutMs?: number;
}

export interface JobInfo {
	id: string;
	command: string;
	description?: string;
	cwd: string;
	status: JobStatus;
	owner: JobOwner;
	exitCode: number | null;
	signal: NodeJS.Signals | null;
	error?: string;
	startedAt: number;
	endedAt?: number;
	timeoutMs?: number;
	logPath: string;
	logBytes: number;
	logDeleted: boolean;
	delivered: boolean;
	pid?: number;
}

export interface TailResult {
	text: string;
	/** Output before the returned text was left out. */
	truncated: boolean;
	totalBytes: number;
}

export interface JobManagerOptions {
	/** Private directory for this manager's logs. Created with mode 0700 if missing. */
	logDir: string;
	shell: string;
	shellArgs: string[];
	/** Pass the command on stdin instead of argv (Pi's ShellConfig.commandTransport). */
	commandOnStdin?: boolean;
	maxRunning?: number;
	maxCompleted?: number;
	maxLogBytes?: number;
	maxTotalLogBytes?: number;
	/** Delay between SIGTERM and SIGKILL when stopping a process group. */
	killGraceMs?: number;
	monitorIntervalMs?: number;
	/** Do not keep the Node event loop alive for background jobs (the extension sets this). */
	unref?: boolean;
	/**
	 * Path of the JSONL job-event log (`PI_ASYNC_BASH_EVENT_LOG`). Each job appends one
	 * `started` line at launch and one `settled` line at completion, each via a single
	 * synchronous append. Unset disables the log.
	 */
	eventLogPath?: string;
	/** Called once when a background-owned job finishes and its result was not yet delivered. */
	onBackgroundComplete?: (job: Job) => void;
	/** Called at most once per job, when a new output line first matches its readiness watch. */
	onReady?: (job: Job) => void;
	/** Observer for display state: launch, handoff, stop request, finish. Never affects ownership. */
	onChange?: (job: Job, change: JobChange) => void;
	now?: () => number;
}

export class Job {
	readonly id: string;
	readonly command: string;
	readonly description: string | undefined;
	readonly cwd: string;
	readonly logPath: string;
	readonly startedAt: number;
	readonly timeoutMs: number | undefined;
	status: JobStatus = "running";
	owner: JobOwner;
	exitCode: number | null = null;
	signal: NodeJS.Signals | null = null;
	error: string | undefined;
	endedAt: number | undefined;
	logBytes = 0;
	logDeleted = false;
	/** The model has received this job's completion (foreground result, wait/status, or notification). */
	delivered = false;
	pid: number | undefined;
	/** Resolves with the final info when the job finishes. Never rejects. */
	readonly done: Promise<JobInfo>;
	/** Resolves true once the process spawned, false if it failed to launch. */
	readonly launched: Promise<boolean>;

	/** First stop reason requested; authoritative for the final status once set. */
	stopReason: StopReason | undefined;
	/** The process group was signalled and may still have members to SIGKILL. Independent of `status`. */
	cleanupPending = false;
	/** @internal */ cleanupTimer: NodeJS.Timeout | undefined;
	/** @internal */ child: ChildProcess | undefined;
	/** @internal */ deadlineTimer: NodeJS.Timeout | undefined;
	/** Raw `ready_pattern` source arming this job's readiness watch, if any. */
	readonly readySource: string | undefined;
	/** The completed output line that first matched the readiness watch. Set once, when it fires. */
	readyLine: string | undefined;
	/** The readiness watch fired (at most once per job). */
	readyFired = false;
	/** The readiness watch expired (`readyTimeoutMs`) without a match. */
	readyExpired = false;
	/** The model received this job's readiness notice (or it was dropped as stale). */
	readyDelivered = false;
	/** @internal */ readyRegex: RegExp | undefined;
	/** @internal */ readyMatcher: ReadyLineMatcher | undefined;
	/** @internal */ readyDecoder: StringDecoder | undefined;
	/** @internal */ readyScannedBytes = 0;
	/** @internal */ readyTimer: NodeJS.Timeout | undefined;
	/** @internal */ resolveDone!: (info: JobInfo) => void;
	/** @internal */ resolveLaunched!: (ok: boolean) => void;

	constructor(id: string, spec: JobSpec, logPath: string, startedAt: number) {
		this.id = id;
		this.command = spec.displayCommand ?? spec.command;
		this.description = spec.description;
		this.cwd = spec.cwd;
		this.logPath = logPath;
		this.startedAt = startedAt;
		this.timeoutMs = spec.timeoutMs;
		this.readySource = spec.readyPattern;
		this.owner = spec.background ? "background" : "foreground";
		this.done = new Promise((resolve) => {
			this.resolveDone = resolve;
		});
		this.launched = new Promise((resolve) => {
			this.resolveLaunched = resolve;
		});
	}

	get running(): boolean {
		return this.status === "running";
	}

	info(): JobInfo {
		return {
			id: this.id,
			command: this.command,
			description: this.description,
			cwd: this.cwd,
			status: this.status,
			owner: this.owner,
			exitCode: this.exitCode,
			signal: this.signal,
			error: this.error,
			startedAt: this.startedAt,
			endedAt: this.endedAt,
			timeoutMs: this.timeoutMs,
			logPath: this.logPath,
			logBytes: this.logBytes,
			logDeleted: this.logDeleted,
			delivered: this.delivered,
			pid: this.pid,
		};
	}
}

export function validateTimeoutMs(timeoutMs: number | undefined): void {
	if (timeoutMs === undefined) return;
	if (!Number.isFinite(timeoutMs) || timeoutMs <= 0) throw new Error("Invalid timeout: must be a finite positive number");
	if (timeoutMs > MAX_TIMER_MS) throw new Error(`Invalid timeout: maximum is ${MAX_TIMER_MS / 1000} seconds`);
}

/** Compile a readiness-watch pattern. Throws before spawn, with the pattern echoed back. */
export function compileReadyPattern(source: string): RegExp {
	if (typeof source !== "string" || source === "") {
		throw new Error(`Invalid ready_pattern ${JSON.stringify(source)}: must be a non-empty JS regex source`);
	}
	try {
		return new RegExp(source);
	} catch (err) {
		throw new Error(`Invalid ready_pattern ${JSON.stringify(source)}: ${err instanceof Error ? err.message : String(err)}`);
	}
}

export function validateReadyTimeoutMs(readyTimeoutMs: number | undefined): void {
	if (readyTimeoutMs === undefined) return;
	if (!Number.isFinite(readyTimeoutMs) || readyTimeoutMs <= 0) {
		throw new Error("Invalid readyTimeoutMs: must be a finite positive number");
	}
	if (readyTimeoutMs > MAX_TIMER_MS) throw new Error(`Invalid readyTimeoutMs: maximum is ${MAX_TIMER_MS / 1000} seconds`);
}

/** Per monitor tick, at most this many new log bytes are scanned for a readiness match. */
const READY_SCAN_MAX_BYTES = 8 * 1024 * 1024;

/**
 * Per completed line, at most this many characters are tested against the readiness
 * pattern. Patterns are model-supplied and run on the shared monitor tick, so testing an
 * unbounded line (up to `maxLogBytes`) under a catastrophically backtracking pattern
 * would stall readiness scans and output-limit checks for every job. Only the line prefix
 * is tested; a match past the cap is missed. (There is no non-backtracking engine without
 * new dependencies; the tick's byte cap still bounds total work per tick.)
 */
const READY_LINE_TEST_MAX_CHARS = 4096;

/**
 * Incremental per-line matcher for a readiness watch. Feed only new output text: each complete
 * line is evaluated against the pattern exactly once, in order. Fires at most once ever: after
 * the first match every later `push` returns []. A trailing partial line is held until it
 * completes. Lines are matched raw (only a trailing carriage return is stripped).
 */
export class ReadyLineMatcher {
	private readonly pattern: RegExp;
	private buffer = "";
	private nextIndex = 0;
	private done = false;
	/** The completed line that matched, once fired. */
	matchedLine: string | undefined;

	constructor(pattern: RegExp) {
		this.pattern = pattern;
	}

	get fired(): boolean {
		return this.done;
	}

	/**
	 * Feed new output text. Returns the indices of newly completed lines that match
	 * (at most one entry, and empty on every call after the first match).
	 */
	push(chunk: string): number[] {
		if (this.done || chunk === "") return [];
		this.buffer += chunk;
		let nl: number;
		while ((nl = this.buffer.indexOf("\n")) >= 0) {
			let line = this.buffer.slice(0, nl);
			this.buffer = this.buffer.slice(nl + 1);
			if (line.endsWith("\r")) line = line.slice(0, -1);
			const index = this.nextIndex++;
			if (this.testLine(line)) {
				this.done = true;
				this.matchedLine = line;
				// The watch is over; buffered text (complete or partial) is never scanned.
				this.buffer = "";
				return [index];
			}
		}
		return [];
	}

	/** Test one complete line, capped so a long line cannot stall the shared monitor tick. */
	private testLine(line: string): boolean {
		return this.pattern.test(line.length > READY_LINE_TEST_MAX_CHARS ? line.slice(0, READY_LINE_TEST_MAX_CHARS) : line);
	}

	/**
	 * Treat buffered partial text as one final complete line. Call once at settle, when no
	 * more output is coming: an unterminated trailing line would otherwise never be tested.
	 * The watch is over either way; returns [index] on match, [] otherwise.
	 */
	flush(): number[] {
		if (this.done) return [];
		this.done = true;
		if (this.buffer === "") return [];
		let line = this.buffer;
		this.buffer = "";
		if (line.endsWith("\r")) line = line.slice(0, -1);
		const index = this.nextIndex++;
		if (this.testLine(line)) {
			this.matchedLine = line;
			return [index];
		}
		return [];
	}
}

function signalExitCode(signal: NodeJS.Signals | null): number {
	if (!signal) return 1;
	return 128 + ((osConstants.signals as Record<string, number>)[signal] ?? 0);
}

function killGroup(pid: number | undefined, signal: NodeJS.Signals): void {
	if (!pid) return;
	if (process.platform === "win32") {
		try {
			process.kill(pid, signal);
		} catch {}
		return;
	}
	try {
		process.kill(-pid, signal);
	} catch {
		try {
			process.kill(pid, signal);
		} catch {
			// Already gone.
		}
	}
}

/** Whether any process is still in the group (zombies included). */
function groupAlive(pid: number): boolean {
	if (process.platform === "win32") return false;
	try {
		process.kill(-pid, 0);
		return true;
	} catch (err) {
		return (err as NodeJS.ErrnoException).code === "EPERM";
	}
}

/**
 * OS hooks for the `/proc` leader check below, exposed so tests can simulate a missing
 * `/proc` (macOS/BSD/proc-less Linux). Production code reads the real `/proc` filesystem.
 */
export const procHooks = {
	readPidStat(pid: number): string {
		return fs.readFileSync(`/proc/${pid}/stat`, "utf8");
	},
	/** Probe read proving `/proc` itself is mounted; disambiguates an ENOENT pid read. */
	readSelfStat(): string {
		return fs.readFileSync("/proc/self/stat", "utf8");
	},
};

/**
 * Synchronous OS-level check for a leader that exited before Node delivered its
 * `exit` event (SIGCHLD is observed on the next tick). A zombie (`Z`) or gone
 * (`X`/ENOENT) leader means a stop request raced a natural exit: the caller must
 * not synthesize `stopped` or send signals, but return the pending natural outcome.
 *
 * `/proc` exists only on Linux: elsewhere the check is skipped (assume alive) so
 * stop/abort/timeout/output_limit/shutdown still signal. On Linux an ENOENT pid read is
 * ambiguous (exited leader vs. unmounted `/proc`), so it counts as exited only when
 * `/proc` itself answers; otherwise assume alive and signal.
 */
function leaderExitedBeforeExitEvent(job: Job): boolean {
	const child = job.child;
	if (child && (child.exitCode !== null || child.signalCode !== null)) return true;
	const pid = job.pid;
	if (!pid || process.platform !== "linux") return false;
	let stat: string;
	try {
		stat = procHooks.readPidStat(pid);
	} catch (err) {
		const code = (err as NodeJS.ErrnoException).code;
		if (code !== "ENOENT" && code !== "ESRCH") return false;
		try {
			procHooks.readSelfStat();
		} catch {
			return false;
		}
		return true;
	}
	const close = stat.lastIndexOf(")");
	if (close < 0) return false;
	const state = stat.slice(close + 2, close + 3);
	return state === "Z" || state === "X" || state === "x";
}

const TRIM_HEADER = (dropped: number) => `[pi-async-bash: ${dropped} earlier bytes removed; output exceeded the log limit]\n`;

// biome-ignore lint/suspicious/noControlCharactersInRegex: terminal escape sequences
const ANSI_RE = /\x1b(?:\[[0-9;?]*[ -/]*[@-~]|\][^\x07\x1b]*(?:\x07|\x1b\\)|[@-_])/g;
// biome-ignore lint/suspicious/noControlCharactersInRegex: same set Pi's sanitizeBinaryOutput removes
const CONTROL_RE = /[\x00-\x08\x0B\x0C\x0E-\x1F\uFFF9-\uFFFB]/g;

/** Make raw terminal output readable: drop escapes, apply carriage-return overwrites, drop control bytes. */
export function cleanOutput(raw: string): string {
	return raw
		.replace(ANSI_RE, "")
		.replace(/\r\n/g, "\n")
		.split("\n")
		.map((line) => {
			const cr = line.lastIndexOf("\r", line.length - 2);
			return (cr >= 0 ? line.slice(cr + 1) : line).replace(/\r$/, "");
		})
		.join("\n")
		.replace(CONTROL_RE, "");
}

/** Read the end of a log file: at most `maxBytes` bytes and `maxLines` lines. */
export function readLogTail(path: string, maxBytes: number, maxLines: number): TailResult {
	let fd: number;
	try {
		fd = fs.openSync(path, fs.constants.O_RDONLY | (fs.constants.O_NOFOLLOW ?? 0));
	} catch {
		return { text: "", truncated: false, totalBytes: 0 };
	}
	try {
		const size = fs.fstatSync(fd).size;
		const length = Math.min(size, maxBytes);
		const start = size - length;
		const buf = Buffer.alloc(length);
		let read = 0;
		while (read < length) {
			const n = fs.readSync(fd, buf, read, length - read, start + read);
			if (n <= 0) break;
			read += n;
		}
		let text = buf.subarray(0, read).toString("utf8");
		let truncated = start > 0;
		if (truncated) {
			const nl = text.indexOf("\n");
			// Drop the partial first line, unless the window holds a single enormous line.
			if (nl >= 0 && nl < text.length - 1) text = text.slice(nl + 1);
			else text = text.replace(/^\uFFFD+/, "");
		}
		text = cleanOutput(text);
		const lines = text.split("\n");
		if (lines.length > 0 && lines[lines.length - 1] === "") lines.pop();
		if (lines.length > maxLines) {
			lines.splice(0, lines.length - maxLines);
			truncated = true;
		}
		return { text: lines.join("\n"), truncated, totalBytes: size };
	} finally {
		fs.closeSync(fd);
	}
}

/**
 * Create a private per-session log directory under `base`.
 * The base must be a real directory owned by the current user; it is tightened to 0700.
 */
export function createSessionLogDir(base: string, sessionId: string): string {
	fs.mkdirSync(base, { recursive: true, mode: 0o700 });
	const st = fs.lstatSync(base);
	if (!st.isDirectory() || st.isSymbolicLink()) throw new Error(`Log directory is not a directory: ${base}`);
	if (typeof process.getuid === "function" && st.uid !== process.getuid()) {
		throw new Error(`Log directory is owned by another user: ${base}`);
	}
	if ((st.mode & 0o077) !== 0) fs.chmodSync(base, 0o700);
	const safe = sessionId.replace(/[^A-Za-z0-9_.-]/g, "_").slice(0, 64) || "session";
	return fs.mkdtempSync(join(base, `${safe}-`));
}

/** Delete session log directories under `base` not modified for `maxAgeMs`. Best effort. */
export function pruneSessionLogDirs(base: string, maxAgeMs: number, keep: string | undefined, now = Date.now()): void {
	let names: string[];
	try {
		names = fs.readdirSync(base);
	} catch {
		return;
	}
	for (const name of names) {
		const dir = join(base, name);
		if (dir === keep) continue;
		try {
			const st = fs.lstatSync(dir);
			if (!st.isDirectory() || now - st.mtimeMs < maxAgeMs) continue;
			fs.rmSync(dir, { recursive: true, force: true });
		} catch {}
	}
}

export class JobManager {
	readonly logDir: string;
	private readonly opts: Required<Omit<JobManagerOptions, "onBackgroundComplete" | "onReady" | "commandOnStdin" | "onChange" | "eventLogPath">> &
		Pick<JobManagerOptions, "onBackgroundComplete" | "onReady" | "commandOnStdin" | "onChange" | "eventLogPath">;
	private readonly jobs = new Map<string, Job>();
	/** Cleanup must outlive result release and completed-job retention eviction. */
	private readonly pendingGroupCleanup = new Set<Job>();
	private counter = 0;
	private disposed = false;
	private monitor: NodeJS.Timeout | undefined;
	private readonly exitHook = () => this.killAllSync();
	private exitHookInstalled = false;

	constructor(options: JobManagerOptions) {
		this.opts = {
			maxRunning: 8,
			maxCompleted: 50,
			maxLogBytes: 64 * 1024 * 1024,
			maxTotalLogBytes: 256 * 1024 * 1024,
			killGraceMs: 2000,
			monitorIntervalMs: 500,
			unref: false,
			now: Date.now,
			...options,
		};
		this.opts.maxLogBytes = Math.min(this.opts.maxLogBytes, this.opts.maxTotalLogBytes);
		this.logDir = options.logDir;
		fs.mkdirSync(this.logDir, { recursive: true, mode: 0o700 });
	}

	get isDisposed(): boolean {
		return this.disposed;
	}

	get(id: string): Job | undefined {
		return this.jobs.get(id);
	}

	list(): Job[] {
		return [...this.jobs.values()];
	}

	runningCount(): number {
		let n = 0;
		for (const job of this.jobs.values()) if (job.running) n++;
		return n;
	}

	start(spec: JobSpec): Job {
		if (this.disposed) throw new Error("The shell job manager for this session has shut down.");
		validateTimeoutMs(spec.timeoutMs);
		// The readiness pattern is compiled before any side effect: an invalid regex is a
		// pre-spawn error and no process, log, or job is created for the call.
		let readyRegex: RegExp | undefined;
		if (spec.readyPattern !== undefined) readyRegex = compileReadyPattern(spec.readyPattern);
		validateReadyTimeoutMs(spec.readyTimeoutMs);
		if (spec.readyTimeoutMs !== undefined && spec.readyPattern === undefined) {
			throw new Error("Invalid readyTimeoutMs: requires readyPattern");
		}
		if (this.runningCount() >= this.opts.maxRunning) {
			throw new Error(
				`Too many running shell jobs (limit ${this.opts.maxRunning}). Stop or wait for one with bash_job first.`,
			);
		}
		let cwdStat: fs.Stats | undefined;
		try {
			cwdStat = fs.statSync(spec.cwd);
		} catch {}
		if (!cwdStat?.isDirectory()) throw new Error(`Working directory does not exist: ${spec.cwd}`);

		const id = `job-${++this.counter}-${randomBytes(2).toString("hex")}`;
		const logPath = join(this.logDir, `${id}.log`);
		const fd = fs.openSync(
			logPath,
			fs.constants.O_WRONLY |
				fs.constants.O_CREAT |
				fs.constants.O_EXCL |
				fs.constants.O_APPEND |
				(fs.constants.O_NOFOLLOW ?? 0),
			0o600,
		);
		const job = new Job(id, spec, logPath, this.opts.now());
		this.jobs.set(id, job);
		this.installExitHook();
		this.appendJobEvent(job, "started");

		let child: ChildProcess;
		try {
			child = spawn(this.opts.shell, this.opts.commandOnStdin ? this.opts.shellArgs : [...this.opts.shellArgs, spec.command], {
				cwd: spec.cwd,
				env: spec.env ?? process.env,
				detached: process.platform !== "win32",
				stdio: [this.opts.commandOnStdin ? "pipe" : "ignore", fd, fd],
				windowsHide: true,
			});
		} catch (err) {
			fs.closeSync(fd);
			this.finish(job, "spawn_error", null, null, err instanceof Error ? err.message : String(err));
			return job;
		}
		// The child holds its own copy of the descriptor.
		fs.closeSync(fd);
		job.child = child;
		job.pid = child.pid;
		if (this.opts.commandOnStdin) {
			child.stdin?.on("error", () => {});
			child.stdin?.end(spec.command);
		}
		child.once("spawn", () => job.resolveLaunched(true));
		child.once("error", (err) => {
			// "error" without a pid means the shell never started; later errors (failed kill) are ignored.
			if (!child.pid) this.finish(job, "spawn_error", null, null, err.message);
		});
		child.once("exit", (code, signal) => this.onExit(job, code, signal));
		// A foreground caller is awaiting the job, so keep the event loop alive until handoff.
		if (this.opts.unref && job.owner === "background") child.unref();

		if (spec.timeoutMs !== undefined) {
			job.deadlineTimer = setTimeout(() => this.requestStop(job, "timed_out"), spec.timeoutMs);
			if (this.opts.unref) job.deadlineTimer.unref();
		}
		if (readyRegex) {
			job.readyRegex = readyRegex;
			job.readyMatcher = new ReadyLineMatcher(readyRegex);
			job.readyDecoder = new StringDecoder("utf8");
			if (spec.readyTimeoutMs !== undefined) {
				// Expiry only disarms the watch; the job keeps running and still reports completion.
				job.readyTimer = setTimeout(() => {
					job.readyExpired = true;
					job.readyMatcher = undefined;
				}, spec.readyTimeoutMs);
				if (this.opts.unref) job.readyTimer.unref();
			}
		}
		this.ensureMonitor();
		this.notifyChange(job, "start");
		return job;
	}

	/**
	 * Append one event to the JSONL event log, if configured. One synchronous append call
	 * per event; parent directories are created as needed. Never throws into the job
	 * lifecycle: I/O failure leaves one short fixed stderr note instead.
	 */
	private appendJobEvent(job: Job, event: JobEvent["event"]): void {
		const path = this.opts.eventLogPath;
		if (!path) return;
		const entry: JobEvent =
			event === "started"
				? { event, job_id: job.id, command: job.command, ts: this.opts.now() }
				: { event, job_id: job.id, command: job.command, status: job.status, exitCode: job.exitCode, ts: this.opts.now() };
		try {
			fs.mkdirSync(dirname(path), { recursive: true });
			fs.appendFileSync(path, `${JSON.stringify(entry)}\n`);
		} catch {
			try {
				process.stderr.write("[pi-async-bash: event log append failed]\n");
			} catch {}
		}
	}

	private notifyChange(job: Job, change: JobChange): void {
		if (!this.opts.onChange) return;
		try {
			this.opts.onChange(job, change);
		} catch {}
	}

	/**
	 * Move a running foreground job to the background. Returns false when the job already
	 * finished; the caller then reports the completion itself.
	 */
	handoff(job: Job): boolean {
		if (!job.running) return false;
		job.owner = "background";
		if (this.opts.unref) job.child?.unref();
		this.notifyChange(job, "handoff");
		return true;
	}

	/**
	 * Stop a job's process group. The first reason wins and becomes the final status, even if the
	 * shell then exits 0. SIGTERM first, SIGKILL to the group after the grace period unless the group
	 * is already empty; an `output_limit` stop uses SIGKILL at once to limit overshoot. Repeated
	 * calls do not send more signals or arm more timers.
	 */
	requestStop(job: Job, reason: StopReason): Promise<JobInfo> {
		if (!job.running || job.stopReason) return job.done;
		// Stop-race hardening: the leader may already be a zombie/gone at the OS level
		// while `job.running` is still true (exit event pending). Synthesizing `stopped`
		// then would lie; return the pending natural outcome with no signals or reason rewrite.
		if (leaderExitedBeforeExitEvent(job)) return job.done;
		job.stopReason = reason;
		const pid = job.pid;
		if (!pid) return job.done;
		job.cleanupPending = true;
		this.pendingGroupCleanup.add(job);
		this.installExitHook();
		if (reason === "output_limit") {
			killGroup(pid, "SIGKILL");
			// Still verify the group is empty once the shell exits (see onExit).
		} else {
			killGroup(pid, "SIGTERM");
		}
		job.cleanupTimer = setTimeout(() => this.finishGroupCleanup(job, true), reason === "output_limit" ? 0 : this.opts.killGraceMs);
		job.cleanupTimer.unref();
		this.notifyChange(job, "stop");
		return job.done;
	}

	/**
	 * End a pending group cleanup. With `force`, SIGKILL whatever is still in the group.
	 * Signals go only to a group we signalled within the grace period; the PGID cannot be reused
	 * while any member exists, and the cleanup is dropped as soon as the group is seen empty.
	 */
	private finishGroupCleanup(job: Job, force: boolean): void {
		if (!job.cleanupPending) return;
		if (job.cleanupTimer) clearTimeout(job.cleanupTimer);
		job.cleanupTimer = undefined;
		if (force && job.pid && groupAlive(job.pid)) killGroup(job.pid, "SIGKILL");
		job.cleanupPending = false;
		this.pendingGroupCleanup.delete(job);
		this.removeExitHookIfIdle();
	}

	/** Wait up to `ms` for a job to finish. Resolves true if it finished. */
	async wait(job: Job, ms: number, signal?: AbortSignal): Promise<boolean> {
		if (!job.running) return true;
		let timer: NodeJS.Timeout | undefined;
		let onAbort: (() => void) | undefined;
		try {
			return await new Promise<boolean>((resolve) => {
				job.done.then(() => resolve(true));
				timer = setTimeout(() => resolve(!job.running), Math.min(Math.max(0, ms), MAX_TIMER_MS));
				if (signal) {
					onAbort = () => resolve(!job.running);
					if (signal.aborted) onAbort();
					else signal.addEventListener("abort", onAbort, { once: true });
				}
			});
		} finally {
			if (timer) clearTimeout(timer);
			if (signal && onAbort) signal.removeEventListener("abort", onAbort);
		}
	}

	/** Jobs whose process or process group still needs this manager (running or group cleanup pending). */
	activeCount(): number {
		const active = new Set(this.pendingGroupCleanup);
		for (const job of this.jobs.values()) if (job.running) active.add(job);
		return active.size;
	}

	tail(job: Job, maxBytes: number, maxLines: number): TailResult {
		if (job.logDeleted) return { text: "", truncated: false, totalBytes: job.logBytes };
		return readLogTail(job.logPath, maxBytes, maxLines);
	}

	/**
	 * Delete a finished job's log and forget it (a foreground completion whose output fit).
	 * Jobs whose log is still referenced must stay registered so their logs remain in the disk accounting.
	 */
	release(job: Job): void {
		if (job.running) return;
		this.deleteLog(job);
		this.jobs.delete(job.id);
	}

	/**
	 * Stop every running job and refuse new ones. Idempotent. Logs stay on disk.
	 * The process exit hook remains until every signalled process group is confirmed empty or
	 * SIGKILLed. Descendants left behind by jobs that already exited normally are not touched.
	 */
	dispose(): void {
		if (this.disposed) return;
		this.disposed = true;
		for (const job of this.jobs.values()) {
			if (job.running) void this.requestStop(job, "shutdown");
		}
		this.stopMonitorIfIdle();
		this.removeExitHookIfIdle();
	}

	/** Synchronous last resort on process exit: SIGKILL running jobs and groups still being cleaned up. */
	killAllSync(): void {
		const active = new Set(this.pendingGroupCleanup);
		for (const job of this.jobs.values()) if (job.running) active.add(job);
		for (const job of active) killGroup(job.pid, "SIGKILL");
	}

	private onExit(job: Job, code: number | null, signal: NodeJS.Signals | null): void {
		this.finish(job, job.stopReason ?? "exited", code ?? signalExitCode(signal), signal);
		// The shell is gone; if nothing else is in its group, no delayed SIGKILL is needed.
		if (job.cleanupPending && job.pid && !groupAlive(job.pid)) this.finishGroupCleanup(job, false);
	}

	private finish(job: Job, status: JobStatus, exitCode: number | null, signal: NodeJS.Signals | null, error?: string): void {
		if (!job.running) return;
		// Settle-time readiness scan (output before the first tick, unterminated final
		// line): must run while the job is still marked running.
		this.finishReadyScan(job);
		job.endedAt = this.opts.now();
		job.exitCode = exitCode;
		job.signal = signal;
		job.error = error;
		if (job.deadlineTimer) clearTimeout(job.deadlineTimer);
		job.deadlineTimer = undefined;
		// The job is done; the readiness watch is over (a match now would be stale next to completion).
		if (job.readyTimer) clearTimeout(job.readyTimer);
		job.readyTimer = undefined;
		job.child = undefined;
		try {
			job.logBytes = fs.statSync(job.logPath).size;
		} catch {}
		// Output written between monitor ticks (or by a command that exited before the first tick).
		if (job.logBytes > this.opts.maxLogBytes) {
			if (status === "exited") status = "output_limit";
			this.trimLog(job);
		}
		job.status = status;
		// finish() returns early once the job is done, so `settled` is appended exactly once.
		this.appendJobEvent(job, "settled");
		job.resolveLaunched(status !== "spawn_error");
		// Disk accounting and retention run before anyone observes the result.
		this.enforceRetention();
		this.checkOutputLimits();
		const info = job.info();
		job.resolveDone(info);
		if (job.owner === "background" && !job.delivered && !this.disposed) {
			try {
				this.opts.onBackgroundComplete?.(job);
			} catch {}
		}
		this.notifyChange(job, "finish");
		this.stopMonitorIfIdle();
		this.removeExitHookIfIdle();
	}

	/** Keep only the last `maxLogBytes` of a finished job's log, behind a one-line marker. */
	private trimLog(job: Job): void {
		try {
			const size = fs.statSync(job.logPath).size;
			if (size <= this.opts.maxLogBytes) return;
			const header = Buffer.from(TRIM_HEADER(size));
			const keep = Math.max(0, this.opts.maxLogBytes - header.length);
			const dropped = size - keep;
			const marker = Buffer.from(TRIM_HEADER(dropped));
			const buf = Buffer.alloc(keep);
			const fd = fs.openSync(job.logPath, fs.constants.O_RDONLY | (fs.constants.O_NOFOLLOW ?? 0));
			try {
				fs.readSync(fd, buf, 0, keep, size - keep);
			} finally {
				fs.closeSync(fd);
			}
			const tmp = `${job.logPath}.trim`;
			fs.writeFileSync(tmp, Buffer.concat([marker, buf]), { mode: 0o600, flag: "wx" });
			fs.renameSync(tmp, job.logPath);
			job.logBytes = marker.length + keep;
		} catch {
			// If trimming fails, the total budget still deletes the log.
		}
	}

	private deleteLog(job: Job): void {
		if (job.logDeleted) return;
		try {
			fs.unlinkSync(job.logPath);
		} catch {}
		job.logDeleted = true;
	}

	private enforceRetention(): void {
		const finished = this.list()
			.filter((job) => !job.running)
			.sort((a, b) => (a.endedAt ?? 0) - (b.endedAt ?? 0));
		while (finished.length > this.opts.maxCompleted) {
			const job = finished.shift()!;
			this.deleteLog(job);
			this.jobs.delete(job.id);
		}
	}

	/** Enforce per-job and total log size limits. Exposed for tests. */
	checkOutputLimits(): void {
		let total = 0;
		const running: { job: Job; size: number }[] = [];
		for (const job of this.jobs.values()) {
			if (job.logDeleted) continue;
			let size = job.logBytes;
			if (job.running) {
				try {
					size = fs.statSync(job.logPath).size;
				} catch {
					size = 0;
				}
				job.logBytes = size;
				if (size > this.opts.maxLogBytes) void this.requestStop(job, "output_limit");
				else running.push({ job, size });
			}
			total += size;
		}
		if (total <= this.opts.maxTotalLogBytes) return;
		// Oldest first, logs the model already saw before logs it has not.
		const finished = this.list()
			.filter((job) => !job.running && !job.logDeleted)
			.sort((a, b) => Number(b.delivered) - Number(a.delivered) || (a.endedAt ?? 0) - (b.endedAt ?? 0));
		for (const job of finished) {
			if (total <= this.opts.maxTotalLogBytes) return;
			total -= job.logBytes;
			this.deleteLog(job);
		}
		if (total <= this.opts.maxTotalLogBytes) return;
		running.sort((a, b) => b.size - a.size);
		const largest = running.find(({ job }) => !job.stopReason);
		if (largest) void this.requestStop(largest.job, "output_limit");
	}

	/**
	 * Scan log bytes written since the last scan for the job's readiness pattern.
	 * Old bytes are never re-scanned: the scan offset only advances over bytes read.
	 * Fires at most once per job through `onReady`; the job keeps running.
	 * Exposed for tests; the monitor tick calls it for every running watched job.
	 */
	checkReady(job: Job): boolean {
		if (!job.readyRegex || !job.readyMatcher || !job.readyDecoder) return job.readyFired;
		if (job.readyFired || job.readyExpired || !job.running) return job.readyFired;
		let size: number;
		try {
			size = fs.statSync(job.logPath).size;
		} catch {
			return false;
		}
		// The log only grows while the job runs; a smaller file means it was rewritten
		// under us, so continue from its start rather than skipping everything.
		if (size < job.readyScannedBytes) job.readyScannedBytes = size;
		if (size <= job.readyScannedBytes) return false;
		const length = Math.min(size - job.readyScannedBytes, READY_SCAN_MAX_BYTES);
		let fd: number;
		try {
			fd = fs.openSync(job.logPath, fs.constants.O_RDONLY | (fs.constants.O_NOFOLLOW ?? 0));
		} catch {
			return false;
		}
		try {
			const buf = Buffer.alloc(length);
			let read = 0;
			while (read < length) {
				const n = fs.readSync(fd, buf, read, length - read, job.readyScannedBytes + read);
				if (n <= 0) break;
				read += n;
			}
			job.readyScannedBytes += read;
			if (read === 0) return false;
			// The decoder holds a split trailing character; the matcher holds a split trailing line.
			const text = job.readyDecoder.write(buf.subarray(0, read));
			if (job.readyMatcher.push(text).length === 0) return false;
			this.fireReady(job);
			return true;
		} finally {
			fs.closeSync(fd);
		}
	}

	/**
	 * Record a readiness watch firing and notify once. The job keeps running; delivery
	 * decides whether the notice still goes out (a match found at settle is reported
	 * ahead of the completion notice).
	 */
	private fireReady(job: Job): void {
		job.readyFired = true;
		job.readyLine = job.readyMatcher?.matchedLine;
		if (job.readyTimer) clearTimeout(job.readyTimer);
		job.readyTimer = undefined;
		if (!this.disposed) {
			try {
				this.opts.onReady?.(job);
			} catch {}
		}
	}

	/**
	 * One last readiness scan at settle, before the timer is cleared: output written since
	 * the last tick (or before the first tick for fast jobs) still deserves its one-shot
	 * notice. Runs while the job is still marked running so `checkReady` does not skip it,
	 * then flushes the decoder and treats a trailing unterminated line as complete, since
	 * no more output is coming. Never throws into `finish()`.
	 */
	private finishReadyScan(job: Job): void {
		if (!job.readyRegex || !job.readyMatcher || !job.readyDecoder) return;
		if (job.readyFired || job.readyExpired) return;
		try {
			this.checkReady(job);
		} catch {}
		try {
			if (job.readyFired || !job.readyMatcher || !job.readyDecoder) return;
			const tail = job.readyDecoder.end();
			if (tail !== "" && job.readyMatcher.push(tail).length > 0) {
				this.fireReady(job);
				return;
			}
			if (job.readyMatcher.flush().length > 0) this.fireReady(job);
		} catch {}
	}

	/** Scan every running watched job for a readiness match. Called on each monitor tick. */
	private checkReadyJobs(): void {
		for (const job of this.jobs.values()) {
			if (!job.running || job.readyFired || job.readyExpired || !job.readyRegex) continue;
			try {
				this.checkReady(job);
			} catch {}
		}
	}

	private ensureMonitor(): void {
		if (this.monitor) return;
		this.monitor = setInterval(() => {
			this.checkOutputLimits();
			this.checkReadyJobs();
		}, this.opts.monitorIntervalMs);
		this.monitor.unref();
	}

	private stopMonitorIfIdle(): void {
		if (this.monitor && this.runningCount() === 0) {
			clearInterval(this.monitor);
			this.monitor = undefined;
		}
	}

	private installExitHook(): void {
		if (this.exitHookInstalled) return;
		process.on("exit", this.exitHook);
		this.exitHookInstalled = true;
	}

	private removeExitHookIfIdle(): void {
		if (this.exitHookInstalled && this.activeCount() === 0) {
			process.off("exit", this.exitHook);
			this.exitHookInstalled = false;
		}
	}
}
