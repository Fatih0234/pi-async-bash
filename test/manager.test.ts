import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import * as fs from "node:fs";
import { join } from "node:path";
import { after, describe, test } from "node:test";
import {
	cleanOutput,
	compileReadyPattern,
	createSessionLogDir,
	type Job,
	JobManager,
	type JobManagerOptions,
	procHooks,
	pruneSessionLogDirs,
	readLogTail,
	ReadyLineMatcher,
} from "../src/manager.ts";
import { resolveConfig } from "../src/index.ts";
import { alive, sleep, tempDir, waitFor } from "./helpers.ts";

const managers: JobManager[] = [];
after(() => {
	for (const m of managers) m.dispose();
});

function make(opts: Partial<JobManagerOptions> = {}): { mgr: JobManager; completed: Job[] } {
	const completed: Job[] = [];
	const mgr = new JobManager({
		logDir: join(tempDir(), "logs"),
		shell: "/bin/bash",
		shellArgs: ["-c"],
		killGraceMs: 300,
		onBackgroundComplete: (job) => completed.push(job),
		...opts,
	});
	managers.push(mgr);
	return { mgr, completed };
}

describe("JobManager", () => {
	test("quick exit with stdout and stderr in one private log", async () => {
		const { mgr, completed } = make();
		const job = mgr.start({ command: "echo out; echo err >&2", cwd: process.cwd() });
		assert.equal(await job.launched, true);
		const info = await job.done;
		assert.equal(info.status, "exited");
		assert.equal(info.exitCode, 0);
		assert.equal(mgr.tail(job, 1000, 10).text, "out\nerr");
		assert.equal(fs.statSync(job.logPath).mode & 0o777, 0o600);
		assert.equal(fs.statSync(mgr.logDir).mode & 0o777, 0o700);
		assert.equal(completed.length, 0, "foreground-owned jobs are not reported as background completions");
	});

	test("nonzero and signal exits", async () => {
		const { mgr } = make();
		assert.equal((await mgr.start({ command: "exit 3", cwd: "/" }).done).exitCode, 3);
		const sig = await mgr.start({ command: "kill -TERM $$", cwd: "/" }).done;
		assert.equal(sig.status, "exited");
		assert.equal(sig.signal, "SIGTERM");
		assert.equal(sig.exitCode, 143);
	});

	test("cwd and env are passed through", async () => {
		const { mgr } = make();
		const dir = tempDir();
		const job = mgr.start({ command: 'pwd; echo "$PAB_X"', cwd: dir, env: { ...process.env, PAB_X: "hello" } });
		await job.done;
		assert.equal(mgr.tail(job, 1000, 10).text, `${fs.realpathSync(dir)}\nhello`);
	});

	test("spawn failure and missing cwd", async () => {
		const { mgr } = make({ shell: "/nonexistent/shell-binary" });
		const job = mgr.start({ command: "true", cwd: "/" });
		assert.equal(await job.launched, false);
		const info = await job.done;
		assert.equal(info.status, "spawn_error");
		assert.match(info.error ?? "", /ENOENT/);
		assert.throws(() => mgr.start({ command: "true", cwd: "/does/not/exist" }), /Working directory does not exist/);
	});

	test("invalid timeouts are rejected", () => {
		const { mgr } = make();
		for (const t of [0, -1, Number.NaN, Number.POSITIVE_INFINITY, 2 ** 31]) {
			assert.throws(() => mgr.start({ command: "true", cwd: "/", timeoutMs: t }), /Invalid timeout/);
		}
	});

	test("hard timeout survives handoff and reports once", async () => {
		const { mgr, completed } = make();
		const job = mgr.start({ command: "sleep 5", cwd: "/", timeoutMs: 300 });
		await sleep(50);
		assert.equal(mgr.handoff(job), true);
		const started = Date.now();
		const info = await job.done;
		assert.ok(Date.now() - started < 2000);
		assert.equal(info.status, "timed_out");
		assert.equal(completed.length, 1);
		assert.equal(completed[0], job);
	});

	test("explicit background job reports completion", async () => {
		const { mgr, completed } = make();
		const job = mgr.start({ command: "echo bg", cwd: "/", background: true });
		await job.done;
		assert.deepEqual(completed, [job]);
	});

	test("handoff after exit returns false (completion race)", async () => {
		const { mgr, completed } = make();
		const job = mgr.start({ command: "true", cwd: "/" });
		await job.done;
		assert.equal(mgr.handoff(job), false);
		assert.equal(job.owner, "foreground");
		assert.equal(completed.length, 0);
	});

	test("stop kills the whole process tree", async () => {
		const { mgr } = make();
		const job = mgr.start({ command: "sleep 30 & echo $!; sleep 30", cwd: "/", background: true });
		await waitFor(() => /^\d+$/m.test(mgr.tail(job, 100, 5).text), 3000, "descendant pid");
		const pid = Number(mgr.tail(job, 100, 5).text.trim());
		assert.ok(alive(pid));
		const info = await mgr.requestStop(job, "stopped");
		assert.equal(info.status, "stopped");
		await waitFor(() => !alive(pid), 3000, "descendant to die");
	});

	test("stop escalates to SIGKILL when TERM is ignored", async () => {
		const { mgr } = make({ killGraceMs: 200 });
		const job = mgr.start({ command: "trap '' TERM; sleep 30; true", cwd: "/" });
		await sleep(100);
		const info = await mgr.requestStop(job, "stopped");
		assert.equal(info.status, "stopped");
		assert.equal(info.signal, "SIGKILL");
	});

	test("per-job output limit kills a job producing enormous no-newline output", async () => {
		const { mgr } = make({ maxLogBytes: 200_000, monitorIntervalMs: 25 });
		const job = mgr.start({ command: "yes abcdefgh | tr -d '\\n'", cwd: "/", background: true });
		const info = await job.done;
		assert.equal(info.status, "output_limit");
		const tail = mgr.tail(job, 4096, 100);
		assert.ok(tail.truncated);
		assert.ok(tail.text.length <= 4096);
		assert.match(tail.text, /^[a-h]+$/);
	});

	test("total log budget evicts the oldest finished logs", async () => {
		const { mgr } = make({ maxTotalLogBytes: 150_000 });
		const a = mgr.start({ command: "head -c 100000 /dev/zero", cwd: "/" });
		await a.done;
		const b = mgr.start({ command: "head -c 100000 /dev/zero", cwd: "/" });
		await b.done;
		mgr.checkOutputLimits();
		assert.equal(a.logDeleted, true);
		assert.equal(fs.existsSync(a.logPath), false);
		assert.equal(b.logDeleted, false);
	});

	test("finished-job bookkeeping is bounded", async () => {
		const { mgr } = make({ maxCompleted: 2 });
		const jobs: Job[] = [];
		for (let i = 0; i < 3; i++) {
			const job = mgr.start({ command: `echo ${i}`, cwd: "/" });
			await job.done;
			jobs.push(job);
		}
		assert.equal(mgr.get(jobs[0]!.id), undefined);
		assert.equal(fs.existsSync(jobs[0]!.logPath), false);
		assert.ok(mgr.get(jobs[2]!.id));
	});

	test("retention eviction reports each forgotten job through onEvict", async () => {
		const evicted: string[] = [];
		const { mgr } = make({ maxCompleted: 2, onEvict: (job) => void evicted.push(job.id) });
		const jobs: Job[] = [];
		for (let i = 0; i < 4; i++) {
			const job = mgr.start({ command: `echo ${i}`, cwd: "/" });
			await job.done;
			jobs.push(job);
		}
		assert.deepEqual(evicted, [jobs[0]!.id, jobs[1]!.id]);
		assert.equal(mgr.get(jobs[0]!.id), undefined);
		assert.equal(mgr.get(jobs[1]!.id), undefined);
		assert.ok(mgr.get(jobs[2]!.id));
		assert.ok(mgr.get(jobs[3]!.id));
	});

	test("running job limit", async () => {
		const { mgr } = make({ maxRunning: 1 });
		const job = mgr.start({ command: "sleep 5", cwd: "/" });
		assert.throws(() => mgr.start({ command: "true", cwd: "/" }), /Too many running/);
		await mgr.requestStop(job, "stopped");
		await mgr.start({ command: "true", cwd: "/" }).done;
	});

	test("dispose stops running jobs, refuses new ones, and suppresses callbacks", async () => {
		const { mgr, completed } = make();
		const job = mgr.start({ command: "sleep 30", cwd: "/", background: true });
		await job.launched;
		const pid = job.pid!;
		mgr.dispose();
		mgr.dispose();
		const info = await job.done;
		assert.equal(info.status, "shutdown");
		assert.equal(completed.length, 0);
		await waitFor(() => !alive(pid), 3000, "process to die");
		assert.throws(() => mgr.start({ command: "true", cwd: "/" }), /shut down/);
		assert.equal(fs.existsSync(job.logPath), true, "logs survive teardown");
	});

	test("bounded wait returns without finishing the job", async () => {
		const { mgr } = make();
		const job = mgr.start({ command: "sleep 5", cwd: "/" });
		assert.equal(await mgr.wait(job, 100), false);
		assert.equal(job.running, true);
		const ac = new AbortController();
		setTimeout(() => ac.abort(), 50);
		assert.equal(await mgr.wait(job, 10_000, ac.signal), false);
		await mgr.requestStop(job, "stopped");
		assert.equal(await mgr.wait(job, 100), true);
	});
});

describe("stop ownership and cleanup", () => {
	const TERM_EXITS_0 = "trap 'exit 0' TERM; echo READY; while :; do sleep 0.01; done";

	test("a requested stop stays authoritative when the shell exits 0 on TERM", async () => {
		const { mgr } = make();
		const job = mgr.start({ command: TERM_EXITS_0, cwd: "/" });
		await waitFor(() => mgr.tail(job, 100, 5).text.includes("READY"), 3000, "READY");
		const info = await mgr.requestStop(job, "timed_out");
		assert.equal(info.exitCode, 0);
		assert.equal(info.status, "timed_out");
	});

	test("deadline after handoff reports timed_out even with a clean exit, once", async () => {
		const { mgr, completed } = make();
		const job = mgr.start({ command: TERM_EXITS_0, cwd: "/", timeoutMs: 400 });
		mgr.handoff(job);
		const info = await job.done;
		assert.equal(info.status, "timed_out");
		assert.equal(info.exitCode, 0);
		assert.deepEqual(completed, [job]);
	});

	test("the first stop reason wins and repeated stops add no cleanup", async () => {
		const { mgr } = make({ killGraceMs: 5000 });
		const job = mgr.start({ command: TERM_EXITS_0, cwd: "/" });
		await waitFor(() => mgr.tail(job, 100, 5).text.includes("READY"), 3000, "READY");
		void mgr.requestStop(job, "stopped");
		const timer = job.cleanupTimer;
		void mgr.requestStop(job, "timed_out");
		void mgr.requestStop(job, "aborted");
		assert.equal(job.cleanupTimer, timer);
		const info = await job.done;
		assert.equal(info.status, "stopped");
		// The group was empty when the shell exited, so no delayed SIGKILL remains armed.
		assert.equal(job.cleanupPending, false);
		assert.equal(job.cleanupTimer, undefined);
	});

	test('stop() issued after natural exit resolves to the exit outcome, not "stopped"', async () => {
		const { mgr } = make();
		const job = mgr.start({ command: "exit 7", cwd: "/" });
		// Let the OS process exit while blocking Node from observing the exit event,
		// so requestStop races the pending exit delivery.
		const end = Date.now() + 500;
		while (Date.now() < end) {}
		const res = await mgr.requestStop(job, "stopped");
		assert.notStrictEqual(res.status, "stopped");
		assert.equal(res.status, "exited");
		assert.equal(res.exitCode, 7);
	});

	test("a TERM-ignoring descendant is SIGKILLed after the shell exits", async () => {
		const { mgr } = make({ killGraceMs: 400 });
		const job = mgr.start({ command: "(trap '' TERM; exec sleep 30) & echo $!; wait", cwd: "/", background: true });
		await waitFor(() => /^\d+$/m.test(mgr.tail(job, 100, 5).text), 3000, "descendant pid");
		const pid = Number(mgr.tail(job, 100, 5).text.trim());
		try {
			const info = await mgr.requestStop(job, "stopped");
			assert.equal(info.status, "stopped");
			assert.equal(job.cleanupPending, true, "cleanup outlives the shell");
			assert.ok(alive(pid));
			await waitFor(() => !alive(pid), 3000, "descendant SIGKILL");
			await waitFor(() => !job.cleanupPending, 1000, "cleanup to finish");
		} finally {
			if (alive(pid)) process.kill(pid, "SIGKILL");
		}
	});

	test("process exit during the grace period still kills the group", async () => {
		const logDir = join(tempDir(), "l");
		const script = `
			import { JobManager } from ${JSON.stringify(new URL("../src/manager.ts", import.meta.url).pathname)};
			const mgr = new JobManager({ logDir: ${JSON.stringify(logDir)}, shell: "/bin/bash", shellArgs: ["-c"], unref: true, killGraceMs: 30000 });
			const job = mgr.start({ command: "(trap '' TERM; exec sleep 30) & echo $!; wait", cwd: "/", background: true });
			const keepAlive = setTimeout(() => {}, 60000); // stands in for Pi's own event loop handles
			const timer = setInterval(async () => {
				const text = mgr.tail(job, 100, 5).text.trim();
				if (!/^\\d+$/.test(text)) return;
				clearInterval(timer);
				await mgr.requestStop(job, "shutdown");
				console.log(text, job.status, job.cleanupPending);
				process.exit(0);
			}, 20);
		`;
		const res = spawnSync(process.execPath, ["--input-type=module", "-e", script], { encoding: "utf8", timeout: 10_000 });
		const [pidText, status, pending] = res.stdout.trim().split(" ");
		const pid = Number(pidText);
		try {
			assert.equal(res.status, 0, res.stderr);
			assert.equal(status, "shutdown");
			assert.equal(pending, "true");
			await waitFor(() => !alive(pid), 2000, "exit hook to kill the descendant");
		} finally {
			if (pid > 0 && alive(pid)) process.kill(pid, "SIGKILL");
		}
	});
});

describe("cleanup independent of result retention", () => {
	for (const removal of ["release", "retention eviction"] as const) {
		test(`process exit kills pending descendants after ${removal}`, async () => {
			const logDir = join(tempDir(), "logs");
			const script = `
				import { JobManager } from ${JSON.stringify(new URL("../src/manager.ts", import.meta.url).pathname)};
				const mgr = new JobManager({ logDir: ${JSON.stringify(logDir)}, shell: "/bin/bash", shellArgs: ["-c"], unref: true, killGraceMs: 30000, maxCompleted: 1 });
				const job = mgr.start({ command: "(trap '' TERM; exec sleep 30) & echo $!; wait", cwd: "/" });
				while (!/^\\d+$/.test(mgr.tail(job, 100, 5).text.trim())) await new Promise(r => setTimeout(r, 10));
				const pid = Number(mgr.tail(job, 100, 5).text.trim());
				console.log(JSON.stringify({ pid }));
				await mgr.requestStop(job, "timed_out");
				${removal === "release" ? "mgr.release(job);" : 'await mgr.start({ command: "true", cwd: "/" }).done;'}
				console.log(JSON.stringify({ removed: !mgr.get(job.id), pending: job.cleanupPending, active: mgr.activeCount() }));
				process.exit(0);
			`;
			const result = spawnSync(process.execPath, ["--input-type=module", "-e", script], { encoding: "utf8", timeout: 10_000 });
			let pid: number | undefined;
			try {
				const lines = result.stdout.trim().split("\n");
				pid = JSON.parse(lines[0]!).pid;
				assert.equal(result.status, 0, result.stderr);
				assert.deepEqual(JSON.parse(lines[1]!), { removed: true, pending: true, active: 1 });
				await waitFor(() => !alive(pid!), 2000, "exit hook to kill released job descendant");
			} finally {
				if (pid && alive(pid)) process.kill(pid, "SIGKILL");
			}
		});
	}
});

describe("output limits at completion", () => {
	test("a command that exits before any monitor tick is still limited", async () => {
		const { mgr } = make({ maxLogBytes: 1024, monitorIntervalMs: 500 });
		const job = mgr.start({ command: "yes x | head -c 16384", cwd: "/" });
		const info = await job.done;
		assert.equal(info.status, "output_limit");
		assert.equal(info.exitCode, 0);
		assert.ok(info.logBytes <= 1024, `logBytes ${info.logBytes}`);
		assert.equal(fs.statSync(job.logPath).size, info.logBytes);
		const content = fs.readFileSync(job.logPath, "utf8");
		assert.match(content, /^\[pi-async-bash: \d+ earlier bytes removed/);
		assert.match(content, /x\n$/);
	});

	test("session budget holds for finished jobs with no process left", async () => {
		const { mgr } = make({ maxLogBytes: 1000, maxTotalLogBytes: 3000 });
		for (let i = 0; i < 6; i++) await mgr.start({ command: "yes y | head -c 900", cwd: "/" }).done;
		const total = fs.readdirSync(mgr.logDir).reduce((sum, f) => sum + fs.statSync(join(mgr.logDir, f)).size, 0);
		assert.ok(total <= 3000, `total ${total}`);
		assert.equal(mgr.list().filter((j) => !j.logDeleted).length, 3);
	});
});

describe("event loop ownership", () => {
	test("with unref, a foreground job keeps Node alive but a handed-off job does not", () => {
		const script = (handoff: boolean) => `
			import { JobManager } from ${JSON.stringify(new URL("../src/manager.ts", import.meta.url).pathname)};
			const mgr = new JobManager({ logDir: ${JSON.stringify(join(tempDir(), "l"))}, shell: "/bin/bash", shellArgs: ["-c"], unref: true });
			const job = mgr.start({ command: "sleep 0.4", cwd: "/" });
			${handoff ? "mgr.handoff(job);" : ""}
			job.done.then((info) => console.log("finished", info.status));
		`;
		const run = (handoff: boolean) =>
			spawnSync(process.execPath, ["--input-type=module", "-e", script(handoff)], { encoding: "utf8", timeout: 10_000 });
		const fg = run(false);
		assert.equal(fg.stdout.trim(), "finished exited", fg.stderr);
		const bg = run(true);
		assert.equal(bg.status, 0, bg.stderr);
		assert.equal(bg.stdout.trim(), "", "a background job does not hold the process open");
	});
});

describe("event log", () => {
	test("start+settle appends exactly 2 whole JSON lines", async () => {
		const dir = tempDir();
		const logPath = join(dir, "nested", "events.jsonl");
		const { mgr } = make({ eventLogPath: logPath });
		const job = mgr.start({ command: "echo hi", cwd: "/" });
		const info = await job.done;
		const raw = fs.readFileSync(logPath, "utf8");
		assert.ok(raw.endsWith("\n"), "each event is one whole line");
		const lines = raw.split("\n").filter((line) => line !== "");
		assert.strictEqual(lines.length, 2);
		const started = JSON.parse(lines[0]!);
		const settled = JSON.parse(lines[1]!);
		assert.deepStrictEqual(Object.keys(started).sort(), ["command", "event", "job_id", "ts"]);
		assert.equal(started.event, "started");
		assert.equal(started.job_id, job.id);
		assert.equal(started.command, "echo hi");
		assert.equal(typeof started.ts, "number");
		assert.equal(settled.event, "settled");
		assert.equal(settled.job_id, job.id);
		assert.equal(settled.command, "echo hi");
		assert.equal(settled.status, info.status);
		assert.equal(typeof settled.ts, "number");
	});

	test("settled carries exitCode, started carries none", async () => {
		const dir = tempDir();
		const logPath = join(dir, "events.jsonl");
		const { mgr } = make({ eventLogPath: logPath });
		const failing = mgr.start({ command: "exit 1", cwd: "/" });
		const clean = mgr.start({ command: "true", cwd: "/" });
		const [failingInfo, cleanInfo] = await Promise.all([failing.done, clean.done]);
		assert.equal(failingInfo.exitCode, 1);
		assert.equal(cleanInfo.exitCode, 0);
		const lines = fs.readFileSync(logPath, "utf8").split("\n").filter((line) => line !== "");
		assert.strictEqual(lines.length, 4);
		const events = lines.map((line) => JSON.parse(line));
		for (const started of events.filter((e) => e.event === "started")) {
			assert.ok(!("exitCode" in started), "started carries no exitCode");
		}
		const byId = new Map(events.map((e) => [`${e.event}:${e.job_id}`, e]));
		assert.equal(byId.get(`settled:${failing.id}`)?.exitCode, 1);
		assert.equal(byId.get(`settled:${clean.id}`)?.exitCode, 0);
	});
});

describe("log helpers", () => {
	test("tail of an enormous single line is bounded", () => {
		const dir = tempDir();
		const path = join(dir, "x.log");
		fs.writeFileSync(path, "a".repeat(1_000_000));
		const tail = readLogTail(path, 1000, 10);
		assert.equal(tail.text.length, 1000);
		assert.equal(tail.truncated, true);
		assert.equal(tail.totalBytes, 1_000_000);
	});

	test("tail drops the partial first line and limits lines", () => {
		const dir = tempDir();
		const path = join(dir, "x.log");
		fs.writeFileSync(path, Array.from({ length: 100 }, (_, i) => `line${i}`).join("\n") + "\n");
		const tail = readLogTail(path, 30, 100);
		assert.ok(!tail.text.startsWith("ine"));
		assert.ok(tail.text.endsWith("line99"));
		assert.equal(readLogTail(path, 100_000, 3).text, "line97\nline98\nline99");
	});

	test("cleanOutput strips escapes and applies carriage returns", () => {
		assert.equal(cleanOutput("\x1b[31mred\x1b[0m\n10%\r50%\r100%\nok\r\n"), "red\n100%\nok\n");
		assert.equal(cleanOutput("a\x00b\x07c"), "abc");
	});

	test("createSessionLogDir refuses a symlinked base and prune keeps the live dir", () => {
		const root = tempDir();
		const real = join(root, "real");
		fs.mkdirSync(real);
		fs.symlinkSync(real, join(root, "link"));
		assert.throws(() => createSessionLogDir(join(root, "link"), "s1"), /not a directory/);
		const base = join(root, "base");
		const live = createSessionLogDir(base, "s/../1");
		assert.equal(fs.statSync(base).mode & 0o777, 0o700);
		assert.ok(!live.includes("/../"));
		const old = createSessionLogDir(base, "old");
		const past = new Date(Date.now() - 10 * 86400_000);
		fs.utimesSync(old, past, past);
		fs.utimesSync(live, past, past);
		pruneSessionLogDirs(base, 86400_000, live);
		assert.equal(fs.existsSync(old), false);
		assert.equal(fs.existsSync(live), true);
	});
});

describe("stop without /proc", () => {
	test("stop signals when /proc is missing (macOS/BSD/proc-less Linux)", async () => {
		const { mgr } = make();
		const job = mgr.start({ command: "sleep 30", cwd: "/", background: true });
		await job.launched;
		const pid = job.pid!;
		assert.ok(alive(pid));
		const origPidStat = procHooks.readPidStat;
		const origSelfStat = procHooks.readSelfStat;
		const missingProc = (): string => {
			const err = new Error("ENOENT: no such file or directory, open '/proc/1/stat'") as NodeJS.ErrnoException;
			err.code = "ENOENT";
			throw err;
		};
		procHooks.readPidStat = missingProc;
		procHooks.readSelfStat = missingProc;
		try {
			// Every /proc read reports ENOENT, as on a system without /proc: the stop
			// must still signal instead of mistaking the leader for exited.
			const info = await mgr.requestStop(job, "stopped");
			assert.equal(info.status, "stopped");
			await waitFor(() => !alive(pid), 3000, "process to die after stop without /proc");
		} finally {
			procHooks.readPidStat = origPidStat;
			procHooks.readSelfStat = origSelfStat;
		}
	});
});

describe("readiness settle scan", () => {
	test("output before the first tick still fires the ready notice", async () => {
		const fired: Job[] = [];
		const { mgr } = make({ onReady: (job) => fired.push(job) });
		const job = mgr.start({ command: "echo READY-FLUSH", cwd: "/", background: true, readyPattern: "READY-FLUSH" });
		const info = await job.done;
		assert.equal(info.status, "exited");
		assert.equal(job.readyFired, true);
		assert.equal(job.readyLine, "READY-FLUSH");
		assert.deepEqual(fired, [job]);
	});

	test("an unterminated final line matches at settle", async () => {
		const fired: Job[] = [];
		const { mgr } = make({ onReady: (job) => fired.push(job) });
		const job = mgr.start({ command: "printf 'READY-NO-NL'", cwd: "/", background: true, readyPattern: "READY-NO-NL" });
		await job.done;
		assert.equal(job.readyFired, true);
		assert.equal(job.readyLine, "READY-NO-NL");
		assert.deepEqual(fired, [job]);
	});
});

describe("event log branches", () => {
	test("unset log disables events without affecting jobs", async () => {
		const { mgr } = make();
		const job = mgr.start({ command: "echo no-log", cwd: "/" });
		const info = await job.done;
		assert.equal(info.status, "exited");
		assert.equal(info.exitCode, 0);
	});

	test("blank PI_ASYNC_BASH_EVENT_LOG resolves to no log", () => {
		assert.equal(resolveConfig(undefined, "tui", {}).eventLogPath, undefined);
		assert.equal(resolveConfig(undefined, "tui", { PI_ASYNC_BASH_EVENT_LOG: "   " }).eventLogPath, undefined);
		const logPath = join(tempDir(), "events.jsonl");
		assert.equal(resolveConfig(undefined, "tui", { PI_ASYNC_BASH_EVENT_LOG: logPath }).eventLogPath, logPath);
	});

	test("log I/O failure never breaks jobs", async () => {
		const dir = tempDir();
		const blocker = join(dir, "blocker");
		fs.writeFileSync(blocker, "not a directory");
		const { mgr } = make({ eventLogPath: join(blocker, "events.jsonl") });
		let warnings = 0;
		const origWrite = process.stderr.write;
		// biome-ignore lint/suspicious/noExplicitAny: test-only stderr probe
		(process.stderr as any).write = (...args: any[]) => {
			warnings++;
			// biome-ignore lint/suspicious/noExplicitAny: test-only stderr probe
			return (origWrite as (...a: any[]) => boolean).apply(process.stderr, args);
		};
		try {
			const job = mgr.start({ command: "echo resilient", cwd: "/" });
			const info = await job.done;
			assert.equal(info.status, "exited");
			assert.equal(info.exitCode, 0);
			assert.equal(mgr.tail(job, 1000, 10).text, "resilient");
			assert.ok(warnings >= 2, `expected started+settled warnings, got ${warnings}`);
		} finally {
			// biome-ignore lint/suspicious/noExplicitAny: test-only stderr probe
			(process.stderr as any).write = origWrite;
		}
	});
});
describe("readiness line matcher", () => {
	test("fires once, on the second line only", () => {
		// lines ["starting", "listening on :3000"] with pattern "listening on :\\d+" fires once, on the second line only
		const matcher = new ReadyLineMatcher(compileReadyPattern("listening on :\\d+"));
		const fired: number[] = [];
		fired.push(...matcher.push("starting\n"));
		fired.push(...matcher.push("listening on :3000\n"));
		fired.push(...matcher.push("listening on :3001\n"));
		assert.deepStrictEqual(fired, [1]);
	});

	test("a line split across chunks matches once when it completes", () => {
		const matcher = new ReadyLineMatcher(compileReadyPattern("ready on :\\d+"));
		const fired: number[] = [];
		fired.push(...matcher.push("booting\nready on :51"));
		fired.push(...matcher.push("73\n"));
		assert.deepStrictEqual(fired, [1]);
		assert.equal(matcher.matchedLine, "ready on :5173");
	});

	test("invalid regex is rejected with the pattern echoed back", () => {
		assert.throws(() => compileReadyPattern("listening on :("), /Invalid ready_pattern.*"listening on :\("/);
		assert.throws(() => compileReadyPattern(""), /Invalid ready_pattern/);
	});
});
