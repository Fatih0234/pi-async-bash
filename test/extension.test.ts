import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import * as fs from "node:fs";
import { dirname, join } from "node:path";
import { after, describe, test } from "node:test";
import { __notifyPolicyForTest, NOTIFY_CUSTOM_TYPE, parseForegroundMs, resolveConfig, resolveForegroundMs, resolveTimeoutMs } from "../src/index.ts";
import { FakeRuntime, text } from "./fake-pi.ts";
import { alive, sleep, tempDir, waitFor } from "./helpers.ts";

process.env.PI_ASYNC_BASH_LOG_DIR = tempDir("pab-ext-logs-");
delete process.env.PI_ASYNC_BASH_FOREGROUND_MS;

const runtimes: FakeRuntime[] = [];
function runtime(sessionId = "session-a", budget = "300"): FakeRuntime {
	const rt = new FakeRuntime(sessionId, { "bash-foreground-ms": budget });
	runtimes.push(rt);
	return rt;
}
after(async () => {
	for (const rt of runtimes) await rt.emit("session_shutdown", { reason: "quit" });
});

const noticeOf = (rt: FakeRuntime) => rt.sent.filter((s) => s.message.customType === NOTIFY_CUSTOM_TYPE);

describe("registration and config", () => {
	test("registers only bash and bash_job (no Codex tools)", () => {
		const rt = runtime();
		assert.deepEqual([...rt.tools.keys()].sort(), ["bash", "bash_job"]);
		for (const name of ["exec", "wait", "exec_command", "write_stdin", "apply_patch"]) assert.equal(rt.tools.has(name), false);
		const params = (rt.tools.get("bash") as any).parameters;
		assert.deepEqual(Object.keys(params.properties).sort(), [
			"command",
			"description",
			"notify",
			"ready_pattern",
			"ready_timeout_s",
			"run_in_background",
			"task_id",
			"timeout",
		]);
		assert.deepEqual(params.required, ["command"]);
	});

	test("timeout validation in seconds", () => {
		assert.equal(resolveTimeoutMs(undefined), undefined);
		assert.equal(resolveTimeoutMs(1.5), 1500);
		for (const bad of [0, -2, Number.NaN, Number.POSITIVE_INFINITY, 2_147_484]) assert.throws(() => resolveTimeoutMs(bad), /Invalid timeout/);
	});

	test("foreground budget is user configuration, off in one-shot modes", () => {
		assert.equal(parseForegroundMs("off"), null);
		assert.equal(parseForegroundMs("0"), null);
		assert.equal(parseForegroundMs("1500"), 1500);
		assert.throws(() => parseForegroundMs("2s"));
		assert.equal(resolveConfig(undefined, "tui", {}).foregroundMs, 10000);
		assert.equal(resolveConfig(undefined, "rpc", {}).foregroundMs, 10000);
		assert.equal(resolveConfig(undefined, "print", {}).foregroundMs, null);
		assert.equal(resolveConfig(undefined, "json", { PI_ASYNC_BASH_FOREGROUND_MS: "700" }).foregroundMs, 700);
		assert.equal(resolveConfig("900", "tui", { PI_ASYNC_BASH_FOREGROUND_MS: "700" }).foregroundMs, 900);
	});

	test("default foreground wait is 10 s with flag > env > settings precedence", () => {
		// default (no flag, no env, non-headless) resolves to 10000, not 2000
		assert.strictEqual(resolveForegroundMs({ mode: "tui", env: {}, flag: undefined }), 10000);
		assert.strictEqual(resolveConfig(undefined, "tui", {}).foregroundMs, 10000);
		// settings key sits between env and the default
		assert.strictEqual(resolveConfig(undefined, "tui", {}, { asyncBash: { foregroundMs: 5000 } }).foregroundMs, 5000);
		assert.strictEqual(resolveConfig(undefined, "rpc", {}, { asyncBash: { foregroundMs: "4000" } }).foregroundMs, 4000);
		assert.strictEqual(resolveConfig(undefined, "tui", {}, { asyncBash: { foregroundMs: "off" } }).foregroundMs, null);
		assert.strictEqual(resolveConfig(undefined, "tui", {}, { asyncBash: { foregroundMs: 0 } }).foregroundMs, null);
		// env beats settings, flag beats both
		assert.strictEqual(
			resolveConfig(undefined, "tui", { PI_ASYNC_BASH_FOREGROUND_MS: "700" }, { asyncBash: { foregroundMs: 5000 } }).foregroundMs,
			700,
		);
		assert.strictEqual(resolveConfig("900", "tui", {}, { asyncBash: { foregroundMs: 5000 } }).foregroundMs, 900);
		assert.strictEqual(
			resolveConfig("900", "tui", { PI_ASYNC_BASH_FOREGROUND_MS: "700" }, { asyncBash: { foregroundMs: 5000 } }).foregroundMs,
			900,
		);
		// an explicit setting overrides the one-shot off default, like env does
		assert.strictEqual(resolveConfig(undefined, "print", {}, { asyncBash: { foregroundMs: 5000 } }).foregroundMs, 5000);
		assert.strictEqual(resolveForegroundMs({ mode: "print", env: {}, flag: undefined }), null);
		// invalid settings values throw
		assert.throws(() => resolveConfig(undefined, "tui", {}, { asyncBash: { foregroundMs: "2s" } }), /asyncBash\.foregroundMs/);
		assert.throws(() => resolveConfig(undefined, "tui", {}, { asyncBash: { foregroundMs: -5 } }), /asyncBash\.foregroundMs/);
		assert.throws(() => resolveConfig(undefined, "tui", {}, { asyncBash: { foregroundMs: true } }), /asyncBash\.foregroundMs/);
	});

	test("an explicit off keeps precedence over defaults and env", () => {
		assert.equal(resolveConfig("off", "tui", {}).foregroundMs, null);
		assert.equal(resolveConfig("0", "rpc", {}).foregroundMs, null);
		assert.equal(resolveConfig("off", "tui", { PI_ASYNC_BASH_FOREGROUND_MS: "700" }).foregroundMs, null);
		assert.equal(resolveConfig(undefined, "tui", { PI_ASYNC_BASH_FOREGROUND_MS: "off" }).foregroundMs, null);
		assert.equal(resolveConfig(undefined, "tui", { PI_ASYNC_BASH_FOREGROUND_MS: "0" }).foregroundMs, null);
		assert.equal(resolveConfig("900", "print", { PI_ASYNC_BASH_FOREGROUND_MS: "off" }).foregroundMs, 900);
		assert.equal(resolveConfig("", "tui", { PI_ASYNC_BASH_FOREGROUND_MS: "off" }).foregroundMs, null);
	});

	test("the off flag makes bash wait for completion", async () => {
		const rt = runtime("off-flag", "off");
		const result = await rt.bash({ command: "sleep 0.6; echo waited-fully" });
		assert.equal(result.details.background, undefined);
		assert.equal(text(result), "waited-fully");
	});

	test("settings off makes bash wait for completion", async () => {
		const rt = new FakeRuntime("settings-off");
		runtimes.push(rt);
		rt.settings = { asyncBash: { foregroundMs: "off" } };
		await rt.emit("session_start", { reason: "start" });
		const result = await rt.bash({ command: "sleep 0.6; echo waited-via-settings" });
		assert.equal(result.details.background, undefined);
		assert.equal(text(result), "waited-via-settings");
	});

	test("PI_ASYNC_BASH_EVENT_LOG wires through resolveConfig into the manager", async () => {
		const dir = tempDir("pab-evt-");
		const logPath = join(dir, "events.jsonl");
		process.env.PI_ASYNC_BASH_EVENT_LOG = logPath;
		try {
			assert.equal(resolveConfig(undefined, "tui", process.env).eventLogPath, logPath);
			const rt = runtime("evt-log");
			const result = await rt.bash({ command: "echo wired" });
			assert.equal(text(result), "wired");
			const lines = fs.readFileSync(logPath, "utf8").split("\n").filter((line) => line !== "");
			assert.equal(lines.length, 2);
			const started = JSON.parse(lines[0]!);
			const settled = JSON.parse(lines[1]!);
			assert.equal(started.event, "started");
			assert.equal(settled.event, "settled");
			assert.equal(settled.job_id, started.job_id);
			assert.equal(settled.status, "exited");
			assert.equal(settled.exitCode, 0);
		} finally {
			delete process.env.PI_ASYNC_BASH_EVENT_LOG;
		}
	});
});

describe("bash tool", () => {
	test("quick foreground exit returns output; log of a short run is removed", async () => {
		const rt = runtime();
		const result = await rt.bash({ command: "echo hi; echo there >&2" });
		assert.equal(text(result), "hi\nthere");
		assert.equal(result.isError, undefined);
		assert.equal(result.structuredContent.exit_code, 0);
		assert.equal(result.structuredContent.status, "exited");
		assert.equal((await rt.job({ action: "list" })).content[0].text, "No background jobs in this session.");
	});

	test("nonzero exit is a tool error", async () => {
		const rt = runtime();
		const result = await rt.bash({ command: "echo nope; exit 4" });
		assert.equal(result.isError, true);
		assert.match(text(result), /nope\n\nCommand exited with code 4/);
	});

	test("session env and shellCommandPrefix match the builtin", async () => {
		const rt = runtime();
		rt.settings = { shellCommandPrefix: "export PAB_PREFIX=yes" };
		const result = await rt.bash({ command: 'echo "$PAB_PREFIX $PI_SESSION_ID $PI_MODEL"' });
		assert.equal(text(result), "yes session-a m1");
	});

	test("large output is truncated with a retained full log", async () => {
		const rt = runtime();
		const result = await rt.bash({ command: "seq 1 3000" });
		const out = text(result);
		assert.match(out, /^1001\n/);
		assert.match(out, /3000\n\n\[Output truncated/);
		assert.ok(fs.existsSync(result.details.logPath));
		assert.equal(result.structuredContent.full_output_path, result.details.logPath);
	});

	test("automatic handoff after the budget, then one idle notification", async () => {
		const rt = runtime();
		const started = Date.now();
		const result = await rt.bash({ command: "echo early; sleep 0.7; echo late-done" });
		const elapsed = Date.now() - started;
		assert.ok(elapsed >= 280 && elapsed < 900, `handoff after ${elapsed} ms`);
		assert.equal(result.isError, undefined);
		assert.equal(result.details.background, true);
		assert.equal(result.structuredContent.status, "running");
		assert.match(text(result), /NOT completed/);
		assert.match(text(result), /early/);
		const jobId = result.details.jobId;
		await waitFor(() => noticeOf(rt).length === 1, 3000, "notification");
		await sleep(300);
		const notices = noticeOf(rt);
		assert.equal(notices.length, 1);
		assert.equal(notices[0]!.options?.triggerTurn, true);
		const body = notices[0]!.message.content;
		assert.match(body, new RegExp(`${jobId} exited with code 0`));
		assert.match(body, /late-done/);
		assert.match(body, new RegExp(result.details.logPath.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")));
	});

	test("output activity does not extend the foreground budget", async () => {
		const rt = runtime();
		const started = Date.now();
		const updates: unknown[] = [];
		const result = await rt.bash({ command: "for i in $(seq 1 100); do echo $i; sleep 0.02; done" }, undefined, (u: unknown) =>
			updates.push(u),
		);
		const elapsed = Date.now() - started;
		assert.equal(result.details.background, true);
		assert.ok(elapsed < 900, `returned after ${elapsed} ms`);
		await rt.job({ action: "stop", job_id: result.details.jobId });
	});

	test("explicit background returns after launch", async () => {
		const rt = runtime();
		const started = Date.now();
		const result = await rt.bash({ command: "sleep 0.3; echo bg-done", run_in_background: true });
		assert.ok(Date.now() - started < 250);
		assert.equal(result.details.background, true);
		assert.match(text(result), /started in the background/);
		await waitFor(() => noticeOf(rt).length === 1, 3000, "notification");
		assert.match(noticeOf(rt)[0]!.message.content, /bg-done/);
	});

	test("hard timeout still applies after handoff", async () => {
		const rt = runtime();
		const result = await rt.bash({ command: "sleep 10", timeout: 0.8 });
		assert.equal(result.details.background, true);
		assert.match(text(result), /killed if still running 0.8 seconds/);
		await waitFor(() => noticeOf(rt).length === 1, 3000, "timeout notification");
		assert.match(noticeOf(rt)[0]!.message.content, /killed after its 0.8-second timeout/);
	});

	test("a timeout is reported even when the command exits 0 on SIGTERM", async () => {
		const cmd = "trap 'exit 0' TERM; echo READY; while :; do sleep 0.01; done";
		const fg = await runtime("t1", "off").bash({ command: cmd, timeout: 0.4 });
		assert.equal(fg.isError, true);
		assert.match(text(fg), /Command timed out after 0.4 seconds/);
		assert.equal(fg.structuredContent.status, "timed_out");
		const rt = runtime("t2", "150");
		const bg = await rt.bash({ command: cmd, timeout: 0.6 });
		assert.equal(bg.details.background, true);
		await waitFor(() => noticeOf(rt).length === 1, 3000, "timeout notice");
		assert.match(noticeOf(rt)[0]!.message.content, /killed after its 0.6-second timeout/);
		assert.doesNotMatch(noticeOf(rt)[0]!.message.content, /exited with code 0/);
	});

	test("repeated truncated foreground results stay inside the session log budget", async () => {
		process.env.PI_ASYNC_BASH_SESSION_LOG_MB = "1";
		process.env.PI_ASYNC_BASH_MAX_LOG_MB = "1";
		try {
			const rt = runtime("budget");
			let dir = "";
			let lastLog = "";
			for (let i = 0; i < 6; i++) {
				const result = await rt.bash({ command: "seq 1 60000" }); // ~350 KB, truncated to 2000 lines
				assert.match(text(result), /Output truncated/);
				lastLog = result.details.logPath;
				dir = dirname(lastLog);
			}
			const sizes = fs.readdirSync(dir).map((f) => fs.statSync(join(dir, f)).size);
			const total = sizes.reduce((a, b) => a + b, 0);
			assert.ok(total <= 1024 * 1024, `retained ${total} bytes in ${sizes.length} logs`);
			assert.ok(sizes.length < 6, "older truncated logs were evicted");
			assert.ok(fs.existsSync(lastLog), "the newest referenced log is kept");
			assert.equal(text(await rt.job({ action: "list" })), "No background jobs in this session.");
		} finally {
			delete process.env.PI_ASYNC_BASH_SESSION_LOG_MB;
			delete process.env.PI_ASYNC_BASH_MAX_LOG_MB;
		}
	});

	test("foreground timeout is a tool error", async () => {
		const rt = runtime("s", "off");
		const result = await rt.bash({ command: "echo x; sleep 10", timeout: 0.3 });
		assert.equal(result.isError, true);
		assert.match(text(result), /x\n\nCommand timed out after 0.3 seconds/);
	});

	test("aborting a foreground call kills its process tree", async () => {
		const rt = runtime("s", "5000");
		const ac = new AbortController();
		let latest = "";
		const pending = rt.bash({ command: "sleep 30 & echo $!; wait" }, ac.signal, (u: any) => {
			latest = u.content.map((c: any) => c.text).join("");
		});
		await waitFor(() => /^\d+$/m.test(latest), 3000, "descendant pid in progress update");
		const pid = Number(latest.trim());
		assert.ok(alive(pid));
		ac.abort();
		await assert.rejects(pending, /Command aborted/);
		await waitFor(() => !alive(pid), 3000, "descendant to die");
	});

	test("abort after handoff does not stop the background job", async () => {
		const rt = runtime();
		const ac = new AbortController();
		const result = await rt.bash({ command: "sleep 30" }, ac.signal);
		assert.equal(result.details.background, true);
		ac.abort();
		await sleep(100);
		const status = await rt.job({ action: "status", job_id: result.details.jobId });
		assert.match(text(status), /still running/);
		const pid = status.details.pid as number;
		assert.ok(alive(pid));
		const stopped = await rt.job({ action: "stop", job_id: result.details.jobId });
		assert.match(text(stopped), /was stopped/);
		await waitFor(() => !alive(pid), 3000, "stopped process to exit");
		await sleep(400);
		assert.equal(noticeOf(rt).length, 0, "explicitly stopped jobs are not re-announced");
	});
});

describe("delivery", () => {
	test("busy agent: delivered once at turn_end through a boundary entry", async () => {
		const rt = runtime();
		rt.idle = false;
		const result = await rt.bash({ command: "sleep 0.5; echo busy-done" });
		assert.equal(result.details.background, true);
		await sleep(700);
		assert.equal(noticeOf(rt).length, 0, "no direct send while busy");
		const [first] = await rt.emit("turn_end", { outcome: "completed" });
		assert.equal(first.continue, true);
		assert.equal(first.entries.length, 1);
		assert.equal(first.entries[0].type, "custom_message");
		assert.equal(first.entries[0].customType, NOTIFY_CUSTOM_TYPE);
		assert.match(first.entries[0].content, /busy-done/);
		const [second] = await rt.emit("turn_end", { outcome: "completed" });
		assert.equal(second, undefined);
		rt.idle = true;
		await sleep(1200);
		assert.equal(noticeOf(rt).length, 0, "no duplicate after the agent becomes idle");
	});

	test("aborted run: notice is recorded without triggering a turn", async () => {
		const rt = runtime();
		rt.idle = false;
		await rt.bash({ command: "sleep 0.4", run_in_background: true });
		await sleep(600);
		assert.deepEqual(await rt.emit("turn_end", { outcome: "aborted" }), [undefined]);
		await rt.emit("agent_settled", { aborted: true });
		assert.equal(noticeOf(rt).length, 1);
		assert.equal(noticeOf(rt)[0]!.options?.triggerTurn, false);
	});

	test("after a user abort, later completions are recorded without starting a turn until a new run", async () => {
		const rt = runtime();
		await rt.emit("agent_start");
		await rt.emit("agent_settled", { aborted: true });
		await rt.bash({ command: "sleep 0.4; echo after-abort", run_in_background: true });
		await waitFor(() => noticeOf(rt).length === 1, 3000, "late notice");
		assert.equal(noticeOf(rt)[0]!.options?.triggerTurn, false);
		assert.match(noticeOf(rt)[0]!.message.content, /after-abort/);
		await rt.emit("agent_start");
		await rt.emit("agent_settled", { aborted: false });
		await rt.bash({ command: "sleep 0.2", run_in_background: true });
		await waitFor(() => noticeOf(rt).length === 2, 3000, "second notice");
		assert.equal(noticeOf(rt)[1]!.options?.triggerTurn, true);
	});

	test("bash_job wait delivers the result and suppresses the notification", async () => {
		const rt = runtime();
		const result = await rt.bash({ command: "sleep 0.5; echo waited" });
		const waited = await rt.job({ action: "wait", job_id: result.details.jobId, timeout: 5 });
		assert.match(text(waited), /exited with code 0/);
		assert.match(text(waited), /waited/);
		await sleep(500);
		assert.equal(noticeOf(rt).length, 0);
	});

	test("bounded wait returns while the job keeps running", async () => {
		const rt = runtime();
		const result = await rt.bash({ command: "sleep 30", run_in_background: true });
		const started = Date.now();
		const waited = await rt.job({ action: "wait", job_id: result.details.jobId, timeout: 0.3 });
		assert.ok(Date.now() - started < 1000);
		assert.match(text(waited), /still running/);
		await rt.job({ action: "stop", job_id: result.details.jobId });
		await assert.rejects(rt.job({ action: "status", job_id: "job-404" }), /Unknown job/);
		await assert.rejects(rt.job({ action: "wait" }), /job_id is required/);
	});

	test("simultaneous completions are coalesced into one notice", async () => {
		const rt = runtime();
		await rt.bash({ command: "sleep 0.3; echo one", run_in_background: true });
		await rt.bash({ command: "sleep 0.3; echo two", run_in_background: true });
		await waitFor(() => noticeOf(rt).length >= 1, 3000, "notice");
		await sleep(300);
		assert.equal(noticeOf(rt).length, 1);
		assert.match(noticeOf(rt)[0]!.message.content, /2 bash background jobs finished/);
	});

	test("handoff/exit race: exactly one of foreground result or notification", async () => {
		const rt = runtime("race", "150");
		let notified = 0;
		let foreground = 0;
		for (let i = 0; i < 12; i++) {
			const before = noticeOf(rt).length;
			const result = await rt.bash({ command: `sleep 0.1${i % 10}` });
			if (result.details.background) {
				await waitFor(() => noticeOf(rt).length === before + 1, 3000, "race notification");
				notified++;
			} else {
				foreground++;
			}
			await sleep(250);
			assert.equal(noticeOf(rt).length, before + (result.details.background ? 1 : 0), `iteration ${i}`);
		}
		assert.equal(notified + foreground, 12);
	});
});

describe("session lifecycle", () => {
	test("session_shutdown stops jobs and nothing is delivered afterwards", async () => {
		const rt = runtime();
		const result = await rt.bash({ command: "sleep 30", run_in_background: true });
		const status = await rt.job({ action: "status", job_id: result.details.jobId });
		const pid = status.details.pid as number;
		await rt.emit("session_shutdown", { reason: "new" });
		await waitFor(() => !alive(pid), 3000, "job to die on shutdown");
		await sleep(400);
		assert.equal(rt.sent.length, 0);
		await assert.rejects(rt.bash({ command: "true" }), /shut down/);
	});

	test("reload: the old runtime's late completion never reaches the new runtime", async () => {
		const oldRt = runtime("same-session");
		await oldRt.bash({ command: "sleep 0.3", run_in_background: true });
		await oldRt.emit("session_shutdown", { reason: "reload" });
		oldRt.stale = true;
		const newRt = runtime("same-session");
		await newRt.emit("session_start", { reason: "reload" });
		await sleep(800);
		assert.equal(oldRt.sent.length, 0);
		assert.equal(newRt.sent.length, 0);
		assert.equal((await newRt.job({ action: "list" })).content[0].text, "No background jobs in this session.");
	});

	test("session id change inside one runtime retires the old jobs", async () => {
		const rt = runtime("first");
		const result = await rt.bash({ command: "sleep 30", run_in_background: true });
		const pid = (await rt.job({ action: "status", job_id: result.details.jobId })).details.pid as number;
		rt.sessionId = "second";
		await rt.bash({ command: "true" });
		await waitFor(() => !alive(pid), 3000, "old job to stop");
		await assert.rejects(rt.job({ action: "status", job_id: result.details.jobId }), /Unknown job/);
		await sleep(300);
		assert.equal(noticeOf(rt).length, 0);
	});

	test("job controls are session-guarded even when bash_job is the first call in a new session", async () => {
		const rt = runtime("old-session");
		const result = await rt.bash({ command: "sleep 30", run_in_background: true });
		const pid = (await rt.job({ action: "status", job_id: result.details.jobId })).details.pid as number;
		try {
			rt.sessionId = "new-session";
			await assert.rejects(rt.job({ action: "status", job_id: result.details.jobId }), /Unknown job/);
			await assert.rejects(rt.job({ action: "stop", job_id: result.details.jobId }), /Unknown job/);
			assert.equal(text(await rt.job({ action: "list" })), "No background jobs in this session.");
			await waitFor(() => !alive(pid), 3000, "old-session job to stop");
			// The turn boundary of the new session delivers nothing from the old one.
			assert.deepEqual(await rt.emit("turn_end", { outcome: "completed" }), [undefined]);
			await sleep(300);
			assert.equal(noticeOf(rt).length, 0);
		} finally {
			if (alive(pid)) process.kill(pid, "SIGKILL");
		}
	});

	test("stale sendMessage is swallowed", async () => {
		const rt = runtime();
		rt.stale = true;
		await rt.bash({ command: "true", run_in_background: true });
		await sleep(400);
		assert.equal(rt.sent.length, 0);
	});

	test("same-session model switch keeps job controls usable", async () => {
		const rt = runtime();
		const result = await rt.bash({ command: "sleep 30", run_in_background: true });
		rt.model = { provider: "p2", id: "m2" };
		await rt.emit("model_select", { model: rt.model, source: "set" });
		assert.match(text(await rt.job({ action: "status", job_id: result.details.jobId })), /still running/);
		assert.match(text(await rt.job({ action: "list" })), new RegExp(result.details.jobId));
		assert.equal(text(await rt.bash({ command: 'echo "$PI_PROVIDER/$PI_MODEL"' })), "p2/m2");
		assert.match(text(await rt.job({ action: "stop", job_id: result.details.jobId })), /was stopped/);
	});
});

describe("notify policy", () => {
	test("silent job completion enqueues no wake notice", async () => {
		const rt = runtime("silent-1");
		const result = await rt.bash({ command: "sleep 0.5; echo silent-done", notify: "silent" });
		assert.equal(result.details.background, true);
		assert.match(text(result), /NOT be notified/);
		const jobId = result.details.jobId as string;
		await sleep(1200);
		assert.strictEqual(noticeOf(rt).length, 0);
		assert.deepEqual(await rt.emit("turn_end", { outcome: "completed" }), [undefined]);
		const status = await rt.job({ action: "status", job_id: jobId });
		assert.match(text(status), /silent-done/);
		assert.match(text(status), /Notify: silent/);
		assert.equal((status.details as any).notify, "silent");
		const list = await rt.job({ action: "list" });
		assert.match(text(list), new RegExp(`${jobId}.*notify:silent`));
	});

	test("default and explicit wake still notify", async () => {
		const rt = runtime("wake-1");
		const result = await rt.bash({ command: "sleep 0.5; echo wake-done", notify: "wake" });
		assert.equal(result.details.background, true);
		assert.match(text(result), /will be notified/);
		await waitFor(() => noticeOf(rt).length === 1, 3000, "wake notification");
		assert.equal(noticeOf(rt)[0]!.options?.triggerTurn, true);
		assert.match(noticeOf(rt)[0]!.message.content, /wake-done/);
	});

	test("silent explicit-background job stays visible via wait without ever waking", async () => {
		const rt = runtime("silent-2");
		const result = await rt.bash({ command: "sleep 0.4; echo silent-bg-done", run_in_background: true, notify: "silent" });
		assert.equal(result.details.background, true);
		const jobId = result.details.jobId as string;
		const running = await rt.job({ action: "status", job_id: jobId });
		assert.match(text(running), /Notify: silent/);
		const waited = await rt.job({ action: "wait", job_id: jobId, timeout: 5 });
		assert.match(text(waited), /silent-bg-done/);
		assert.match(text(waited), /Notify: silent/);
		await sleep(600);
		assert.strictEqual(noticeOf(rt).length, 0);
		assert.deepEqual(await rt.emit("turn_end", { outcome: "completed" }), [undefined]);
	});

	test("invalid notify is rejected", async () => {
		const rt = runtime("silent-bad");
		await assert.rejects(rt.bash({ command: "true", notify: "loud" }), /Invalid notify/);
	});
});

describe("notifyPolicy hygiene (issue #3)", () => {
	test("settled entries survive passive status polls and silent still never wakes", async () => {
		const rt = runtime("hygiene-settle");
		const result = await rt.bash({ command: "sleep 0.5; echo settle-hygiene", notify: "silent" });
		assert.equal(result.details.background, true);
		const jobId = result.details.jobId as string;
		await sleep(1200);
		// status merely observes: repeated polls keep reporting the policy truthfully.
		const first = await rt.job({ action: "status", job_id: jobId });
		assert.match(text(first), /settle-hygiene/);
		assert.match(text(first), /Notify: silent/);
		const second = await rt.job({ action: "status", job_id: jobId });
		assert.match(text(second), /Notify: silent/);
		assert.match(text(await rt.job({ action: "list" })), new RegExp(`${jobId}.*notify:silent`));
		assert.deepEqual(await rt.emit("turn_end", { outcome: "completed" }), [undefined]);
		assert.strictEqual(noticeOf(rt).length, 0, "silent job still never wakes");
	});

	test("stop keeps the entry so later status stays truthful", async () => {
		const rt = runtime("hygiene-stop");
		const result = await rt.bash({ command: "sleep 30", run_in_background: true, notify: "silent" });
		const jobId = result.details.jobId as string;
		// While running the entry must stay: delivery still depends on it.
		assert.match(text(await rt.job({ action: "status", job_id: jobId })), /Notify: silent/);
		const stopped = await rt.job({ action: "stop", job_id: jobId });
		assert.match(text(stopped), /was stopped/);
		assert.match(text(stopped), /Notify: silent/);
		// The entry lives until retention eviction (or take): a re-query still reports the real policy.
		const after = await rt.job({ action: "status", job_id: jobId });
		assert.match(text(after), /Notify: silent/, "stopped silent job still reports its real policy");
		await sleep(400);
		assert.strictEqual(noticeOf(rt).length, 0, "stopped job is not re-announced");
	});

	test("wait keeps the entry so later status stays truthful", async () => {
		const rt = runtime("hygiene-wait");
		const result = await rt.bash({ command: "sleep 0.4; echo wait-hygiene", run_in_background: true, notify: "silent" });
		const jobId = result.details.jobId as string;
		assert.match(text(await rt.job({ action: "status", job_id: jobId })), /Notify: silent/);
		const waited = await rt.job({ action: "wait", job_id: jobId, timeout: 5 });
		assert.match(text(waited), /wait-hygiene/);
		assert.match(text(waited), /Notify: silent/);
		// The entry lives until retention eviction (or take): a re-query still reports the real policy.
		const after = await rt.job({ action: "status", job_id: jobId });
		assert.match(text(after), /Notify: silent/, "waited silent job still reports its real policy");
		assert.strictEqual(noticeOf(rt).length, 0, "silent job still never wakes");
	});

	test("retention eviction still forgets jobs and list reconciles hygiene", async () => {
		const rt = runtime("hygiene-evict");
		const ids: string[] = [];
		for (let i = 0; i < 52; i++) {
			const result = await rt.bash({ command: "true", run_in_background: true, notify: "silent" });
			ids.push(result.details.jobId as string);
			// Settle each launch: retention counts finished jobs and running slots stay free.
			await rt.job({ action: "wait", job_id: result.details.jobId, timeout: 5 });
		}
		// Retention keeps only the newest 50 finished jobs; the oldest are evicted.
		const deadline = Date.now() + 5000;
		for (;;) {
			let evicted = false;
			try {
				await rt.job({ action: "status", job_id: ids[0]! });
			} catch (err) {
				assert.match((err as Error).message, /Unknown job/);
				evicted = true;
			}
			if (evicted) break;
			if (Date.now() > deadline) throw new Error("timed out waiting for oldest job to be evicted");
			await sleep(20);
		}
		const list = await rt.job({ action: "list" });
		assert.ok((list.details?.jobs?.length ?? 0) <= 50, "retention still bounds visible jobs");
		await assert.rejects(rt.job({ action: "status", job_id: ids[0]! }), /Unknown job/);
		assert.match(text(await rt.job({ action: "status", job_id: ids[ids.length - 1]! })), /exited with code 0/);
	});

	test("wake delivery is unchanged when the entry is spent at take", async () => {
		const rt = runtime("hygiene-wake");
		const result = await rt.bash({ command: "sleep 0.5; echo wake-hygiene", notify: "wake" });
		assert.equal(result.details.background, true);
		await waitFor(() => noticeOf(rt).length === 1, 3000, "wake notification");
		await sleep(300);
		assert.equal(noticeOf(rt).length, 1, "exactly one completion notice");
		assert.match(noticeOf(rt)[0]!.message.content, /wake-hygiene/);
		assert.match(text(await rt.job({ action: "status", job_id: result.details.jobId })), /Notify: wake/);
	});
});

describe("notifyPolicy eviction hook (issue #3 round 2)", () => {
	/** Poll until a retained job settles. Status merely observes, so entries stay put in every design. */
	async function waitSettled(rt: FakeRuntime, jobId: string, what: string): Promise<void> {
		const deadline = Date.now() + 15000;
		for (;;) {
			const s = text(await rt.job({ action: "status", job_id: jobId }));
			if (/exited with code 0/.test(s)) return;
			if (Date.now() > deadline) throw new Error(`timed out waiting for ${what} to settle: ${s}`);
			await sleep(20);
		}
	}

	/**
	 * Poll until the manager forgot this id. Eviction is oldest-first, so this id being
	 * unknown implies every older id is evicted too. Never probe the id under assertion:
	 * the Unknown path deletes the probed id's entry.
	 */
	async function waitEvicted(rt: FakeRuntime, jobId: string): Promise<void> {
		const deadline = Date.now() + 10000;
		for (;;) {
			try {
				await rt.job({ action: "status", job_id: jobId });
			} catch (err) {
				assert.match((err as Error).message, /Unknown job/);
				return;
			}
			if (Date.now() > deadline) throw new Error(`timed out waiting for ${jobId} to be evicted`);
			await sleep(20);
		}
	}

	test("status-only silent workload leaves no residue after eviction", async () => {
		const marker = __notifyPolicyForTest.length;
		const rt = runtime("hook-status-only");
		const policies = __notifyPolicyForTest[marker]!;
		const ids: string[] = [];
		for (let i = 0; i < 40; i++) {
			const result = await rt.bash({ command: "true", run_in_background: true, notify: "silent" });
			ids.push(result.details.jobId as string);
			// Settle each launch without spending its entry (no wait/list): retention
			// counts finished jobs and running slots stay free.
			await sleep(30);
		}
		await waitSettled(rt, ids[ids.length - 1]!, "newest of first batch");
		// Status merely observes: every retained job still reports its real policy.
		// (40 < 50, so no eviction can interleave with these polls.)
		for (const id of ids) assert.match(text(await rt.job({ action: "status", job_id: id })), /Notify: silent/);
		for (let i = 0; i < 15; i++) {
			const result = await rt.bash({ command: "true", run_in_background: true, notify: "silent" });
			ids.push(result.details.jobId as string);
			await sleep(30);
		}
		await waitSettled(rt, ids[ids.length - 1]!, "newest filler");
		await sleep(300);
		await waitEvicted(rt, ids[1]!);
		assert.equal(policies.has(ids[0]!), false, "status-polled entry dies at eviction");
		assert.ok(policies.size <= 50, `map stays bounded by the job cap (got ${policies.size})`);
		// Retained jobs stay truthful under eviction pressure.
		assert.match(text(await rt.job({ action: "status", job_id: ids[ids.length - 1]! })), /Notify: silent/);
		assert.strictEqual(noticeOf(rt).length, 0, "silent jobs never wake");
	});

	test("unobserved eviction cleans the map without any list call", async () => {
		const marker = __notifyPolicyForTest.length;
		const rt = runtime("hook-evict");
		const policies = __notifyPolicyForTest[marker]!;
		const ids: string[] = [];
		for (let i = 0; i < 55; i++) {
			const result = await rt.bash({ command: "true", run_in_background: true, notify: "silent" });
			ids.push(result.details.jobId as string);
			await sleep(30);
		}
		// Deliberately no list call anywhere in this test: the eviction hook, not list
		// reconciliation, must clean entries for jobs the manager forgot.
		await waitSettled(rt, ids[ids.length - 1]!, "newest filler");
		await sleep(300);
		await waitEvicted(rt, ids[1]!);
		assert.equal(policies.has(ids[0]!), false, "evicted silent entry dies without any list call");
		assert.ok(policies.size <= 50, `map stays bounded by the job cap (got ${policies.size})`);
		assert.strictEqual(noticeOf(rt).length, 0, "silent jobs never wake");
	});

	test("stop-then-status re-query stays truthful for silent jobs", async () => {
		const rt = runtime("hook-stop-truth");
		const result = await rt.bash({ command: "sleep 30", run_in_background: true, notify: "silent" });
		const jobId = result.details.jobId as string;
		assert.match(text(await rt.job({ action: "status", job_id: jobId })), /Notify: silent/);
		const stopped = await rt.job({ action: "stop", job_id: jobId });
		assert.match(text(stopped), /Notify: silent/);
		assert.match(
			text(await rt.job({ action: "status", job_id: jobId })),
			/Notify: silent/,
			"stopped silent job still reports its real policy",
		);
		await sleep(400);
		assert.strictEqual(noticeOf(rt).length, 0, "stopped silent job never wakes");
	});

	test("status-before-flush wake entry still dies at eviction", async () => {
		const marker = __notifyPolicyForTest.length;
		const rt = runtime("hook-flush");
		const policies = __notifyPolicyForTest[marker]!;
		rt.idle = false; // defer the idle flush so status observes the settle first
		const result = await rt.bash({ command: "sleep 0.4; echo flush-wake", run_in_background: true, notify: "wake" });
		const jobId = result.details.jobId as string;
		// Settle while busy: the completion sits in pending until status marks it delivered.
		const settleDeadline = Date.now() + 10000;
		for (;;) {
			const s = text(await rt.job({ action: "status", job_id: jobId }));
			if (/flush-wake/.test(s)) {
				assert.match(s, /Notify: wake/);
				break;
			}
			if (Date.now() > settleDeadline) throw new Error(`timed out waiting for wake job to settle: ${s}`);
			await sleep(20);
		}
		rt.idle = true;
		await sleep(1400); // the deferred flush fires, finds it delivered, drops it quietly
		assert.strictEqual(noticeOf(rt).length, 0, "status-observed completion is not re-announced");
		const fillers: string[] = [];
		for (let i = 0; i < 52; i++) {
			const f = await rt.bash({ command: "true", run_in_background: true, notify: "silent" });
			fillers.push(f.details.jobId as string);
			await sleep(30);
		}
		await waitSettled(rt, fillers[fillers.length - 1]!, "newest filler");
		await sleep(300);
		await waitEvicted(rt, fillers[1]!);
		assert.equal(policies.has(jobId), false, "status-observed wake entry dies at eviction");
		assert.ok(policies.size <= 50, `map stays bounded by the job cap (got ${policies.size})`);
		assert.strictEqual(noticeOf(rt).length, 0, "wake job observed via status never re-wakes");
	});
});


describe("readiness watch", () => {
	test("a matching line wakes once while the job keeps running", async () => {
		const rt = runtime("ready-1");
		const result = await rt.bash({
			command: "echo boot; sleep 0.6; echo ready on :5173; sleep 30",
			run_in_background: true,
			ready_pattern: "ready on :\\d+",
		});
		assert.equal(result.details.background, true);
		assert.match(text(result), /watches output for the pattern/);
		const jobId = result.details.jobId as string;
		await waitFor(() => noticeOf(rt).length === 1, 5000, "ready notification");
		await sleep(400);
		const notices = noticeOf(rt);
		assert.equal(notices.length, 1, "exactly one ready notice, no completion yet");
		assert.equal(notices[0]!.options?.triggerTurn, true);
		assert.match(notices[0]!.message.content, /\[bash job ready\]/);
		assert.match(notices[0]!.message.content, new RegExp(jobId));
		assert.match(notices[0]!.message.content, /ready on :5173/);
		assert.match(notices[0]!.message.content, /keeps running/);
		const status = await rt.job({ action: "status", job_id: jobId });
		assert.match(text(status), /still running/);
		const stopped = await rt.job({ action: "stop", job_id: jobId });
		assert.match(text(stopped), /was stopped/);
		await sleep(600);
		assert.equal(noticeOf(rt).length, 1, "stopping after ready sends no second notice");
	});

	test("busy agent: ready notice arrives through turn_end as its own entry", async () => {
		const rt = runtime("ready-2");
		rt.idle = false;
		await rt.bash({
			command: "sleep 0.4; echo ready on :5173; sleep 30",
			run_in_background: true,
			ready_pattern: "ready on :\\d+",
		});
		await sleep(1500);
		assert.equal(noticeOf(rt).length, 0, "no direct send while busy");
		const [first] = await rt.emit("turn_end", { outcome: "completed" });
		assert.equal(first.continue, true);
		assert.equal(first.entries.length, 1);
		assert.match(first.entries[0].content, /\[bash job ready\]/);
		assert.match(first.entries[0].content, /ready on :5173/);
		const [second] = await rt.emit("turn_end", { outcome: "completed" });
		assert.equal(second, undefined, "ready notice is delivered once");
		await rt.job({ action: "stop", job_id: (await rt.job({ action: "list" })).details.jobs[0].id });
	});

	test("silent jobs never send a ready notice", async () => {
		const rt = runtime("ready-3");
		const result = await rt.bash({
			command: "echo ready on :5173; sleep 30",
			run_in_background: true,
			notify: "silent",
			ready_pattern: "ready on :\\d+",
		});
		const jobId = result.details.jobId as string;
		await sleep(1500);
		assert.strictEqual(noticeOf(rt).length, 0);
		assert.deepEqual(await rt.emit("turn_end", { outcome: "completed" }), [undefined]);
		const status = await rt.job({ action: "status", job_id: jobId });
		assert.match(text(status), /still running/);
		await rt.job({ action: "stop", job_id: jobId });
		await sleep(600);
		assert.strictEqual(noticeOf(rt).length, 0);
	});

	test("invalid ready_pattern is a pre-spawn tool error and starts no job", async () => {
		const rt = runtime("ready-4");
		await assert.rejects(rt.bash({ command: "echo hi", ready_pattern: "ready (:" }), /Invalid ready_pattern.*ready \(\:/);
		assert.equal(text(await rt.job({ action: "list" })), "No background jobs in this session.");
	});

	test("invalid ready_timeout_s is rejected", async () => {
		const rt = runtime("ready-5");
		await assert.rejects(rt.bash({ command: "true", ready_pattern: "x", ready_timeout_s: 0 }), /Invalid ready_timeout_s/);
		await assert.rejects(
			rt.bash({ command: "true", ready_timeout_s: 5 }),
			/Invalid ready_timeout_s: requires ready_pattern/,
		);
		assert.equal(text(await rt.job({ action: "list" })), "No background jobs in this session.");
	});

	test("expired watch disarms: job keeps running with no notice, even for a later match", async () => {
		const rt = runtime("ready-expiry");
		const result = await rt.bash({
			command: "sleep 0.6; echo READY-AFTER-EXPIRY; sleep 30",
			run_in_background: true,
			ready_pattern: "READY-AFTER-EXPIRY",
			ready_timeout_s: 0.3,
		});
		const jobId = result.details.jobId as string;
		await sleep(1500);
		assert.strictEqual(noticeOf(rt).length, 0, "no ready notice after expiry");
		assert.deepEqual(await rt.emit("turn_end", { outcome: "completed" }), [undefined]);
		const status = await rt.job({ action: "status", job_id: jobId });
		assert.match(text(status), /still running/);
		assert.match(text(status), /READY-AFTER-EXPIRY/, "the line arrived after the watch expired");
		await rt.job({ action: "stop", job_id: jobId });
		await sleep(600);
		assert.strictEqual(noticeOf(rt).length, 0, "stopping an expired watch sends no notice");
	});
});

describe("ready watch across stop (issues #1)", () => {
	test("(a) stop-survived job keeps an armed watch and a later match still notifies", async () => {
		const rt = runtime("ready-stop-survived");
		const result = await rt.bash({
			command: "trap '' TERM; sleep 0.6; echo READY-SURVIVED-AAA; sleep 30",
			run_in_background: true,
			ready_pattern: "READY-SURVIVED-AAA",
		});
		const jobId = result.details.jobId as string;
		await sleep(200);
		assert.strictEqual(noticeOf(rt).length, 0, "no notice before the match");
		// Abort the 5 s stop wait early so the stop reply returns while the TERM-ignoring
		// job is still alive (SIGKILL grace is 2 s out). The watch must stay armed.
		const ac = new AbortController();
		const stopP = rt.job({ action: "stop", job_id: jobId }, ac.signal);
		setTimeout(() => ac.abort(), 100);
		const stopped = await stopP;
		const stoppedText = text(stopped);
		assert.match(stoppedText, /has not exited yet/, "survived stop reports still running");
		assert.match(stoppedText, /still armed/, "survived stop discloses the armed watch");
		// The READY line prints at ~0.6 s, after the stop reply: it must still notify.
		await waitFor(() => noticeOf(rt).length === 1, 5000, "ready notice after survived stop");
		assert.match(noticeOf(rt)[0]!.message.content, /\[bash job ready\]/);
		assert.match(noticeOf(rt)[0]!.message.content, /READY-SURVIVED-AAA/);
		await rt.job({ action: "stop", job_id: jobId });
		await sleep(400);
	});

	test("(b) stop-killed job disarms with truthful reply text in both cases", async () => {
		// Killed case: the job exits from the stop, the watch is disarmed, the reply says so.
		const rt1 = runtime("ready-stop-killed");
		const r1 = await rt1.bash({
			command: "sleep 30",
			run_in_background: true,
			ready_pattern: "READY-NEVER-PRINTS",
		});
		const id1 = r1.details.jobId as string;
		const stopped1 = await rt1.job({ action: "stop", job_id: id1 });
		const t1 = text(stopped1);
		assert.match(t1, /was stopped/, "killed stop reports the completion");
		assert.match(t1, /disarmed/, "killed stop discloses the disarmed watch");
		await sleep(600);
		assert.strictEqual(noticeOf(rt1).length, 0, "killed stop sends no later ready notice");

		// Survived case: the reply must instead say the watch is still armed (truthful).
		const rt2 = runtime("ready-stop-truth");
		const r2 = await rt2.bash({
			command: "trap '' TERM; sleep 0.6; echo READY-TRUTH-BBB; sleep 30",
			run_in_background: true,
			ready_pattern: "READY-TRUTH-BBB",
		});
		const id2 = r2.details.jobId as string;
		await sleep(150);
		const ac2 = new AbortController();
		const p2 = rt2.job({ action: "stop", job_id: id2 }, ac2.signal);
		setTimeout(() => ac2.abort(), 100);
		const s2 = await p2;
		assert.match(text(s2), /has not exited yet/);
		assert.match(text(s2), /still armed/, "survived stop discloses the armed watch");
		await waitFor(() => noticeOf(rt2).length === 1, 5000, "truth-case later match notifies");
		await rt2.job({ action: "stop", job_id: id2 });
		await sleep(300);
	});
});

describe("event log startup validation (issue #4)", () => {
	test("(a) directory path warns in start text and jobs run unlogged", async () => {
		const dir = tempDir("pab-evt-dir-");
		process.env.PI_ASYNC_BASH_EVENT_LOG = dir; // a directory, not a file
		try {
			const rt = runtime("evt-dir");
			const result = await rt.bash({ command: "echo dir-unlogged", run_in_background: true });
			const startText = text(result);
			assert.match(startText, /started in the background/, "job still starts");
			assert.match(startText, /event log disabled/, "model-visible warning in start text");
			assert.match(startText, /unlogged/, "warning says jobs run unlogged");
			const waited = await rt.job({ action: "wait", job_id: result.details.jobId, timeout: 5 });
			assert.match(text(waited), /exited with code 0/, "lifecycle never breaks for logging's sake");
			assert.match(text(waited), /dir-unlogged/);
			assert.deepEqual(fs.readdirSync(dir), [], "no event file is created");
		} finally {
			delete process.env.PI_ASYNC_BASH_EVENT_LOG;
		}
	});

	test("(b) FIFO path warns in start text and jobs run unlogged (no hang)", async () => {
		const dir = tempDir("pab-evt-fifo-");
		const fifo = join(dir, "events.jsonl");
		assert.equal(spawnSync("mkfifo", [fifo]).status, 0);
		process.env.PI_ASYNC_BASH_EVENT_LOG = fifo;
		try {
			const rt = runtime("evt-fifo");
			// Pre-fix this hangs inside the synchronous append with no reader; the
			// validation must reject the FIFO before any append is attempted.
			const result = await rt.bash({ command: "echo fifo-unlogged", run_in_background: true });
			const startText = text(result);
			assert.match(startText, /event log disabled/, "model-visible warning in start text");
			assert.match(startText, /unlogged/, "warning says jobs run unlogged");
			const waited = await rt.job({ action: "wait", job_id: result.details.jobId, timeout: 5 });
			assert.match(text(waited), /exited with code 0/, "lifecycle never breaks for logging's sake");
			assert.match(text(waited), /fifo-unlogged/);
		} finally {
			delete process.env.PI_ASYNC_BASH_EVENT_LOG;
		}
	});

	test("(c) unwritable path warns in start text and jobs run unlogged", async () => {
		const dir = tempDir("pab-evt-ro-");
		fs.chmodSync(dir, 0o555);
		process.env.PI_ASYNC_BASH_EVENT_LOG = join(dir, "events.jsonl");
		try {
			const rt = runtime("evt-ro");
			const result = await rt.bash({ command: "echo ro-unlogged", run_in_background: true });
			const startText = text(result);
			assert.match(startText, /event log disabled/, "model-visible warning in start text");
			assert.match(startText, /unlogged/, "warning says jobs run unlogged");
			const waited = await rt.job({ action: "wait", job_id: result.details.jobId, timeout: 5 });
			assert.match(text(waited), /exited with code 0/, "lifecycle never breaks for logging's sake");
			assert.match(text(waited), /ro-unlogged/);
			assert.deepEqual(fs.readdirSync(dir), [], "no event file is created");
		} finally {
			delete process.env.PI_ASYNC_BASH_EVENT_LOG;
			fs.chmodSync(dir, 0o755);
		}
	});

	test("(d) valid path logs with no warning", async () => {
		const dir = tempDir("pab-evt-ok-");
		const logPath = join(dir, "events.jsonl");
		process.env.PI_ASYNC_BASH_EVENT_LOG = logPath;
		try {
			const rt = runtime("evt-ok");
			const result = await rt.bash({ command: "echo valid-logged", run_in_background: true });
			assert.doesNotMatch(text(result), /event log disabled/, "no warning when logging works");
			const waited = await rt.job({ action: "wait", job_id: result.details.jobId, timeout: 5 });
			assert.match(text(waited), /valid-logged/);
			const lines = fs.readFileSync(logPath, "utf8").split("\n").filter((line) => line !== "");
			assert.equal(lines.length, 2, "started + settled are logged");
		} finally {
			delete process.env.PI_ASYNC_BASH_EVENT_LOG;
		}
	});
});

describe("coverage gaps (issue #7)", () => {
	test("foreground job with ready_pattern delivers its output with no ready notice", async () => {
		const rt = runtime("fg-ready", "off"); // off: wait for completion in the foreground
		const result = await rt.bash({ command: "echo READY-FG-LINE", ready_pattern: "READY-FG" });
		assert.equal(result.details.background, undefined, "job stays in the foreground");
		assert.equal(text(result), "READY-FG-LINE");
		await sleep(600);
		assert.strictEqual(noticeOf(rt).length, 0, "foreground match never wakes (owner filter + drop path)");
		assert.deepEqual(await rt.emit("turn_end", { outcome: "completed" }), [undefined]);
		assert.equal(text(await rt.job({ action: "list" })), "No background jobs in this session.");
	});

	test("non-string and empty ready_pattern are rejected at extension level", async () => {
		const rt = runtime("ready-invalid-ext");
		await assert.rejects(rt.bash({ command: "echo hi", ready_pattern: 123 }), /Invalid ready_pattern/);
		await assert.rejects(rt.bash({ command: "echo hi", ready_pattern: null }), /Invalid ready_pattern/);
		await assert.rejects(rt.bash({ command: "echo hi", ready_pattern: "" }), /Invalid ready_pattern/);
		assert.equal(text(await rt.job({ action: "list" })), "No background jobs in this session.");
	});

	test("stop on a settled job returns the recorded completion", async () => {
		const rt = runtime("stop-settled");
		const result = await rt.bash({ command: "echo settled-stop", run_in_background: true });
		const jobId = result.details.jobId as string;
		const waited = await rt.job({ action: "wait", job_id: jobId, timeout: 5 });
		assert.match(text(waited), /exited with code 0/);
		const stopped = await rt.job({ action: "stop", job_id: jobId });
		const stoppedText = text(stopped);
		assert.match(stoppedText, /exited with code 0/, "stop reports the recorded outcome");
		assert.doesNotMatch(stoppedText, /was stopped/, "stop never synthesizes stopped after exit");
	});
});

describe("explicit-background spawn failure warning (issue #13)", () => {
	test("bg spawn failure carries the event-log warning for parity with foreground", async () => {
		const dir = tempDir("pab-evt-bgspawn-");
		process.env.PI_ASYNC_BASH_EVENT_LOG = dir; // a directory, not a file: logging disabled with warning
		try {
			const rt = runtime("evt-bg-spawn");
			rt.settings.shellPath = dir; // exists but not executable: spawn fails with EACCES
			await assert.rejects(rt.bash({ command: "echo hi", run_in_background: true }), (err: unknown) => {
				const message = (err as Error).message;
				assert.match(message, /failed to start/, "spawn failure is reported");
				assert.match(message, /event log disabled/, "warning present on the bg-spawn-failure path");
				return true;
			});
		} finally {
			delete process.env.PI_ASYNC_BASH_EVENT_LOG;
		}
	});
});

describe("task_id event linkage (issue #8, extension)", () => {
	test("task_id is present verbatim on started+settled", async () => {
		const dir = tempDir("pab-taskid-");
		const logPath = join(dir, "events.jsonl");
		process.env.PI_ASYNC_BASH_EVENT_LOG = logPath;
		try {
			const rt = runtime("taskid-verbatim");
			const result = await rt.bash({ command: "echo hi", task_id: "my-task-42" });
			assert.equal(text(result), "hi");
			const lines = fs.readFileSync(logPath, "utf8").split("\n").filter((line) => line !== "");
			assert.strictEqual(lines.length, 2);
			const started = JSON.parse(lines[0]!);
			const settled = JSON.parse(lines[1]!);
			assert.equal(started.task_id, "my-task-42");
			assert.equal(settled.task_id, "my-task-42");
			assert.equal(settled.job_id, started.job_id);
		} finally {
			delete process.env.PI_ASYNC_BASH_EVENT_LOG;
		}
	});

	test("omitted task_id leaves no field at all", async () => {
		const dir = tempDir("pab-taskid-omit-");
		const logPath = join(dir, "events.jsonl");
		process.env.PI_ASYNC_BASH_EVENT_LOG = logPath;
		try {
			const rt = runtime("taskid-omitted");
			await rt.bash({ command: "echo hi" });
			const lines = fs.readFileSync(logPath, "utf8").split("\n").filter((line) => line !== "");
			assert.strictEqual(lines.length, 2);
			const started = JSON.parse(lines[0]!);
			const settled = JSON.parse(lines[1]!);
			assert.ok(!("task_id" in started), "started carries no task_id when omitted");
			assert.ok(!("task_id" in settled), "settled carries no task_id when omitted");
		} finally {
			delete process.env.PI_ASYNC_BASH_EVENT_LOG;
		}
	});

	test("empty task_id is treated as omitted", async () => {
		const dir = tempDir("pab-taskid-empty-");
		const logPath = join(dir, "events.jsonl");
		process.env.PI_ASYNC_BASH_EVENT_LOG = logPath;
		try {
			const rt = runtime("taskid-empty");
			await rt.bash({ command: "echo hi", task_id: "" });
			const lines = fs.readFileSync(logPath, "utf8").split("\n").filter((line) => line !== "");
			assert.strictEqual(lines.length, 2);
			const started = JSON.parse(lines[0]!);
			const settled = JSON.parse(lines[1]!);
			assert.ok(!("task_id" in started), "started carries no task_id for empty string");
			assert.ok(!("task_id" in settled), "settled carries no task_id for empty string");
		} finally {
			delete process.env.PI_ASYNC_BASH_EVENT_LOG;
		}
	});

	test("overlong task_id is rejected, never truncated", async () => {
		const rt = runtime("taskid-long");
		await assert.rejects(rt.bash({ command: "echo hi", task_id: "x".repeat(257) }), /Invalid task_id.*256/);
		assert.equal(text(await rt.job({ action: "list" })), "No background jobs in this session.");
	});

	test("task_id with newline is rejected (JSONL must survive unchanged)", async () => {
		const rt = runtime("taskid-newline");
		await assert.rejects(rt.bash({ command: "echo hi", task_id: "abc\ndef" }), /Invalid task_id/);
		assert.equal(text(await rt.job({ action: "list" })), "No background jobs in this session.");
	});

	test("task_id with control characters is rejected", async () => {
		const rt = runtime("taskid-control");
		await assert.rejects(rt.bash({ command: "echo hi", task_id: "abc\u0001def" }), /Invalid task_id/);
		await assert.rejects(rt.bash({ command: "echo hi", task_id: "abc\rdef" }), /Invalid task_id/);
		await assert.rejects(rt.bash({ command: "echo hi", task_id: "abc\tdef" }), /Invalid task_id/);
		assert.equal(text(await rt.job({ action: "list" })), "No background jobs in this session.");
	});
});
