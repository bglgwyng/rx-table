import { Subject } from "rxjs";
// src/Dynamic.test.mts
import { describe, expect, it, vi } from "vitest";
import { createDynamic } from "./Dynamic.mjs";

describe("createDynamic", () => {
	it("can read the initial value with read()", () => {
		const subj = new Subject<[number, number]>();
		const dyn = createDynamic(1, subj);
		expect(dyn.read()).toBe(1);
	});

	it("updated observable only emits delta values", async () => {
		const subj = new Subject<[number, number]>();
		const dyn = createDynamic(1, subj);

		const results: number[] = [];
		const sub = dyn.updated.subscribe((delta) => results.push(delta));

		subj.next([10, 2]);
		subj.next([20, 3]);
		expect(results).toEqual([10, 20]);
		sub.unsubscribe();
	});

	it("read() reflects changes when the value is updated", () => {
		const subj = new Subject<[number, number]>();
		const dyn = createDynamic(1, subj);
		subj.next([10, 2]);
		expect(dyn.read()).toBe(2);
		subj.next([20, 3]);
		expect(dyn.read()).toBe(3);
	});

	it("read() throws an error after disconnect", () => {
		const subj = new Subject<[number, number]>();
		const dyn = createDynamic(1, subj);
		dyn.disconnect();
		expect(() => dyn.read()).toThrow(/disconnected/);
	});

	it("updated observable completes after disconnect", () => {
		const subj = new Subject<[number, number]>();
		const dyn = createDynamic(1, subj);

		const onComplete = vi.fn();
		const sub = dyn.updated.subscribe({ complete: onComplete });

		dyn.disconnect();
		expect(onComplete).toHaveBeenCalled();
		sub.unsubscribe();
	});

	it("fork() clones the current value and stream", () => {
		const subj = new Subject<[number, number]>();
		const dyn = createDynamic(1, subj);

		subj.next([10, 2]);
		const forked = dyn.fork();
		expect(forked.read()).toBe(2);

		const results: number[] = [];
		const sub = forked.updated.subscribe((delta) => results.push(delta));
		subj.next([99, 5]);
		expect(results).toEqual([99]);
		sub.unsubscribe();
	});

	it("fork() throws an error after disconnect", () => {
		const subj = new Subject<[number, number]>();
		const dyn = createDynamic(1, subj);
		dyn.disconnect();
		expect(() => dyn.fork()).toThrow(/disconnected/);
	});

	it("disconnect does not affect other forks", () => {
		const subj = new Subject<[number, number]>();
		const dyn = createDynamic(1, subj);

		const fork1 = dyn.fork();
		const fork2 = dyn.fork();

		fork1.disconnect();
		// fork2 should still work independently
		expect(() => fork2.read()).not.toThrow();

		const onNext = vi.fn();
		const sub = fork2.updated.subscribe(onNext);
		subj.next([42, 2]);
		expect(onNext).toHaveBeenCalledWith(42);
		sub.unsubscribe();
	});
});
