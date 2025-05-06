import { describe, it, expect } from "vitest";
import { TestScheduler } from "rxjs/testing";
import { mergeWithKey } from "./mergeWithKey.mjs";

describe("mergeWithKey", () => {
	const scheduler = () =>
		new TestScheduler((actual, expected) => {
			expect(actual).toEqual(expected);
		});

	it("should emit partial records as each observable emits", () => {
		scheduler().run(({ cold, expectObservable }) => {
			// foo:   --a---b---|
			// bar:   ----x---y|
			// out:   --A-B-C-D|
			// A: { foo: 1 }
			// B: { foo: 1, bar: 'a' }
			// C: { foo: 2, bar: 'a' }
			// D: { foo: 2, bar: 'b' }
			const foo$ = cold("--a---b---|", { a: 1, b: 2 });
			const bar$ = cold("----x---y|", { x: "a", y: "b" });
			const expected = " --A-B-C-D-|";
			const values = {
				A: { foo: 1 },
				B: { bar: "a" },
				C: { foo: 2 },
				D: { bar: "b" },
			};
			const result$ = mergeWithKey({ foo: foo$, bar: bar$ });
			expectObservable(result$).toBe(expected, values);
		});
	});

	it("should work with a single observable", () => {
		scheduler().run(({ cold, expectObservable }) => {
			const foo$ = cold("a-b|", { a: 10, b: 20 });
			const expected = "a-b|";
			const values = {
				a: { foo: 10 },
				b: { foo: 20 },
			};
			const result$ = mergeWithKey({ foo: foo$ });
			expectObservable(result$).toBe(expected, values);
		});
	});

	it("should work with no observables", () => {
		scheduler().run(({ expectObservable }) => {
			const result$ = mergeWithKey({});
			// No emissions, just completes
			expectObservable(result$).toBe("|");
		});
	});
});
