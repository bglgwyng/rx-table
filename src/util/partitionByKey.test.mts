import ManyKeysMap from "many-keys-map";
import { EMPTY, NEVER, Observable, Subject, concat, from, of } from "rxjs";
import { catchError, map, switchMap, take } from "rxjs/operators";
import { TestScheduler } from "rxjs/testing";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { partitionByKey } from "./partitionByKey.mjs";
import "expose-gc";
import type { KeyChanges } from "./KeyChanges.js";

const scheduler = () =>
	new TestScheduler((actual, expected) => {
		expect(actual).toEqual(expected);
	});

describe("partitionByKey", () => {
	describe("behaviour", () => {
		it("groups observables by using the key function", () => {
			scheduler().run(({ expectObservable, cold }) => {
				const source = cold("-12-3456-");
				const expectOdd = "  -1--3-5--";
				const expectEven = " --2--4-6-";

				const [getInstance$] = partitionByKey(source, (v) => [Number(v) % 2]);

				expectObservable(getInstance$([0])).toBe(expectEven);
				expectObservable(getInstance$([1])).toBe(expectOdd);
			});
		});

		it("unsubscribes from all streams when refcount reaches 0", () => {
			let innerSubs = 0;
			const inner = new Observable<number>(() => {
				innerSubs++;
				return () => {
					innerSubs--;
				};
			});

			const sourceSubject = new Subject<number>();
			let sourceSubs = 0;
			const source = new Observable<number>((obs) => {
				sourceSubs++;
				sourceSubject.subscribe(obs);
				return () => {
					sourceSubs--;
				};
			});

			const [getObs] = partitionByKey(
				source,
				(v) => [v],
				() => inner,
			);
			const observable = getObs([1]);

			expect(sourceSubs).toBe(0);
			expect(innerSubs).toBe(0);

			const sub1 = observable.subscribe();

			expect(sourceSubs).toBe(1);
			expect(innerSubs).toBe(0);

			sourceSubject.next(1);

			expect(sourceSubs).toBe(1);
			expect(innerSubs).toBe(1);

			const sub2 = observable.subscribe();

			expect(sourceSubs).toBe(1);
			expect(innerSubs).toBe(1);

			sub1.unsubscribe();

			expect(sourceSubs).toBe(1);
			expect(innerSubs).toBe(1);

			sub2.unsubscribe();

			expect(sourceSubs).toBe(0);
			expect(innerSubs).toBe(0);
		});

		it("emits a complete on the inner observable when the source completes", () => {
			scheduler().run(({ expectObservable, cold }) => {
				const source = cold("-ab-a-|");
				const expectA = "    -a--a-(c|)";
				const expectB = "    --b---(c|)";

				const [getInstance$] = partitionByKey(
					source,
					(v) => [v],
					(v$) => concat(v$, ["c"]),
				);

				expectObservable(getInstance$(["a"])).toBe(expectA);
				expectObservable(getInstance$(["b"])).toBe(expectB);
			});
		});

		it("emits the error on the inner observable when the source errors", () => {
			scheduler().run(({ expectObservable, cold }) => {
				const source = cold("-ab-a-#");
				const expectA = "    -a--a-(e|)";
				const expectB = "    --b---(e|)";

				const [getInstance$] = partitionByKey(
					source,
					(v) => [v],
					(v$) => v$.pipe(catchError(() => of("e"))),
				);

				expectObservable(getInstance$(["a"])).toBe(expectA);
				expectObservable(getInstance$(["b"])).toBe(expectB);
			});
		});

		it("handles an empty Observable", () => {
			scheduler().run(({ expectSubscriptions, expectObservable, cold }) => {
				const e1 = cold("  |");
				const e1subs = "   (^!)";
				const expectObs = "|";
				const expectKey = "|";

				const [getObs, keys$] = partitionByKey(
					e1,
					(v) => [v],
					(v$) => v$,
				);

				expectObservable(getObs([""])).toBe(expectObs);
				expectSubscriptions(e1.subscriptions).toBe(e1subs);
				expectObservable(keys$).toBe(expectKey);
			});
		});

		it("handles a never Observable", () => {
			scheduler().run(({ expectSubscriptions, expectObservable, cold }) => {
				const e1 = cold("  --");
				const e1subs = "   ^-";
				const expectObs = "--";
				const expectKey = "--";

				const [getObs, keys$] = partitionByKey(
					e1,
					(v) => [v],
					(v$) => v$,
				);

				expectObservable(getObs([""])).toBe(expectObs);
				expectSubscriptions(e1.subscriptions).toBe(e1subs);
				expectObservable(keys$).toBe(expectKey);
			});
		});

		it("handles a just-throw Observable", () => {
			scheduler().run(({ expectSubscriptions, expectObservable, cold }) => {
				const e1 = cold("  #");
				const e1subs = "   (^!)";
				const expectObs = "#";
				const expectKey = "#";

				const [getObs, keys$] = partitionByKey(
					e1,
					(v) => [v],
					(v$) => v$,
				);

				expectObservable(getObs([""])).toBe(expectObs);
				expectSubscriptions(e1.subscriptions).toBe(e1subs);
				expectObservable(keys$).toBe(expectKey);
			});
		});

		it("handles synchronous values", () => {
			scheduler().run(({ expectObservable }) => {
				const e1 = from(["1", "2", "3", "4", "5"]);
				const expectOdd = " (135|)";
				const expectEven = "(24|)";
				const expectKeys = "(wxyz|)";
				const [getObs, keys$] = partitionByKey(
					e1,
					(v) => [Number(v) % 2],
					(v$) => v$,
				);
				expectObservable(deltasToPOJO(keys$)).toBe(expectKeys, {
					w: {
						type: "add",
						keys: [[1]],
					},
					x: {
						type: "add",
						keys: [[0]],
					},
					y: {
						type: "remove",
						keys: [[1]],
					},
					z: {
						type: "remove",
						keys: [[0]],
					},
				});
				expectObservable(getObs([0])).toBe(expectEven);
				expectObservable(getObs([1])).toBe(expectOdd);
			});
		});
	});

	describe("activeKeys$", () => {
		it("emits deltas when keys are added", () => {
			scheduler().run(({ expectObservable, cold }) => {
				const source = cold("-ab-a-cd---");
				const expectedStr = "-fg---hi---";
				const [, result] = partitionByKey(
					source,
					(v) => [v],
					() => NEVER,
				);

				expectObservable(deltasToPOJO(result)).toBe(expectedStr, {
					f: {
						type: "add",
						keys: [["a"]],
					},
					g: {
						type: "add",
						keys: [["b"]],
					},
					h: {
						type: "add",
						keys: [["c"]],
					},
					i: {
						type: "add",
						keys: [["d"]],
					},
				});
			});
		});

		it("removes a key when its inner stream completes", () => {
			scheduler().run(({ expectObservable, cold }) => {
				const source = cold("-ab---c--");
				const a = cold("      --1---2-");
				const b = cold("       ---|");
				const c = cold("           1-|");
				const expectedStr = "-fg--hi-j";
				const innerStreams: ManyKeysMap<
					[string],
					Observable<string>
				> = new ManyKeysMap([
					[["a"], a],
					[["b"], b],
					[["c"], c],
				]);
				const [, result] = partitionByKey(
					source,
					(v) => [v],
					(v$) =>
						v$.pipe(
							take(1),
							// biome-ignore lint/style/noNonNullAssertion: 테스트 상 반드시 존재함
							switchMap((v) => innerStreams.get([v])!),
						),
				);

				expectObservable(deltasToPOJO(result)).toBe(expectedStr, {
					f: {
						type: "add",
						keys: [["a"]],
					},
					g: {
						type: "add",
						keys: [["b"]],
					},
					h: {
						type: "remove",
						keys: [["b"]],
					},
					i: {
						type: "add",
						keys: [["c"]],
					},
					j: {
						type: "remove",
						keys: [["c"]],
					},
				});
			});
		});

		it("emits the changes on a key even if it's removed synchronously", () => {
			scheduler().run(({ expectObservable, cold }) => {
				const source = cold("-ae----s----");
				const expectedStr = "-f(gh)-(ij)-";
				const [, result] = partitionByKey(
					source,
					(v) => [v],
					(_, [key]) => (key === "e" ? EMPTY : key === "s" ? of(1) : NEVER),
				);

				expectObservable(deltasToPOJO(result)).toBe(expectedStr, {
					f: {
						type: "add",
						keys: [["a"]],
					},
					g: {
						type: "add",
						keys: [["e"]],
					},
					h: {
						type: "remove",
						keys: [["e"]],
					},
					i: {
						type: "add",
						keys: [["s"]],
					},
					j: {
						type: "remove",
						keys: [["s"]],
					},
				});
			});
		});

		it("emits all the existing keys when subscribing late", () => {
			scheduler().run(({ expectObservable, cold }) => {
				const source = cold("-abe-a-cd---");
				const sub1 = "       ^--------";
				const sub2 = "       ----^----";
				const expectedStr = "----f--gh---";
				const [, result] = partitionByKey(
					source,
					(v) => [v],
					(_, [key]) => (key === "e" ? EMPTY : NEVER),
				);

				expectObservable(deltasToPOJO(result), sub1);
				expectObservable(deltasToPOJO(result), sub2).toBe(expectedStr, {
					f: {
						type: "add",
						keys: [["a"], ["b"]],
					},
					g: {
						type: "add",
						keys: [["c"]],
					},
					h: {
						type: "add",
						keys: [["d"]],
					},
				});
			});
		});

		it("keeps the existing keys alive when the source completes", () => {
			scheduler().run(({ expectObservable, cold }) => {
				const source = cold("-ab-|");
				const a = cold("      --1---2-|");
				const b = cold("       ---|");
				const expectedStr = "-fg--h---(i|)";
				const innerStreams: ManyKeysMap<
					[string],
					Observable<string>
				> = new ManyKeysMap([
					[["a"], a],
					[["b"], b],
				]);
				const [, result] = partitionByKey(
					source,
					(v) => [v],
					(v$) =>
						v$.pipe(
							take(1),
							// biome-ignore lint/style/noNonNullAssertion: <explanation>
							switchMap((v) => innerStreams.get([v])!),
						),
				);

				expectObservable(deltasToPOJO(result)).toBe(expectedStr, {
					f: {
						type: "add",
						keys: [["a"]],
					},
					g: {
						type: "add",
						keys: [["b"]],
					},
					h: {
						type: "remove",
						keys: [["b"]],
					},
					i: {
						type: "remove",
						keys: [["a"]],
					},
				});
			});
		});

		it("completes when no key is alive and the source completes", () => {
			scheduler().run(({ expectObservable, cold }) => {
				const source = cold("-ab---|");
				const a = cold("      --1|");
				const b = cold("       ---|");
				const expectedStr = "-fg-hi|";
				const innerStreams: ManyKeysMap<
					[string],
					Observable<string>
				> = new ManyKeysMap([
					[["a"], a],
					[["b"], b],
				]);
				const [, result] = partitionByKey(
					source,
					(v) => [v],
					(v$) =>
						v$.pipe(
							take(1),
							// biome-ignore lint/style/noNonNullAssertion: 테스트 상 반드시 존재함
							switchMap((v) => innerStreams.get([v])!),
						),
				);

				expectObservable(deltasToPOJO(result)).toBe(expectedStr, {
					f: {
						type: "add",
						keys: [["a"]],
					},
					g: {
						type: "add",
						keys: [["b"]],
					},
					h: {
						type: "remove",
						keys: [["a"]],
					},
					i: {
						type: "remove",
						keys: [["b"]],
					},
				});
			});
		});

		it("errors when the source emits an error and no group is active", () => {
			scheduler().run(({ expectObservable, cold }) => {
				const source = cold("-ab--#");
				const a = cold("      --1|");
				const b = cold("       -|");
				const expectedStr = "-fghi#";
				const innerStreams: ManyKeysMap<
					[string],
					Observable<string>
				> = new ManyKeysMap([
					[["a"], a],
					[["b"], b],
				]);
				const [, result] = partitionByKey(
					source,
					(v) => [v],
					(v$) =>
						v$.pipe(
							take(1),
							// biome-ignore lint/style/noNonNullAssertion: 테스트 상 반드시 존재함
							switchMap((v) => innerStreams.get([v])!),
						),
				);

				expectObservable(deltasToPOJO(result)).toBe(expectedStr, {
					f: {
						type: "add",
						keys: [["a"]],
					},
					g: {
						type: "add",
						keys: [["b"]],
					},
					h: {
						type: "remove",
						keys: [["b"]],
					},
					i: {
						type: "remove",
						keys: [["a"]],
					},
				});
			});
		});

		it("doesn't error when the source errors and its inner streams stop the error", () => {
			scheduler().run(({ expectObservable, cold }) => {
				const source = cold("-ab--#");
				const a = cold("      --1--2--3|");
				const b = cold("       ----|");
				const expectedStr = "-fg---h---(i|)";
				const innerStreams: ManyKeysMap<
					[string],
					Observable<string>
				> = new ManyKeysMap([
					[["a"], a],
					[["b"], b],
				]);
				const [, result] = partitionByKey(
					source,
					(v) => [v],
					(v$) =>
						v$.pipe(
							take(1),
							// biome-ignore lint/style/noNonNullAssertion: 테스트 상 반드시 존재함
							switchMap((v) => innerStreams.get([v])!),
						),
				);

				expectObservable(deltasToPOJO(result)).toBe(expectedStr, {
					f: {
						type: "add",
						keys: [["a"]],
					},
					g: {
						type: "add",
						keys: [["b"]],
					},
					h: {
						type: "remove",
						keys: [["b"]],
					},
					i: {
						type: "remove",
						keys: [["a"]],
					},
				});
			});
		});

		it("errors when one of its inner stream emits an error", () => {
			scheduler().run(({ expectObservable, cold }) => {
				const source = cold("-ab-----");
				const a = cold("      --1-#");
				const b = cold("       ------");
				const expectedStr = "-fg--#";
				const innerStreams: ManyKeysMap<
					[string],
					Observable<string>
				> = new ManyKeysMap([
					[["a"], a],
					[["b"], b],
				]);

				const [, result] = partitionByKey(
					source,
					(v) => [v] as [string],
					// biome-ignore lint/style/noNonNullAssertion: 테스트 상 반드시 존재함
					(_, v) => innerStreams.get(v)!,
				);

				expectObservable(deltasToPOJO(result)).toBe(expectedStr, {
					f: {
						type: "add",
						keys: [["a"]],
					},
					g: {
						type: "add",
						keys: [["b"]],
					},
				});
			});
		});
	});

	describe("getInstance$", () => {
		it("returns the values for the selected key", () => {
			scheduler().run(({ expectObservable, cold }) => {
				const source = cold("-ab---c--");
				const a = cold("      --1---2-");
				const b = cold("       ---|");
				const c = cold("           1-|");
				const expectA = "    ---1---2--";
				const expectB = "    -----|";
				const expectC = "    ------1-|";

				const innerStreams: ManyKeysMap<
					[string],
					Observable<string>
				> = new ManyKeysMap([
					[["a"], a],
					[["b"], b],
					[["c"], c],
				]);
				const [getInstance$] = partitionByKey(
					source,
					(v) => [v],
					(v$) =>
						v$.pipe(
							take(1),
							// biome-ignore lint/style/noNonNullAssertion: 테스트 상 반드시 존재함
							switchMap((v) => innerStreams.get([v])!),
						),
				);

				expectObservable(getInstance$(["a"])).toBe(expectA);
				expectObservable(getInstance$(["b"])).toBe(expectB);
				expectObservable(getInstance$(["c"])).toBe(expectC);
			});
		});

		it("replays the latest value for each key", () => {
			const source$ = new Subject<string>();
			const inner$ = new Subject<number>();
			const [getInstance$] = partitionByKey(
				source$,
				(v) => [v],
				() => inner$,
			);

			const next = vi.fn();
			getInstance$(["a"]).subscribe(next);

			source$.next("a");
			expect(next).not.toHaveBeenCalled();

			inner$.next(1);
			expect(next).toHaveBeenCalledTimes(1);
			expect(next).toHaveBeenCalledWith(1);

			const lateNext = vi.fn();
			getInstance$(["a"]).subscribe(lateNext);
			expect(lateNext).toHaveBeenCalledTimes(1);
			expect(lateNext).toHaveBeenCalledWith(1);
		});

		it("lets the projection function handle completions", () => {
			scheduler().run(({ expectObservable, cold }) => {
				const source = cold("-ab-a-|");
				const concatenated = cold("123|");
				const expectA = "    -a--a-123|";
				const expectB = "    --b---123|";

				const [getInstance$] = partitionByKey(
					source,
					(v) => [v],
					(v$) => concat(v$, concatenated),
				);

				expectObservable(getInstance$(["a"])).toBe(expectA);
				expectObservable(getInstance$(["b"])).toBe(expectB);
			});
		});

		it("lets the projection function catch source errors", () => {
			scheduler().run(({ expectObservable, cold }) => {
				const source = cold("-ab-a-#");
				const expectA = "    -a--a-(e|)";
				const expectB = "    --b---(e|)";

				const [getInstance$] = partitionByKey(
					source,
					(v) => [v],
					(v$) => v$.pipe(catchError(() => of("e"))),
				);

				expectObservable(getInstance$(["a"])).toBe(expectA);
				expectObservable(getInstance$(["b"])).toBe(expectB);
			});
		});

		it("synchronously emits when the group observable notifies of a new GroupedObservable", () => {
			const subject = new Subject<[number]>();
			const [getInner$, keys$] = partitionByKey(subject, (x) => x, take(1));

			const key: [number] = [8];
			let receivedValue: [number] | undefined = undefined;
			let deleted: [number] | undefined = undefined;
			let done = false;
			const order: string[] = [];
			keys$.subscribe((keys) => {
				if (keys.type === "add") {
					order.push("outer add");
					// biome-ignore lint/style/noNonNullAssertion: <explanation>
					getInner$([...keys.keys][0]!).subscribe({
						next: (x) => {
							receivedValue = x;
							order.push("inner next");
						},
						complete: () => {
							order.push("inner complete");
							done = true;
						},
					});
				} else {
					order.push("outer delete");
					deleted = [...keys.keys][0];
				}
			});

			subject.next(key);

			expect(receivedValue).toBe(key);
			expect(done).toBe(true);
			expect(deleted).toEqual(key);
			expect(order).toEqual([
				"outer add",
				"inner next",
				"outer delete",
				"inner complete",
			]);
		});
	});

	describe("performance", () => {
		beforeEach(() => {
			global.gc?.();
		});
		it("has an acceptable performance when it synchronously receives a gust of new keys", () => {
			const array = new Array(15_000).fill(0).map((_, i) => i);

			const [, keys$] = partitionByKey(from(array), (v) => [v]);

			const start = performance.now();
			keys$.subscribe();
			const result = performance.now() - start;
			expect(result).toBeLessThan(800);
		});

		// TODO: implement `combineKeys` first
		// it("has an acceptable performance when it synchronously receives a gust of new keys and subscriptions are created on every inner observable", () => {
		// 	const array = new Array(7_500).fill(0).map((_, i) => i);

		// 	const [getInner$, keys$] = partitionByKey(from(array), (v) => [v]);
		// 	const result$ = combineKeys(keys$, getInner$);

		// 	const start = performance.now();
		// 	result$.subscribe();
		// 	const result = performance.now() - start;
		// 	expect(result).toBeLessThan(800);
		// });
	});
});

function deltasToPOJO<T>(observable: Observable<KeyChanges<T>>) {
	return observable.pipe(
		map((change) => ({
			type: change.type,
			keys: Array.from(change.keys),
		})),
	);
}
