import { merge, type Observable } from "rxjs";
import { map, scan } from "rxjs/operators";

/**
 * 여러 Observable을 {key: Observable} 객체로 받아 Partial<Record<K, V>>로 내보내는 유틸리티
 * 각 observable이 emit할 때마다 partial record로 최신값을 emit합니다.
 *
 * @example
 *   mergeWithKey({ foo: obs1, bar: obs2 })
 *   // => Observable<{ foo?: T1; bar?: T2 }>
 */
export function mergeWithKey<T extends Record<string, Observable<unknown>>>(
	sources: T,
): Observable<
	Partial<{ [K in keyof T]: T[K] extends Observable<infer V> ? V : never }>
> {
	const entries = Object.entries(sources) as [keyof T, Observable<unknown>][];
	const taggedStreams = entries.map(([key, obs]) =>
		obs.pipe(
			map((value) => ({
				key,
				value: value as T[keyof T],
			})),
		),
	);
	return merge(...taggedStreams).pipe(
		map(
			({ key, value }) =>
				({
					[key]: value,
				}) as Partial<{
					[K in keyof T]: T[K] extends Observable<infer V> ? V : never;
				}>,
		),
	);
}
