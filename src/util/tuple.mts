import assert from "assert";

export function compareTuple<T extends unknown[]>(x: T, y: T): number {
	assert(x.length === y.length, "Tuples must have the same length");
	for (let i = 0; i < x.length; i++) {
		// biome-ignore lint/style/noNonNullAssertion: <explanation>
		if (x[i]! < y[i]!) return -1;
		// biome-ignore lint/style/noNonNullAssertion: <explanation>
		if (x[i]! > y[i]!) return 1;
	}
	return 0;
}

// Compare two tuples (arrays) shallowly for equality
export function eqTuple<T extends unknown[]>(a: T, b: T): boolean {
	return compareTuple(a, b) === 0;
}
