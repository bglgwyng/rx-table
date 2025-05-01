import { type Observable, Subject, map, takeUntil } from "rxjs";

export type Dynamic<Value, Delta> = {
	read(): Value;
	disconnect(): void;

	updated: Observable<Delta>;

	fork(): Dynamic<Value, Delta>;
};

export function createDynamic<Value, Delta>(
	initialValue: Value,
	updated: Observable<readonly [delta: Delta, newValue: Value]>,
): Dynamic<Value, Delta> {
	let value = initialValue;
	const disconnect$ = new Subject<void>();
	const subscription = updated.subscribe(([delta, newValue]) => {
		value = newValue;
	});

	return {
		read: () => {
			if (subscription.closed) {
				throw new Error("You can't read a disconnected dynamic");
			}
			return value;
		},
		updated: updated.pipe(
			map(([delta]) => delta),
			takeUntil(disconnect$),
		),
		disconnect() {
			subscription.unsubscribe();
			disconnect$.next();
			disconnect$.complete();
		},
		fork(): Dynamic<Value, Delta> {
			if (subscription.closed) {
				throw new Error("You can't fork a disconnected dynamic");
			}
			return createDynamic(value, updated);
		},
	};
}
