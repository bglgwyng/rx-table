import { useCallback, useSyncExternalStore } from "react";
import type { Dynamic } from "../core/Dynamic.mjs";

export function useDynamic<T>(dynamic: Dynamic<T, unknown>): T {
	return useSyncExternalStore(
		useCallback(
			(onChange) => {
				const unsub = dynamic.updated.subscribe(onChange);
				return unsub.unsubscribe.bind(unsub);
			},
			[dynamic],
		),
		dynamic.read,
	);
}
