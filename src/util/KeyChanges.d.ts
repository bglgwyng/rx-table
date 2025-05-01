export interface KeyChanges<K> {
	type: "add" | "remove";
	keys: Iterable<K>;
}
