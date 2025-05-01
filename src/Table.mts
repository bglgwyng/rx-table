import assert from "assert";
import ManyKeyMap from "many-keys-map";
import { Subject, concatAll, map, share, tap, timer } from "rxjs";
import type { Storage } from "./Storage.mjs";
import { type Dynamic, createDynamic } from "./core/Dynamic.mjs";
import type {
	Page,
	PageDelta,
	PageInput,
	PrimaryKey,
	PrimaryKeyRecord,
	PrimaryKeyTuple,
	ReadableTable,
	Row,
	TableBase,
	TableEvent,
	WritableTable,
} from "./types.mjs";
import { partitionByKey } from "./util/partitionByKey.mjs";

export class Table<T extends TableBase>
	implements ReadableTable<T>, WritableTable<T>
{
	constructor(
		private primaryKeys: PrimaryKey<T>,
		storage: Storage<T>,
	) {
		this.storage = storage;
	}
	insert(row: Row<T>): void {
		this.storage.insert(row);
		this.events.next([{ kind: "insert", row }]);
	}
	upsert(row: Row<T>): void {
		assert.fail("Not implemented");
	}
	update(
		key: PrimaryKeyRecord<T>,
		partialRow: Partial<Omit<Row<T>, PrimaryKey<T>[number]>>,
	): void {
		this.storage.update(key, partialRow);
		this.events.next([{ kind: "update", key, row: partialRow }]);
	}
	delete(key: PrimaryKeyRecord<T>): void {
		this.storage.delete(key);
		this.events.next([{ kind: "delete", key }]);
	}

	findUnique(key: PrimaryKeyRecord<T>): Dynamic<Row<T> | null, void> {
		const keyTuple = this.primaryKeys.map(
			(pk: PrimaryKey<T>[number]) => key[pk],
		) as unknown as PrimaryKeyTuple<T>;

		let dynamic = this.rows.get(keyTuple);
		if (dynamic) return dynamic.fork();

		const row: Row<T> | null = this.storage.findUnique(key);

		dynamic = createDynamic<Row<T> | null, void>(
			row,
			this.getRowEvent(keyTuple).pipe(
				map((e) => {
					if (e.kind === "insert") {
						return [void 0, e.row] as const;
					}
					if (e.kind === "update") {
						return [void 0, { ...row, ...e.row }] as const;
					}
					if (e.kind === "delete") {
						return [void 0, null] as const;
					}
					throw new Error("Invalid event kind");
				}),
				share({
					resetOnRefCountZero: () =>
						timer(10 * 1000).pipe(
							tap(() => {
								this.rows.delete(keyTuple);
							}),
						),
				}),
			),
		);
		this.rows.set(keyTuple, dynamic);
		return dynamic;
	}
	findMany(pageInput: PageInput<T>): Dynamic<Page<T>, PageDelta<T>> {
		assert.fail("Not implemented");
	}

	private getKeyTuple<T extends TableBase>(
		event: TableEvent<T>,
	): PrimaryKeyTuple<T> {
		switch (event.kind) {
			case "insert":
				return this.primaryKeys.map(
					(pk: PrimaryKey<T>[number]) => event.row[pk],
				) as unknown as PrimaryKeyTuple<T>;
			case "update":
				return this.primaryKeys.map(
					(pk: PrimaryKey<T>[number]) => event.key[pk],
				) as unknown as PrimaryKeyTuple<T>;
			case "delete":
				return this.primaryKeys.map(
					(pk: PrimaryKey<T>[number]) => event.key[pk],
				) as unknown as PrimaryKeyTuple<T>;
		}
	}

	private storage: Storage<T>;
	private events: Subject<TableEvent<T>[]> = new Subject();
	private partition = partitionByKey(this.events.pipe(concatAll()), (e) =>
		this.getKeyTuple(e),
	);
	private getRowEvent = this.partition[0];

	private rows: ManyKeyMap<PrimaryKeyTuple<T>, Dynamic<Row<T> | null, void>> =
		new ManyKeyMap();
}
