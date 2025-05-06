import assert from "assert";
import ManyKeyMap from "many-keys-map";
import {
	EMPTY,
	type Observable,
	Subject,
	concatAll,
	map,
	mergeMap,
	of,
	share,
	tap,
	timer,
} from "rxjs";
import type { Page, PageDelta, PageEvent, PageInit } from "./Page.mjs";
import type { Parameter } from "./RSql/Expression.mjs";
import {
	mkDelete,
	mkEq,
	mkInsert,
	mkParameter,
	mkPkColumns,
	mkPkParams,
	mkUpdate,
} from "./RSql/mks.mjs";
import type { PreparedMutation, PreparedQuery, Storage } from "./Storage.mjs";
import { type Dynamic, createDynamic } from "./core/Dynamic.mjs";
import type { PreparedQueryOne } from "./storages/better-sqlite3/BetterSqlite3Storage.mjs";
import type {
	ReadableTable,
	TableEvent,
	WritableTable,
} from "./types/TableSchema.mjs";
import type {
	PrimaryKey,
	PrimaryKeyRecord,
	PrimaryKeyTuple,
	Row,
} from "./types/TableSchema.mjs";
import type { TableSchemaBase } from "./types/TableSchema.mjs";
import { partitionByKey } from "./util/partitionByKey.mjs";
import { rsqlExpressionToFilterFn } from "./util/rsqlExpressionToFilterFn.mjs";

export class Table<T extends TableSchemaBase>
	implements ReadableTable<T>, WritableTable<T>
{
	constructor(
		private tableSchema: T,
		storage: Storage<T>,
	) {
		this.storage = storage;

		this.preparedInsertRow = this.storage.prepareMutation<Row<T>>(
			mkInsert(
				this.tableSchema,
				Object.fromEntries(
					Object.entries(this.tableSchema.columns).map(
						([col]) =>
							[
								col,
								mkParameter((row: Row<T>) => row[col as keyof Row<T>]),
							] as const,
					),
				) as Record<keyof Row<T>, Parameter>,
			),
		);
		this.preparedDeleteRow = this.storage.prepareMutation<PrimaryKeyRecord<T>>(
			mkDelete(this.tableSchema, mkPkColumns(this.tableSchema)),
		);
	}

	private preparedInsertRow: PreparedMutation<Row<T>>;
	private preparedDeleteRow: PreparedMutation<PrimaryKeyRecord<T>>;

	insert(row: Row<T>): void {
		this.preparedInsertRow(row);
		this.events.next([{ kind: "insert", row }]);
	}
	upsert(row: Row<T>): void {
		assert.fail("Not implemented");
	}
	update(
		key: PrimaryKeyRecord<T>,
		changes: Partial<Omit<Row<T>, PrimaryKey<T>[number]>>,
	): void {
		type Context = {
			key: PrimaryKeyRecord<T>;
			changes: Partial<Omit<Row<T>, PrimaryKey<T>[number]>>;
		};
		const preparedUpdateRow = this.storage.prepareMutation<Context>(
			mkUpdate(
				this.tableSchema,
				Object.fromEntries(
					Object.entries(changes).map(
						([col]) =>
							[
								col,
								mkParameter(
									(ctx: Context) => ctx.changes[col as keyof typeof changes],
								),
							] as const,
					),
				) as Record<keyof Row<T>, Parameter>,
				mkEq(
					mkPkColumns(this.tableSchema),
					mkPkParams<T, Context>(this.tableSchema, (ctx: Context) => ctx.key),
				),
			),
		);
		preparedUpdateRow({ key, changes: changes });
		this.events.next([{ kind: "update", key, row: changes }]);
	}
	delete(key: PrimaryKeyRecord<T>): void {
		this.preparedDeleteRow(key);
		this.events.next([{ kind: "delete", key }]);
	}

	findUnique(key: PrimaryKeyRecord<T>): Dynamic<Row<T> | null, void> {
		const keyTuple = this.tableSchema.primaryKey.map(
			(pk: PrimaryKey<T>[number]) => key[pk],
		) as unknown as PrimaryKeyTuple<T>;

		let dynamic = this.rows.get(keyTuple);
		if (dynamic) return dynamic.fork();

		const row: Row<T> | null = this.storage.findUnique(key);

		dynamic = createDynamic<Row<T> | null, void>(
			row,
			this.getRowEvent(keyTuple).pipe(
				// biome-ignore lint/suspicious/noConfusingVoidType: <explanation>
				map((e): [void, Row<T> | null] => {
					if (e.kind === "insert") {
						return [void 0, e.row] as const;
					}
					if (e.kind === "update") {
						// biome-ignore lint/style/noNonNullAssertion: <explanation>
						return [void 0, { ...row!, ...e.row }] as const;
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
	findMany<Cursor extends PrimaryKeyRecord<T>>(
		pageInput: PageInit<T, Cursor>,
		pageEvent: Observable<PageEvent>,
	): Dynamic<Page<T, Cursor>, PageDelta<T>> {
		const page = this.storage.findMany(pageInput);
		const filter = pageInput.filter
			? rsqlExpressionToFilterFn(pageInput.filter)
			: () => true;

		return createDynamic<Page<T, Cursor>, PageDelta<T>>(
			page,
			this.events.pipe(
				concatAll(),
				mergeMap((e): Observable<[PageDelta<T>, Page<T, Cursor>]> => {
					const row = this.getRow(this.getKeyTuple(e));
					if (!filter(row as Row<T>)) return EMPTY;
					if (e.kind === "insert") {
						return of([
							[{ kind: "add", row: e.row }],
							{
								rows: page.rows,
								rowCount: page.rowCount,
								endCursor: page.endCursor,
								startCursor: page.startCursor,
								itemBeforeCount: page.itemBeforeCount,
								itemAfterCount: page.itemAfterCount,
							},
						]);
					}
					if (e.kind === "delete") {
						return of([
							[{ kind: "remove", key: e.key }],
							{
								rows: page.rows,
								rowCount: page.rowCount,
								endCursor: page.endCursor,
								startCursor: page.startCursor,
								itemBeforeCount: page.itemBeforeCount,
								itemAfterCount: page.itemAfterCount,
							},
						]);
					}

					return EMPTY;
				}),
				share({
					resetOnRefCountZero: () =>
						timer(10 * 1000).pipe(
							tap(() => {
								// FIXME:
								// this.rows.delete(this.getKeyTuple(e));
							}),
						),
				}),
			),
		);
	}

	private getKeyTuple<T extends TableSchemaBase>(
		event: TableEvent<T>,
	): PrimaryKeyTuple<T> {
		switch (event.kind) {
			case "insert":
				return this.tableSchema.primaryKey.map(
					(pk: PrimaryKey<T>[number]) => event.row[pk],
				) as unknown as PrimaryKeyTuple<T>;
			case "update":
				return this.tableSchema.primaryKey.map(
					(pk: PrimaryKey<T>[number]) => event.key[pk],
				) as unknown as PrimaryKeyTuple<T>;
			case "delete":
				return this.tableSchema.primaryKey.map(
					(pk: PrimaryKey<T>[number]) => event.key[pk],
				) as unknown as PrimaryKeyTuple<T>;
		}
	}

	private getRow(key: PrimaryKeyTuple<T>) {
		const cacheRow = this.rows.get(key);
		if (cacheRow !== undefined) return cacheRow;
		return this.storage.findUnique(
			Object.fromEntries(
				this.tableSchema.primaryKey.map((pk, i) => [pk, key[i]] as const),
			) as unknown as PrimaryKeyRecord<T>,
		);
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
