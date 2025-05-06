import type { Page, PageInit } from "./Page.mjs";
import type { Delete, Insert, Select, Update } from "./RSql/RSql.mjs";
import type {
	PreparedMutation,
	PreparedQueryAll,
	PreparedQueryOne,
} from "./types/PreparedStatement.mjs";
import type {
	PrimaryKey,
	PrimaryKeyRecord,
	Row,
} from "./types/TableSchema.mjs";
import type { TableSchemaBase } from "./types/TableSchema.mjs";

export type Storage<T extends TableSchemaBase> = ReadableStorage<T> &
	WritableStorage<T> &
	Partial<TransactionalStorage<T>> & {
		batch?(mutations: Mutation<T>[]): void;
	};

export type ReadableStorage<T extends TableSchemaBase> = {
	prepareQueryOne<Context, Row>(
		query: Select<T>,
	): PreparedQueryOne<Context, Row>;
	prepareQueryAll<Context, Row>(
		query: Select<T>,
	): PreparedQueryAll<Context, Row>;

	findUnique(key: PrimaryKeyRecord<T>): Row<T> | null;
	findMany<Cursor extends PrimaryKeyRecord<T>>(
		pageInput: PageInit<T, Cursor>,
	): Page<T, Cursor>;
};

export type WritableStorage<T extends TableSchemaBase> = {
	prepareMutation<Context>(
		mutation: Insert<T> | Update<T> | Delete<T>,
	): PreparedMutation<Context>;

	insert(row: Row<T>): void;
	upsert(row: Row<T>): void;
	update(
		key: PrimaryKeyRecord<T>,
		partialRow: Partial<Omit<Row<T>, PrimaryKey<T>[number]>>,
	): void;
	delete(key: PrimaryKeyRecord<T>): void;
};

export interface TransactionalStorage<T extends TableSchemaBase> {
	/**
	 * Run a set of operations in a transaction. All changes are committed atomically.
	 * If the callback throws, the transaction is rolled back.
	 */
	transaction<R>(fn: () => R): R;
}

export type Mutation<T extends TableSchemaBase> =
	| { type: "insert"; row: Row<T> }
	| { type: "upsert"; row: Row<T> }
	| { type: "update"; key: PrimaryKeyRecord<T>; partialRow: Partial<Row<T>> }
	| { type: "delete"; key: PrimaryKeyRecord<T> };
