import type { Page, PageInit } from "./Page.mjs";
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
	findUnique(key: PrimaryKeyRecord<T>): Row<T> | null;
	findMany(pageInput: PageInit<T>): Page<T>;
};

export type WritableStorage<T extends TableSchemaBase> = {
	insert(row: Row<T>): void;
	upsert(row: Row<T>): void;
	update(
		key: PrimaryKeyRecord<T>,
		partialRow: Partial<Omit<Row<T>, PrimaryKey<T>[number]>>,
	): void;
	delete(key: PrimaryKeyRecord<T>): void;

	mutate(mutation: Mutation<T>): void;
	mutateMany(mutations: Mutation<T>[]): void;
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
