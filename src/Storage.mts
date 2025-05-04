import type { Page, PageInput } from "./types/Page.mjs";
import type { PrimaryKey, PrimaryKeyRecord, Row } from "./types/Table.mjs";
import type { TableBase } from "./types/Table.mjs";

export type Storage<T extends TableBase> = ReadableStorage<T> &
	WritableStorage<T> &
	Partial<TransactionalStorage<T>> & {
		batch?(mutations: Mutation<T>[]): void;
	};

export type ReadableStorage<T extends TableBase> = {
	findUnique(key: PrimaryKeyRecord<T>): Row<T> | null;
	findMany(pageInput: PageInput<T>): Page<T>;
};

export type WritableStorage<T extends TableBase> = {
	insert(row: Row<T>): void;
	upsert(row: Row<T>): void;
	update(key: PrimaryKeyRecord<T>, partialRow: Partial<Row<T>>): void;
	delete(key: PrimaryKeyRecord<T>): void;

	mutate(mutation: Mutation<T>): void;
	mutateMany(mutations: Mutation<T>[]): void;
};

export interface TransactionalStorage<T extends TableBase> {
	/**
	 * Run a set of operations in a transaction. All changes are committed atomically.
	 * If the callback throws, the transaction is rolled back.
	 */
	transaction<R>(fn: () => R): R;
}

export type Mutation<T extends TableBase> =
	| { type: "insert"; row: Row<T> }
	| { type: "upsert"; row: Row<T> }
	| { type: "update"; key: PrimaryKeyRecord<T>; partialRow: Partial<Row<T>> }
	| { type: "delete"; key: PrimaryKeyRecord<T> };
