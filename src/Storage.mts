import assert from "assert";
import { findMany, type Page, type PageInit } from "./Page.mjs";
import {
	mkDeleteRow,
	mkFindUnique,
	mkInsertRow,
	mkUpsertRow,
} from "./RSql/mkHelpers.mjs";
import { mkInsert, mkParameter, mkPkRecords, mkUpdate } from "./RSql/mks.mjs";
import type {
	Count,
	Delete,
	Insert,
	Mutation,
	Select,
	Update,
} from "./RSql/RSql.mjs";
import type {
	PreparedCount,
	PreparedMutation,
	PreparedQueryAll,
	PreparedQueryOne,
} from "./types/PreparedStatement.mjs";
import type {
	PrimaryKey,
	PrimaryKeyRecord,
	Row,
	TableRef,
} from "./types/TableSchema.mjs";
import type { TableSchemaBase } from "./types/TableSchema.mjs";
import type { Parameter } from "./RSql/Expression.mjs";
import { compileMutationToSql } from "./RSql/compileToSql.mjs";

export type Storage<Schema extends Record<string, TableSchemaBase>> = {
	getTable<K extends string & keyof Schema>(table: K): TableRef<Schema[K]>;

	prepareCount<Context, Table extends TableSchemaBase>(
		query: Count<Table>,
	): PreparedCount<Context>;
	prepareFindOne<Context, Table extends TableSchemaBase>(
		query: Select<Table>,
	): PreparedQueryOne<Context, Row<Table>>;
	prepareFindMany<Context, Table extends TableSchemaBase>(
		query: Select<Table>,
	): PreparedQueryAll<Context, Row<Table>>;
	prepareMutation<Context, Table extends Schema[string]>(
		mutation: Mutation<Table>,
	): PreparedMutation<Context>;
};

// TODO: remove
export type TableStorage<TableSchema extends TableSchemaBase> = {
	storage: Storage<Record<string, TableSchemaBase>>;

	insert(row: Row<TableSchema>): void;
	upsert(row: Row<TableSchema>): void;
	update(
		key: PrimaryKeyRecord<TableSchema>,
		partialRow: Partial<
			Omit<Row<TableSchema>, PrimaryKey<TableSchema>[number]>
		>,
	): void;
	delete(key: PrimaryKeyRecord<TableSchema>): void;

	prepareQueryOne<Context, Row>(
		query: Select<TableSchema>,
	): PreparedQueryOne<Context, Row>;
	prepareQueryAll<Context, Row>(
		query: Select<TableSchema>,
	): PreparedQueryAll<Context, Row>;

	findUnique(key: PrimaryKeyRecord<TableSchema>): Row<TableSchema> | null;
	findMany<Cursor extends PrimaryKeyRecord<TableSchema>>(
		pageInput: PageInit<TableSchema, Cursor>,
	): Page<TableSchema, Cursor>;

	mutate(mutation: Mutation2<TableSchema>): void;
	mutateMany(mutations: Mutation2<TableSchema>[]): void;
};

export type Mutation2<T extends TableSchemaBase> =
	| { type: "insert"; row: Row<T> }
	| { type: "upsert"; row: Row<T> }
	| { type: "update"; key: PrimaryKeyRecord<T>; partialRow: Partial<Row<T>> }
	| { type: "delete"; key: PrimaryKeyRecord<T> };

export function createTableStorage<
	Schema extends Record<string, TableSchemaBase>,
	K extends string & keyof Schema,
>(storage: Storage<Schema>, tableName: K): TableStorage<Schema[K]> {
	const tableRef = storage.getTable(tableName);
	const preparedInsert = storage.prepareMutation(mkInsertRow(tableRef));
	const preparedUpsert = storage.prepareMutation(mkUpsertRow(tableRef));
	const preparedDelete = storage.prepareMutation(mkDeleteRow(tableRef));
	const preparedFindUnique = storage.prepareFindOne(
		mkFindUnique(tableRef.schema),
	);
	return {
		storage,
		insert(row) {
			preparedInsert(row);
		},
		upsert(row) {
			preparedUpsert(row);
		},
		update(
			key: PrimaryKeyRecord<Schema[K]>,
			changes: Partial<Omit<Row<Schema[K]>, PrimaryKey<Schema[K]>[number]>>,
		) {
			const columns = Object.keys(changes) as (keyof Row<Schema[K]>)[];
			if (columns.length === 0) return;

			const set = Object.fromEntries(
				columns.map((col) => [
					col,
					mkParameter(
						(ctx: {
							changes: Partial<Row<Schema[K]>>;
							key: PrimaryKeyRecord<Schema[K]>;
						}) => ctx.changes[col],
					),
				]),
			) as Record<keyof Row<Schema[K]>, Parameter>;

			const pkParams = mkPkRecords(
				tableRef.schema,
				({ key }: { key: PrimaryKeyRecord<Schema[K]> }) => key,
			);
			const updateAst: Update<Schema[K]> = mkUpdate(tableRef, set, pkParams);

			const stmt = storage.prepareMutation(updateAst);
			stmt({ key, changes });
		},
		delete(key: PrimaryKeyRecord<Schema[K]>) {
			preparedDelete(key);
		},
		prepareQueryOne: <Context, Row>(
			query: Select<Schema[K]>,
		): PreparedQueryOne<Context, Row> => {
			throw new Error("Function not implemented.");
		},
		prepareQueryAll: <Context, Row>(
			query: Select<Schema[K]>,
		): PreparedQueryAll<Context, Row> => {
			throw new Error("Function not implemented.");
		},
		findUnique: (key: PrimaryKeyRecord<Schema[K]>): Row<Schema[K]> | null => {
			return preparedFindUnique(key);
		},
		findMany: <Cursor extends PrimaryKeyRecord<Schema[K]>>(
			pageInput: PageInit<Schema[K], Cursor>,
		): Page<Schema[K], Cursor> => {
			return findMany(storage, tableRef, pageInput);
		},
		mutate(mutation: Mutation2<Schema[K]>) {
			switch (mutation.type) {
				case "insert":
					this.insert(mutation.row);
					break;
				case "upsert":
					this.upsert(mutation.row);
					break;
				case "update":
					this.update(mutation.key, mutation.partialRow);
					break;
				case "delete":
					this.delete(mutation.key);
					break;
			}
		},
		mutateMany(mutations: Mutation2<Schema[K]>[]) {
			for (const mutation of mutations) {
				this.mutate(mutation);
			}
		},
	};
}
