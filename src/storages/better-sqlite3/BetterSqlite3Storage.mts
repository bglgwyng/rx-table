import type { Database } from "better-sqlite3";
import type { Count, Mutation, Select } from "../../RSql/RSql.mjs";
import {
	compileMutationToSql,
	compileQueryToSql,
} from "../../RSql/compileToSql.mjs";
import type { Row, TableRef } from "../../types/TableSchema.mjs";
import type { TableSchemaBase } from "../../types/TableSchema.mjs";
import type {
	PreparedCount,
	PreparedMutation,
	PreparedQueryAll,
	PreparedQueryOne,
} from "../../types/PreparedStatement.mjs";
import type { Storage } from "../../Storage.mjs";

export class BetterSqlite3Storage<
	Schema extends Record<string, TableSchemaBase>,
> implements Storage<Schema>
{
	constructor(
		public readonly schema: Schema,
		public readonly database: Database,
	) {
		// this.preparedInsert = this.prepareMutation(mkInsertRow(this.table));
		// this.preparedUpsert = this.prepareMutation(mkUpsertRow(this.table));
		// this.preparedDelete = this.prepareMutation(mkDeleteRow(this.table));
		// this.preparedFindUnique = this.prepareQueryOne(mkFindUnique(this.schema));
	}

	getTable<K extends string & keyof Schema>(name: K): TableRef<Schema[K]> {
		return { kind: "base", name, schema: this.schema[name] };
	}

	get primaryKeys() {
		return this.schema.primaryKey;
	}

	prepareFindOne<Context, Table extends TableSchemaBase>(
		query: Select<Table>,
	): PreparedQueryOne<Context, Row<Table>> {
		const [sql, getParams] = compileQueryToSql(query);
		const stmt = this.database.prepare(sql);
		return (context?: Context) =>
			(stmt.get(...getParams(context)) as Row<Table> | undefined) ?? null;
	}

	prepareFindMany<Context, Table extends TableSchemaBase>(
		query: Select<Table>,
	): PreparedQueryAll<Context, Row<Table>> {
		const [sql, getParams] = compileQueryToSql(query);
		const stmt = this.database.prepare(sql);
		return (context?: Context) =>
			stmt.all(...getParams(context)) as Row<Table>[];
	}

	prepareCount<Context, Table extends TableSchemaBase>(
		query: Count<Table>,
	): PreparedCount<Context> {
		const [sql, getParams] = compileQueryToSql(query);
		const stmt = this.database.prepare(sql);
		return (context?: Context) =>
			(stmt.get(...getParams(context)) as { "COUNT(*)": number })["COUNT(*)"];
	}

	prepareMutation<Context, Table extends TableSchemaBase>(
		mutation: Mutation<Table>,
	): PreparedMutation<Context> {
		const [sql, getParams] = compileMutationToSql<Table, Context>(mutation);
		const stmt = this.database.prepare(sql);

		return (context?: Context) => stmt.run(...getParams(context));
	}

	// mutate(mutation: Mutation<TableSchema>): void {
	// 	switch (mutation.type) {
	// 		case "insert":
	// 			this.insert(mutation.row);
	// 			break;
	// 		case "upsert":
	// 			this.upsert(mutation.row);
	// 			break;
	// 		case "update":
	// 			this.update(mutation.key, mutation.partialRow);
	// 			break;
	// 		case "delete":
	// 			this.delete(mutation.key);
	// 			break;
	// 		default:
	// 			throw new Error("Unknown mutation type");
	// 	}
	// }

	// mutateMany<Table extends TableSchemaBase>(
	// 	mutations: Mutation<Table>[],
	// ): void {
	// 	this.database.transaction(() => {
	// 		for (const m of mutations) {
	// 			this.mutate(m);
	// 		}
	// 	})();
	// }

	// insertz<Table extends TableSchemaBase>(row: Row<Table>): void {
	// 	this.preparedInsert(row);
	// }

	// upsert<Table extends TableSchemaBase>(row: Row<Table>): void {
	// 	this.preparedUpsert(row);
	// }

	// update<Table extends TableSchemaBase>(
	// 	key: PrimaryKeyRecord<Table>,
	// 	changes: Partial<Row<Table>>,
	// ): void {
	// 	const columns = Object.keys(changes) as (keyof Row<Table>)[];
	// 	if (columns.length === 0) return;

	// 	const set = Object.fromEntries(
	// 		columns.map((col) => [
	// 			col,
	// 			mkParameter(
	// 				(ctx: {
	// 					changes: Partial<Row<Table>>;
	// 					key: PrimaryKeyRecord<Table>;
	// 				}) => ctx.changes[col],
	// 			),
	// 		]),
	// 	) as Record<keyof Row<Table>, Parameter>;

	// 	const pkParams = mkPkRecords(
	// 		this.schema,
	// 		({ key }: { key: PrimaryKeyRecord<Table> }) => key,
	// 	);
	// 	const updateAst: Update<Table> = mkUpdate(this.table, set, pkParams);

	// 	const [sql, getParamsRaw] = compileMutationToSql(updateAst);

	// 	const stmt = this.database.prepare(sql);
	// 	stmt.run(...getParamsRaw({ changes, key }));
	// }

	// delete(key: PrimaryKeyRecord<TableSchema>): void {
	// 	this.preparedDelete(key);
	// }

	// private preparedInsert: PreparedMutation<Row<TableSchema>>;
	// private preparedUpsert: PreparedMutation<Row<TableSchema>>;
	// private preparedDelete: PreparedMutation<PrimaryKeyRecord<TableSchema>>;
	// private preparedFindUnique: PreparedQueryOne<
	// 	PrimaryKeyRecord<TableSchema>,
	// 	Row<TableSchema>
	// >;
}
