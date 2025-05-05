import type { Database, Statement } from "better-sqlite3";
import type {
	Mutation,
	ReadableStorage,
	WritableStorage,
} from "../../Storage.mjs";
import { invertDirection, type Page, type PageInput } from "../../Page.mjs";
import type {
	PrimaryKey,
	PrimaryKeyRecord,
	Row,
} from "../../types/TableSchema.mjs";
import type { TableSchemaBase } from "../../types/TableSchema.mjs";
import { compileSql } from "../../sql/compileSql.mjs";
import type { Delete, Insert, Select, Source, Update } from "../../sql/Sql.mjs";
import {
	ands,
	type ParameterExpression,
	type Parameterizable,
	type SqlExpression,
} from "../../sql/SqlExpression.mjs";
import {
	mkDelete,
	mkEq,
	mkGT,
	mkGTE,
	mkInsert,
	mkLT,
	mkParameter,
	mkPkColumns,
	mkPkParams,
	mkSelect,
	mkUpdate,
} from "../../sql/mks.mjs";
import assert from "assert";

export class BetterSqlite3Storage<Table extends TableSchemaBase>
	implements WritableStorage<Table>, ReadableStorage<Table>
{
	constructor(
		public readonly schema: Table,
		public readonly database: Database,
	) {}

	get tableName() {
		return this.schema.name;
	}

	get primaryKeys() {
		return this.schema.primaryKey;
	}

	mutate(mutation: Mutation<Table>): void {
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
			default:
				throw new Error("Unknown mutation type");
		}
	}

	mutateMany(mutations: Mutation<Table>[]): void {
		this.database.transaction(() => {
			for (const m of mutations) {
				this.mutate(m);
			}
		})();
	}

	/**
	 * Insert a new row into the table
	 */
	insert(row: Row<Table>): void {
		const { statement, getParams } = this.compiledInsert;
		statement.run(...getParams(row));
	}

	/**
	 * Insert a row or update it if it already exists (based on primary key)
	 */
	upsert(row: Row<Table>): void {
		const columns = Object.keys(row) as (keyof Row<Table>)[];
		const values = Object.fromEntries(
			columns.map((col) => [col, mkParameter((row: Row<Table>) => row[col])]),
		) as Record<keyof Row<Table>, ParameterExpression>;
		const set = Object.fromEntries(
			columns
				.filter((col) => !this.primaryKeys.includes(col as string))
				.map((col) => [col, mkParameter((row: Row<Table>) => row[col])]),
		) as Record<string, ParameterExpression>;
		const insertAst: Insert<Table> = mkInsert(this.schema, values, {
			onConflict: {
				columns: this.primaryKeys.map((pk) => pk.toString()),
				do: {
					kind: "update" as const,
					set,
				},
			},
		});
		const [sql, getParams] = compileSql(insertAst);
		const stmt = this.database.prepare(sql);
		stmt.run(...getParams(row));
	}

	/**
	 * Update a row based on its primary key
	 */
	update(key: PrimaryKeyRecord<Table>, changes: Partial<Row<Table>>): void {
		const columns = Object.keys(changes) as (keyof Row<Table>)[];
		if (columns.length === 0) return;

		const set = Object.fromEntries(
			columns.map((col) => [
				col,
				mkParameter(
					(ctx: {
						changes: Partial<Row<Table>>;
						key: PrimaryKeyRecord<Table>;
					}) => ctx.changes[col],
				),
			]),
		) as Record<keyof Row<Table>, ParameterExpression>;

		const pkColumns = mkPkColumns(this.schema);
		const pkParams = mkPkParams(
			this.schema,
			({ key }: { key: PrimaryKeyRecord<Table> }) => key,
		);
		const where: SqlExpression<Table> = mkEq(pkColumns, pkParams);

		const updateAst: Update<Table> = mkUpdate(this.schema, set, where);

		const [sql, getParamsRaw] = compileSql(updateAst);
		const getParams = (
			changes: Partial<Row<Table>>,
			key: PrimaryKeyRecord<Table>,
		) => {
			return getParamsRaw({ changes, key });
		};

		const stmt = this.database.prepare(sql);
		stmt.run(...getParams(changes, key));
	}

	delete(key: PrimaryKeyRecord<Table>): void {
		const { statement, getParams } = this.compiledDelete;
		statement.run(...getParams(key));
	}

	findUnique(key: PrimaryKeyRecord<Table>): Row<Table> | null {
		const stmt = this.database.prepare(`
			SELECT * FROM ${this.tableName}
			WHERE ${this.primaryKeys.map((pk) => `${pk} = ?`).join(" AND ")}
		`);
		const row = stmt.get(
			...this.primaryKeys.map((pk: PrimaryKey<Table>[number]) => key[pk]),
		);

		return row === undefined ? null : (row as Row<Table>);
	}

	findMany(pageInput: PageInput<Table>): Page<Table> {
		// Build SELECT clause
		const { statement: stmt, getParams } = this.compileFindMany(
			pageInput,
			false,
		);
		const { statement: stmtWithCursor, getParams: getParamsWithCursor } =
			this.compileFindMany(pageInput, true);

		let rows: Row<Table>[] = [];
		let limit = 0;

		if (pageInput.kind === "backward") {
			// Right-closed: fetch all rows before the cursor, take last N

			if (pageInput.before === undefined) {
				limit = pageInput.last;
				rows = stmt.all(
					...getParams({ limit: pageInput.last }),
				) as Row<Table>[];
			} else {
				limit = pageInput.last;
				rows = stmtWithCursor.all(
					...getParamsWithCursor({
						cursor: pageInput.before,
						limit: pageInput.last,
					}),
				) as Row<Table>[];
			}
		} else {
			// Left-closed: fetch first N rows after the cursor
			if (pageInput.after === undefined) {
				limit = pageInput.first;
				rows = stmt.all(...getParams({ limit })) as Row<Table>[];
			} else {
				limit = pageInput.first;
				rows = stmtWithCursor.all(
					...getParamsWithCursor({
						cursor: pageInput.after,
						limit: pageInput.first,
					}),
				) as Row<Table>[];
			}
		}

		// For rowCount, count all rows matching the filter (ignoring limit)
		// const whereClause = stmt.sql.match(/WHERE .+?(?= ORDER BY|$)/)?.[0] ?? "";
		// const countSql = `SELECT COUNT(*) as cnt FROM ${this.tableName} ${whereClause}`;
		// const countStmt = this.database.prepare(countSql);
		const rowCount = 0;
		// (countStmt.get(...getParams()) as { cnt: number }).cnt;

		const getCursor = (row: Row<Table>): PrimaryKeyRecord<Table> =>
			Object.fromEntries(
				this.primaryKeys.map((pk) => [pk, row[pk]]),
			) as PrimaryKeyRecord<Table>;

		return {
			rows: (pageInput.kind === "forward" ? rows : rows.reverse()).map(
				getCursor,
			),
			rowCount,
			// biome-ignore lint/style/noNonNullAssertion: <explanation>
			startCursor: rows.length > 0 ? getCursor(rows[0]!) : undefined,
			// biome-ignore lint/style/noNonNullAssertion: <explanation>
			endCursor: rows.length > 0 ? getCursor(rows.at(-1)!) : undefined,
		};
	}

	private _compiledInsert?: CompiledQuery<Row<Table>>;
	private _compiledDelete?: CompiledQuery<PrimaryKeyRecord<Table>>;

	private get compiledInsert(): CompiledQuery<Row<Table>> {
		if (!this._compiledInsert) {
			const columns = Object.keys(this.schema.columns);
			const insertAst = mkInsert(
				this.schema,
				Object.fromEntries(
					columns.map((col) => [
						col,
						mkParameter((row: Row<Table>) => row[col]),
					]),
				) as { [key in keyof Row<Table>]: Parameterizable },
			);
			const [sql, getParams] = compileSql(insertAst);
			this._compiledInsert = {
				statement: this.database.prepare(sql),
				getParams,
			};
		}
		return this._compiledInsert;
	}

	private get compiledDelete(): CompiledQuery<PrimaryKeyRecord<Table>> {
		if (!this._compiledDelete) {
			const pkColumns = mkPkColumns(this.schema);
			const pkParams = mkPkParams(
				this.schema,
				(key: PrimaryKeyRecord<Table>) => key,
			);
			const where: SqlExpression<Table> = mkEq(pkColumns, pkParams);
			const deleteAst: Delete<Table> = mkDelete(this.schema, where);

			const [sql, getParams] = compileSql(deleteAst);
			this._compiledDelete = {
				statement: this.database.prepare(sql),
				getParams,
			};
		}
		return this._compiledDelete;
	}

	private compileFindMany<HasCursor extends boolean>(
		pageInput: PageInput<Table>,
		hasCursor: HasCursor,
	): CompiledQuery<PageParameter<Table, HasCursor>> {
		assert(
			pageInput.orderBy.every((o) => o.direction === "asc") ||
				pageInput.orderBy.every((o) => o.direction === "desc"),
			"orderBy must be all ascending or all descending",
		);
		assert(pageInput.orderBy.length > 0, "orderBy must not be empty");
		assert(
			this.schema.primaryKey.every((pk) =>
				pageInput.orderBy.some((o) => o.column === pk),
			),
			"orderBy must include all primary key columns",
		);
		const selectCols = "*";

		const pkColumns = mkPkColumns(this.schema);
		const pkParams = mkPkParams(
			this.schema,
			(context: PageParameter<Table, true>) => context.cursor,
		);

		const cursorWhere: SqlExpression<Table> | undefined = hasCursor
			? (pageInput.kind === "forward" ? mkGT : mkLT)(pkColumns, pkParams)
			: undefined;

		const ast: Select<Table> = mkSelect(this.schema, selectCols, {
			where: ands(
				[pageInput.filter, cursorWhere].filter((x) => x !== undefined),
			),
			orderBy: pageInput.orderBy.map((o) => ({
				column: o.column,
				direction:
					pageInput.kind === "forward"
						? o.direction
						: invertDirection(o.direction),
			})),
			limit: mkParameter(
				(context: PageParameter<Table, HasCursor>) => context.limit,
			),
		});

		const [queryString, getParams] = compileSql(ast);
		return {
			statement: this.database.prepare(queryString),
			getParams,
		};
	}
}

export type CompiledQuery<Context extends Record<string, unknown>> = {
	statement: Statement;
	getParams: (context?: Context) => unknown[];
};

type PageParameter<
	TableSchema extends TableSchemaBase,
	HasCursor extends boolean,
> = {
	limit: number;
} & (HasCursor extends true
	? { cursor: PrimaryKeyRecord<TableSchema> }
	: Record<string, unknown>);
