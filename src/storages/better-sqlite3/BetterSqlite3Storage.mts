import type { Database, Statement } from "better-sqlite3";
import type {
	Mutation,
	ReadableStorage,
	WritableStorage,
} from "../../Storage.mjs";
import {
	invertDirection,
	type Page,
	type PageInput,
} from "../../types/Page.mjs";
import type {
	PrimaryKey,
	PrimaryKeyRecord,
	Row,
} from "../../types/TableSchema.mjs";
import type { TableSchemaBase } from "../../types/TableSchema.mjs";
import { compileSql } from "../../sql/compileSql.mjs";
import type { Source } from "../../sql/Sql.mjs";
import { ands, type SqlExpression } from "../../sql/SqlExpression.mjs";

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
	upsert(
		row: Row<Table>,
		update?: Partial<Omit<Row<Table>, PrimaryKey<Table>[number]>>,
	): void {
		const columns = Object.keys(row);
		const placeholders = columns.map(() => "?").join(", ");
		const updateSet = columns
			.filter((col) => !this.primaryKeys.includes(col))
			.map((col) => `${col} = excluded.${col}`)
			.join(", ");
		const values = columns.map((col) => row[col]);

		const stmt = this.database.prepare(`
      INSERT INTO ${this.tableName} (${columns.join(", ")})
      VALUES (${placeholders})
      ON CONFLICT (${this.primaryKeys.join(", ")}) DO UPDATE SET
      ${updateSet}
    `);

		stmt.run(...values);
	}

	/**
	 * Update a row based on its primary key
	 */
	update(key: PrimaryKeyRecord<Table>, changes: Partial<Row<Table>>): void {
		const setClause = Object.keys(changes)
			.map((k) => `${k} = ?`)
			.join(", ");
		const values = Object.values(changes);
		const keyArr = this.primaryKeys.map(
			(pk: PrimaryKey<Table>[number]) => key[pk],
		);
		const stmt = this.database.prepare(`
			UPDATE ${this.tableName}
			SET ${setClause}
			WHERE ${this.primaryKeys.map((pk) => `${pk} = ?`).join(" AND ")}
		`);
		stmt.run(...values, ...keyArr);
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

	private compileFindMany<HasCursor extends boolean>(
		pageInput: PageInput<Table>,
		hasCursor: HasCursor,
	): CompiledQuery<PageParameter<HasCursor>> {
		const selectCols = "*";

		const pkColumns: SqlExpression<Table> = {
			kind: "tuple",
			expressions: this.primaryKeys.map((pk) => ({ kind: "column", name: pk })),
		};
		const pkParams: SqlExpression<Table> = {
			kind: "tuple",
			expressions: this.primaryKeys.map(
				(pk) =>
					({
						kind: "parameter",
						getValue: (context: PageParameter<true>) =>
							context.cursor ? context.cursor[pk] : undefined,
					}) as SqlExpression<Table>,
			),
		};
		const cursorWhere: SqlExpression<Table> | undefined = hasCursor
			? {
					kind: "binOp",
					operator: pageInput.kind === "leftClosed" ? ">" : "<",
					left: pkColumns,
					right: pkParams,
				}
			: undefined;

		const ast: Source = {
			kind: "select",
			table: this.tableName,
			columns: selectCols,
			where: ands(
				[pageInput.filter, cursorWhere].filter((x) => x !== undefined),
			),
			orderBy:
				pageInput.orderBy && pageInput.orderBy.length > 0
					? pageInput.orderBy.map((o) => ({
							column: o.column,
							direction:
								pageInput.kind === "leftClosed"
									? o.direction
									: invertDirection(o.direction),
						}))
					: this.primaryKeys.map((pk) => ({
							column: pk,
							direction: pageInput.kind === "leftClosed" ? "asc" : "desc",
						})),
			limit: {
				kind: "parameter",
				getValue: (context: PageParameter<HasCursor>) => context.limit,
			},
		};

		const [queryString, getParams] = compileSql(ast);
		return {
			statement: this.database.prepare(queryString),
			getParams,
		};
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

		if (pageInput.kind === "rightClosed") {
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
			rows: rows.map(getCursor),
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
			const insertAst = {
				kind: "insert" as const,
				table: this.tableName,
				values: Object.fromEntries(
					columns.map((col) => [
						col,
						{
							kind: "parameter" as const,
							getValue: (row: Row<Table>) => row[col],
						} as import("../../sql/SqlExpression.mts").ParameterExpression,
					]),
				),
			};
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
			const pkColumns: SqlExpression<Table> = {
				kind: "tuple",
				expressions: this.primaryKeys.map((pk) => ({
					kind: "column",
					name: pk as string & keyof Table["columns"],
				})),
			};
			const pkParams: SqlExpression<Table> = {
				kind: "tuple",
				expressions: this.primaryKeys.map(
					(pk) =>
						({
							kind: "parameter",
							getValue: (key: PrimaryKeyRecord<Table>) =>
								key[pk as string & keyof Table["columns"]],
						}) as SqlExpression<Table>,
				),
			};
			const where: SqlExpression<Table> = {
				kind: "binOp",
				operator: "=",
				left: pkColumns,
				right: pkParams,
			};
			const deleteAst = {
				kind: "delete" as const,
				table: this.tableName,
				where,
			};
			const [sql, getParams] = compileSql(deleteAst);
			this._compiledDelete = {
				statement: this.database.prepare(sql),
				getParams,
			};
		}
		return this._compiledDelete;
	}
}

export type CompiledQuery<Context extends Record<string, unknown>> = {
	statement: Statement;
	getParams: (context?: Context) => unknown[];
};

type PageParameter<HasCursor extends boolean> = {
	limit: number;
} & (HasCursor extends true
	? { cursor: Record<string, unknown> }
	: Record<string, unknown>);
