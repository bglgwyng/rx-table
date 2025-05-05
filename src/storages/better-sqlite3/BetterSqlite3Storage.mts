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
import type { PrimaryKey, PrimaryKeyRecord, Row } from "../../types/Table.mjs";
import type { TableBase } from "../../types/Table.mjs";
import { compileSql } from "../../sql/compileSql.mjs";
import assert from "assert";
import type { Source } from "../../sql/Sql.mjs";
import {
	ands,
	type Parameterizable,
	type SqlExpression,
} from "../../sql/SqlExpression.mjs";

export class BetterSqlite3Storage<Table extends TableBase>
	implements WritableStorage<Table>, ReadableStorage<Table>
{
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

	constructor(
		public readonly database: Database,
		public readonly tableName: string,
		public readonly primaryKeys: PrimaryKey<Table>,
	) {}

	/**
	 * Insert a new row into the table
	 */
	insert(row: Row<Table>): void {
		const columns = Object.keys(row);
		const placeholders = columns.map(() => "?").join(", ");
		const values = columns.map((col) => row[col]);

		const stmt = this.database.prepare(`
      INSERT INTO ${this.tableName} (${columns.join(", ")})
      VALUES (${placeholders})
    `);

		stmt.run(...values);
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
		const keyArr = this.primaryKeys.map(
			(pk: PrimaryKey<Table>[number]) => key[pk],
		);
		const stmt = this.database.prepare(`
			DELETE FROM ${this.tableName}
			WHERE ${this.primaryKeys.map((pk) => `${pk} = ?`).join(" AND ")}
		`);
		stmt.run(...keyArr);
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

		const cursorWhere: SqlExpression<Table> | undefined = hasCursor
			? pageInput.kind === "leftClosed"
				? ands(
						this.primaryKeys.map(
							(pk): SqlExpression<Table> => ({
								kind: "binOp",
								operator: ">",
								left: { kind: "column", name: pk },
								right: {
									kind: "parameter",
									getValue: (context: PageParameter<true>) =>
										context.cursor ? context.cursor[pk] : undefined,
								},
							}),
						),
					)
				: ands(
						this.primaryKeys.map(
							(pk): SqlExpression<Table> => ({
								kind: "binOp",
								operator: "<",
								left: { kind: "column", name: pk },
								right: {
									kind: "parameter",
									getValue: (context: PageParameter<true>) =>
										context.cursor[pk],
								},
							}),
						),
					)
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
		return [this.database.prepare(queryString), getParams];
	}

	findMany(pageInput: PageInput<Table>): Page<Table> {
		// Build SELECT clause
		const [stmt, getParams] = this.compileFindMany(pageInput, false);
		const [stmtWithCursor, getParamsWithCursor] = this.compileFindMany(
			pageInput,
			true,
		);

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
			startCursor: getCursor(rows[0]!),
			// biome-ignore lint/style/noNonNullAssertion: <explanation>
			endCursor: getCursor(rows.at(-1)!),
		};
	}
}

export type CompiledQuery<Context extends Record<string, unknown>> = readonly [
	statment: Statement,
	params: (context?: Context) => unknown[],
];

type PageParameter<HasCursor extends boolean> = {
	limit: number;
} & (HasCursor extends true
	? { cursor: Record<string, unknown> }
	: Record<string, unknown>);
