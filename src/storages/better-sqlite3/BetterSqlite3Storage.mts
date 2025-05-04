import type { Database } from "better-sqlite3";
import type {
	Mutation,
	ReadableStorage,
	WritableStorage,
} from "../../Storage.mjs";
import type { Page, PageInput } from "../../types/Page.mjs";
import type { PrimaryKey, PrimaryKeyRecord, Row } from "../../types/Table.mjs";
import type { TableBase } from "../../types/Table.mjs";
import { compileSql } from "./renderExpression.mjs";
import assert from "assert";

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

	/**
	 * Build SQL and params for findMany (for debugging/testing)
	 */
	compileFindMany(pageInput: PageInput<Table>): CompiledQuery {
		const selectCols = "*";
		const whereClauses: string[] = [];
		const params: unknown[] = [];

		// Filtering
		if (pageInput.filter) {
			const [whereSql, filterParams] = compileSql(pageInput.filter)(() =>
				assert.fail("Unsupported parameter in filter"),
			);
			whereClauses.push(whereSql);
			params.push(...filterParams);
		}

		const orderBy =
			pageInput.orderBy && pageInput.orderBy.length > 0
				? pageInput.orderBy
				: this.primaryKeys.map((pk) => ({
						column: pk,
						direction: "asc" as const,
					}));
		const orderByClause = orderBy
			.map((o) => `${o.column} ${o.direction}`)
			.join(", ");

		if ("after" in pageInput && pageInput.after) {
			for (const pk of this.primaryKeys) {
				whereClauses.push(`${pk} > ?`);
				params.push(pageInput.after[pk as PrimaryKey<Table>[number]]);
			}
		}
		if ("before" in pageInput && pageInput.before) {
			for (const pk of this.primaryKeys) {
				whereClauses.push(`${pk} < ?`);
				params.push(pageInput.before[pk as PrimaryKey<Table>[number]]);
			}
		}

		const whereClause =
			whereClauses.length > 0 ? `WHERE ${whereClauses.join(" AND ")}` : "";

		let sql = `SELECT ${selectCols} FROM ${this.tableName} ${whereClause} ORDER BY ${orderByClause}`;
		if ("first" in pageInput && typeof pageInput.first === "number") {
			sql += " LIMIT ?";
		}
		return [sql, params];
	}

	findMany(pageInput: PageInput<Table>): Page<Table> {
		// Build SELECT clause
		const [baseSql, params] = this.compileFindMany(pageInput);

		let rows: Row<Table>[] = [];
		let limit = 0;

		if ("last" in pageInput && typeof pageInput.last === "number") {
			// Right-closed: fetch all rows before the cursor, take last N
			const stmt = this.database.prepare(baseSql);
			const allRows = stmt.all(...params) as Row<Table>[];
			rows = allRows.slice(-pageInput.last);
		} else if ("first" in pageInput && typeof pageInput.first === "number") {
			// Left-closed: fetch first N rows after the cursor
			limit = pageInput.first;
			const stmt = this.database.prepare(baseSql);
			rows = stmt.all(...params, limit) as Row<Table>[];
		} else {
			// fallback: fetch all
			const stmt = this.database.prepare(baseSql);
			rows = stmt.all(...params) as Row<Table>[];
		}

		// For rowCount, count all rows matching the filter (ignoring limit)
		const whereClause = baseSql.match(/WHERE .+?(?= ORDER BY|$)/)?.[0] ?? "";
		const countSql = `SELECT COUNT(*) as cnt FROM ${this.tableName} ${whereClause}`;
		const countStmt = this.database.prepare(countSql);
		const rowCount = (countStmt.get(...params) as { cnt: number }).cnt;

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

export type CompiledQuery = [sql: string, params: unknown[]];
