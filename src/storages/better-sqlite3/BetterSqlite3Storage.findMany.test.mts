import { describe, it, expect, beforeEach } from "vitest";
import Database from "better-sqlite3";
import { BetterSqlite3Storage } from "./BetterSqlite3Storage.mjs";
import { sqlExpressionToFilterFn } from "../../util/sqlExpressionToFilterFn.mjs";
import type { PageInput } from "../../types/Page.mjs";
import type { Row } from "../../types/Table.mjs";
import type {
	SqlExpression,
	TupleExpression,
} from "../../types/SqlExpression.mjs";
import { compileSql, renderSql } from "./renderExpression.mjs";
import type { Sql } from "../../types/Sql.mjs";

type UserTable = {
	columns: {
		id: number;
		name: string;
		age: number;
	};
	primaryKey: ["id"];
};

describe("SqliteStorage.findMany", () => {
	let db: Database.Database;
	let storage: BetterSqlite3Storage<UserTable>;

	beforeEach(() => {
		db = new Database(":memory:");
		db.exec(
			"CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)",
		);
		storage = new BetterSqlite3Storage<UserTable>(db, "users", ["id"]);
		// Insert sample data
		for (let i = 1; i <= 10; ++i) {
			storage.insert({ id: i, name: `User${i}`, age: 20 + i });
		}
	});

	it("left-closed: paginates forward with after+first", () => {
		const pageInput: PageInput<UserTable> = {
			kind: "leftClosed",
			after: { id: 3 },
			first: 4,
			orderBy: [{ column: "id", direction: "asc" }],
		};
		const page = storage.findMany(pageInput);
		const ids = Array.from(page.rows).map((pk) => pk.id);
		expect(ids).toEqual([4, 5, 6, 7]);
		// expect(page.rowCount).toBe(7); // 10 total - 3 skipped
		expect(page.startCursor).toEqual({ id: 4 });
		expect(page.endCursor).toEqual({ id: 7 });
	});

	it("right-closed: paginates backward with before+last", () => {
		const pageInput: PageInput<UserTable> = {
			kind: "rightClosed",
			before: { id: 8 },
			last: 3,
			orderBy: [{ column: "id", direction: "asc" }],
		};
		const page = storage.findMany(pageInput);
		const ids = Array.from(page.rows).map((pk) => pk.id);
		expect(ids).toEqual([7, 6, 5]);
		// expect(page.rowCount).toBe(7); // 7 rows before id=8
		expect(page.startCursor).toEqual({ id: 7 });
		expect(page.endCursor).toEqual({ id: 5 });
	});

	it("supports filtering and all results satisfy conditionToFilter", () => {
		const filterExpr: SqlExpression<UserTable> = {
			kind: "binOp",
			operator: "=",
			left: { kind: "column", name: "age" },
			right: { kind: "constant", value: 25 },
		};
		const pageInput: PageInput<UserTable> = {
			kind: "leftClosed",
			first: 2,
			orderBy: [{ column: "id", direction: "asc" }],
			filter: filterExpr,
		};
		const page = storage.findMany(pageInput);
		const ids = Array.from(page.rows).map((pk) => pk.id);
		expect(ids).toEqual([5]);
		// expect(page.rowCount).toBe(1);
		expect(page.startCursor).toEqual({ id: 5 });
		expect(page.endCursor).toEqual({ id: 5 });

		// Validate all returned rows satisfy the filter
		const filterFn = sqlExpressionToFilterFn(filterExpr);
		for (const pk of page.rows) {
			const row = storage.findUnique(pk) as Row<UserTable>;
			expect(filterFn(row)).toBe(true);
		}
	});

	it("supports orderBy descending", () => {
		const pageInput: PageInput<UserTable> = {
			kind: "leftClosed",
			first: 3,
			orderBy: [{ column: "id", direction: "desc" }],
		};
		const page = storage.findMany(pageInput);
		const ids = Array.from(page.rows).map((pk) => pk.id);
		expect(ids).toEqual([10, 9, 8]);
		// expect(page.rowCount).toBe(10);
		expect(page.startCursor).toEqual({ id: 10 });
		expect(page.endCursor).toEqual({ id: 8 });
	});

	it("should render tuple of constants and parameters", () => {
		const expr: Sql = {
			kind: "select",
			table: "users",
			columns: "*",
			where: {
				kind: "tuple",
				expressions: [
					{ kind: "constant", value: 1 },
					{ kind: "parameter", getValue: (ctx: { name: string }) => ctx.name },
					{ kind: "constant", value: 42 },
				],
			},
		};
		const [sql, getParams] = compileSql(expr);
		const params = getParams({ name: "hello" });
		expect(sql).toBe("SELECT * FROM users WHERE (?, ?, ?)");
		expect(params.length).toBe(3);
		expect(params[0]).toBe(1);
		expect(params[1]).toBe("hello");
		expect(params[2]).toBe(42);
	});
});
