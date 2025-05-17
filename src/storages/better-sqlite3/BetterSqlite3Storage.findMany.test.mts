import Database from "better-sqlite3";
import { beforeEach, describe, expect, it } from "vitest";
import type { Page, PageInit } from "../../Page.mjs";
import type { Expression } from "../../RSql/Expression.mjs";
import type { Statement } from "../../RSql/RSql.mjs";
import { compileStatementToSql } from "../../RSql/compileToSql.mjs";
import type { Row } from "../../types/TableSchema.mjs";
import type { TableSchemaBase } from "../../types/TableSchema.mjs";
import { rsqlExpressionToFilterFn } from "../../util/rsqlExpressionToFilterFn.mjs";
import { BetterSqlite3Storage } from "./BetterSqlite3Storage.mjs";

const userSchema = {
	name: "users",
	columns: {
		id: { kind: "number" },
		name: { kind: "string" },
		age: { kind: "number" },
	},
	primaryKey: ["id"] as const,
} satisfies TableSchemaBase;

type UserTable = typeof userSchema;

describe("SqliteStorage.findMany", () => {
	const table = "users";
	it("returns results in orderBy direction for before+last (backward) pagination (Relay spec)", () => {
		const db = new Database(":memory:");
		db.exec(
			"CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)",
		);
		const storage = new BetterSqlite3Storage<UserTable>(userSchema, db);
		for (let i = 1; i <= 10; ++i) {
			storage.insert({ id: i, name: `User${i}`, age: 20 + i });
		}
		// forward pagination
		const forwardInput: PageInit<UserTable, { id: number }> = {
			kind: "forward",
			after: { id: 3 },
			first: 3,
			orderBy: [{ column: "id", direction: "asc" }],
		};
		const forwardPage = storage.findMany(forwardInput);
		const forwardIds = Array.from(forwardPage.rows).map((pk) => pk.id);
		expect(forwardPage.itemBeforeCount).toBe(3);
		expect(forwardPage.itemAfterCount).toBe(4);
		expect(forwardIds).toEqual([4, 5, 6]);
		// backward pagination
		const backwardInput: PageInit<UserTable, { id: number }> = {
			kind: "backward",
			before: { id: 7 },
			last: 3,
			orderBy: [{ column: "id", direction: "asc" }],
		};
		const backwardPage = storage.findMany(backwardInput);
		const backwardIds = Array.from(backwardPage.rows).map((pk) => pk.id);

		expect(backwardPage.itemBeforeCount).toBe(3);
		expect(backwardPage.itemAfterCount).toBe(4);
		expect(backwardIds).toEqual([4, 5, 6]);
	});

	it("throws if orderBy directions are mixed (asc/desc)", () => {
		const db = new Database(":memory:");
		db.exec(
			"CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)",
		);
		const storage = new BetterSqlite3Storage<UserTable>(userSchema, db);
		for (let i = 1; i <= 3; ++i) {
			storage.insert({ id: i, name: `User${i}`, age: 20 + i });
		}
		const pageInput: PageInit<UserTable, { id: number; name: string }> = {
			kind: "forward",
			first: 2,
			orderBy: [
				{ column: "id", direction: "asc" },
				{ column: "name", direction: "desc" },
			],
		};
		expect(() => storage.findMany(pageInput)).toThrow(
			/orderBy must be all ascending or all descending/,
		);
	});

	let db: Database.Database;
	let storage: BetterSqlite3Storage<UserTable>;

	beforeEach(() => {
		db = new Database(":memory:");
		db.exec(
			"CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)",
		);
		storage = new BetterSqlite3Storage<UserTable>(userSchema, db);
		// Insert sample data
		for (let i = 1; i <= 10; ++i) {
			storage.insert({ id: i, name: `User${i}`, age: 20 + i });
		}
	});

	it("left-closed: paginates forward with after+first", () => {
		const pageInput: PageInit<UserTable, { id: number }> = {
			kind: "forward",
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
		const pageInput: PageInit<UserTable, { id: number }> = {
			kind: "backward",
			before: { id: 8 },
			last: 3,
			orderBy: [{ column: "id", direction: "asc" }],
		};
		const page = storage.findMany(pageInput);
		const ids = Array.from(page.rows).map((pk) => pk.id);
		expect(ids).toEqual([5, 6, 7]);
		// expect(page.rowCount).toBe(7); // 7 rows before id=8
		expect(page.startCursor).toEqual({ id: 5 });
		expect(page.endCursor).toEqual({ id: 7 });
	});

	it("supports filtering and all results satisfy conditionToFilter", () => {
		const filterExpr: Expression<UserTable> = {
			kind: "binOp",
			operator: "=",
			left: { kind: "column", name: "age" },
			right: { kind: "constant", value: 25 },
		};
		const pageInput: PageInit<UserTable, { id: number }> = {
			kind: "forward",
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
		const filterFn = rsqlExpressionToFilterFn(filterExpr);
		for (const pk of page.rows) {
			const row = storage.findUnique(pk) as Row<UserTable>;
			expect(filterFn(row)).toBe(true);
		}
	});

	it("supports orderBy descending", () => {
		const pageInput: PageInit<UserTable, { id: number }> = {
			kind: "forward",
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
		const expr: Statement = {
			kind: "select",
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
		const [sql, getParams] = compileStatementToSql(table, expr);
		const params = getParams({ name: "hello" });
		expect(sql).toBe("SELECT * FROM (users) WHERE (?, ?, ?)");
		expect(params.length).toBe(3);
		expect(params[0]).toBe(1);
		expect(params[1]).toBe("hello");
		expect(params[2]).toBe(42);
	});

	it("fetches all users sequentially using cursor pagination", () => {
		const pageSize = 3;
		let after: { id: number } | undefined = undefined;
		const allFetchedIds: number[] = [];
		while (true) {
			const pageInput: PageInit<UserTable, { id: number }> = {
				kind: "forward" as const,
				first: pageSize,
				orderBy: [
					{
						column: "id",
						direction: "asc",
					},
				],
				...(after ? { after } : {}),
			};
			const page: Page<UserTable, { id: number }> = storage.findMany(pageInput);
			const ids = Array.from(page.rows).map((pk) => pk.id);
			if (ids.length === 0) break;
			allFetchedIds.push(...ids);
			if (ids.length < pageSize) break;
			const lastId = ids[ids.length - 1];
			after = lastId !== undefined ? { id: lastId } : undefined;
		}
		expect(allFetchedIds).toEqual([1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
	});
});

const compositeTableSchema = {
	name: "composite",
	columns: {
		id: { kind: "number" },
		sub_id: { kind: "number" },
		name: { kind: "string" },
	},
	primaryKey: ["id", "sub_id"] as const,
} satisfies TableSchemaBase;
type CompositeTable = typeof compositeTableSchema;

describe("SqliteStorage.findMany with composite key", () => {
	let db: Database.Database;
	let storage: BetterSqlite3Storage<CompositeTable>;

	beforeEach(() => {
		db = new Database(":memory:");
		db.exec(
			"CREATE TABLE composite (id INTEGER, sub_id INTEGER, name TEXT, PRIMARY KEY (id, sub_id))",
		);
		storage = new BetterSqlite3Storage<CompositeTable>(
			compositeTableSchema,
			db,
		);
		// Insert sample data: id 1~3, sub_id 1~2
		for (let id = 1; id <= 3; ++id) {
			for (let sub_id = 1; sub_id <= 2; ++sub_id) {
				storage.insert({ id, sub_id, name: `User${id}_${sub_id}` });
			}
		}
	});

	it("fetches all rows sequentially using composite cursor", () => {
		const pageSize = 2;
		let after: { id: number; sub_id: number } | undefined = undefined;
		const allFetched: Array<{ id: number; sub_id: number }> = [];
		while (true) {
			const pageInput: PageInit<
				CompositeTable,
				{ id: number; sub_id: number }
			> = {
				kind: "forward" as const,
				first: pageSize,
				orderBy: [
					{ column: "id", direction: "asc" },
					{ column: "sub_id", direction: "asc" },
				],
				...(after ? { after } : {}),
			};
			const page: Page<CompositeTable, { id: number; sub_id: number }> =
				storage.findMany(pageInput);
			const keys = Array.from(page.rows);
			// Debug: log actual rows returned
			if (keys.length === 0) break;
			allFetched.push(...keys);
			if (keys.length < pageSize) break;
			after = keys[keys.length - 1];
		}
		expect(allFetched).toEqual([
			{ id: 1, sub_id: 1 },
			{ id: 1, sub_id: 2 },
			{ id: 2, sub_id: 1 },
			{ id: 2, sub_id: 2 },
			{ id: 3, sub_id: 1 },
			{ id: 3, sub_id: 2 },
		]);
	});
});
