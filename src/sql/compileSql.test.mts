import { describe, it, expect } from "vitest";
import { compileSql, compileSqlExpression } from "./compileSql.mjs";
import type { SqlExpression, TupleExpression } from "./SqlExpression.mjs";
import type { Source } from "./Sql.mjs";

type ColumnType = {
	kind: "number" | "string";
};

type TableBase = {
	name: string;
	columns: Record<string, ColumnType>;
	primaryKey: string[];
};

type Table = TableBase & {
	columns: {
		foo: { kind: "number" };
		bar: { kind: "string" };
	};
	primaryKey: ["foo"];
};

describe("compileSql", () => {
	it("renders simple column expression with compileSqlExpression", () => {
		const expr: SqlExpression<Table> = { kind: "column", name: "foo" };

		const [sql, getParams] = compileSqlExpression(expr);
		expect(sql).toBe("foo");
		const params = getParams({});
		expect(params).toEqual([]);
	});

	it("renders constant parameter with compileSqlExpression", () => {
		const expr: SqlExpression<Table> = { kind: "constant", value: 123 };

		const [sql, getParams] = compileSqlExpression(expr);
		expect(sql).toEqual("?");
		const params = getParams({});
		expect(params).toEqual([123]);
	});

	it("renders tuple expression with compileSql", () => {
		const expr: Source = {
			kind: "select",
			table: "dummy",
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
		expect(sql).toBe("SELECT * FROM dummy WHERE (?, ?, ?)");
		const params = getParams({ name: "hello" });
		expect(params).toEqual([1, "hello", 42]);
	});

	it("renders binary operation expression with compileSql", () => {
		const expr: Source = {
			kind: "select",
			table: "dummy",
			columns: "*",
			where: {
				kind: "binOp",
				operator: ">",
				left: { kind: "column", name: "foo" },
				right: { kind: "constant", value: 5 },
			},
		};
		const [sql, getParams] = compileSql(expr);
		expect(sql).toBe("SELECT * FROM dummy WHERE (foo > ?)");
		const params = getParams({});
		expect(params).toEqual([5]);
	});

	it("renders unary operation expression (NOT) with compileSqlExpression", () => {
		const expr: SqlExpression<Table> = {
			kind: "unOp",
			operator: "NOT",
			expression: { kind: "column", name: "foo" },
		};

		const [sql, getParams] = compileSqlExpression(expr);
		expect(sql).toBe("(NOT foo)");
		const params = getParams({});
		expect(params).toEqual([]);
	});
	it("renders function expression with compileSqlExpression", () => {
		const expr: SqlExpression<Table> = {
			kind: "function",
			name: "MAX",
			args: [
				{ kind: "column", name: "foo" },
				{ kind: "constant", value: 100 },
			],
		};
		const [sql, getParams] = compileSqlExpression(expr);
		expect(sql).toBe("MAX(foo, ?)");
		const params = getParams({});
		expect(params).toEqual([100]);
	});

	it("renders insert statement with compileSql", () => {
		const expr: Source = {
			kind: "insert",
			table: "dummy",
			values: {
				foo: { kind: "parameter", getValue: (ctx: { foo: string }) => ctx.foo },
			},
		};
		const [sql, getParams] = compileSql(expr);
		expect(sql).toBe("INSERT INTO dummy (foo) VALUES (?)");
		const params = getParams({ foo: "bar" });
		expect(params).toEqual(["bar"]);
	});

	it("renders update statement with compileSql", () => {
		const expr: Source = {
			kind: "update",
			table: "dummy",
			set: {
				foo: { kind: "parameter", getValue: (ctx: { foo: string }) => ctx.foo },
			},
			where: {
				kind: "binOp",
				operator: "=",
				left: { kind: "column", name: "foo" },
				right: { kind: "constant", value: "bar" },
			},
		};
		const [sql, getParams] = compileSql(expr);
		expect(sql).toBe("UPDATE dummy SET foo = ? WHERE (foo = ?)");
		const params = getParams({ foo: "baz" });
		expect(params).toEqual(["baz", "bar"]);
	});

	it("renders update statement without where", () => {
		const expr: Source = {
			kind: "update",
			table: "dummy",
			set: {
				foo: { kind: "constant", value: "baz" },
			},
		};
		const [sql, getParams] = compileSql(expr);
		expect(sql).toBe("UPDATE dummy SET foo = ?");
		const params = getParams({});
		expect(params).toEqual(["baz"]);
	});

	it("renders delete statement with where", () => {
		const expr: Source = {
			kind: "delete",
			table: "dummy",
			where: {
				kind: "binOp",
				operator: "=",
				left: { kind: "column", name: "foo" },
				right: { kind: "constant", value: "bar" },
			},
		};
		const [sql, getParams] = compileSql(expr);
		expect(sql).toBe("DELETE FROM dummy WHERE (foo = ?)");
		const params = getParams({});
		expect(params).toEqual(["bar"]);
	});

	it("renders delete statement without where", () => {
		const expr: Source = {
			kind: "delete",
			table: "dummy",
		};
		const [sql, getParams] = compileSql(expr);
		expect(sql).toBe("DELETE FROM dummy");
		const params = getParams({});
		expect(params).toEqual([]);
	});
});
