import { describe, expect, it } from "vitest";
import type { Expression, Tuple } from "./Expression.mjs";
import type { Delete, Select, Statement, Update } from "./RSql.mjs";
import {
	compileExpressionToSql,
	compileStatementToSql,
} from "./compileToSql.mjs";
import type { TableRef } from "../types/TableSchema.mjs";

type Table = {
	name: string;
	columns: {
		foo: { kind: "number" };
		bar: { kind: "string" };
	};
	primaryKey: ["foo"];
};

describe("compileSql", () => {
	const table: TableRef<Table> = {
		kind: "base",
		name: "dummy",
		schema: {
			name: "dummy",
			columns: {
				foo: { kind: "number" },
				bar: { kind: "string" },
			},
			primaryKey: ["foo"],
		},
	};
	it("renders simple column expression with compileSqlExpression", () => {
		const expr: Expression<Table> = { kind: "column", name: "foo" };

		const [sql, getParams] = compileExpressionToSql(expr);
		expect(sql).toBe("foo");
		const params = getParams({});
		expect(params).toEqual([]);
	});

	it("renders constant parameter with compileSqlExpression", () => {
		const expr: Expression<Table> = { kind: "constant", value: 123 };

		const [sql, getParams] = compileExpressionToSql(expr);
		expect(sql).toEqual("?");
		const params = getParams({});
		expect(params).toEqual([123]);
	});

	it("renders tuple expression with compileSql", () => {
		const expr: Select<Table> = {
			kind: "select",
			from: table,
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
		expect(sql).toBe("SELECT * FROM (dummy) WHERE (?, ?, ?)");
		const params = getParams({ name: "hello" });
		expect(params).toEqual([1, "hello", 42]);
	});

	it("renders binary operation expression with compileSql", () => {
		const expr: Select<Table> = {
			kind: "select",
			from: table,
			columns: "*",
			where: {
				kind: "binOp",
				operator: ">",
				left: { kind: "column", name: "foo" },
				right: { kind: "constant", value: 5 },
			},
		};
		const [sql, getParams] = compileStatementToSql(table, expr);
		expect(sql).toBe("SELECT * FROM (dummy) WHERE (foo > ?)");
		const params = getParams({});
		expect(params).toEqual([5]);
	});

	it("renders unary operation expression (NOT) with compileSqlExpression", () => {
		const expr: Expression<Table> = {
			kind: "unOp",
			operator: "NOT",
			expression: { kind: "column", name: "foo" },
		};

		const [sql, getParams] = compileExpressionToSql(expr);
		expect(sql).toBe("(NOT foo)");
		const params = getParams({});
		expect(params).toEqual([]);
	});
	it("renders function expression with compileSqlExpression", () => {
		const expr: Expression<Table> = {
			kind: "function",
			name: "MAX",
			args: [
				{ kind: "column", name: "foo" },
				{ kind: "constant", value: 100 },
			],
		};
		const [sql, getParams] = compileExpressionToSql(expr);
		expect(sql).toBe("MAX(foo, ?)");
		const params = getParams({});
		expect(params).toEqual([100]);
	});

	it("renders insert statement with compileSql", () => {
		const expr: Statement = {
			kind: "insert",
			values: {
				foo: { kind: "parameter", getValue: (ctx: { foo: string }) => ctx.foo },
			},
		};
		const [sql, getParams] = compileStatementToSql(table, expr);
		expect(sql).toBe("INSERT INTO dummy (foo) VALUES (?)");
		const params = getParams({ foo: "bar" });
		expect(params).toEqual(["bar"]);
	});

	it("renders update statement with compileSql", () => {
		const expr: Update<Table> = {
			kind: "update",
			set: {
				bar: { kind: "parameter", getValue: (ctx: { foo: string }) => ctx.foo },
			},
			key: {
				foo: { kind: "constant", value: "bar" },
			},
		};
		const [sql, getParams] = compileStatementToSql(table, expr);
		expect(sql).toBe("UPDATE dummy SET bar = ? WHERE foo = ?");
		const params = getParams({ foo: "baz" });
		expect(params).toEqual(["baz", "bar"]);
	});

	it("renders update statement without where", () => {
		const expr: Update<Table> = {
			kind: "update",
			set: {
				bar: { kind: "constant", value: "qux" },
			},
			key: {
				foo: { kind: "constant", value: "bar" },
			},
		};
		const [sql, getParams] = compileStatementToSql(table, expr);
		expect(sql).toBe("UPDATE dummy SET bar = ? WHERE foo = ?");
		const params = getParams({});
		expect(params).toEqual(["qux", "bar"]);
	});

	it("renders delete statement with where", () => {
		const expr = {
			kind: "delete",
			key: {
				foo: { kind: "constant", value: "bar" },
			},
		} satisfies Delete<Table>;
		const [sql, getParams] = compileStatementToSql(table, expr);
		expect(sql).toBe("DELETE FROM dummy WHERE foo = ?");
		const params = getParams({});
		expect(params).toEqual(["bar"]);
	});
});
