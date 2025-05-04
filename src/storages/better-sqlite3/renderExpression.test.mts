import { describe, it, expect } from "vitest";
import { compileSql } from "./renderExpression.mjs";
import type { SqlExpression, TableBase } from "../../types.mjs";

// Dummy table type for testing
interface DummyTable extends TableBase {
	id: number;
	name: string;
	age: number;
}

describe("compileSql / runRenderExpression", () => {
	it("renders simple column = constant", () => {
		const expr: SqlExpression<DummyTable> = {
			kind: "binOp",
			operator: "=",
			left: { kind: "column", name: "age" },
			right: { kind: "constant", value: 30 },
		};
		const [sql, params] = compileSql(expr);
		expect(sql).toBe("(age = ?)");
		expect(params).toEqual([30]);
	});

	it("renders AND/OR and nested expressions", () => {
		const expr: SqlExpression<DummyTable> = {
			kind: "binOp",
			operator: "AND",
			left: {
				kind: "binOp",
				operator: ">",
				left: { kind: "column", name: "age" },
				right: { kind: "constant", value: 18 },
			},
			right: {
				kind: "binOp",
				operator: "OR",
				left: {
					kind: "binOp",
					operator: "=",
					left: { kind: "column", name: "name" },
					right: { kind: "constant", value: "Alice" },
				},
				right: {
					kind: "binOp",
					operator: "=",
					left: { kind: "column", name: "name" },
					right: { kind: "constant", value: "Bob" },
				},
			},
		};
		const [sql, params] = compileSql(expr);
		expect(sql).toBe("((age > ?) AND ((name = ?) OR (name = ?)))");
		expect(params).toEqual([18, "Alice", "Bob"]);
	});

	it("renders NOT expression", () => {
		const expr: SqlExpression<DummyTable> = {
			kind: "unOp",
			operator: "NOT",
			expression: {
				kind: "binOp",
				operator: "=",
				left: { kind: "column", name: "id" },
				right: { kind: "constant", value: 1 },
			},
		};
		const [sql, params] = compileSql(expr);
		expect(sql).toBe("(NOT (id = ?))");
		expect(params).toEqual([1]);
	});

	it("renders column to column comparison", () => {
		const expr: SqlExpression<DummyTable> = {
			kind: "binOp",
			operator: "=",
			left: { kind: "column", name: "id" },
			right: { kind: "column", name: "age" },
		};
		const [sql, params] = compileSql(expr);
		expect(sql).toBe("(id = age)");
		expect(params).toEqual([]);
	});

	it("throws on unsupported expression", () => {
		// @ts-expect-error: purposely invalid
		expect(() => compileSql({ kind: "unknown" })).toThrow();
	});
});
