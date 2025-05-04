import { describe, it, expect } from "vitest";
import { sqlExpressionToFilterFn } from "./sqlExpressionToFilterFn.mjs";
import type { Row } from "../types/Table.mjs";
import type { TableBase } from "../types/Table.mjs";
import type { SqlExpression } from "../types/SqlExpression.mjs";

type SimpleTable = {
	columns: {
		id: number;
		age: number;
		name: string;
	};
	primaryKey: ["id"];
};

const row: Row<SimpleTable> = { id: 1, age: 30, name: "Alice" };

describe("sqlExpressionToFilterFn", () => {
	it("handles column = constant", () => {
		const expr: SqlExpression<SimpleTable> = {
			kind: "binOp",
			operator: "=",
			left: { kind: "column", name: "age" },
			right: { kind: "constant", value: 30 },
		};
		const fn = sqlExpressionToFilterFn(expr);
		expect(fn(row)).toBe(true);
		expect(fn({ ...row, age: 25 })).toBe(false);
	});

	it("handles != operator", () => {
		const expr: SqlExpression<SimpleTable> = {
			kind: "binOp",
			operator: "!=",
			left: { kind: "column", name: "name" },
			right: { kind: "constant", value: "Bob" },
		};
		const fn = sqlExpressionToFilterFn(expr);
		expect(fn(row)).toBe(true);
		expect(fn({ ...row, name: "Bob" })).toBe(false);
	});

	it("handles <, <=, >, >= operators", () => {
		const expr: SqlExpression<SimpleTable> = {
			kind: "binOp",
			operator: ">",
			left: { kind: "column", name: "age" },
			right: { kind: "constant", value: 20 },
		};
		const fn = sqlExpressionToFilterFn(expr);
		expect(fn(row)).toBe(true);
		expect(fn({ ...row, age: 20 })).toBe(false);
	});

	it("handles arithmetic operators", () => {
		const expr: SqlExpression<SimpleTable> = {
			kind: "binOp",
			operator: "+",
			left: { kind: "column", name: "age" },
			right: { kind: "constant", value: 10 },
		};
		const fn = sqlExpressionToFilterFn({
			kind: "binOp",
			operator: "=",
			left: expr,
			right: { kind: "constant", value: 40 },
		});
		expect(fn(row)).toBe(true);
		expect(fn({ ...row, age: 20 })).toBe(false);
	});

	it("handles unary operators", () => {
		const expr: SqlExpression<SimpleTable> = {
			kind: "binOp",
			operator: "=",
			left: {
				kind: "unOp",
				operator: "-",
				expression: { kind: "constant", value: 5 },
			},
			right: { kind: "constant", value: -5 },
		};
		const fn = sqlExpressionToFilterFn(expr);
		expect(fn(row)).toBe(true);
	});

	it("handles nested expressions", () => {
		const expr: SqlExpression<SimpleTable> = {
			kind: "binOp",
			operator: "=",
			left: {
				kind: "binOp",
				operator: "+",
				left: { kind: "column", name: "age" },
				right: { kind: "constant", value: 10 },
			},
			right: {
				kind: "binOp",
				operator: "*",
				left: { kind: "constant", value: 2 },
				right: { kind: "constant", value: 20 },
			},
		};
		const fn = sqlExpressionToFilterFn(expr);
		expect(fn({ ...row, age: 30 })).toBe(true); // 30 + 10 == 2 * 20
		expect(fn({ ...row, age: 25 })).toBe(false);
	});
});
