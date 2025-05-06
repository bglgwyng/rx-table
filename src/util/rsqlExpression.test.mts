import { describe, expect, it } from "vitest";
import type { Expression } from "../RSql/Expression.mjs";
import type { Row } from "../types/TableSchema.mjs";
import type { TableSchemaBase } from "../types/TableSchema.mjs";
import { rsqlExpressionToFilterFn } from "./rsqlExpressionToFilterFn.mjs";

const userTableSchema = {
	name: "User",
	columns: {
		id: { kind: "number" },
		age: { kind: "number" },
		name: { kind: "string" },
	},
	primaryKey: ["id"],
} satisfies TableSchemaBase;
type UserTable = typeof userTableSchema;

const row: Row<UserTable> = { id: 1, age: 30, name: "Alice" };

describe("rsqlExpressionToFilterFn", () => {
	it("handles column = constant", () => {
		const expr: Expression<UserTable> = {
			kind: "binOp",
			operator: "=",
			left: { kind: "column", name: "age" },
			right: { kind: "constant", value: 30 },
		};
		const fn = rsqlExpressionToFilterFn(expr);
		expect(fn(row)).toBe(true);
		expect(fn({ ...row, age: 25 })).toBe(false);
	});

	it("handles != operator", () => {
		const expr: Expression<UserTable> = {
			kind: "binOp",
			operator: "!=",
			left: { kind: "column", name: "name" },
			right: { kind: "constant", value: "Bob" },
		};
		const fn = rsqlExpressionToFilterFn(expr);
		expect(fn(row)).toBe(true);
		expect(fn({ ...row, name: "Bob" })).toBe(false);
	});

	it("handles <, <=, >, >= operators", () => {
		const expr: Expression<UserTable> = {
			kind: "binOp",
			operator: ">",
			left: { kind: "column", name: "age" },
			right: { kind: "constant", value: 20 },
		};
		const fn = rsqlExpressionToFilterFn(expr);
		expect(fn(row)).toBe(true);
		expect(fn({ ...row, age: 20 })).toBe(false);
	});

	it("handles arithmetic operators", () => {
		const expr: Expression<UserTable> = {
			kind: "binOp",
			operator: "+",
			left: { kind: "column", name: "age" },
			right: { kind: "constant", value: 10 },
		};
		const fn = rsqlExpressionToFilterFn({
			kind: "binOp",
			operator: "=",
			left: expr,
			right: { kind: "constant", value: 40 },
		});
		expect(fn(row)).toBe(true);
		expect(fn({ ...row, age: 20 })).toBe(false);
	});

	it("handles unary operators", () => {
		const expr: Expression<UserTable> = {
			kind: "binOp",
			operator: "=",
			left: {
				kind: "unOp",
				operator: "-",
				expression: { kind: "constant", value: 5 },
			},
			right: { kind: "constant", value: -5 },
		};
		const fn = rsqlExpressionToFilterFn(expr);
		expect(fn(row)).toBe(true);
	});

	it("handles nested expressions", () => {
		const expr: Expression<UserTable> = {
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
		const fn = rsqlExpressionToFilterFn(expr);
		expect(fn({ ...row, age: 30 })).toBe(true); // 30 + 10 == 2 * 20
		expect(fn({ ...row, age: 25 })).toBe(false);
	});
});
