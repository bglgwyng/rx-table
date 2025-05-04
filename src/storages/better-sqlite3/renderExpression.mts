import assert from "assert";
import type { TableBase, Row } from "../../types/Table.mts";
import {
	isParameterizable,
	type Parameterizable,
	type SqlExpression,
} from "../../types/SqlExpression.mjs";
import type { Sql } from "../../types/Sql.mts";

export function* renderSqlExpression(
	expr: SqlExpression<TableBase>,
): Generator<Parameterizable, string> {
	if (expr.kind === "binOp") {
		const left = yield* renderSqlExpression(expr.left);
		const right = yield* renderSqlExpression(expr.right);
		const op = expr.operator;

		return `(${left} ${op} ${right})`;
	}
	if (expr.kind === "unOp") {
		if (expr.operator === "NOT") {
			const operand = yield* renderSqlExpression(expr.expression);
			return `(NOT ${operand})`;
		}
	}
	if (expr.kind === "column") {
		return expr.name;
	}
	if (isParameterizable(expr)) {
		return yield expr;
	}

	if (expr.kind === "tuple") {
		const placeholders: string[] = [];
		for (const elems of expr.expressions) {
			const placeholder = yield* renderSqlExpression(elems);
			placeholders.push(placeholder);
		}
		return `(${placeholders.join(", ")})`;
	}

	assert.fail("Unsupported expression type in renderExpression");
}

export function* renderSql(
	sqlAst: Sql<TableBase>,
): Generator<Parameterizable, string> {
	switch (sqlAst.kind) {
		case "select": {
			const cols = Array.isArray(sqlAst.columns)
				? sqlAst.columns.join(", ")
				: "*";
			let sql = `SELECT ${cols} FROM ${sqlAst.table}`;
			let paramCount = 0;
			if (sqlAst.where) {
				const whereSql = yield* renderSqlExpression(sqlAst.where);
				sql += ` WHERE ${whereSql}`;
			}
			if (sqlAst.orderBy && sqlAst.orderBy.length > 0) {
				sql += ` ORDER BY ${sqlAst.orderBy.map((o) => `${o.column} ${o.direction}`).join(", ")}`;
			}
			if (sqlAst.limit !== undefined) {
				sql += " LIMIT ?";
				yield sqlAst.limit;
				paramCount++;
			}
			return sql;
		}
		case "insert": {
			const keys: (keyof Row<TableBase>)[] = Object.keys(
				sqlAst.values,
			) as (keyof Row<TableBase>)[];
			let sql = `INSERT INTO ${sqlAst.table} (${keys.join(", ")}) VALUES (`;
			const placeholders: string[] = [];
			for (const k of keys) {
				// biome-ignore lint/style/noNonNullAssertion: <explanation>
				placeholders.push(yield sqlAst.values[k as keyof Row<TableBase>]!);
			}
			sql += placeholders.join(", ");
			sql += ")";
			return sql;
		}
	}
}

export function compileSqlExpression<Table extends TableBase = TableBase>(
	expr: SqlExpression<Table>,
): [sql: string, (context: unknown) => unknown[]] {
	const gen = renderSqlExpression(expr);
	const params: Parameterizable[] = [];
	let next = gen.next();
	while (!next.done) {
		params.push(next.value);
		next = gen.next("?");
	}
	const sql = next.value;
	return [
		sql,
		(context) =>
			params.map((p) =>
				p.kind === "constant" ? p.value : p.getValue(context),
			),
	];
}

export function compileSql<Table extends TableBase = TableBase>(
	sqlAst: Sql<Table>,
): [sql: string, (context: unknown) => unknown[]] {
	const gen = renderSql(sqlAst);
	const params: Parameterizable[] = [];
	let next = gen.next();
	while (!next.done) {
		params.push(next.value);
		next = gen.next("?");
	}
	const sql = next.value;
	return [
		sql,
		(context) =>
			params.map((p) =>
				p.kind === "constant" ? p.value : p.getValue(context),
			),
	];
}
