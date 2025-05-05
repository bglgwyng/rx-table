import assert from "assert";
import type { TableSchemaBase, Row } from "../types/TableSchema.mjs";
import {
	isParameterizable,
	type Parameterizable,
	type SqlExpression,
} from "./SqlExpression.mjs";
import type { Source } from "./Sql.mjs";

function* renderSqlExpression(
	expr: SqlExpression<TableSchemaBase>,
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

	if (expr.kind === "function") {
		const args: string[] = [];
		for (const arg of expr.args) {
			const argSql = yield* renderSqlExpression(arg);
			args.push(argSql);
		}
		return `${expr.name}(${args.join(", ")})`;
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
	sqlAst: Source<TableSchemaBase>,
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
			const keys: (keyof Row<TableSchemaBase>)[] = Object.keys(
				sqlAst.values,
			) as (keyof Row<TableSchemaBase>)[];
			let sql = `INSERT INTO ${sqlAst.table} (${keys.join(", ")}) VALUES (`;
			const placeholders: string[] = [];
			for (const k of keys) {
				placeholders.push(
					// biome-ignore lint/style/noNonNullAssertion: <explanation>
					yield sqlAst.values[k as keyof Row<TableSchemaBase>]!,
				);
			}
			sql += placeholders.join(", ");
			sql += ")";

			if (sqlAst.onConflict) {
				sql += ` ON CONFLICT (${sqlAst.onConflict.columns.join(", ")}) DO UPDATE SET `;
				const setKeys = Object.keys(sqlAst.onConflict.do.set);
				sql += setKeys.map((k) => `${k} = ?`).join(", ");
				for (const k of setKeys) {
					// biome-ignore lint/style/noNonNullAssertion: <explanation>
					yield sqlAst.onConflict.do.set[k as keyof Row<TableSchemaBase>]!;
				}
			}

			return sql;
		}
		case "update": {
			const keys = Object.keys(sqlAst.set) as (keyof Row<TableSchemaBase>)[];
			let sql = `UPDATE ${sqlAst.table} SET `;
			const setClauses: string[] = [];
			for (const k of keys) {
				setClauses.push(`${k} = ?`);
			}
			sql += setClauses.join(", ");
			for (const k of keys) {
				// biome-ignore lint/style/noNonNullAssertion: <explanation>
				yield sqlAst.set[k as keyof Row<TableSchemaBase>]!;
			}
			if (sqlAst.where) {
				const whereSql = yield* renderSqlExpression(sqlAst.where);
				sql += ` WHERE ${whereSql}`;
			}
			return sql;
		}
		case "delete": {
			let sql = `DELETE FROM ${sqlAst.table}`;
			if (sqlAst.where) {
				const whereSql = yield* renderSqlExpression(sqlAst.where);
				sql += ` WHERE ${whereSql}`;
			}
			return sql;
		}
	}
}

export function compileSqlExpression<
	Table extends TableSchemaBase = TableSchemaBase,
>(expr: SqlExpression<Table>): [sql: string, (context: unknown) => unknown[]] {
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

export function compileSql<Table extends TableSchemaBase = TableSchemaBase>(
	sqlAst: Source<Table>,
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
