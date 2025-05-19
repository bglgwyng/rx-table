import assert from "assert";
import type {
	Row,
	TableRef,
	TableSchemaBase,
	UpdatableColumnName,
} from "../types/TableSchema.mjs";
import {
	type Expression,
	type Parameterizable,
	isParameterizable,
} from "./Expression.mjs";
import type { Statement } from "./RSql.mjs";

export type CompiledQuery<Context> = readonly [
	sql: string,
	getParams: (context?: Context) => unknown[],
];

function* renderExpressionToSql(
	expr: Expression<TableSchemaBase>,
): Generator<Parameterizable, string> {
	if (expr.kind === "binOp") {
		const left = yield* renderExpressionToSql(expr.left);
		const right = yield* renderExpressionToSql(expr.right);
		const op = expr.operator;

		return `(${left} ${op} ${right})`;
	}
	if (expr.kind === "unOp") {
		if (expr.operator === "NOT") {
			const operand = yield* renderExpressionToSql(expr.expression);
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
			const argSql = yield* renderExpressionToSql(arg);

			args.push(argSql);
		}
		return `${expr.name}(${args.join(", ")})`;
	}

	if (expr.kind === "tuple") {
		const placeholders: string[] = [];
		for (const elems of expr.expressions) {
			const placeholder = yield* renderExpressionToSql(elems);
			placeholders.push(placeholder);
		}
		return `(${placeholders.join(", ")})`;
	}

	if (expr.kind === "asterisk") {
		return "*";
	}

	assert.fail(`Unsupported expression type in renderExpression ${expr.kind}`);
}

// biome-ignore lint/correctness/useYield: <explanation>
export function* renderTableRefToSql<TableSchema extends TableSchemaBase>(
	table: TableRef<TableSchema>,
): Generator<Parameterizable, string> {
	return table.name;
}
export function* renderStatementToSql<TableSchema extends TableSchemaBase>(
	table: TableRef<TableSchema>,
	sqlAst: Statement<TableSchema>,
): Generator<Parameterizable, string> {
	switch (sqlAst.kind) {
		case "select": {
			let selection: string;
			if (sqlAst.columns === "*") {
				selection = "*";
			} else {
				const cols: string[] = [];
				for (const col of sqlAst.columns) {
					cols.push(yield* renderExpressionToSql(col));
				}
				selection = cols.join(", ");
			}
			let sql = `SELECT ${selection} FROM (${yield* renderTableRefToSql(table)})`;
			let paramCount = 0;
			if (sqlAst.where) {
				const whereSql = yield* renderExpressionToSql(sqlAst.where);
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
		case "count": {
			let sql = `SELECT COUNT(*) FROM (${yield* renderTableRefToSql(table)})`;
			if (sqlAst.where) {
				const whereSql = yield* renderExpressionToSql(sqlAst.where);
				sql += ` WHERE ${whereSql}`;
			}
			return sql;
		}
		case "insert": {
			const keys: (keyof Row<TableSchemaBase>)[] = Object.keys(
				sqlAst.values,
			) as (keyof Row<TableSchemaBase>)[];
			let sql = `INSERT INTO ${yield* renderTableRefToSql(table)} (${keys.join(", ")}) VALUES (`;
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
			const keys = Object.keys(
				sqlAst.set,
			) as UpdatableColumnName<TableSchema>[];
			let sql = `UPDATE ${yield* renderTableRefToSql(table)} SET `;
			const setClauses: string[] = [];
			for (const k of keys) {
				setClauses.push(`${k} = ?`);
			}
			sql += setClauses.join(", ");
			for (const k of keys) {
				yield sqlAst.set[k as UpdatableColumnName<TableSchema>];
			}

			const where: string[] = [];
			for (const [k, v] of Object.entries(sqlAst.key)) {
				const whereSql = yield* renderExpressionToSql(v as Parameterizable);
				where.push(`${k} = ${whereSql}`);
			}
			return `${sql} WHERE ${where.join(" AND ")}`;
		}
		case "delete": {
			const sql = `DELETE FROM ${yield* renderTableRefToSql(table)}`;
			const where: string[] = [];
			for (const [k, v] of Object.entries(sqlAst.key)) {
				const whereSql = yield* renderExpressionToSql(v as Parameterizable);
				where.push(`${k} = ${whereSql}`);
			}
			return `${sql} WHERE ${where.join(" AND ")}`;
		}
	}
}

export function compileExpressionToSql<
	Table extends TableSchemaBase = TableSchemaBase,
>(expr: Expression<Table>): [sql: string, (context: unknown) => unknown[]] {
	const gen = renderExpressionToSql(expr);
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

export function compileStatementToSql<
	TableSchema extends TableSchemaBase,
	Context,
>(
	table: TableRef<TableSchema>,
	sqlAst: Statement<TableSchema>,
): CompiledQuery<Context> {
	const gen = renderStatementToSql(table, sqlAst);
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
