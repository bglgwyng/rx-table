import assert from "assert";
import type { SqlExpression, TableBase } from "../../types.mjs";

export function* renderSqlExpression(
	expr: SqlExpression<TableBase>,
): Generator<unknown, string> {
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
	if (expr.kind === "constant") {
		yield expr.value;
		return "?";
	}

	assert.fail("Unsupported expression type in renderExpression");
}

export function compileSql(
	expr: SqlExpression<TableBase>,
): [sql: string, params: unknown[]] {
	const gen = renderSqlExpression(expr);
	const params: unknown[] = [];
	let next = gen.next();
	while (!next.done) {
		params.push(next.value);
		next = gen.next();
	}
	const sql = next.value;
	return [sql, params];
}
