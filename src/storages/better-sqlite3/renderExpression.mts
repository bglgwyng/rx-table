import assert from "assert";
import type { TableBase } from "../../types/Table.mjs";
import type { SqlExpression } from "../../types/SqlExpression.mjs";

export function* renderSqlExpression(
	expr: SqlExpression<TableBase>,
): Generator<Placeholder, string> {
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
		yield ["constant", expr.value];
		return "?";
	}
	if (expr.kind === "parameter") {
		yield ["parameter", expr.name];
		return "?";
	}

	assert.fail("Unsupported expression type in renderExpression");
}

export function compileSql(
	expr: SqlExpression<TableBase>,
): (getParam?: (name: string) => unknown) => [sql: string, params: unknown[]] {
	const gen = renderSqlExpression(expr);
	const params: Placeholder[] = [];
	let next = gen.next();
	while (!next.done) {
		params.push(next.value);
		next = gen.next();
	}
	const sql = next.value;
	return (
		getParam: (name: string) => unknown = () =>
			assert.fail("Parameter not supported"),
	) => [
		sql,
		params.map(([kind, value]) =>
			kind === "constant" ? value : getParam(value),
		),
	];
}

export type Placeholder =
	| readonly ["parameter", string]
	| readonly ["constant", unknown];
