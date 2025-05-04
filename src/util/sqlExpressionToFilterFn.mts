import type { SqlExpression } from "../types.mjs";
import type { TableBase, Row } from "../types.mjs";

export function sqlExpressionToFilterFn<T extends TableBase>(
	condition: SqlExpression<T>,
): (value: Row<T>) => boolean {
	// biome-ignore lint/suspicious/noExplicitAny: <explanation>
	function evalExpr(expr: SqlExpression<T>, row: Row<T>): any {
		switch (expr.kind) {
			case "column":
				return row[expr.name];
			case "constant":
				return expr.value;
			case "binOp": {
				const left = evalExpr(expr.left, row);
				const right = evalExpr(expr.right, row);
				switch (expr.operator) {
					case "=":
						return left === right;
					case "!=":
						return left !== right;
					case "<":
						return left < right;
					case "<=":
						return left <= right;
					case ">":
						return left > right;
					case ">=":
						return left >= right;
					case "+":
						return left + right;
					case "-":
						return left - right;
					case "*":
						return left * right;
					case "/":
						return left / right;
					case "^":
						return left ** right;
					default:
						throw new Error(`Unsupported operator: ${expr}`);
				}
			}
			case "unOp": {
				const val = evalExpr(expr.expression, row);
				switch (expr.operator) {
					case "+":
						return +val;
					case "-":
						return -val;
					default:
						throw new Error(`Unsupported unary operator: ${expr}`);
				}
			}
			default:
				throw new Error("Unknown SqlExpression kind");
		}
	}
	return (row: Row<T>) => !!evalExpr(condition, row);
}
