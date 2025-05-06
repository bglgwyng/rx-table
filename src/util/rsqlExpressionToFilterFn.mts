import type { Expression } from "../RSql/Expression.mjs";
import type { Row } from "../types/TableSchema.mjs";
import type { TableSchemaBase } from "../types/TableSchema.mjs";

export function rsqlExpressionToFilterFn<T extends TableSchemaBase>(
	condition: Expression<T>,
): (value: Row<T>) => boolean {
	// biome-ignore lint/suspicious/noExplicitAny: <explanation>
	function evalExpr(expr: Expression<T>, row: Row<T>): any {
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
