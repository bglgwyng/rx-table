import type { TableBase } from "../types/Table.mjs";

export type ColumnExpression<T extends TableBase> = {
	kind: "column";
	name: string & keyof T["columns"];
};
export type ConstantExpression<V = unknown> = {
	kind: "constant";
	value: V;
};
export type ParameterExpression = {
	kind: "parameter";
	getValue(context: unknown): unknown;
};

export type BinOpExpression<T extends TableBase> = {
	kind: "binOp";
	left: SqlExpression<T, unknown>;
	right: SqlExpression<T, unknown>;
	operator:
		| "="
		| "<"
		| ">"
		| "<="
		| ">="
		| "!="
		| "+"
		| "-"
		| "*"
		| "/"
		| "^"
		| "AND"
		| "OR";
};
export type UnOpExpression<T extends TableBase> = {
	kind: "unOp";
	expression: SqlExpression<T, unknown>;
	operator: "-" | "+" | "NOT";
};

export type TupleExpression<T extends TableBase> = {
	kind: "tuple";
	expressions: SqlExpression<T, unknown>[];
};

export type SqlExpression<T extends TableBase, V = unknown> =
	| ColumnExpression<T>
	| ConstantExpression<V>
	| ParameterExpression
	| BinOpExpression<T>
	| UnOpExpression<T>
	| TupleExpression<T>;

export type Parameterizable = ConstantExpression | ParameterExpression;

export function isParameterizable(
	expr: SqlExpression<TableBase>,
): expr is Parameterizable {
	return expr.kind === "constant" || expr.kind === "parameter";
}

export function ands<T extends TableBase>(
	exprs: SqlExpression<T>[],
): SqlExpression<T> | undefined {
	if (exprs.length === 0) return undefined;
	if (exprs.length === 1) return exprs[0];

	return {
		kind: "binOp",
		// biome-ignore lint/style/noNonNullAssertion: <explanation>
		left: exprs[0]!,
		// biome-ignore lint/style/noNonNullAssertion: <explanation>
		right: ands(exprs.slice(1))!,
		operator: "AND",
	};
}
