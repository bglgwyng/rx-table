import type { TableSchemaBase } from "../types/TableSchema.mjs";

export type Column<T extends TableSchemaBase> = {
	kind: "column";
	name: string & keyof T["columns"];
};
export type Constant<V = unknown> = {
	kind: "constant";
	value: V;
};
export type Parameter = {
	kind: "parameter";
	getValue(context: unknown): unknown;
};

export type BinOp<T extends TableSchemaBase> = {
	kind: "binOp";
	left: Expression<T, unknown>;
	right: Expression<T, unknown>;
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
export type UnOp<T extends TableSchemaBase> = {
	kind: "unOp";
	expression: Expression<T, unknown>;
	operator: "-" | "+" | "NOT";
};
export type Fn<T extends TableSchemaBase> = {
	kind: "function";
	name: string;
	args: Expression<T, unknown>[];
};
export type Tuple<T extends TableSchemaBase> = {
	kind: "tuple";
	expressions: Expression<T, unknown>[];
};
export type Asterisk<T extends TableSchemaBase> = {
	kind: "asterisk";
};

export type Expression<T extends TableSchemaBase, V = unknown> =
	| Column<T>
	| Constant<V>
	| Parameter
	| BinOp<T>
	| UnOp<T>
	| Fn<T>
	| Tuple<T>
	| Asterisk<T>;

export type Parameterizable = Constant | Parameter;

export function isParameterizable(
	expr: Expression<TableSchemaBase>,
): expr is Parameterizable {
	return expr.kind === "constant" || expr.kind === "parameter";
}

export function ands<T extends TableSchemaBase>(
	exprs: Expression<T>[],
): Expression<T> | undefined {
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
