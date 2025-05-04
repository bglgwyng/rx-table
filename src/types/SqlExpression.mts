import type { TableBase } from "./Table.mjs";

export type SqlExpression<T extends TableBase, V = unknown> =
	| {
			kind: "column";
			name: string & keyof T["columns"];
	  }
	| {
			kind: "constant";
			value: V;
	  }
	| {
			kind: "parameter";
			name: string;
	  }
	| {
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
	  }
	| {
			kind: "unOp";
			expression: SqlExpression<T, unknown>;
			operator: "-" | "+" | "NOT";
	  };
