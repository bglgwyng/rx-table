import type { Row, TableSchemaBase } from "../types/TableSchema.mjs";
import type { Expression, Parameterizable } from "./Expression.mjs";

export type Statement<Table extends TableSchemaBase = TableSchemaBase> =
	| Select<Table>
	| Insert<Table>
	| Update<Table>
	| Delete<Table>;

export type Select<Table extends TableSchemaBase = TableSchemaBase> = {
	kind: "select";
	columns: "*" | Expression<Table>[];
	where?: Expression<Table>;
	orderBy?: OrderBy[];
	limit?: Parameterizable;
};

export type Insert<Table extends TableSchemaBase = TableSchemaBase> = {
	kind: "insert";
	values: Record<keyof Row<Table>, Parameterizable>;
	onConflict?: {
		columns: (keyof Row<Table>)[];
		do: {
			kind: "update";
			set: Partial<Record<keyof Row<Table>, Parameterizable>>;
		};
	};
};

export type Update<Table extends TableSchemaBase = TableSchemaBase> = {
	kind: "update";
	set: Record<keyof Row<Table>, Parameterizable>;
	where?: Expression<Table>;
};

export type Delete<Table extends TableSchemaBase = TableSchemaBase> = {
	kind: "delete";
	where?: Expression<Table>;
};

export type OrderBy = {
	column: string;
	direction: "asc" | "desc";
};
