import type { Row, TableSchemaBase } from "../types/TableSchema.mjs";
import type { Expression, Parameterizable } from "./Expression.mjs";

export type OrderBy = {
	column: string;
	direction: "asc" | "desc";
};

export type Select<Table extends TableSchemaBase = TableSchemaBase> = {
	kind: "select";
	table: string;
	columns: "*" | Expression<Table>[];
	where?: Expression<Table>;
	orderBy?: OrderBy[];
	limit?: Parameterizable;
};

export type Insert<Table extends TableSchemaBase = TableSchemaBase> = {
	kind: "insert";
	table: string;
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
	table: string;
	set: Record<keyof Row<Table>, Parameterizable>;
	where?: Expression<Table>;
};

export type Delete<Table extends TableSchemaBase = TableSchemaBase> = {
	kind: "delete";
	table: string;
	where?: Expression<Table>;
};

export type Statement<Table extends TableSchemaBase = TableSchemaBase> =
	| Select<Table>
	| Insert<Table>
	| Update<Table>
	| Delete<Table>;
