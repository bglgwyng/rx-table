import type {
	ColumnName,
	PrimaryKey,
	PrimaryKeyRecord,
	Row,
	TableSchemaBase,
	UpdatableColumnName,
} from "../types/TableSchema.mjs";
import type { Expression, Parameterizable } from "./Expression.mjs";

export type Statement<Table extends TableSchemaBase = TableSchemaBase> =
	| Select<Table>
	| Count<Table>
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

export type Count<Table extends TableSchemaBase = TableSchemaBase> = {
	kind: "count";
	where?: Expression<Table>;
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
	set: Record<UpdatableColumnName<Table>, Parameterizable>;
	key: Record<PrimaryKey<Table>[number], Parameterizable>;
};

export type Delete<Table extends TableSchemaBase = TableSchemaBase> = {
	kind: "delete";
	key: Record<PrimaryKey<Table>[number], Parameterizable>;
};

export type OrderBy = {
	column: string;
	direction: "asc" | "desc";
};
