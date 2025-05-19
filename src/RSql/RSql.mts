import type {
	ColumnName,
	PrimaryKey,
	PrimaryKeyRecord,
	Row,
	TableRef,
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

export type Select<
	Table extends TableSchemaBase = TableSchemaBase,
	Context = unknown,
> = {
	kind: "select";
	columns: "*" | Expression<Table>[];
	from: TableRef<Table>;
	where?: Expression<Table>;
	orderBy?: OrderBy[];
	limit?: Parameterizable;
};

export type Count<
	Table extends TableSchemaBase = TableSchemaBase,
	Context = unknown,
> = {
	kind: "count";
	from: TableRef<Table>;
	where?: Expression<Table>;
};

export type Insert<Table extends TableSchemaBase = TableSchemaBase> = {
	kind: "insert";
	into: TableRef<Table>;
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
	into: TableRef<Table>;
	set: Record<UpdatableColumnName<Table>, Parameterizable>;
	key: Record<PrimaryKey<Table>[number], Parameterizable>;
};

export type Delete<Table extends TableSchemaBase = TableSchemaBase> = {
	kind: "delete";
	from: TableRef<Table>;
	key: Record<PrimaryKey<Table>[number], Parameterizable>;
};

export type OrderBy = {
	column: string;
	direction: "asc" | "desc";
};
