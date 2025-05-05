import type { TableSchemaBase, Row } from "../types/TableSchema.mjs";
import type { Parameterizable, SqlExpression } from "./SqlExpression.mjs";

export type SqlOrderBy = {
	column: string;
	direction: "asc" | "desc";
};

export type Select<Table extends TableSchemaBase = TableSchemaBase> = {
	kind: "select";
	table: string;
	columns: "*";
	where?: SqlExpression<Table>;
	orderBy?: SqlOrderBy[];
	limit?: Parameterizable;
};

export type Insert<Table extends TableSchemaBase = TableSchemaBase> = {
	kind: "insert";
	table: string;
	values: {
		[key in keyof Row<Table>]: Parameterizable;
	};
};

export type Update<Table extends TableSchemaBase = TableSchemaBase> = {
	kind: "update";
	table: string;
	set: {
		[key in keyof Row<Table>]: Parameterizable;
	};
	where?: SqlExpression<Table>;
};

export type Delete<Table extends TableSchemaBase = TableSchemaBase> = {
	kind: "delete";
	table: string;
	where?: SqlExpression<Table>;
};

export type Source<Table extends TableSchemaBase = TableSchemaBase> =
	| Select<Table>
	| Insert<Table>
	| Update<Table>
	| Delete<Table>;

// update/delete 등도 추가됨
