import type { TableBase, Row } from "../types/Table.mjs";
import type { Parameterizable, SqlExpression } from "./SqlExpression.mjs";

export type SqlOrderBy = {
	column: string;
	direction: "asc" | "desc";
};

export type Select<Table extends TableBase = TableBase> = {
	kind: "select";
	table: string;
	columns: "*";
	where?: SqlExpression<Table>;
	orderBy?: SqlOrderBy[];
	limit?: Parameterizable;
};

export type Insert<Table extends TableBase = TableBase> = {
	kind: "insert";
	table: string;
	values: {
		[key in keyof Row<Table>]: Parameterizable;
	};
};

export type Update<Table extends TableBase = TableBase> = {
	kind: "update";
	table: string;
	set: {
		[key in keyof Row<Table>]: Parameterizable;
	};
	where?: SqlExpression<Table>;
};

export type Delete<Table extends TableBase = TableBase> = {
	kind: "delete";
	table: string;
	where?: SqlExpression<Table>;
};

export type Source<Table extends TableBase = TableBase> =
	| Select<Table>
	| Insert<Table>
	| Update<Table>
	| Delete<Table>;

// update/delete 등도 추가됨
