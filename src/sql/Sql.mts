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

export type Source<Table extends TableBase = TableBase> =
	| Select<Table>
	| Insert<Table>;

// 추후 update/delete 등 추가 가능
