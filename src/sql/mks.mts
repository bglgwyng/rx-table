import type {
	ParameterExpression,
	Parameterizable,
	SqlExpression,
} from "./SqlExpression.mjs";
import type {
	TableSchemaBase,
	PrimaryKeyRecord,
	PrimaryKey,
	Row,
} from "../types/TableSchema.mjs";
import type { Delete, Insert, Select, SqlOrderBy, Update } from "./Sql.mjs";
import type { Table } from "../Table.mjs";

export function mkInsert<Table extends TableSchemaBase>(
	table: Table,
	values: { [key in keyof Row<Table>]: Parameterizable },
	options?: {
		onConflict?: {
			columns: (keyof Row<Table>)[];
			do: {
				kind: "update";
				set: Partial<Record<keyof Row<Table>, Parameterizable>>;
			};
		};
	},
): Insert<Table> {
	return {
		kind: "insert",
		table: table.name,
		values,
		onConflict: options?.onConflict,
	};
}

export function mkUpdate<Table extends TableSchemaBase>(
	table: Table,
	set: Record<keyof Row<Table>, Parameterizable>,
	where: SqlExpression<Table>,
): Update<Table> {
	return {
		kind: "update",
		table: table.name,
		set,
		where,
	};
}

export function mkDelete<Table extends TableSchemaBase>(
	table: Table,
	where: SqlExpression<Table>,
): Delete<Table> {
	return {
		kind: "delete",
		table: table.name,
		where,
	};
}

export function mkSelect<Table extends TableSchemaBase>(
	table: Table,
	columns: "*" | SqlExpression<Table>[],
	options?: {
		where?: SqlExpression<Table>;
		orderBy?: SqlOrderBy[];
		limit?: Parameterizable;
	},
): Select<TableSchemaBase> {
	return {
		kind: "select",
		table: table.name,
		columns,
		...options,
	};
}

export function mkColumn(column: string): SqlExpression<TableSchemaBase> {
	return {
		kind: "column",
		name: column,
	};
}

export function mkEq(
	left: SqlExpression<TableSchemaBase>,
	right: SqlExpression<TableSchemaBase>,
): SqlExpression<TableSchemaBase> {
	return {
		kind: "binOp",
		operator: "=",
		left,
		right,
	};
}

export function mkLT(
	left: SqlExpression<TableSchemaBase>,
	right: SqlExpression<TableSchemaBase>,
): SqlExpression<TableSchemaBase> {
	return {
		kind: "binOp",
		operator: "<",
		left,
		right,
	};
}
export function mkLTE(
	left: SqlExpression<TableSchemaBase>,
	right: SqlExpression<TableSchemaBase>,
): SqlExpression<TableSchemaBase> {
	return {
		kind: "binOp",
		operator: "<=",
		left,
		right,
	};
}
export function mkGT(
	left: SqlExpression<TableSchemaBase>,
	right: SqlExpression<TableSchemaBase>,
): SqlExpression<TableSchemaBase> {
	return {
		kind: "binOp",
		operator: ">",
		left,
		right,
	};
}
export function mkGTE(
	left: SqlExpression<TableSchemaBase>,
	right: SqlExpression<TableSchemaBase>,
): SqlExpression<TableSchemaBase> {
	return {
		kind: "binOp",
		operator: ">=",
		left,
		right,
	};
}

export function mkParameter<Context>(
	getValue: (ctx: Context) => unknown,
): ParameterExpression {
	return {
		kind: "parameter",
		getValue,
	};
}

export function mkTuple<Table extends TableSchemaBase>(
	expressions: SqlExpression<Table>[],
): SqlExpression<Table> {
	return {
		kind: "tuple",
		expressions,
	};
}

export function mkPkColumns<Table extends TableSchemaBase>(
	table: Table,
): SqlExpression<Table> {
	return {
		kind: "tuple",
		expressions: table.primaryKey.map((pk) => ({ kind: "column", name: pk })),
	};
}
export function mkPkParams<Table extends TableSchemaBase, Context>(
	table: Table,
	getValue: (ctx: Context) => PrimaryKeyRecord<Table>,
): SqlExpression<Table> {
	return {
		kind: "tuple",
		expressions: table.primaryKey.map((pk) => ({
			kind: "parameter",
			getValue: (ctx: Context) => getValue(ctx)[pk],
		})),
	};
}
