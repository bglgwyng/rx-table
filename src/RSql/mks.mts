import type {
	PrimaryKey,
	PrimaryKeyRecord,
	Row,
	TableSchemaBase,
} from "../types/TableSchema.mjs";
import type { Expression, Parameter, Parameterizable } from "./Expression.mjs";
import type { Delete, Insert, OrderBy, Select, Update } from "./RSql.mjs";

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
	where: Expression<Table>,
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
	where: Expression<Table>,
): Delete<Table> {
	return {
		kind: "delete",
		table: table.name,
		where,
	};
}

export function mkSelect<Table extends TableSchemaBase>(
	table: Table,
	columns: "*" | Expression<Table>[],
	options?: {
		where?: Expression<Table>;
		orderBy?: OrderBy[];
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

export function mkColumn(column: string): Expression<TableSchemaBase> {
	return {
		kind: "column",
		name: column,
	};
}

export function mkEq(
	left: Expression<TableSchemaBase>,
	right: Expression<TableSchemaBase>,
): Expression<TableSchemaBase> {
	return {
		kind: "binOp",
		operator: "=",
		left,
		right,
	};
}

export function mkLT(
	left: Expression<TableSchemaBase>,
	right: Expression<TableSchemaBase>,
): Expression<TableSchemaBase> {
	return {
		kind: "binOp",
		operator: "<",
		left,
		right,
	};
}
export function mkLTE(
	left: Expression<TableSchemaBase>,
	right: Expression<TableSchemaBase>,
): Expression<TableSchemaBase> {
	return {
		kind: "binOp",
		operator: "<=",
		left,
		right,
	};
}
export function mkGT(
	left: Expression<TableSchemaBase>,
	right: Expression<TableSchemaBase>,
): Expression<TableSchemaBase> {
	return {
		kind: "binOp",
		operator: ">",
		left,
		right,
	};
}
export function mkGTE(
	left: Expression<TableSchemaBase>,
	right: Expression<TableSchemaBase>,
): Expression<TableSchemaBase> {
	return {
		kind: "binOp",
		operator: ">=",
		left,
		right,
	};
}

export function mkParameter<Context>(
	getValue: (ctx: Context) => unknown,
): Parameter {
	return {
		kind: "parameter",
		getValue,
	};
}

export function mkTuple<Table extends TableSchemaBase>(
	expressions: Expression<Table>[],
): Expression<Table> {
	return {
		kind: "tuple",
		expressions,
	};
}

export function mkPkColumns<Table extends TableSchemaBase>(
	table: Table,
): Expression<Table> {
	return {
		kind: "tuple",
		expressions: table.primaryKey.map((pk) => ({ kind: "column", name: pk })),
	};
}
export function mkPkParams<Table extends TableSchemaBase, Context>(
	table: Table,
	getValue: (ctx: Context) => PrimaryKeyRecord<Table>,
): Expression<Table> {
	return {
		kind: "tuple",
		expressions: table.primaryKey.map((pk) => ({
			kind: "parameter",
			getValue: (ctx: Context) => getValue(ctx)[pk],
		})),
	};
}
