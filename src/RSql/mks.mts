import type {
	PrimaryKey,
	PrimaryKeyRecord,
	Row,
	TableRef,
	TableSchemaBase,
	UpdatableColumnName,
} from "../types/TableSchema.mjs";
import type { Expression, Parameter, Parameterizable } from "./Expression.mjs";
import type {
	Count,
	Delete,
	Insert,
	OrderBy,
	Select,
	Update,
} from "./RSql.mjs";

export function mkInsert<Table extends TableSchemaBase>(
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
		values,
		onConflict: options?.onConflict,
	};
}

export function mkUpdate<Table extends TableSchemaBase>(
	set: Record<UpdatableColumnName<Table>, Parameterizable>,
	key: Record<PrimaryKey<Table>[number], Parameterizable>,
): Update<Table> {
	return {
		kind: "update",
		set,
		key,
	};
}

export function mkDelete<Table extends TableSchemaBase>(
	key: Record<PrimaryKey<Table>[number], Parameterizable>,
): Delete<Table> {
	return {
		kind: "delete",
		key,
	};
}

export function mkSelect<Table extends TableSchemaBase>(
	from: TableRef<Table>,
	columns: "*" | Expression<Table>[],
	options?: {
		where?: Expression<Table>;
		orderBy?: OrderBy[];
		limit?: Parameterizable;
	},
): Select<Table> {
	return {
		kind: "select",
		from,
		columns,
		...options,
	};
}

export function mkCount<Table extends TableSchemaBase>(
	where?: Expression<Table>,
): Count<TableSchemaBase> {
	return {
		kind: "count",
		where,
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

export function mkPkRecords<Table extends TableSchemaBase, Context>(
	table: Table,
	getValue: (ctx: Context) => PrimaryKeyRecord<Table>,
): Record<PrimaryKey<Table>[number], Parameter> {
	return Object.fromEntries(
		table.primaryKey.map((pk) => [
			pk,
			mkParameter((ctx: Context) => getValue(ctx)[pk]),
		]),
	) as Record<PrimaryKey<Table>[number], Parameter>;
}
