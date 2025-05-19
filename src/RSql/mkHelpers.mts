import {
	mkColumn,
	mkDelete,
	mkEq,
	mkInsert,
	mkParameter,
	mkPkColumns,
	mkPkParams,
	mkPkRecords,
	mkSelect,
} from "../RSql/mks.mjs";
import type { PrimaryKeyRecord, Row, TableRef } from "../types/TableSchema.mjs";
import type { TableSchemaBase } from "../types/TableSchema.mjs";
import type { Expression, Parameter, Parameterizable } from "./Expression.mjs";

export function mkInsertRow<T extends TableSchemaBase>(table: TableRef<T>) {
	return mkInsert(
		table,
		Object.fromEntries(
			Object.keys(table.schema.columns).map((col) => [
				col,
				mkParameter((row: Row<T>) => row[col]),
			]),
		) as { [key in keyof Row<T>]: Parameterizable },
	);
}

export function mkUpsertRow<T extends TableSchemaBase>(table: TableRef<T>) {
	const columns = Object.keys(table.schema.columns) as (keyof Row<T>)[];
	const values = Object.fromEntries(
		columns.map((col) => [col, mkParameter((row: Row<T>) => row[col])]),
	) as Record<keyof Row<T>, Parameter>;
	const set = Object.fromEntries(
		columns
			.filter((col) => !table.schema.primaryKey.includes(col as string))
			.map((col) => [col, mkParameter((row: Row<T>) => row[col])]),
	) as Record<keyof Partial<Row<T>>, Parameter>;

	return mkInsert(table, values, {
		onConflict: {
			columns: table.schema.primaryKey.map((pk) => pk.toString()),
			do: {
				kind: "update" as const,
				set,
			},
		},
	});
}

export function mkDeleteRow<T extends TableSchemaBase>(table: TableRef<T>) {
	return mkDelete(
		table,
		mkPkRecords(table.schema, (key: PrimaryKeyRecord<T>) => key),
	);
}

export function mkFindUnique<T extends TableSchemaBase>(schema: T) {
	return mkSelect<T>(
		// FIXME:
		{ kind: "base", name: schema.name, schema },
		Object.keys(schema.columns).map((pk) => mkColumn(pk)),
		{
			where: mkEq(
				mkPkColumns(schema),
				mkPkParams(schema, (key: PrimaryKeyRecord<T>) => key),
			),
		},
	);
}
