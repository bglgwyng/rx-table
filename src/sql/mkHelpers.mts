import type { Database, Statement } from "better-sqlite3";
import type {
	Mutation,
	PreparedMutation,
	PreparedQuery,
	ReadableStorage,
	WritableStorage,
} from "../Storage.mjs";
import {
	invertDirection,
	type BackwardPageInit,
	type ForwardPageInit,
	type Page,
	type PageInit,
} from "../Page.mjs";
import type {
	ColumnName,
	PrimaryKey,
	PrimaryKeyRecord,
	Row,
} from "../types/TableSchema.mjs";
import type { TableSchemaBase } from "../types/TableSchema.mjs";
import { compileSql, type CompiledQuery } from "../sql/compileSql.mjs";
import type { Delete, Insert, Select, Update } from "../sql/Sql.mjs";
import {
	ands,
	type ParameterExpression,
	type Parameterizable,
	type SqlExpression,
	type TupleExpression,
} from "../sql/SqlExpression.mjs";
import {
	mkColumn,
	mkDelete,
	mkEq,
	mkGT,
	mkInsert,
	mkLT,
	mkParameter,
	mkPkColumns,
	mkPkParams,
	mkSelect,
	mkTuple,
	mkUpdate,
} from "../sql/mks.mjs";
import assert from "assert";

export function mkInsertRow<T extends TableSchemaBase>(schema: T) {
	return mkInsert(
		schema,
		Object.fromEntries(
			Object.keys(schema.columns).map((col) => [
				col,
				mkParameter((row: Row<T>) => row[col]),
			]),
		) as { [key in keyof Row<T>]: Parameterizable },
	);
}

export function mkUpsertRow<T extends TableSchemaBase>(schema: T) {
	const columns = Object.keys(schema.columns) as (keyof Row<T>)[];
	const values = Object.fromEntries(
		columns.map((col) => [col, mkParameter((row: Row<T>) => row[col])]),
	) as Record<keyof Row<T>, ParameterExpression>;
	const set = Object.fromEntries(
		columns
			.filter((col) => !schema.primaryKey.includes(col as string))
			.map((col) => [col, mkParameter((row: Row<T>) => row[col])]),
	) as Record<keyof Partial<Row<T>>, ParameterExpression>;

	return mkInsert(schema, values, {
		onConflict: {
			columns: schema.primaryKey.map((pk) => pk.toString()),
			do: {
				kind: "update" as const,
				set,
			},
		},
	});
}

export function mkDeleteRow<T extends TableSchemaBase>(schema: T) {
	const pkColumns = mkPkColumns(schema);
	const pkParams = mkPkParams(schema, (key: PrimaryKeyRecord<T>) => key);
	const where: SqlExpression<T> = mkEq(pkColumns, pkParams);

	return mkDelete(schema, where);
}

export function mkFindUnique<T extends TableSchemaBase>(schema: T) {
	return mkSelect<T>(
		schema,
		Object.keys(schema.columns).map((pk) => mkColumn(pk)),
		{
			where: mkEq(
				mkPkColumns(schema),
				mkPkParams(schema, (key: PrimaryKeyRecord<T>) => key),
			),
		},
	);
}
