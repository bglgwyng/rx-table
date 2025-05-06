import type { ColumnName, TableSchemaBase } from "./TableSchema.mjs";

export type LeftJoin<
	T1 extends TableSchemaBase,
	T2 extends TableSchemaBase,
	A1 extends string,
	A2 extends string,
	On extends Partial<Record<ColumnName<T1>, ColumnName<T2>>>,
> = {
	columns: {
		[key in ColumnName<T1> as `${A1}.${key}`]: T1["columns"][key];
	} & {
		[key in ColumnName<T2> as `${A2}.${key}`]: T2["columns"][key];
	};
	primaryKey: T1["primaryKey"] & T2["primaryKey"];
};
