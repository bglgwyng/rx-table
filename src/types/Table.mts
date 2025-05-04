import type { Dynamic } from "../core/Dynamic.mjs";
import type { PageInput, Page, PageDelta } from "./Page.mjs";

export interface TableBase {
	columns: Record<string, unknown>;
	primaryKey: (keyof this["columns"])[];
}
export type Row<T extends TableBase> = T["columns"];
export type ColumnName<T extends TableBase> = string & keyof T["columns"];
export type PrimaryKey<T extends TableBase> = T["primaryKey"];
export type PrimaryKeyRecord<T extends TableBase> = {
	[key in PrimaryKey<T>[number]]: T["columns"][key];
};
export type PrimaryKeyTuple<T extends TableBase> =
	PrimaryKey<T> extends readonly (infer K)[]
		? {
				[I in keyof PrimaryKey<T>]: K extends keyof T["columns"]
					? T["columns"][K]
					: never;
			}
		: never;

export type ReadableTable<T extends TableBase> = {
	findUnique(key: PrimaryKeyRecord<T>): Dynamic<Row<T> | null, void>;
	findMany(pageInput: PageInput<T>): Dynamic<Page<T>, PageDelta<T>>;
};

export type WritableTable<T extends TableBase> = {
	insert(row: Row<T>): void;
	upsert(row: Row<T>): void;
	update(key: PrimaryKeyRecord<T>, partialRow: Partial<Row<T>>): void;
	delete(key: PrimaryKeyRecord<T>): void;
};

export type TableEvent<T extends TableBase> =
	| {
			kind: "insert";
			row: Row<T>;
	  }
	| {
			kind: "update";
			key: PrimaryKeyRecord<T>;
			row: Partial<Omit<Row<T>, PrimaryKey<T>[number]>>;
	  }
	| {
			kind: "delete";
			key: PrimaryKeyRecord<T>;
	  };
