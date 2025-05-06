import type { Observable } from "rxjs";
import type { Page, PageDelta, PageEvent, PageInit } from "../Page.mjs";
import type { Dynamic } from "../core/Dynamic.mjs";

export interface TableSchemaBase {
	name: string;
	columns: Record<string, ColumnType>;
	primaryKey: readonly (string & keyof this["columns"])[];
}

export type ColumnType = {
	kind: "string" | "number" | "boolean" | "date";
};

export type ScalarToType<T extends ColumnType> = {
	string: string;
	number: number;
	boolean: boolean;
	date: Date;
}[T["kind"]];

export type Row<T extends TableSchemaBase> = {
	[key in keyof T["columns"]]: ScalarToType<T["columns"][key]>;
};
export type ColumnName<T extends TableSchemaBase> = string & keyof T["columns"];
export type PrimaryKey<T extends TableSchemaBase> = T["primaryKey"];
export type PrimaryKeyRecord<T extends TableSchemaBase> = {
	[key in PrimaryKey<T>[number]]: ScalarToType<T["columns"][key]>;
};
export type PrimaryKeyTuple<T extends TableSchemaBase> =
	PrimaryKey<T> extends readonly (infer K)[]
		? {
				[I in keyof PrimaryKey<T>]: K extends keyof T["columns"]
					? T["columns"][K]
					: never;
			}
		: never;

export type ReadableTable<T extends TableSchemaBase> = {
	findUnique(key: PrimaryKeyRecord<T>): Dynamic<Row<T> | null, void>;
	findMany<Cursor extends PrimaryKeyRecord<T>>(
		pageInput: PageInit<T, Cursor>,
		pageEvent: Observable<PageEvent>,
	): Dynamic<Page<T, Cursor>, PageDelta<T>>;
};

export type WritableTable<T extends TableSchemaBase> = {
	insert(row: Row<T>): void;
	upsert(row: Row<T>): void;
	update(key: PrimaryKeyRecord<T>, partialRow: Partial<Row<T>>): void;
	delete(key: PrimaryKeyRecord<T>): void;
};

export type TableEvent<T extends TableSchemaBase> =
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
