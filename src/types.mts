import type { Dynamic } from "./core/Dynamic.mjs";

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

export type PageDelta<T extends TableBase> = (
	| {
			kind: "remove";
			key: PrimaryKey<T>;
	  }
	| {
			kind: "add";
			row: Row<T>;
	  }
)[];

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

export type Page<T extends TableBase> = {
	rows: Iterable<PrimaryKey<T>>;
	rowCount: number;
	endCursor: unknown;
	startCursor: unknown;
};

export type PageInput<T extends TableBase> = {
	filter?: SqlExpression<T, unknown>;
	after?: PrimaryKey<T>;
	before?: PrimaryKey<T>;
	first?: number;
	last?: number;
	orderBy?: {
		column: ColumnName<T>;
		direction: "asc" | "desc";
	}[];
};

export type SqlExpression<T extends TableBase, V = unknown> =
	| {
			kind: "column";
			name: keyof T["columns"];
	  }
	| {
			kind: "constant";
			value: V;
	  }
	| {
			kind: "binOp";
			left: SqlExpression<T, unknown>;
			right: SqlExpression<T, unknown>;
			operator:
				| "="
				| "<"
				| ">"
				| "<="
				| ">="
				| "!="
				| "+"
				| "-"
				| "*"
				| "/"
				| "^";
	  }
	| {
			kind: "unOp";
			expression: SqlExpression<T, unknown>;
			operator: "-" | "+";
	  };

function findOne<T extends TableBase>(table: T) {
	const columns = table.columns as T["columns"];
	const key = table.primaryKey[0];
}

type LeftJoin<
	T1 extends TableBase,
	T2 extends TableBase,
	A1 extends string,
	A2 extends string,
	On extends Partial<Record<ColumnName<T1>, ColumnName<T2>>>,
> = {
	columns: { [key in ColumnName<T1> as `${A1}.${key}`]: T1["columns"][key] } & {
		[key in ColumnName<T2> as `${A2}.${key}`]: T2["columns"][key];
	};
	primaryKey: T1["primaryKey"] & T2["primaryKey"];
};

type User = {
	columns: {
		id: number;
		name: string;
		email: string;
	};
	primaryKey: ["id"];
};

type Post = {
	columns: {
		id: number;
		title: string;
		content: string;
	};
	primaryKey: ["id"];
};
type A = LeftJoin<User, Post, "user", "post", { id: "id" }>;

// const x:A = {};
// x.columns["post.id"]
