import type { Expression } from "./RSql/Expression.mjs";
import type {
	ColumnName,
	PrimaryKeyRecord,
	Row,
	TableSchemaBase,
} from "./types/TableSchema.mjs";

export type PageDelta<T extends TableSchemaBase> = (
	| {
			kind: "remove";
			key: PrimaryKeyRecord<T>;
	  }
	| {
			kind: "add";
			row: Row<T>;
	  }
)[];

export type Page<
	TableSchema extends TableSchemaBase,
	Cursor extends PrimaryKeyRecord<TableSchema>,
> = {
	rows: Iterable<Cursor>;
	rowCount: number;
	endCursor: unknown;
	startCursor: unknown;
	itemBeforeCount: number;
	itemAfterCount: number;
};
// Left-closed: only after is set

export type ForwardPageInit<
	TableSchema extends TableSchemaBase,
	Cursor extends PrimaryKeyRecord<TableSchema>,
> = {
	kind: "forward";
	after?: Cursor;
	first: number;
};
// Right-closed: only before is set

export type BackwardPageInit<
	TableSchema extends TableSchemaBase,
	Cursor extends PrimaryKeyRecord<TableSchema>,
> = {
	kind: "backward";
	before?: Cursor;
	last: number;
};

export type PageInit<
	TableSchema extends TableSchemaBase,
	Cursor extends PrimaryKeyRecord<TableSchema>,
> = (
	| ForwardPageInit<TableSchema, Cursor>
	| BackwardPageInit<TableSchema, Cursor>
) & {
	orderBy: {
		column: string & keyof Cursor;
		direction: Direction;
	}[];
	filter?: Expression<TableSchema, unknown>;
};

export type PageEvent = {
	kind: "loadMore" | "loadPrev";
	count: number;
	retainCount: number;
};

export type PageInputDelta =
	| { kind: "loadPrev"; count?: number }
	| { kind: "loadNext"; count?: number };

export type Direction = "asc" | "desc";

export function invertDirection(dir: Direction): Direction {
	return dir === "asc" ? "desc" : "asc";
}
