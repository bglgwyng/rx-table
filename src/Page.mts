import type { SqlExpression } from "./sql/SqlExpression.mjs";
import type {
	TableSchemaBase,
	PrimaryKeyRecord,
	Row,
	ColumnName,
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
};
// Left-closed: only after is set

export type ForwardPageInput<
	TableSchema extends TableSchemaBase,
	Cursor extends PrimaryKeyRecord<TableSchema>,
> = {
	kind: "forward";
	after?: Cursor;
	first: number;
};
// Right-closed: only before is set

export type BackwardPageInput<
	TableSchema extends TableSchemaBase,
	Cursor extends PrimaryKeyRecord<TableSchema>,
> = {
	kind: "backward";
	before?: Cursor;
	last: number;
};

export type PageInput<
	TableSchema extends TableSchemaBase,
	Cursor extends PrimaryKeyRecord<TableSchema>,
> = (
	| ForwardPageInput<TableSchema, Cursor>
	| BackwardPageInput<TableSchema, Cursor>
) & {
	orderBy: {
		column: ColumnName<TableSchema>;
		direction: "asc" | "desc";
	}[];
	filter?: SqlExpression<TableSchema, unknown>;
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
