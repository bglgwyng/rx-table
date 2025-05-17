import assert from "assert";
import { type Expression, type Tuple, ands } from "./RSql/Expression.mjs";
import type { Count, Select } from "./RSql/RSql.mjs";
import {
	mkColumn,
	mkCount,
	mkGT,
	mkLT,
	mkParameter,
	mkSelect,
	mkTuple,
} from "./RSql/mks.mjs";
import type { PrimaryKeyRecord, Row } from "./types/TableSchema.mjs";
import type { TableSchemaBase } from "./types/TableSchema.mjs";

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
export type PageParameter<
	TableSchema extends TableSchemaBase,
	Cursor extends PrimaryKeyRecord<TableSchema>,
	HasCursor extends boolean,
> = {
	limit: number;
} & (HasCursor extends true ? { cursor: Cursor } : Record<string, unknown>);

export type PreparedQueriesForFindMany<
	TableSchema extends TableSchemaBase,
	Cursor extends PrimaryKeyRecord<TableSchema>,
> = {
	loadFirst: Select<TableSchema, never>;
	loadLast: Select<TableSchema, never>;
	loadNext: Select<TableSchema, Cursor>;
	loadPrev: Select<TableSchema, Cursor>;
	countTotal: Count<TableSchema, unknown>;
	countAfter: Count<TableSchema, { after: Cursor }>;
	countBefore: Count<TableSchema, { before: Cursor }>;
};

export function compileFindMany<
	Table extends TableSchemaBase,
	Cursor extends PrimaryKeyRecord<Table>,
>(
	table: Table,
	pageInput: {
		filter?: Expression<Table>;
		orderBy: readonly {
			column: string & keyof Cursor;
			direction: "asc" | "desc";
		}[];
	},
): PreparedQueriesForFindMany<Table, Cursor> {
	assert(
		table.primaryKey.every((pk) =>
			pageInput.orderBy.some((o) => o.column === pk),
		),
		"orderBy must include all primary key columns",
	);
	assert(
		pageInput.orderBy.every((o) => o.direction === "asc") ||
			pageInput.orderBy.every((o) => o.direction === "desc"),
		"orderBy must be all ascending or all descending",
	);
	const cursorCols = pageInput.orderBy.map((o) => mkColumn(o.column));
	const cursorAsTuple = mkTuple(cursorCols);

	const mkCursorParams = <Context, _ = unknown>(
		getCursor: (context: Context) => Cursor,
	): Tuple<Table> => ({
		kind: "tuple",
		expressions: pageInput.orderBy.map((col) => ({
			kind: "parameter",
			getValue: (ctx: Context) => getCursor(ctx)[col.column],
		})),
	});

	// Use pkColumns, pkParams, selectCols from upper scope
	const filter = pageInput.filter;
	const orderBy = pageInput.orderBy;

	// for load: no cursor, orderBy as is
	const loadHeadAst: Select<Table> = mkSelect(cursorCols, {
		where: filter,
		orderBy: orderBy.map((o) => ({
			column: o.column,
			direction: o.direction,
		})),
		limit: mkParameter(
			(context: PageParameter<Table, Cursor, false>) => context.limit,
		),
	});

	// for load: no cursor, orderBy as is
	const loadTailAst: Select<Table> = mkSelect(cursorCols, {
		where: filter,
		orderBy: orderBy.map((o) => ({
			column: o.column,
			direction: invertDirection(o.direction),
		})),
		limit: mkParameter(
			(context: PageParameter<Table, Cursor, false>) => context.limit,
		),
	});

	// for loadMore: after cursor, forward order
	const loadNextAst: Select<Table> = mkSelect(cursorCols, {
		where: ands([
			...(filter ? [filter] : []),
			mkGT(
				cursorAsTuple,
				mkCursorParams(
					(context: PageParameter<Table, Cursor, true>) => context.cursor,
				),
			),
		]),
		orderBy: orderBy.map((o) => ({
			column: o.column,
			direction: o.direction,
		})),
		limit: mkParameter(
			(context: PageParameter<Table, Cursor, true>) => context.limit,
		),
	});

	// for loadPrevious: before cursor, reverse order
	const loadPreviousAst: Select<Table> = mkSelect(cursorCols, {
		where: ands([
			...(filter ? [filter] : []),
			mkLT(
				cursorAsTuple,
				mkCursorParams(
					(context: PageParameter<Table, Cursor, true>) => context.cursor,
				),
			),
		]),
		orderBy: orderBy.map((o) => ({
			column: o.column,
			direction: invertDirection(o.direction),
		})),
		limit: mkParameter(
			(context: PageParameter<Table, Cursor, true>) => context.limit,
		),
	});

	const totalCountAst: Count<Table> = mkCount(filter);

	// Count rows after the cursor (for forward pagination)
	const countAfterAst: Count<Table> = mkCount(
		ands([
			...(filter ? [filter] : []),
			mkGT(
				cursorAsTuple,
				mkCursorParams((context: { after: Cursor }) => context.after),
			),
		]),
	);

	// Count rows before the cursor (for backward pagination)
	const countBeforeAst: Count<Table> = mkCount(
		ands([
			...(filter ? [filter] : []),
			mkLT(
				cursorAsTuple,
				mkCursorParams((context: { before: Cursor }) => context.before),
			),
		]),
	);

	return {
		loadFirst: loadHeadAst,
		loadLast: loadTailAst,
		loadNext: loadNextAst,
		loadPrev: loadPreviousAst,
		countTotal: totalCountAst,
		countAfter: countAfterAst,
		countBefore: countBeforeAst,
	};
}
