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
import type {
	PrimaryKey,
	PrimaryKeyRecord,
	Row,
	TableRef,
} from "./types/TableSchema.mjs";
import type { TableSchemaBase } from "./types/TableSchema.mjs";
import type { PreparedQueryOne } from "./types/PreparedStatement.mjs";
import type { Storage } from "./Storage.mjs";

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
	TableSchema extends TableSchemaBase,
	Cursor extends PrimaryKeyRecord<TableSchema>,
>(
	table: TableRef<TableSchema>,
	pageInput: {
		filter?: Expression<TableSchema>;
		orderBy: readonly {
			column: string & keyof Cursor;
			direction: "asc" | "desc";
		}[];
	},
): PreparedQueriesForFindMany<TableSchema, Cursor> {
	assert(
		table.schema.primaryKey.every((pk) =>
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
	): Tuple<TableSchema> => ({
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
	const loadHeadAst: Select<TableSchema> = mkSelect(table, cursorCols, {
		where: filter,
		orderBy: orderBy.map((o) => ({
			column: o.column,
			direction: o.direction,
		})),
		limit: mkParameter(
			(context: PageParameter<TableSchema, Cursor, false>) => context.limit,
		),
	});

	// for load: no cursor, orderBy as is
	const loadTailAst: Select<TableSchema> = mkSelect(table, cursorCols, {
		where: filter,
		orderBy: orderBy.map((o) => ({
			column: o.column,
			direction: invertDirection(o.direction),
		})),
		limit: mkParameter(
			(context: PageParameter<TableSchema, Cursor, false>) => context.limit,
		),
	});

	// for loadMore: after cursor, forward order
	const loadNextAst: Select<TableSchema> = mkSelect(table, cursorCols, {
		where: ands([
			...(filter ? [filter] : []),
			mkGT(
				cursorAsTuple,
				mkCursorParams(
					(context: PageParameter<TableSchema, Cursor, true>) => context.cursor,
				),
			),
		]),
		orderBy: orderBy.map((o) => ({
			column: o.column,
			direction: o.direction,
		})),
		limit: mkParameter(
			(context: PageParameter<TableSchema, Cursor, true>) => context.limit,
		),
	});

	// for loadPrevious: before cursor, reverse order
	const loadPreviousAst: Select<TableSchema> = mkSelect(table, cursorCols, {
		where: ands([
			...(filter ? [filter] : []),
			mkLT(
				cursorAsTuple,
				mkCursorParams(
					(context: PageParameter<TableSchema, Cursor, true>) => context.cursor,
				),
			),
		]),
		orderBy: orderBy.map((o) => ({
			column: o.column,
			direction: invertDirection(o.direction),
		})),
		limit: mkParameter(
			(context: PageParameter<TableSchema, Cursor, true>) => context.limit,
		),
	});

	const totalCountAst: Count<TableSchema> = mkCount(table, filter);

	// Count rows after the cursor (for forward pagination)
	const countAfterAst: Count<TableSchema> = mkCount(
		table,
		ands([
			...(filter ? [filter] : []),
			mkGT(
				cursorAsTuple,
				mkCursorParams((context: { after: Cursor }) => context.after),
			),
		]),
	);

	// Count rows before the cursor (for backward pagination)
	const countBeforeAst: Count<TableSchema> = mkCount(
		table,
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

export function findUnique<Table extends TableSchemaBase>(
	table: TableRef<Table>,
	preparedFindUnique: PreparedQueryOne<PrimaryKeyRecord<Table>, Row<Table>>,
	key: PrimaryKeyRecord<Table>,
): Row<Table> | null {
	const row = preparedFindUnique(key);

	return row === undefined ? null : (row as Row<Table>);
}

export function findMany<
	Schema extends Record<string, TableSchemaBase>,
	Table extends Schema[string],
	Cursor extends PrimaryKeyRecord<Table>,
>(
	storage: Storage<Schema>,
	table: TableRef<Table>,
	pageInput: PageInit<Table, Cursor>,
): Page<Table, Cursor> {
	const { loadForward, loadBackward, countAfter, countBefore, countTotal } =
		prepareFindMany<Schema, Table, Cursor>(storage, table, {
			filter: pageInput.filter,
			orderBy: pageInput.orderBy.map(({ column, direction }) => ({
				column: column as PrimaryKey<Table>[number],
				direction,
			})),
		});

	let rows: Cursor[] = [];

	if (pageInput.kind === "forward") {
		const forwardInput: ForwardPageInit<Table, Cursor> = {
			kind: "forward",
			first: pageInput.first,
			after: pageInput.after,
		};
		rows = loadForward(forwardInput) as Cursor[];
	} else {
		const backwardInput: BackwardPageInit<Table, Cursor> = {
			kind: "backward",
			last: pageInput.last,
			before: pageInput.before,
		};
		rows = loadBackward(backwardInput) as Cursor[];
		rows = rows.reverse();
	}

	const startCursor = rows[0];
	const endCursor = rows.at(-1);

	const rowCount = countTotal();

	const itemBeforeCount =
		pageInput.kind === "forward"
			? pageInput.after === undefined
				? 0
				: rows.length > 0
					? countBefore(rows[0]!)
					: rowCount
			: rows.length > 0
				? countBefore(rows[0]!)
				: 0;

	const itemAfterCount =
		pageInput.kind === "backward"
			? pageInput.before === undefined
				? 0
				: rows.length > 0
					? countAfter(rows.at(-1)!)
					: rowCount
			: rows.length > 0
				? countAfter(rows.at(-1)!)
				: 0;

	// Get total count

	return {
		rows,
		rowCount,
		startCursor,
		endCursor,
		itemBeforeCount,
		itemAfterCount,
	};
}

function prepareFindMany<
	Schema extends Record<string, TableSchemaBase>,
	Table extends Schema[string],
	Cursor extends PrimaryKeyRecord<Table>,
>(
	storage: Storage<Schema>,
	table: TableRef<Table>,
	options: {
		filter?: Expression<Table>;
		orderBy: readonly {
			column: PrimaryKey<Table>[number];
			direction: "asc" | "desc";
		}[];
	},
) {
	const { filter, orderBy } = options;
	const {
		loadFirst,
		loadLast,
		loadNext,
		loadPrev,
		countTotal,
		countAfter,
		countBefore,
	} = compileFindMany(table, {
		filter,
		orderBy,
	});

	return {
		loadForward: (pageInput: ForwardPageInit<Table, Cursor>) =>
			pageInput.after === undefined
				? storage.prepareFindMany(loadFirst)({ limit: pageInput.first })
				: storage.prepareFindMany(loadNext)({
						cursor: pageInput.after,
						limit: pageInput.first,
					}),
		loadBackward: (pageInput: BackwardPageInit<Table, Cursor>) =>
			pageInput.before === undefined
				? storage.prepareFindMany(loadLast)({ limit: pageInput.last })
				: storage.prepareFindMany(loadPrev)({
						cursor: pageInput.before,
						limit: pageInput.last,
					}),
		countAfter: (cursor: Cursor) =>
			storage.prepareCount(countAfter)({ after: cursor }),
		countBefore: (cursor: Cursor) =>
			storage.prepareCount(countBefore)({ before: cursor }),
		countTotal: storage.prepareCount(countTotal),
	};
}
