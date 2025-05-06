import type { Database, Statement } from "better-sqlite3";
import type {
	Mutation,
	ReadableStorage,
	WritableStorage,
} from "../../Storage.mjs";
import {
	invertDirection,
	type BackwardPageInit,
	type ForwardPageInit,
	type Page,
	type PageInit,
} from "../../Page.mjs";
import type {
	ColumnName,
	PrimaryKey,
	PrimaryKeyRecord,
	Row,
} from "../../types/TableSchema.mjs";
import type { TableSchemaBase } from "../../types/TableSchema.mjs";
import { compileSql, type CompiledQuery } from "../../sql/compileSql.mjs";
import type { Delete, Insert, Select, Update } from "../../sql/Sql.mjs";
import {
	ands,
	type ParameterExpression,
	type Parameterizable,
	type SqlExpression,
	type TupleExpression,
} from "../../sql/SqlExpression.mjs";
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
} from "../../sql/mks.mjs";
import assert from "assert";

export class BetterSqlite3Storage<Table extends TableSchemaBase>
	implements WritableStorage<Table>, ReadableStorage<Table>
{
	constructor(
		public readonly schema: Table,
		public readonly database: Database,
	) {}

	get tableName() {
		return this.schema.name;
	}

	get primaryKeys() {
		return this.schema.primaryKey;
	}

	mutate(mutation: Mutation<Table>): void {
		switch (mutation.type) {
			case "insert":
				this.insert(mutation.row);
				break;
			case "upsert":
				this.upsert(mutation.row);
				break;
			case "update":
				this.update(mutation.key, mutation.partialRow);
				break;
			case "delete":
				this.delete(mutation.key);
				break;
			default:
				throw new Error("Unknown mutation type");
		}
	}

	mutateMany(mutations: Mutation<Table>[]): void {
		this.database.transaction(() => {
			for (const m of mutations) {
				this.mutate(m);
			}
		})();
	}

	/**
	 * Insert a new row into the table
	 */
	insert(row: Row<Table>): void {
		const { statement, getParams } = this.compiledInsert;
		statement.run(...getParams(row));
	}

	/**
	 * Insert a row or update it if it already exists (based on primary key)
	 */
	upsert(row: Row<Table>): void {
		const columns = Object.keys(row) as (keyof Row<Table>)[];
		const values = Object.fromEntries(
			columns.map((col) => [col, mkParameter((row: Row<Table>) => row[col])]),
		) as Record<keyof Row<Table>, ParameterExpression>;
		const set = Object.fromEntries(
			columns
				.filter((col) => !this.primaryKeys.includes(col as string))
				.map((col) => [col, mkParameter((row: Row<Table>) => row[col])]),
		) as Record<keyof Partial<Row<Table>>, ParameterExpression>;
		const insertAst: Insert<Table> = mkInsert(this.schema, values, {
			onConflict: {
				columns: this.primaryKeys.map((pk) => pk.toString()),
				do: {
					kind: "update" as const,
					set,
				},
			},
		});
		const [sql, getParams] = compileSql(insertAst);
		const stmt = this.database.prepare(sql);
		stmt.run(...getParams(row));
	}

	/**
	 * Update a row based on its primary key
	 */
	update(key: PrimaryKeyRecord<Table>, changes: Partial<Row<Table>>): void {
		const columns = Object.keys(changes) as (keyof Row<Table>)[];
		if (columns.length === 0) return;

		const set = Object.fromEntries(
			columns.map((col) => [
				col,
				mkParameter(
					(ctx: {
						changes: Partial<Row<Table>>;
						key: PrimaryKeyRecord<Table>;
					}) => ctx.changes[col],
				),
			]),
		) as Record<keyof Row<Table>, ParameterExpression>;

		const pkColumns = mkPkColumns(this.schema);
		const pkParams = mkPkParams(
			this.schema,
			({ key }: { key: PrimaryKeyRecord<Table> }) => key,
		);
		const where: SqlExpression<Table> = mkEq(pkColumns, pkParams);

		const updateAst: Update<Table> = mkUpdate(this.schema, set, where);

		const [sql, getParamsRaw] = compileSql(updateAst);
		const getParams = (
			changes: Partial<Row<Table>>,
			key: PrimaryKeyRecord<Table>,
		) => {
			return getParamsRaw({ changes, key });
		};

		const stmt = this.database.prepare(sql);
		stmt.run(...getParams(changes, key));
	}

	delete(key: PrimaryKeyRecord<Table>): void {
		const { statement, getParams } = this.compiledDelete;
		statement.run(...getParams(key));
	}

	findUnique(key: PrimaryKeyRecord<Table>): Row<Table> | null {
		const stmt = this.database.prepare(`
			SELECT * FROM ${this.tableName}
			WHERE ${this.primaryKeys.map((pk) => `${pk} = ?`).join(" AND ")}
		`);
		const row = stmt.get(
			...this.primaryKeys.map((pk: PrimaryKey<Table>[number]) => key[pk]),
		);

		return row === undefined ? null : (row as Row<Table>);
	}

	findMany<Cursor extends PrimaryKeyRecord<Table>>(
		pageInput: PageInit<Table, Cursor>,
	): Page<Table, Cursor> {
		const {
			loadFirst,
			loadLast,
			loadNext,
			loadPrev,
			countTotal,
			countAfter,
			countBefore,
		} = this.compileFindMany<Cursor>(pageInput);

		let rows: Cursor[] = [];
		let itemAfterCount: number | undefined = undefined;
		let itemBeforeCount: number | undefined = undefined;

		if (pageInput.kind === "forward") {
			const limit = pageInput.first;
			if (pageInput.after === undefined) {
				rows = loadFirst({ limit }) as Cursor[];
				itemBeforeCount = 0;
			} else {
				rows = loadNext({
					cursor: pageInput.after,
					limit,
				}) as Cursor[];
			}
		} else {
			const limit = pageInput.last;
			if (pageInput.before === undefined) {
				rows = loadLast({ limit }) as Cursor[];
			} else {
				rows = loadPrev({
					cursor: pageInput.before,
					limit,
				}) as Cursor[];
			}
		}

		rows = pageInput.kind === "forward" ? rows : rows.reverse();
		const { startCursor, endCursor } =
			rows.length > 0
				? { startCursor: rows[0], endCursor: rows.at(-1) }
				: { startCursor: undefined, endCursor: undefined };

		if (itemAfterCount === undefined) {
			if (rows.length > 0) {
				// biome-ignore lint/style/noNonNullAssertion: <explanation>
				const endCursor = rows.at(-1)!;
				// biome-ignore lint/style/noNonNullAssertion: <explanation>
				itemAfterCount = countAfter({ after: endCursor })!["COUNT(*)"];
			} else {
				itemAfterCount = 0;
			}
		}
		if (itemBeforeCount === undefined) {
			if (rows.length > 0) {
				// biome-ignore lint/style/noNonNullAssertion: <explanation>
				const startCursor = rows[0]!;
				// biome-ignore lint/style/noNonNullAssertion: <explanation>
				itemBeforeCount = countBefore({ before: startCursor })!["COUNT(*)"];
			} else {
				itemBeforeCount = 0;
			}
		}
		// biome-ignore lint/style/noNonNullAssertion: <explanation>
		const rowCount = countTotal()!["COUNT(*)"];

		console.info({ beforeCount: itemBeforeCount, afterCount: itemAfterCount });

		return {
			rows,
			rowCount,
			startCursor,
			endCursor,
			itemBeforeCount,
			itemAfterCount,
		};
	}

	prepareFindMany<Cursor extends PrimaryKeyRecord<Table>>(options: {
		filter?: SqlExpression<Table>;
		orderBy: readonly {
			column: PrimaryKey<Table>[number];
			direction: "asc" | "desc";
		}[];
	}) {
		const { filter, orderBy } = options;
		const {
			loadFirst,
			loadLast,
			loadNext,
			loadPrev,
			countTotal,
			countAfter,
			countBefore,
		} = this.compileFindMany({
			filter,
			orderBy,
		});

		return {
			loadForward: (pageInput: ForwardPageInit<Table, Cursor>) =>
				pageInput.after === undefined
					? loadFirst({ limit: pageInput.first })
					: loadNext({
							cursor: pageInput.after,
							limit: pageInput.first,
						}),
			loadBackward: (pageInput: BackwardPageInit<Table, Cursor>) =>
				pageInput.before === undefined
					? loadLast({ limit: pageInput.last })
					: loadPrev({
							cursor: pageInput.before,
							limit: pageInput.last,
						}),
			// biome-ignore lint/style/noNonNullAssertion: <explanation>
			countTotal: () => countTotal()!["COUNT(*)"],
		};
	}

	private _compiledInsert?: PreparedStatement<Row<Table>>;
	private _compiledDelete?: PreparedStatement<PrimaryKeyRecord<Table>>;

	private get compiledInsert(): PreparedStatement<Row<Table>> {
		if (!this._compiledInsert) {
			const columns = Object.keys(this.schema.columns);
			const insertAst = mkInsert(
				this.schema,
				Object.fromEntries(
					columns.map((col) => [
						col,
						mkParameter((row: Row<Table>) => row[col]),
					]),
				) as { [key in keyof Row<Table>]: Parameterizable },
			);
			this._compiledInsert = this.prepareStatement<Row<Table>>(
				compileSql(insertAst),
			);
		}
		return this._compiledInsert;
	}

	private get compiledDelete(): PreparedStatement<
		PrimaryKeyRecord<Table>,
		unknown
	> {
		if (!this._compiledDelete) {
			const pkColumns = mkPkColumns(this.schema);
			const pkParams = mkPkParams(
				this.schema,
				(key: PrimaryKeyRecord<Table>) => key,
			);
			const where: SqlExpression<Table> = mkEq(pkColumns, pkParams);
			const deleteAst: Delete<Table> = mkDelete(this.schema, where);

			this._compiledDelete = this.prepareStatement(compileSql(deleteAst));
		}
		return this._compiledDelete;
	}

	private compileFindMany<Cursor extends PrimaryKeyRecord<Table>>(pageInput: {
		filter?: SqlExpression<Table>;
		orderBy: readonly {
			column: string & keyof Cursor;
			direction: "asc" | "desc";
		}[];
	}): CompiledQueriesForFindMany<Table, Cursor> {
		assert(
			this.schema.primaryKey.every((pk) =>
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
		): TupleExpression<Table> => ({
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
		const loadHeadAst: Select<Table> = mkSelect(this.schema, cursorCols, {
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
		const loadTailAst: Select<Table> = mkSelect(this.schema, cursorCols, {
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
		const loadNextAst: Select<Table> = mkSelect(this.schema, cursorCols, {
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
		const loadPreviousAst: Select<Table> = mkSelect(this.schema, cursorCols, {
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

		const totalCountAst: Select<Table> = mkSelect(
			this.schema,
			[
				{
					kind: "function",
					name: "COUNT",
					args: [{ kind: "asterisk" }],
				},
			],
			{
				where: filter,
			},
		);

		// Count rows after the cursor (for forward pagination)
		const countAfterAst: Select<Table> = mkSelect(
			this.schema,
			[
				{
					kind: "function",
					name: "COUNT",
					args: [{ kind: "asterisk" }],
				},
			],
			{
				where: ands([
					...(filter ? [filter] : []),
					mkGT(
						cursorAsTuple,
						mkCursorParams((context: { after: Cursor }) => context.after),
					),
				]),
			},
		);

		// Count rows before the cursor (for backward pagination)
		const countBeforeAst: Select<Table> = mkSelect(
			this.schema,
			[
				{
					kind: "function",
					name: "COUNT",
					args: [{ kind: "asterisk" }],
				},
			],
			{
				where: ands([
					...(filter ? [filter] : []),
					mkLT(
						cursorAsTuple,
						mkCursorParams((context: { before: Cursor }) => context.before),
					),
				]),
			},
		);

		return {
			loadFirst: this.prepareQueryAll(compileSql(loadHeadAst)),
			loadLast: this.prepareQueryAll(compileSql(loadTailAst)),
			loadNext: this.prepareQueryAll(compileSql(loadNextAst)),
			loadPrev: this.prepareQueryAll(compileSql(loadPreviousAst)),
			countTotal: this.prepareQueryOne(compileSql(totalCountAst)),
			countAfter: this.prepareQueryOne(compileSql(countAfterAst)),
			countBefore: this.prepareQueryOne(compileSql(countBeforeAst)),
		};
	}

	private prepareStatement<Context, Row = unknown>([
		sql,
		getParams,
	]: CompiledQuery<Context>): PreparedStatement<Context, Row> {
		return {
			statement: this.database.prepare(sql),
			getParams: getParams as (context?: Context) => Row[],
		};
	}
	private prepareQueryAll<Context, Row = unknown>([
		sql,
		getParams,
	]: CompiledQuery<Context>): PreparedQueryAll<Context, Row> {
		return (context?: Context) =>
			this.database.prepare(sql).all(...getParams(context)) as Row[];
	}

	private prepareQueryOne<Context, Row = unknown>([
		sql,
		getParams,
	]: CompiledQuery<Context>): PreparedQueryOne<Context, Row> {
		return (context?: Context) =>
			this.database.prepare(sql).get(...getParams(context)) as Row;
	}
}

export type PreparedStatement<Context, Row = unknown> = {
	statement: Statement;
	getParams: (context?: Context) => Row[];
};

export type PreparedQueryAll<Context, Row = unknown> = (
	context?: Context,
) => Row[];

export type PreparedQueryOne<Context, Row = unknown> = (
	context?: Context,
) => Row | undefined;

type PageParameter<
	TableSchema extends TableSchemaBase,
	Cursor extends PrimaryKeyRecord<TableSchema>,
	HasCursor extends boolean,
> = {
	limit: number;
} & (HasCursor extends true ? { cursor: Cursor } : Record<string, unknown>);

type CompiledQueriesForFindMany<
	TableSchema extends TableSchemaBase,
	Cursor extends PrimaryKeyRecord<TableSchema>,
> = {
	loadFirst: PreparedQueryAll<
		PageParameter<TableSchema, Cursor, false>,
		Cursor
	>;
	loadLast: PreparedQueryAll<PageParameter<TableSchema, Cursor, false>, Cursor>;
	loadNext: PreparedQueryAll<PageParameter<TableSchema, Cursor, true>, Cursor>;
	loadPrev: PreparedQueryAll<PageParameter<TableSchema, Cursor, true>, Cursor>;
	countTotal: PreparedQueryOne<never, { "COUNT(*)": number }>;
	countAfter: PreparedQueryOne<{ after: Cursor }, { "COUNT(*)": number }>;
	countBefore: PreparedQueryOne<{ before: Cursor }, { "COUNT(*)": number }>;
};
