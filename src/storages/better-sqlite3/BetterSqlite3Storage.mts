import type { Database, Statement } from "better-sqlite3";
import type {
	Mutation,
	ReadableStorage,
	WritableStorage,
} from "../../Storage.mjs";
import {
	invertDirection,
	type BackwardPageInput,
	type ForwardPageInput,
	type Page,
	type PageInput,
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
} from "../../sql/SqlExpression.mjs";
import {
	mkDelete,
	mkEq,
	mkGT,
	mkInsert,
	mkLT,
	mkParameter,
	mkPkColumns,
	mkPkParams,
	mkSelect,
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
		) as Record<string, ParameterExpression>;
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
		pageInput: PageInput<Table, Cursor>,
	): Page<Table, Cursor> {
		const { loadFirst, loadLast, loadNext, loadPrev, totalCount } =
			this.compileFindMany(pageInput);

		let rows: Row<Table>[] = [];

		if (pageInput.kind === "forward") {
			const limit = pageInput.first;
			if (pageInput.after === undefined) {
				rows = loadFirst.statement.all(
					...loadFirst.getParams({ limit }),
				) as Row<Table>[];
			} else {
				rows = loadNext.statement.all(
					...loadNext.getParams({
						cursor: pageInput.after,
						limit: pageInput.first,
					}),
				) as Row<Table>[];
			}
		} else {
			const limit = pageInput.last;
			if (pageInput.before === undefined) {
				rows = loadLast.statement.all(
					...loadLast.getParams({ limit }),
				) as Row<Table>[];
			} else {
				rows = loadPrev.statement.all(
					...loadPrev.getParams({
						cursor: pageInput.before,
						limit,
					}),
				) as Row<Table>[];
			}
		}

		const rowCount = (
			totalCount.statement.get(...totalCount.getParams()) as {
				cnt: number;
			}
		).cnt;

		const getCursor = (row: Row<Table>): Cursor =>
			Object.fromEntries(
				pageInput.orderBy.map(({ column }) => [column, row[column]]),
			) as Cursor;

		return {
			rows: (pageInput.kind === "forward" ? rows : rows.reverse()).map(
				getCursor,
			),
			rowCount,
			// biome-ignore lint/style/noNonNullAssertion: <explanation>
			startCursor: rows.length > 0 ? getCursor(rows[0]!) : undefined,
			// biome-ignore lint/style/noNonNullAssertion: <explanation>
			endCursor: rows.length > 0 ? getCursor(rows.at(-1)!) : undefined,
		};
	}
	prepareFindMany<Cursor extends PrimaryKeyRecord<Table>>(
		pageInput: PageInput<Table, Cursor>,
	) {
		const { loadFirst, loadLast, loadNext, loadPrev, totalCount } =
			this.compileFindMany(pageInput);

		return {
			loadForward: (pageInput: ForwardPageInput<Table, Cursor>) =>
				pageInput.after === undefined
					? loadFirst.statement.all(
							...loadFirst.getParams({ limit: pageInput.first }),
						)
					: loadNext.statement.all(
							...loadNext.getParams({
								cursor: pageInput.after,
								limit: pageInput.first,
							}),
						),
			loadBackward: (pageInput: BackwardPageInput<Table, Cursor>) =>
				pageInput.before === undefined
					? loadLast.statement.all(
							...loadLast.getParams({ limit: pageInput.last }),
						)
					: loadPrev.statement.all(
							...loadPrev.getParams({
								cursor: pageInput.before,
								limit: pageInput.last,
							}),
						),
			loadTotalCount: () =>
				(
					totalCount.statement.get(...totalCount.getParams()) as {
						cnt: number;
					}
				).cnt,
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

	private compileFindMany<Cursor extends PrimaryKeyRecord<Table>>(
		pageInput: PageInput<Table, Cursor>,
	): CompiledQueriesForFindMany<Table, Cursor> {
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
		const selectCols = "*";

		const pkColumns = mkPkColumns(this.schema);

		// Use pkColumns, pkParams, selectCols from upper scope
		const filter = pageInput.filter;
		const orderBy = pageInput.orderBy;

		// for load: no cursor, orderBy as is
		const loadHeadAst: Select<Table> = mkSelect(this.schema, selectCols, {
			where: ands([filter].filter((x) => x !== undefined)),
			orderBy: orderBy.map((o) => ({
				column: o.column,
				direction: o.direction,
			})),
			limit: mkParameter(
				(context: PageParameter<Table, false>) => context.limit,
			),
		});

		// for load: no cursor, orderBy as is
		const loadTailAst: Select<Table> = mkSelect(this.schema, selectCols, {
			where: ands([filter].filter((x) => x !== undefined)),
			orderBy: orderBy.map((o) => ({
				column: o.column,
				direction: invertDirection(o.direction),
			})),
			limit: mkParameter(
				(context: PageParameter<Table, false>) => context.limit,
			),
		});

		// for loadMore: after cursor, forward order
		const loadNextAst: Select<Table> = mkSelect(this.schema, selectCols, {
			where: ands(
				[
					...(filter ? [filter] : []),
					mkGT(
						pkColumns,
						mkPkParams(
							this.schema,
							(context: PageParameter<Table, true>) => context.cursor,
						),
					),
				].filter((x) => x !== undefined),
			),
			orderBy: orderBy.map((o) => ({
				column: o.column,
				direction: o.direction,
			})),
			limit: mkParameter(
				(context: PageParameter<Table, true>) => context.limit,
			),
		});

		// for loadPrevious: before cursor, reverse order
		const loadPreviousAst: Select<Table> = mkSelect(this.schema, selectCols, {
			where: ands([
				...(filter ? [filter] : []),
				mkLT(
					pkColumns,
					mkPkParams(
						this.schema,
						(context: PageParameter<Table, true>) => context.cursor,
					),
				),
			]),
			orderBy: orderBy.map((o) => ({
				column: o.column,
				direction: invertDirection(o.direction),
			})),
			limit: mkParameter(
				(context: PageParameter<Table, true>) => context.limit,
			),
		});

		const totalCountAst: Select<Table> = mkSelect(
			this.schema,
			[
				{
					kind: "function",
					name: "COUNT",
					args: [{ kind: "constant", value: "*" }],
				},
			],
			{
				where: filter,
			},
		);

		return {
			loadFirst: this.prepareStatement(compileSql(loadHeadAst)),
			loadLast: this.prepareStatement(compileSql(loadTailAst)),
			loadNext: this.prepareStatement(compileSql(loadNextAst)),
			loadPrev: this.prepareStatement(compileSql(loadPreviousAst)),
			totalCount: this.prepareStatement(compileSql(totalCountAst)),
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
}

export type PreparedStatement<Context, Row = unknown> = {
	statement: Statement;
	getParams: (context?: Context) => Row[];
};

type PageParameter<
	TableSchema extends TableSchemaBase,
	HasCursor extends boolean,
> = {
	limit: number;
} & (HasCursor extends true
	? { cursor: PrimaryKeyRecord<TableSchema> }
	: Record<string, unknown>);

type CompiledQueriesForFindMany<
	TableSchema extends TableSchemaBase,
	Cursor extends PrimaryKeyRecord<TableSchema>,
> = {
	loadFirst: PreparedStatement<PageParameter<TableSchema, false>, Cursor>;
	loadLast: PreparedStatement<PageParameter<TableSchema, false>, Cursor>;
	loadNext: PreparedStatement<PageParameter<TableSchema, true>, Cursor>;
	loadPrev: PreparedStatement<PageParameter<TableSchema, true>, Cursor>;
	totalCount: PreparedStatement<never, { cnt: number }>;
};
