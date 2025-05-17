import type { Database } from "better-sqlite3";
import {
	type BackwardPageInit,
	type ForwardPageInit,
	type Page,
	type PageInit,
	compileFindMany,
} from "../../Page.mjs";
import type { Expression, Parameter } from "../../RSql/Expression.mjs";
import type {
	Count,
	Delete,
	Insert,
	Select,
	Update,
} from "../../RSql/RSql.mjs";
import { compileStatementToSql } from "../../RSql/compileToSql.mjs";
import {
	mkDeleteRow,
	mkFindUnique,
	mkInsertRow,
	mkUpsertRow,
} from "../../RSql/mkHelpers.mjs";
import { mkParameter, mkPkRecords, mkUpdate } from "../../RSql/mks.mjs";
import type {
	Mutation,
	ReadableStorage,
	WritableStorage,
} from "../../Storage.mjs";
import type {
	PrimaryKey,
	PrimaryKeyRecord,
	Row,
} from "../../types/TableSchema.mjs";
import type { TableSchemaBase } from "../../types/TableSchema.mjs";
import type {
	PreparedCount,
	PreparedMutation,
	PreparedQueryAll,
	PreparedQueryOne,
} from "../../types/PreparedStatement.mjs";

export class BetterSqlite3Storage<Table extends TableSchemaBase>
	implements WritableStorage<Table>, ReadableStorage<Table>
{
	constructor(
		public readonly schema: Table,
		public readonly database: Database,
	) {
		this.preparedInsert = this.prepareMutation(mkInsertRow(this.schema));
		this.preparedUpsert = this.prepareMutation(mkUpsertRow(this.schema));
		this.preparedDelete = this.prepareMutation(mkDeleteRow(this.schema));
		this.preparedFindUnique = this.prepareQueryOne(mkFindUnique(this.schema));
	}

	get tableName() {
		return this.schema.name;
	}

	get primaryKeys() {
		return this.schema.primaryKey;
	}

	prepareQueryOne<Context, Row>(
		query: Select<Table>,
	): PreparedQueryOne<Context, Row> {
		const [sql, getParams] = compileStatementToSql(this.tableName, query);
		const stmt = this.database.prepare(sql);
		return (context?: Context) =>
			(stmt.get(...getParams(context)) as Row | undefined) ?? null;
	}

	prepareQueryAll<Context, Row>(
		query: Select<Table>,
	): PreparedQueryAll<Context, Row> {
		const [sql, getParams] = compileStatementToSql(this.tableName, query);
		const stmt = this.database.prepare(sql);
		return (context?: Context) => stmt.all(...getParams(context)) as Row[];
	}

	prepareCount<Context>(query: Count<Table>): PreparedCount<Context> {
		const [sql, getParams] = compileStatementToSql(this.tableName, query);
		const stmt = this.database.prepare(sql);
		return (context?: Context) => {
			console.info("#", stmt.source, getParams(context));
			return (stmt.get(...getParams(context)) as { "COUNT(*)": number })[
				"COUNT(*)"
			];
		};
	}

	prepareMutation<Context>(
		mutation: Insert<Table> | Update<Table> | Delete<Table>,
	): PreparedMutation<Context> {
		const [sql, getParams] = compileStatementToSql(this.tableName, mutation);
		const stmt = this.database.prepare(sql);

		return (context?: Context) => stmt.run(...getParams(context));
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

	insert(row: Row<Table>): void {
		this.preparedInsert(row);
	}

	upsert(row: Row<Table>): void {
		this.preparedUpsert(row);
	}

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
		) as Record<keyof Row<Table>, Parameter>;

		const pkParams = mkPkRecords(
			this.schema,
			({ key }: { key: PrimaryKeyRecord<Table> }) => key,
		);
		const updateAst: Update<Table> = mkUpdate(set, pkParams);

		const [sql, getParamsRaw] = compileStatementToSql(
			this.tableName,
			updateAst,
		);

		const stmt = this.database.prepare(sql);
		stmt.run(...getParamsRaw({ changes, key }));
	}

	delete(key: PrimaryKeyRecord<Table>): void {
		this.preparedDelete(key);
	}

	findUnique(key: PrimaryKeyRecord<Table>): Row<Table> | null {
		const row = this.preparedFindUnique(key);

		return row === undefined ? null : (row as Row<Table>);
	}

	findMany<Cursor extends PrimaryKeyRecord<Table>>(
		pageInput: PageInit<Table, Cursor>,
	): Page<Table, Cursor> {
		const { loadForward, loadBackward, countAfter, countBefore, countTotal } =
			this.prepareFindMany<Cursor>({
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
			pageInput.kind === "forward" && pageInput.after === undefined
				? 0
				: rows.length > 0
					? countBefore(startCursor!)
					: rowCount;

		const itemAfterCount =
			pageInput.kind === "backward" && pageInput.before === undefined
				? 0
				: rows.length > 0
					? countAfter(rows.at(-1)!)
					: rowCount;

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

	prepareFindMany<Cursor extends PrimaryKeyRecord<Table>>(options: {
		filter?: Expression<Table>;
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
		} = compileFindMany(this.schema, {
			filter,
			orderBy,
		});

		return {
			loadForward: (pageInput: ForwardPageInit<Table, Cursor>) =>
				pageInput.after === undefined
					? this.prepareQueryAll(loadFirst)({ limit: pageInput.first })
					: this.prepareQueryAll(loadNext)({
							cursor: pageInput.after,
							limit: pageInput.first,
						}),
			loadBackward: (pageInput: BackwardPageInit<Table, Cursor>) =>
				pageInput.before === undefined
					? this.prepareQueryAll(loadLast)({ limit: pageInput.last })
					: this.prepareQueryAll(loadPrev)({
							cursor: pageInput.before,
							limit: pageInput.last,
						}),
			countAfter: (cursor: Cursor) =>
				this.prepareCount(countAfter)({ after: cursor }),
			countBefore: (cursor: Cursor) =>
				this.prepareCount(countBefore)({ before: cursor }),
			countTotal: this.prepareCount(countTotal),
		};
	}

	private preparedInsert: PreparedMutation<Row<Table>>;
	private preparedUpsert: PreparedMutation<Row<Table>>;
	private preparedDelete: PreparedMutation<PrimaryKeyRecord<Table>>;
	private preparedFindUnique: PreparedQueryOne<
		PrimaryKeyRecord<Table>,
		Row<Table>
	>;
}
