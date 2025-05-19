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
	TableRef,
} from "../../types/TableSchema.mjs";
import type { TableSchemaBase } from "../../types/TableSchema.mjs";
import type {
	PreparedCount,
	PreparedMutation,
	PreparedQueryAll,
	PreparedQueryOne,
} from "../../types/PreparedStatement.mjs";

export class BetterSqlite3Storage<TableSchema extends TableSchemaBase>
	implements WritableStorage<TableSchema>, ReadableStorage<TableSchema>
{
	constructor(
		public readonly schema: TableSchema,
		public readonly database: Database,
	) {
		this.preparedInsert = this.prepareMutation(mkInsertRow(this.table));
		this.preparedUpsert = this.prepareMutation(mkUpsertRow(this.table));
		this.preparedDelete = this.prepareMutation(mkDeleteRow(this.table));
		this.preparedFindUnique = this.prepareQueryOne(mkFindUnique(this.schema));
	}

	get table(): TableRef<TableSchema> {
		return { kind: "base", name: this.schema.name, schema: this.schema };
	}

	get primaryKeys() {
		return this.schema.primaryKey;
	}

	prepareQueryOne<Context, Row>(
		query: Select<TableSchema>,
	): PreparedQueryOne<Context, Row> {
		const [sql, getParams] = compileStatementToSql(query);
		const stmt = this.database.prepare(sql);
		return (context?: Context) =>
			(stmt.get(...getParams(context)) as Row | undefined) ?? null;
	}

	prepareQueryAll<Context, Row>(
		query: Select<TableSchema>,
	): PreparedQueryAll<Context, Row> {
		const [sql, getParams] = compileStatementToSql(query);
		const stmt = this.database.prepare(sql);
		return (context?: Context) => stmt.all(...getParams(context)) as Row[];
	}

	prepareCount<Context>(query: Count<TableSchema>): PreparedCount<Context> {
		const [sql, getParams] = compileStatementToSql(query);
		const stmt = this.database.prepare(sql);
		return (context?: Context) =>
			(stmt.get(...getParams(context)) as { "COUNT(*)": number })["COUNT(*)"];
	}

	prepareMutation<Context>(
		mutation: Insert<TableSchema> | Update<TableSchema> | Delete<TableSchema>,
	): PreparedMutation<Context> {
		const [sql, getParams] = compileStatementToSql(mutation);
		const stmt = this.database.prepare(sql);

		return (context?: Context) => stmt.run(...getParams(context));
	}

	mutate(mutation: Mutation<TableSchema>): void {
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

	mutateMany(mutations: Mutation<TableSchema>[]): void {
		this.database.transaction(() => {
			for (const m of mutations) {
				this.mutate(m);
			}
		})();
	}

	insert(row: Row<TableSchema>): void {
		this.preparedInsert(row);
	}

	upsert(row: Row<TableSchema>): void {
		this.preparedUpsert(row);
	}

	update(
		key: PrimaryKeyRecord<TableSchema>,
		changes: Partial<Row<TableSchema>>,
	): void {
		const columns = Object.keys(changes) as (keyof Row<TableSchema>)[];
		if (columns.length === 0) return;

		const set = Object.fromEntries(
			columns.map((col) => [
				col,
				mkParameter(
					(ctx: {
						changes: Partial<Row<TableSchema>>;
						key: PrimaryKeyRecord<TableSchema>;
					}) => ctx.changes[col],
				),
			]),
		) as Record<keyof Row<TableSchema>, Parameter>;

		const pkParams = mkPkRecords(
			this.schema,
			({ key }: { key: PrimaryKeyRecord<TableSchema> }) => key,
		);
		const updateAst: Update<TableSchema> = mkUpdate(this.table, set, pkParams);

		const [sql, getParamsRaw] = compileStatementToSql(updateAst);

		const stmt = this.database.prepare(sql);
		stmt.run(...getParamsRaw({ changes, key }));
	}

	delete(key: PrimaryKeyRecord<TableSchema>): void {
		this.preparedDelete(key);
	}

	findUnique(key: PrimaryKeyRecord<TableSchema>): Row<TableSchema> | null {
		const row = this.preparedFindUnique(key);

		return row === undefined ? null : (row as Row<TableSchema>);
	}

	findMany<Cursor extends PrimaryKeyRecord<TableSchema>>(
		pageInput: PageInit<TableSchema, Cursor>,
	): Page<TableSchema, Cursor> {
		const { loadForward, loadBackward, countAfter, countBefore, countTotal } =
			this.prepareFindMany<Cursor>({
				filter: pageInput.filter,
				orderBy: pageInput.orderBy.map(({ column, direction }) => ({
					column: column as PrimaryKey<TableSchema>[number],
					direction,
				})),
			});

		let rows: Cursor[] = [];

		if (pageInput.kind === "forward") {
			const forwardInput: ForwardPageInit<TableSchema, Cursor> = {
				kind: "forward",
				first: pageInput.first,
				after: pageInput.after,
			};
			rows = loadForward(forwardInput) as Cursor[];
		} else {
			const backwardInput: BackwardPageInit<TableSchema, Cursor> = {
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

	prepareFindMany<Cursor extends PrimaryKeyRecord<TableSchema>>(options: {
		filter?: Expression<TableSchema>;
		orderBy: readonly {
			column: PrimaryKey<TableSchema>[number];
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
		} = compileFindMany(this.table, {
			filter,
			orderBy,
		});

		return {
			loadForward: (pageInput: ForwardPageInit<TableSchema, Cursor>) =>
				pageInput.after === undefined
					? this.prepareQueryAll(loadFirst)({ limit: pageInput.first })
					: this.prepareQueryAll(loadNext)({
							cursor: pageInput.after,
							limit: pageInput.first,
						}),
			loadBackward: (pageInput: BackwardPageInit<TableSchema, Cursor>) =>
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

	private preparedInsert: PreparedMutation<Row<TableSchema>>;
	private preparedUpsert: PreparedMutation<Row<TableSchema>>;
	private preparedDelete: PreparedMutation<PrimaryKeyRecord<TableSchema>>;
	private preparedFindUnique: PreparedQueryOne<
		PrimaryKeyRecord<TableSchema>,
		Row<TableSchema>
	>;
}
