import assert from "assert";
import type { Database } from "better-sqlite3";
import type {
	Mutation,
	ReadableStorage,
	WritableStorage,
} from "../Storage.mjs";
import type {
	Page,
	PageInput,
	PrimaryKey,
	PrimaryKeyRecord,
	Row,
	TableBase,
} from "../types.mjs";

export class SqliteStorage<Table extends TableBase>
	implements WritableStorage<Table>, ReadableStorage<Table>
{
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

	constructor(
		public readonly database: Database,
		public readonly tableName: string,
		public readonly primaryKeys: PrimaryKey<Table>,
	) {}

	/**
	 * Insert a new row into the table
	 */
	insert(row: Row<Table>): void {
		const columns = Object.keys(row);
		const placeholders = columns.map(() => "?").join(", ");
		const values = columns.map((col) => row[col]);

		const stmt = this.database.prepare(`
      INSERT INTO ${this.tableName} (${columns.join(", ")})
      VALUES (${placeholders})
    `);

		stmt.run(...values);
	}

	/**
	 * Insert a row or update it if it already exists (based on primary key)
	 */
	upsert(
		row: Row<Table>,
		update?: Partial<Omit<Row<Table>, PrimaryKey<Table>[number]>>,
	): void {
		const columns = Object.keys(row);
		const placeholders = columns.map(() => "?").join(", ");
		const updateSet = columns
			.filter((col) => !this.primaryKeys.includes(col))
			.map((col) => `${col} = excluded.${col}`)
			.join(", ");
		const values = columns.map((col) => row[col]);

		const stmt = this.database.prepare(`
      INSERT INTO ${this.tableName} (${columns.join(", ")})
      VALUES (${placeholders})
      ON CONFLICT (${this.primaryKeys.join(", ")}) DO UPDATE SET
      ${updateSet}
    `);

		stmt.run(...values);
	}

	/**
	 * Update a row based on its primary key
	 */
	update(key: PrimaryKeyRecord<Table>, changes: Partial<Row<Table>>): void {
		const setClause = Object.keys(changes)
			.map((k) => `${k} = ?`)
			.join(", ");
		const values = Object.values(changes);
		const keyArr = this.primaryKeys.map(
			(pk: PrimaryKey<Table>[number]) => key[pk],
		);
		const stmt = this.database.prepare(`
			UPDATE ${this.tableName}
			SET ${setClause}
			WHERE ${this.primaryKeys.map((pk) => `${pk} = ?`).join(" AND ")}
		`);
		stmt.run(...values, ...keyArr);
	}

	delete(key: PrimaryKeyRecord<Table>): void {
		const keyArr = this.primaryKeys.map(
			(pk: PrimaryKey<Table>[number]) => key[pk],
		);
		const stmt = this.database.prepare(`
			DELETE FROM ${this.tableName}
			WHERE ${this.primaryKeys.map((pk) => `${pk} = ?`).join(" AND ")}
		`);
		stmt.run(...keyArr);
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

	findMany(pageInput: PageInput<Table>): Page<Table> {
		assert.fail("Not implemented");
	}
}
