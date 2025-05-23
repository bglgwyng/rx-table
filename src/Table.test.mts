import Database from "better-sqlite3";
import { describe, expect, it } from "vitest";
import { Table } from "./Table.mjs";
import { BetterSqlite3Storage } from "./storages/better-sqlite3/BetterSqlite3Storage.mjs";
import type { TableSchemaBase } from "./types/TableSchema.mjs";

const userTableSchema = {
	name: "User",
	columns: {
		id: { kind: "number" },
		name: { kind: "string" },
	},
	primaryKey: ["id"] as const,
} satisfies TableSchemaBase;
type UserTable = typeof userTableSchema;

function createSqliteStorage() {
	const db = new Database(":memory:");
	db.exec(`CREATE TABLE "User" (
    id INTEGER PRIMARY KEY,
    name TEXT
  )`);
	return new BetterSqlite3Storage<UserTable>(userTableSchema, db);
}

describe("Table", () => {
	function setup() {
		const storage = createSqliteStorage();
		const table = new Table<UserTable>(userTableSchema, storage);
		return { storage, table };
	}

	it("insert and findUnique should work, and persist to storage", () => {
		const { table, storage } = setup();
		table.insert({ id: 1, name: "Alice" });
		const dynamic = table.findUnique({ id: 1 });
		expect(dynamic.read()).toEqual({ id: 1, name: "Alice" });
		// storage 레벨에서 실제로 값이 들어갔는지 확인
		const row = storage.findUnique({ id: 1 });
		expect(row).toEqual({ id: 1, name: "Alice" });
	});

	it("update should trigger dynamic and update value, and persist to storage", () => {
		const { table, storage } = setup();
		table.insert({ id: 1, name: "Alice" });
		const dynamic = table.findUnique({ id: 1 });

		let observed: { id: number; name: string } | null | undefined;
		const sub = dynamic.updated.subscribe(() => {
			observed = dynamic.read();
		});

		table.update({ id: 1 }, { name: "Bob" });
		expect(observed).toEqual({ id: 1, name: "Bob" });
		const row = storage.findUnique({ id: 1 });
		expect(row).toEqual({ id: 1, name: "Bob" });
		sub.unsubscribe();
	});

	it("delete should trigger dynamic and set value to null, and persist to storage", () => {
		const { table } = setup();
		table.insert({ id: 1, name: "Alice" });
		const dynamic = table.findUnique({ id: 1 });

		let observed: { id: number; name: string } | null = null;
		const sub = dynamic.updated.subscribe(() => {
			observed = dynamic.read();
		});

		table.delete({ id: 1 });
		expect(observed).toBe(null);
		// storage 레벨에서 실제로 값이 삭제됐는지 확인
		const { storage } = setup();
		const row = storage.findUnique({ id: 1 });
		expect(row).toBeNull();
		sub.unsubscribe();
	});

	it("multiple findUnique calls return independent dynamics", () => {
		const { table } = setup();
		table.insert({ id: 1, name: "Alice" });
		const d1 = table.findUnique({ id: 1 });
		const d2 = table.findUnique({ id: 1 });
		expect(d1).not.toBe(d2);
		d1.disconnect();
		expect(() => d2.read()).not.toThrow();
	});
});
