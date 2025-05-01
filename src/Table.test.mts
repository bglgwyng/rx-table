import Database from "better-sqlite3";
import { describe, expect, it } from "vitest";
import { Table } from "./Table.mjs";
import { SqliteStorage } from "./storages/SqliteStorage.mjs";

type UserTable = {
	columns: {
		id: number;
		name: string;
	};
	primaryKey: ["id"];
};

function createSqliteStorage() {
	const db = new Database(":memory:");
	db.exec(`CREATE TABLE users (
    id INTEGER PRIMARY KEY,
    name TEXT
  )`);
	return new SqliteStorage<UserTable>(db, "users", ["id"]);
}

describe("Table", () => {
	function setup() {
		const storage = createSqliteStorage();
		const table = new Table(["id"], storage);
		return { storage, table };
	}

	it("insert and findUnique should work", () => {
		const { table } = setup();
		table.insert({ id: 1, name: "Alice" });
		const dynamic = table.findUnique({ id: 1 });
		expect(dynamic.read()).toEqual({ id: 1, name: "Alice" });
	});

	it("update should trigger dynamic and update value", () => {
		const { table } = setup();
		table.insert({ id: 1, name: "Alice" });
		const dynamic = table.findUnique({ id: 1 });

		let observed: { id: number; name: string } | undefined;
		const sub = dynamic.updated.subscribe(() => {
			observed = dynamic.read() ?? undefined;
		});

		table.update({ id: 1 }, { name: "Bob" });
		expect(observed).toEqual({ id: 1, name: "Bob" });
		sub.unsubscribe();
	});

	it("delete should trigger dynamic and set value to null", () => {
		const { table } = setup();
		table.insert({ id: 1, name: "Alice" });
		const dynamic = table.findUnique({ id: 1 });

		let observed: { id: number; name: string } | null = null;
		const sub = dynamic.updated.subscribe(() => {
			observed = dynamic.read();
		});

		table.delete({ id: 1 });
		expect(observed).toBe(null);
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
