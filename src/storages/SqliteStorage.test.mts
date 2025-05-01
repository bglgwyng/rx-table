import Database from "better-sqlite3";
import { beforeEach, describe, expect, it } from "vitest";
import { SqliteStorage } from "./SqliteStorage.mjs";

// UserTable 타입 정의 (TableBase 준수)
type UserTable = {
	columns: {
		id: number;
		name: string;
	};
	primaryKey: ["id"];
};

describe("SqliteStorage", () => {
	let storage: SqliteStorage<UserTable>;

	beforeEach(() => {
		const db = new Database(":memory:");
		db.exec(`CREATE TABLE users (
      id INTEGER PRIMARY KEY,
      name TEXT
    )`);
		storage = new SqliteStorage<UserTable>(db, "users", ["id"]);
	});

	it("insert and findUnique should work", () => {
		storage.insert({ id: 1, name: "Alice" });
		const row = storage.findUnique({ id: 1 });
		expect(row).toEqual({ id: 1, name: "Alice" });
	});

	it("update should change row", () => {
		storage.insert({ id: 1, name: "Alice" });
		storage.update({ id: 1 }, { name: "Bob" });
		const row = storage.findUnique({ id: 1 });
		expect(row).toEqual({ id: 1, name: "Bob" });
	});

	it("delete should remove row", () => {
		storage.insert({ id: 1, name: "Alice" });
		storage.delete({ id: 1 });
		const row = storage.findUnique({ id: 1 });
		expect(row).toBeNull();
	});

	it("upsert should insert or update", () => {
		storage.upsert({ id: 1, name: "Alice" });
		expect(storage.findUnique({ id: 1 })).toEqual({ id: 1, name: "Alice" });
		storage.upsert({ id: 1, name: "Bob" });
		expect(storage.findUnique({ id: 1 })).toEqual({ id: 1, name: "Bob" });
	});

	it("findUnique returns null for missing row", () => {
		expect(storage.findUnique({ id: 999 })).toBeNull();
	});
});
