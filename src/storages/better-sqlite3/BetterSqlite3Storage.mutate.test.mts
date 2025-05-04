import Database from "better-sqlite3";
import { beforeEach, describe, expect, it } from "vitest";
import type { Mutation } from "../../Storage.mjs";
import { BetterSqlite3Storage } from "./BetterSqlite3Storage.mjs";

type UserTable = {
	columns: {
		id: number;
		name: string;
	};
	primaryKey: ["id"];
};

describe("SqliteStorage.mutate", () => {
	let storage: BetterSqlite3Storage<UserTable>;

	beforeEach(() => {
		const db = new Database(":memory:");
		db.exec(`CREATE TABLE users (
      id INTEGER PRIMARY KEY,
      name TEXT
    )`);
		storage = new BetterSqlite3Storage<UserTable>(db, "users", ["id"]);
	});

	it("mutate: insert/update/delete/upsert", () => {
		storage.mutate({ type: "insert", row: { id: 1, name: "Alice" } });
		expect(storage.findUnique({ id: 1 })).toEqual({ id: 1, name: "Alice" });

		storage.mutate({
			type: "update",
			key: { id: 1 },
			partialRow: { name: "Bob" },
		});
		expect(storage.findUnique({ id: 1 })).toEqual({ id: 1, name: "Bob" });

		storage.mutate({ type: "upsert", row: { id: 1, name: "Carol" } });
		expect(storage.findUnique({ id: 1 })).toEqual({ id: 1, name: "Carol" });

		storage.mutate({ type: "delete", key: { id: 1 } });
		expect(storage.findUnique({ id: 1 })).toBeNull();
	});

	it("mutateMany applies all mutations atomically", () => {
		const mutations: Mutation<UserTable>[] = [
			{ type: "insert", row: { id: 1, name: "Alice" } },
			{ type: "insert", row: { id: 2, name: "Bob" } },
			{ type: "update", key: { id: 1 }, partialRow: { name: "Carol" } },
			{ type: "delete", key: { id: 2 } },
		];
		storage.mutateMany(mutations);
		expect(storage.findUnique({ id: 1 })).toEqual({ id: 1, name: "Carol" });
		expect(storage.findUnique({ id: 2 })).toBeNull();
	});
});
