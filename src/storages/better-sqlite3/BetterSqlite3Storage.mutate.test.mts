import Database from "better-sqlite3";
import { beforeEach, describe, expect, it } from "vitest";
import type { TableSchemaBase } from "../../types/TableSchema.mjs";
import { BetterSqlite3Storage } from "./BetterSqlite3Storage.mjs";
import { schema } from "../testStorageImplementation.mjs";
import {
	createTableStorage,
	type Mutation2,
	type TableStorage,
} from "../../Storage.mjs";

// TODO: remove
describe("SqliteStorage.mutate", () => {
	let userTableStorage: TableStorage<(typeof schema)["User"]>;

	beforeEach(() => {
		const db = new Database(":memory:");
		db.exec(`CREATE TABLE User (
      id INTEGER PRIMARY KEY,
      name TEXT
    )`);
		userTableStorage = createTableStorage<typeof schema, "User">(
			new BetterSqlite3Storage<typeof schema>(schema, db),
			"User",
		);
	});

	it("mutate: insert/update/delete/upsert", () => {
		userTableStorage.mutate({ type: "insert", row: { id: 1, name: "Alice" } });
		expect(userTableStorage.findUnique({ id: 1 })).toEqual({
			id: 1,
			name: "Alice",
		});

		userTableStorage.mutate({
			type: "update",
			key: { id: 1 },
			partialRow: { name: "Bob" },
		});
		expect(userTableStorage.findUnique({ id: 1 })).toEqual({
			id: 1,
			name: "Bob",
		});

		userTableStorage.mutate({ type: "upsert", row: { id: 1, name: "Carol" } });
		expect(userTableStorage.findUnique({ id: 1 })).toEqual({
			id: 1,
			name: "Carol",
		});

		userTableStorage.mutate({ type: "delete", key: { id: 1 } });
		expect(userTableStorage.findUnique({ id: 1 })).toBeNull();
	});

	it("mutateMany applies all mutations atomically", () => {
		const mutations: Mutation2<UserTable>[] = [
			{ type: "insert", row: { id: 1, name: "Alice" } },
			{ type: "insert", row: { id: 2, name: "Bob" } },
			{ type: "update", key: { id: 1 }, partialRow: { name: "Carol" } },
			{ type: "delete", key: { id: 2 } },
		];
		userTableStorage.mutateMany(mutations);
		expect(userTableStorage.findUnique({ id: 1 })).toEqual({
			id: 1,
			name: "Carol",
		});
		expect(userTableStorage.findUnique({ id: 2 })).toBeNull();
	});
});
