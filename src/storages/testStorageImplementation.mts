import { beforeEach, describe, expect, it } from "vitest";
import {
	createTableStorage,
	type Storage,
	type TableStorage,
} from "../Storage.mjs";
import type {
	PrimaryKeyRecord,
	Row,
	TableSchemaBase,
} from "../types/TableSchema.mjs";

// UserTable 타입 정의 (테스트 목적)
export const schema = {
	User: {
		name: "User",
		columns: {
			id: { kind: "number" },
			name: { kind: "string" },
		},
		primaryKey: ["id"],
	} satisfies TableSchemaBase,
};
type UserTable = typeof schema.User;

type TestRow = Row<UserTable>;
type TestKey = PrimaryKeyRecord<UserTable>;

export function testStorageImplementation(
	name: string,
	create: () => Storage<typeof schema>,
) {
	describe(`${name} Storage interface`, () => {
		let userTableStorage: TableStorage<UserTable>;

		beforeEach(() => {
			const storage = create();
			userTableStorage = createTableStorage(storage, "User");
		});

		it("insert and findUnique should work", () => {
			userTableStorage.insert({ id: 1, name: "Alice" });
			const row = userTableStorage.findUnique({ id: 1 });
			expect(row).toEqual({ id: 1, name: "Alice" });
		});

		it("update should change row", () => {
			userTableStorage.insert({ id: 1, name: "Alice" });
			userTableStorage.update({ id: 1 } as TestKey, { name: "Bob" });
			const row = userTableStorage.findUnique({ id: 1 } as TestKey);
			expect(row).toEqual({ id: 1, name: "Bob" });
		});

		it("delete should remove row", () => {
			userTableStorage.insert({ id: 1, name: "Alice" });
			userTableStorage.delete({ id: 1 } as TestKey);
			const row = userTableStorage.findUnique({ id: 1 } as TestKey);
			expect(row).toBeNull();
		});

		it("upsert should insert or update", () => {
			userTableStorage.upsert({ id: 1, name: "Alice" });
			expect(userTableStorage.findUnique({ id: 1 } as TestKey)).toEqual({
				id: 1,
				name: "Alice",
			});
			userTableStorage.upsert({ id: 1, name: "Bob" });
			expect(userTableStorage.findUnique({ id: 1 } as TestKey)).toEqual({
				id: 1,
				name: "Bob",
			});
		});

		it("findUnique returns null for missing row", () => {
			expect(userTableStorage.findUnique({ id: 999 } as TestKey)).toBeNull();
		});
	});

	describe("prepared mutations", () => {
		let userTableStorage: TableStorage<UserTable>;

		beforeEach(() => {
			const storage = create();
			userTableStorage = createTableStorage(storage, "User");
		});
		it("should insert a row using preparedInsertRow and persist it", () => {
			userTableStorage.insert({ id: 1, name: "Alice" });
			const row = userTableStorage.findUnique({ id: 1 });
			expect(row).toEqual({ id: 1, name: "Alice" });
		});

		it("should insert multiple rows using preparedInsertRow (via insert)", () => {
			userTableStorage.insert({ id: 1, name: "Alice" });
			userTableStorage.insert({ id: 2, name: "Bob" });

			expect(userTableStorage.findUnique({ id: 1 })).toEqual({
				id: 1,
				name: "Alice",
			});
			expect(userTableStorage.findUnique({ id: 2 })).toEqual({
				id: 2,
				name: "Bob",
			});
		});

		it("should delete a row using preparedDeleteRow and persist it", () => {
			userTableStorage.insert({ id: 1, name: "Alice" });
			userTableStorage.insert({ id: 2, name: "Bob" });
			userTableStorage.delete({ id: 1 });
			expect(userTableStorage.findUnique({ id: 1 })).toBeNull();
			expect(userTableStorage.findUnique({ id: 2 })).toEqual({
				id: 2,
				name: "Bob",
			});
		});

		it("should upsert a row using preparedUpsertRow (insert if not exists)", () => {
			userTableStorage.upsert({ id: 3, name: "Charlie" });
			expect(userTableStorage.findUnique({ id: 3 })).toEqual({
				id: 3,
				name: "Charlie",
			});
		});

		it("should upsert a row using preparedUpsertRow (update if exists)", () => {
			userTableStorage.insert({ id: 4, name: "David" });
			userTableStorage.upsert({ id: 4, name: "Daniel" });
			expect(userTableStorage.findUnique({ id: 4 })).toEqual({
				id: 4,
				name: "Daniel",
			});
		});
	});
}
