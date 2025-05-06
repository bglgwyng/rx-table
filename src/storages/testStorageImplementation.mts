import { describe, it, expect, beforeEach } from "vitest";
import type { Storage } from "../Storage.mjs";
import type {
	Row,
	PrimaryKeyRecord,
	TableSchemaBase,
} from "../types/TableSchema.mjs";

// UserTable 타입 정의 (테스트 목적)
export const userTableSchema = {
	name: "User",
	columns: {
		id: { kind: "number" },
		name: { kind: "string" },
	},
	primaryKey: ["id"] as const,
} satisfies TableSchemaBase;
type UserTable = typeof userTableSchema;

type TestRow = Row<UserTable>;
type TestKey = PrimaryKeyRecord<UserTable>;

export function testStorageImplementation(
	name: string,
	create: () => Storage<UserTable>,
) {
	describe(`${name} Storage 인터페이스`, () => {
		let storage: Storage<UserTable>;

		beforeEach(() => {
			storage = create();
		});

		it("insert and findUnique should work", () => {
			storage.insert({ id: 1, name: "Alice" });
			const row = storage.findUnique({ id: 1 } as TestKey);
			expect(row).toEqual({ id: 1, name: "Alice" });
		});

		it("update should change row", () => {
			storage.insert({ id: 1, name: "Alice" });
			storage.update({ id: 1 } as TestKey, { name: "Bob" });
			const row = storage.findUnique({ id: 1 } as TestKey);
			expect(row).toEqual({ id: 1, name: "Bob" });
		});

		it("delete should remove row", () => {
			storage.insert({ id: 1, name: "Alice" });
			storage.delete({ id: 1 } as TestKey);
			const row = storage.findUnique({ id: 1 } as TestKey);
			expect(row).toBeNull();
		});

		it("upsert should insert or update", () => {
			storage.upsert({ id: 1, name: "Alice" });
			expect(storage.findUnique({ id: 1 } as TestKey)).toEqual({
				id: 1,
				name: "Alice",
			});
			storage.upsert({ id: 1, name: "Bob" });
			expect(storage.findUnique({ id: 1 } as TestKey)).toEqual({
				id: 1,
				name: "Bob",
			});
		});

		it("findUnique returns null for missing row", () => {
			expect(storage.findUnique({ id: 999 } as TestKey)).toBeNull();
		});
	});

	describe("prepared mutations", () => {
		let storage: Storage<UserTable>;

		beforeEach(() => {
			storage = create();
		});
		it("should insert a row using preparedInsertRow and persist it", () => {
			storage.insert({ id: 1, name: "Alice" });
			const row = storage.findUnique({ id: 1 });
			expect(row).toEqual({ id: 1, name: "Alice" });
		});

		it("should insert multiple rows using preparedInsertRow (via insert)", () => {
			storage.insert({ id: 1, name: "Alice" });
			storage.insert({ id: 2, name: "Bob" });

			expect(storage.findUnique({ id: 1 })).toEqual({ id: 1, name: "Alice" });
			expect(storage.findUnique({ id: 2 })).toEqual({ id: 2, name: "Bob" });
		});

		it("should delete a row using preparedDeleteRow and persist it", () => {
			storage.insert({ id: 1, name: "Alice" });
			storage.insert({ id: 2, name: "Bob" });
			storage.delete({ id: 1 });
			expect(storage.findUnique({ id: 1 })).toBeNull();
			expect(storage.findUnique({ id: 2 })).toEqual({ id: 2, name: "Bob" });
		});

		it("should upsert a row using preparedUpsertRow (insert if not exists)", () => {
			storage.upsert({ id: 3, name: "Charlie" });
			expect(storage.findUnique({ id: 3 })).toEqual({ id: 3, name: "Charlie" });
		});

		it("should upsert a row using preparedUpsertRow (update if exists)", () => {
			storage.insert({ id: 4, name: "David" });
			storage.upsert({ id: 4, name: "Daniel" });
			expect(storage.findUnique({ id: 4 })).toEqual({ id: 4, name: "Daniel" });
		});
	});
}
