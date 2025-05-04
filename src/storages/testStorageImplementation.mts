import { describe, it, expect, beforeEach } from "vitest";
import type { Storage } from "../Storage.mjs";
import type { Row, PrimaryKeyRecord } from "../types.mjs";

// UserTable 타입 정의 (테스트 목적)
export type UserTable = {
	columns: {
		id: number;
		name: string;
	};
	primaryKey: ["id"];
};

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
}
