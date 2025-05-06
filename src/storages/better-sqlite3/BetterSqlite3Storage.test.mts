import Database from "better-sqlite3";
import {
	testStorageImplementation,
	userTableSchema,
} from "../testStorageImplementation.mjs";
import { BetterSqlite3Storage } from "./BetterSqlite3Storage.mjs";

type UserTable = typeof userTableSchema;

testStorageImplementation("SqliteStorage", () => {
	const db = new Database(":memory:");
	db.exec(`CREATE TABLE "User" (
    id INTEGER PRIMARY KEY,
    name TEXT
  )`);
	return new BetterSqlite3Storage<UserTable>(userTableSchema, db);
});
