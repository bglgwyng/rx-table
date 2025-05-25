import Database from "better-sqlite3";
import {
	schema,
	testStorageImplementation,
} from "../testStorageImplementation.mjs";
import { BetterSqlite3Storage } from "./BetterSqlite3Storage.mjs";

type UserTable = typeof schema.User;

testStorageImplementation("SqliteStorage", () => {
	const db = new Database(":memory:");
	db.exec(`CREATE TABLE "User" (
    id INTEGER PRIMARY KEY,
    name TEXT
  )`);
	return new BetterSqlite3Storage(schema, db);
});
