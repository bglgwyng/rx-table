import Database from "better-sqlite3";
import { BetterSqlite3Storage } from "./BetterSqlite3Storage.mjs";
import {
	testStorageImplementation,
	type UserTable,
} from "../testStorageImplementation.mjs";

testStorageImplementation("SqliteStorage", () => {
	const db = new Database(":memory:");
	db.exec(`CREATE TABLE users (
    id INTEGER PRIMARY KEY,
    name TEXT
  )`);
	return new BetterSqlite3Storage<UserTable>(db, "users", ["id"]);
});
