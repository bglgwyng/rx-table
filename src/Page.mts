import type { SqlExpression } from "./sql/SqlExpression.mjs";
import type {
	TableSchemaBase,
	PrimaryKeyRecord,
	Row,
	ColumnName,
} from "./types/TableSchema.mjs";

export type PageDelta<T extends TableSchemaBase> = (
	| {
			kind: "remove";
			key: PrimaryKeyRecord<T>;
	  }
	| {
			kind: "add";
			row: Row<T>;
	  }
)[];

export type Page<T extends TableSchemaBase> = {
	rows: Iterable<PrimaryKeyRecord<T>>;
	rowCount: number;
	endCursor: unknown;
	startCursor: unknown;
};
// Left-closed: only after is set

export type ForwardPageInput<T extends TableSchemaBase> = {
	kind: "forward";
	after?: PrimaryKeyRecord<T>;
	first: number;
};
// Right-closed: only before is set

export type BackwardPageInput<T extends TableSchemaBase> = {
	kind: "backward";
	before?: PrimaryKeyRecord<T>;
	last: number;
};

export type PageInput<T extends TableSchemaBase> = (
	| ForwardPageInput<T>
	| BackwardPageInput<T>
) & {
	orderBy: {
		column: ColumnName<T>;
		direction: "asc" | "desc";
	}[];
	filter?: SqlExpression<T, unknown>;
};

export type PageInputDelta =
	| { kind: "loadPrev"; count?: number }
	| { kind: "loadNext"; count?: number };

export type Direction = "asc" | "desc";

export function invertDirection(dir: Direction): Direction {
	return dir === "asc" ? "desc" : "asc";
}
