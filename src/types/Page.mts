import type { SqlExpression } from "./SqlExpression.mjs";
import type { TableBase, PrimaryKeyRecord, Row, ColumnName } from "./Table.mjs";

export type PageDelta<T extends TableBase> = (
	| {
			kind: "remove";
			key: PrimaryKeyRecord<T>;
	  }
	| {
			kind: "add";
			row: Row<T>;
	  }
)[];

export type Page<T extends TableBase> = {
	rows: Iterable<PrimaryKeyRecord<T>>;
	rowCount: number;
	endCursor: unknown;
	startCursor: unknown;
};
// Left-closed: only after is set

export type PageInputLeftClosed<T extends TableBase> = {
	after?: PrimaryKeyRecord<T>;
	first: number;
};
// Right-closed: only before is set

export type PageInputRightClosed<T extends TableBase> = {
	before?: PrimaryKeyRecord<T>;
	last: number;
};

export type PageInput<T extends TableBase> = (
	| PageInputLeftClosed<T>
	| PageInputRightClosed<T>
) & {
	orderBy?: {
		column: ColumnName<T>;
		direction: "asc" | "desc";
	}[];
	filter?: SqlExpression<T, unknown>;
};

export type PageInputDelta<T extends TableBase> =
	| { kind: "loadPrev"; count?: number }
	| { kind: "loadNext"; count?: number };
