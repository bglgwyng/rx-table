export type PreparedQueryAll<Context, Row = unknown> = (
	context?: Context,
) => Row[];

export type PreparedQueryOne<Context, Row = unknown> = (
	context?: Context,
) => Row | null;

export type PreparedMutation<Context> = (context?: Context) => void;
