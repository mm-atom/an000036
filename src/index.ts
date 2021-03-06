import { parse } from 'url';
import config from '@mmstudio/config';
import global from '@mmstudio/global';
import anylogger from 'anylogger';
import Pool from 'pg-pool';
import { Client, types } from 'pg';

const logger = anylogger('@mmstudio/an000036');

const db = config.db as string;

types.setTypeParser(types.builtins.INT8, (v) => {
	return BigInt(v);
});

type Table = Record<string, string | number | boolean>;

/**
 * sql语句查询
 */
export default function sql_query<T1 = Table, T2 = Table, T3 = Table, T4 = Table, T5 = Table, T6 = Table, T7 = Table, T8 = Table, T9 = Table, T10 = Table, T11 = Table, T12 = Table, T13 = Table, T14 = Table, T15 = Table, T16 = Table, T17 = Table, T18 = Table, T19 = Table, T20 = Table>(...sqls: [string, unknown[]][]) {
	return postgres_sql(sqls, db) as Promise<[T1[], T2[], T3[], T4[], T5[], T6[], T7[], T8[], T9[], T10[], T11[], T12[], T13[], T14[], T15[], T16[], T17[], T18[], T19[], T20[]]>;
}

// Position the bindings for the query. The escape sequence for question mark
// is \? (e.g. knex.raw("\\?") since javascript requires '\' to be escaped too...)
function position_bindings(sql: string) {
	let questionCount = 0;
	return sql.replace(/(\\*)(\?)/g, (_match, escapes: string) => {
		if (escapes.length % 2) {
			return '?';
		}
		questionCount++;
		return `$${questionCount}`;
	});
}

async function postgres_sql(sqls: [string, unknown[]][], source: string) {
	logger.debug('postgres sql:', sqls);
	const pool = postgres_get_pool(source);
	const client = await pool.connect();
	try {
		const ret = await Promise.all(sqls.map(async ([sql, values]) => {
			const r = await client.query(position_bindings(sql), values);
			return r.rows as unknown[];
		}));
		logger.debug('postgres sqlquery result:', ret);
		return ret;
	} finally {
		client.release();
	}
}

const key = 'dbpostgres';

function postgres_get_pool(source: string) {
	let db = global(key, null as unknown as Pool<Client>);
	if (!db) {
		const options = parse_url(source);
		db = new Pool(options);
	}
	return global(key, db);
}

function parse_url(url: string) {
	const params = parse(url, true);
	const auth = params.auth!.split(':');
	return {
		debug: config.debug,
		user: auth[0],
		password: auth[1],
		host: params.hostname!,
		port: parseInt(params.port!, 10),
		database: params.pathname!.split('/')[1],
		...params.query
	};
}
