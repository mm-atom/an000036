const test = require('ava');

const { default: a } = require('../dist/index');

test.before(async () => {
	await a(['drop table if exists test_tb001;'],
		['create table test_tb001 (name text);'],
		[`insert into test_tb001 (name) values ($1),($2),($3);`, ['mmstudio001', 'mmstudio002', 'mmstudio003']]
	);
	// await a(['drop table if exists test_tb001;'],
	// 	['create table test_tb001 (name text);'],
	// 	[`insert into test_tb001 (name) values (?),(?),(?);`, ['mmstudio001', 'mmstudio002', 'mmstudio003']]
	// );
});

test.after(async () => {
	await a(['drop table if exists test_tb001;']);
});

test('postgres query', async (t) => {
	const [r] = await a(['select * from test_tb001']);
	console.warn(r);
	t.assert(r.length === 3);
});

test('postgres count', async (t) => {
	const [r] = await a(['select count(*) as cnt from test_tb001']);
	console.error('11111', r);
	t.is(r[0].cnt, 3n);
});
