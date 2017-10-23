# olap
易观olap大赛

1、查询2017年1月份，时间窗口为7天，事件顺序为10001、10004、10008的漏斗，结果为[3999974, 3995900, 3608934]，21s
SELECT ld_sum(xwho_state, 3)
FROM (SELECT ld_count(xwhen, 7 * 86400000, xwhat_id, '10001,10004,10008') AS xwho_state
	FROM t_funnel_devicelog
	WHERE day >= '20170101'
		AND day <= '20170131'
		AND xwhat_id IN (10004, 10001, 10008)
	GROUP BY xwho
	) a;

2、查询2017年1月份，时间窗口为3天，事件顺序为10004、10008、10010的漏斗，结果为[3999422,3573367,697506]，11s
SELECT ld_sum(xwho_state, 3)
FROM (SELECT ld_count(xwhen, 3 * 86400000, xwhat_id, '10004,10008,10010') AS xwho_state
	FROM t_funnel_devicelog
	WHERE day >= '20170101'
		AND day <= '20170131'
		AND xwhat_id IN (10004, 10010, 10008)
	GROUP BY xwho
	) a;

3、查询2017年1月份，时间窗口为3天，事件顺序为10004、10007、10009、10010，并且10004事件的brand属性为’Apple’的漏斗，结果为[3639301, 2449480, 559517, 35795]，14s
SELECT ld_sum(xwho_state, 4)
FROM (SELECT ld_count(xwhen, 3 * 86400000, xwhat_id, '10004,10007,10009,10010') AS xwho_state
	FROM t_funnel_devicelog
	WHERE day >= '20170101'
		AND day <= '20170131'
		AND (xwhat_id IN (10007, 10009, 10010)
			OR xwhat_id = 10004
			AND view_brand = 'Apple')
	GROUP BY xwho
	) a;

