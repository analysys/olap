SELECT
		    sumIf(c, level >= 1) AS _1,
		    sumIf(c, level >= 2) AS _2,
		    sumIf(c, level >= 3) AS _3,
		    sumIf(c, level >= 4) AS _4
		FROM
		(
		    SELECT
		        level,
		        count(*) AS c
		    FROM
		    (
		        SELECT
		            user_id,
		            path(2592000, 10004, 10008, 10009,10010)(timestamp_nc, event_id_nc) AS level
		        FROM event
		        WHERE ( (event_date_nc >= toDate('2017-07-01')) AND (event_date_nc <= toDate('2017-08-31')) AND ( (event_id_nc IN (10008, 10009, 10010)) OR
		                    (event_id_nc = 10004 AND (event_tag_brand = 'Apple' or event_tag_brand = 'LianX')  ) ) )
		        GROUP BY user_id
		    )
		    GROUP BY level
		    ORDER BY level ASC
		);