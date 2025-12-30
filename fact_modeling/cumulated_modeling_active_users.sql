CREATE TABLE user_cumulated (
	user_id			TEXT,
	dates_active  	DATE[],
	date			DATE,
	PRIMARY KEY(user_id, date)
)


INSERT INTO user_cumulated
WITH yesterday AS(
	SELECT 
		*
	FROM user_cumulated
	WHERE date = DATE '2023-01-30'
),
today AS(
	SELECT 
		user_id::TEXT,
		event_time::date AS dates_active
	FROM events
	WHERE event_time::date = DATE '2023-01-31' AND user_id IS NOT NULL
	GROUP BY 1,2
)

SELECT 
	COALESCE(t.user_id, y.user_id) AS user_id,
	CASE 
		WHEN y.dates_active IS NULL THEN ARRAY[t.dates_active]
		WHEN t.dates_active IS NULL THEN y.dates_active
		ELSE y.dates_active || ARRAY[t.dates_active]
	END AS dates_active,
	COALESCE(t.dates_active, y.date + INTERVAL '1 day') AS date
FROM yesterday y
FULL OUTER JOIN today t ON y.user_id = t.user_id

---------------- Bit Map ------------------
WITH users AS(
	SELECT * FROM user_cumulated
	WHERE date = '2023-01-31'
),
series AS(
	SELECT * 
	FROM generate_series(DATE '2023-01-01', DATE '2023-01-31', INTERVAL '1 day') AS series_date
),
place_holder_ints AS(
	SELECT 
		CASE 
			WHEN dates_active @> ARRAY[series_date::DATE] THEN CAST((POW(2, 32 - (date - series_date::DATE))) AS BIGINT)
			ELSE 0
		END AS placeholder_int_value,
		*
	FROM users CROSS JOIN series 
)



------------- Analytical Queries ---------------
select 
	user_id,
	CAST(CAST(SUM(placeholder_int_value) AS BIGINT) AS BIT(32)),
	((CAST(SUM(placeholder_int_value) AS BIGINT) & 2130706432) > 0) AS dim_is_weekly_active
from place_holder_ints
GROUP BY 1

	



