-- =====================================================================
-- 02_populate_dim_date.sql
-- Fills dim_date for 2016-01-01 .. 2018-12-31.
-- Covers the purchase range in the data (2016-09-04 .. 2017-11-07)
-- with margin on both sides. Run after 01_create_schema.sql.
-- =====================================================================
USE OlistDW;
GO

-- deleted the drop statment of the dim date anyway we will run this only once

WITH d AS (
    SELECT CAST('2016-01-01' AS date) AS dt
    UNION ALL
    SELECT DATEADD(day, 1, dt) FROM d WHERE dt < '2018-12-31'
)
INSERT INTO dim_date (date_key, full_date, year, quarter, month, day, weekday_name, is_weekend)
SELECT CAST(FORMAT(dt, 'yyyyMMdd') AS int),
       dt,
       YEAR(dt),
       DATEPART(quarter, dt),
       MONTH(dt),
       DAY(dt),
       DATENAME(weekday, dt),
       CASE WHEN DATENAME(weekday, dt) IN ('Saturday', 'Sunday') THEN 1 ELSE 0 END
FROM d
OPTION (MAXRECURSION 0);
GO

SELECT COUNT(*) AS dim_date_rows FROM dim_date;   -- expect 1096
GO