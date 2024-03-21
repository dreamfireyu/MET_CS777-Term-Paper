--CREATE INDEX FOR ALL FOREIGH KEYS
--Replace Seq Scan with Bitmap Index Scan
CREATE INDEX IF NOT EXISTS idx_fsm_stock_id ON "Trading".Fact_Stock_Minute(Stock_ID);
CREATE INDEX IF NOT EXISTS idx_fsm_minute_id ON "Trading".Fact_Stock_Minute(Minute_ID);
CREATE INDEX IF NOT EXISTS idx_fce_stock_id ON "Trading".Fact_Company_Event(Stock_ID);
CREATE INDEX IF NOT EXISTS idx_fce_date_id ON "Trading".Fact_Company_Event(Date_ID);

-- select any stock from Shanghai Stock Exchange(SSE)
SELECT ds.stock_id, ds.total_value from "Trading".Dim_Stock ds
where stock_id>600000
order by stock_id
limit 10;

--select all stocks that rise over 9.9% on 2024-03-11
SELECT * from "Trading".Fact_Stock_Daily
where Date_ID = 20240311 and percent_of_incre_decre>9.9
order by percent_of_incre_decre desc;

--select infomation from 20231120 9:30-9:40 on stock SH600004
SELECT * from "Trading".Fact_Stock_Minute
where Minute_ID>202311200930 and Minute_ID<202311200940 and stock_id = 600004;

--show total minutes in 2024-03-11
SELECT count(Minute_ID) AS Total_Minutes
from "Trading".Dim_Minute
where DATE_TRUNC('day',CAST(Date AS timestamp))='2024-03-11';

--show total trading days in Feburary 2024
SELECT count(Date_ID) AS Total_Trading_Days
from "Trading".Dim_Date
where year = 2024 and month = 2;

--show recent company events from September
SELECT de.Stock_ID, de.Event_Date, de.Event_Type, de.Current_Flag,
		de.Effective_Timestamp, de.Expire_Timestamp
FROM "Trading".Fact_Company_Event fce
JOIN "Trading".Dim_Event de
ON fce.Event_Dim_ID = de.Event_Dim_ID
where fce.Date_ID>20230901 and fce.Stock_ID>600000 and fce.Stock_ID<600010;

--select Minute Level Data from 2024-03-11 9:30 to 2024-03-11 9:40 
-- where the company is in CN market and its Total_Value is in top 10
SELECT fsm.FSM_ID, fsm.Stock_ID, fsm.Minute_ID, fsm.Close, fsm.High, fsm.Low, fsm.volume
FROM "Trading".Fact_Stock_Minute fsm
WHERE fsm.Stock_ID in (
	SELECT Stock_ID
	FROM "Trading".Dim_Stock
	WHERE Stock_Market = 'CN'
	ORDER BY Total_Value DESC
	LIMIT 10
)
AND fsm.Minute_ID >= 202311210930 AND fsm.Minute_ID <= 202311210940;


--select Daily Situations for every Monday in November of company SZ300796;
--Performance Tuning
--version 1
SELECT dd.Month, dd.WeekDay, fsd.Stock_ID, fsd.Date_ID, fsd.Open,
		fsd.High,fsd.Low, fsd.Volume, fsd.Turnover
FROM "Trading".Fact_Stock_Daily fsd 
JOIN "Trading".Dim_Date dd ON fsd.Date_ID = dd.Date_ID
WHERE dd.Month = 11 and dd.WeekDay=1 and fsd.Stock_ID = 300796;

--version 2
SELECT dd.Month, dd.WeekDay, fsd.Stock_ID, fsd.Date_ID, fsd.Open,
		fsd.High,fsd.Low, fsd.Volume, fsd.Turnover
FROM "Trading".Fact_Stock_Daily fsd 
JOIN (
	SELECT Date_ID, Month, WeekDay
	FROM "Trading".Dim_Date
	WHERE Month = 11 AND WeekDay = 1
) dd ON fsd.Date_ID = dd.Date_ID
WHERE fsd.Stock_ID = 300796;

DROP INDEX IF EXISTS "Trading".idx_fsd_stock_id;
DROP INDEX IF EXISTS "Trading".idx_fsd_date_id;
CREATE INDEX idx_fsd_stock_id ON "Trading".Fact_Stock_Daily(Stock_ID);
CREATE INDEX idx_fsd_date_id ON "Trading".Fact_Stock_Daily(Date_ID);

-- In event, we first filter out the latest events from 2023-11-20 to 2023-11-24, 
-- and from these events, we check the stock price changes of the corresponding 
-- companies on the next day, and from the companies that have increased by more than 3%, 
-- we categorize the companies according to the time and the type of the event, and finally, 
--  we sort them according to the date, and the average growth of the stock price.
--version 1
with event_stock as(
	select distinct ds.stock_id, fce.Date_ID, de.Event_Type
	from "Trading".Fact_Company_Event fce
	join "Trading".dim_event de on fce.Event_Dim_ID = de.Event_Dim_ID
	join "Trading".Dim_Stock ds on ds.Stock_ID = fce.Stock_ID
	where de.Expire_Timestamp isnull 
	and fce.Date_ID between 20231120 and 20231124
	order by fce.Date_ID)
select distinct
	tmp.event_type,
	tmp.date_id,
	tmp.avg_incre
from (
select 
	fsd.stock_id, fsd.date_id, es.event_type,fsd.percent_of_incre_decre,
	AVG(fsd.percent_of_incre_decre) OVER(PARTITION BY es.event_type) avg_incre
from "Trading".Fact_Stock_Daily as fsd
join event_stock es on 
fsd.Stock_ID = es.stock_id and 
fsd.Date_ID = es.Date_ID + 1 and
fsd.percent_of_incre_decre>3.0
group by fsd.stock_id,fsd.date_id, es.event_type,fsd.percent_of_incre_decre
) AS tmp
group by date_id,event_type,tmp.avg_incre
order by tmp.date_id,tmp.avg_incre

--version2
CREATE INDEX IF NOT EXISTS idx_fsm_stock_id ON "Trading".Fact_Stock_Minute(Stock_ID);
CREATE INDEX IF NOT EXISTS idx_fsm_minute_id ON "Trading".Fact_Stock_Minute(Minute_ID);
CREATE INDEX IF NOT EXISTS idx_fce_stock_id ON "Trading".Fact_Company_Event(Stock_ID);
CREATE INDEX IF NOT EXISTS idx_fce_date_id ON "Trading".Fact_Company_Event(Date_ID);

WITH event_stock AS (
  SELECT distinct ds.stock_id, fce.Date_ID, de.Event_Type
  FROM "Trading".Fact_Company_Event fce
  JOIN "Trading".dim_event de ON fce.Event_Dim_ID = de.Event_Dim_ID
  JOIN "Trading".Dim_Stock ds ON ds.Stock_ID = fce.Stock_ID
  WHERE de.Expire_Timestamp isnull 
  AND fce.Date_ID between 20231120 and 20231124
),
tmp AS (
  SELECT 
    fsd.stock_id, fsd.date_id, es.event_type,
    AVG(fsd.percent_of_incre_decre) OVER(PARTITION BY es.event_type) avg_incre
  FROM "Trading".Fact_Stock_Daily as fsd
  JOIN event_stock es ON 
    fsd.Stock_ID = es.stock_id and 
    fsd.Date_ID = es.Date_ID + 1 and
    fsd.percent_of_incre_decre>3.0
  GROUP BY fsd.stock_id,fsd.date_id, es.event_type,fsd.percent_of_incre_decre
)
SELECT distinct
tmp.event_type,
tmp.date_id,
tmp.avg_incre
FROM tmp
GROUP BY date_id,event_type,tmp.avg_incre
ORDER BY tmp.date_id,tmp.avg_incre


