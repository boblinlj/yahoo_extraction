with yahoo_quarterly as (
select a.ticker
, a.asOfDate as Quarterly_Repdate/*reporting month end*/
/*Sum of past three quarters*/
, SUM(a.quarterlyBasicEPS) over (ORDER BY a.asOfDate asc rows between 2 preceding and current row) as BEPS_LAST3Q /*Basic EPS over the past 3 quaters*/
, SUM(a.quarterlyDilutedEPS) over (ORDER BY a.asOfDate asc rows between 2 preceding and current row) as DEPS_LAST3Q /*Diluted EPS over the past 3 quarters*/
, SUM(a.quarterlyTotalRevenue) over (ORDER BY a.asOfDate asc rows between 2 preceding and current row) as REVENUE_LAST3Q /*Total Revenue over the past 3 quarters*/
/*TTM CF and IS Items*/
, SUM(-a.quarterlyCashDividendsPaid) over (ORDER BY a.asOfDate asc rows between 3 preceding and current row) as CASHDIVD_TTM /*Total Cash dividends over the past 4 quarters*/
, SUM(a.quarterlyOperatingCashFlow) over (ORDER BY a.asOfDate asc rows between 3 preceding and current row) as OCF_TTM /*Operating Cash Flow over the past 4 quarters*/
, SUM(a.quarterlyBasicEPS) over (ORDER BY a.asOfDate asc rows between 3 preceding and current row) as BEPS_TTM /*Basic EPS over the past 4 quarters*/
, SUM(a.quarterlyDilutedEPS) over (ORDER BY a.asOfDate asc rows between 3 preceding and current row) as DEPS_TTM /*Diluted EPS over the past 4 quarters*/
, SUM(a.quarterlyNetIncome) over (ORDER BY a.asOfDate asc rows between 3 preceding and current row) as NI_TTM /*Net income over the past 4 quarters*/
, SUM(a.quarterlyResearchAndDevelopment) over (ORDER BY a.asOfDate asc rows between 3 preceding and current row) as RND_TTM /*Research and development cost over the past 4 quarters*/
, SUM(a.quarterlyEbitda) over (ORDER BY a.asOfDate asc rows between 3 preceding and current row) as EBITDA_TTM /*EBITDA over the past 4 quarters*/
, SUM(a.quarterlyTotalRevenue) over (ORDER BY a.asOfDate asc rows between 3 preceding and current row) as REV_TTM /*Revenue over the past 4 quarters*/
, SUM(a.quarterlyFreeCashFlow) over (ORDER BY a.asOfDate asc rows between 3 preceding and current row) as FCF_TTM /*Free cash flow over the past 4 quarters*/
/*to add FCFF*/
/*BS Items*/
, a.quarterlyBasicAverageShares
, a.quarterlyDilutedAverageShares
, a.quarterlyLongTermDebt
, a.quarterlyCurrentDebt
, a.quarterlyTotalAssets
, a.quarterlyEbitda
/*Lag items*/
, a.quarterlyBasicEPS as QBEPS0LAG
, lag1q.quarterlyBasicEPS as QBEPS1LAG
, lag4q.quarterlyBasicEPS as QBEPS4LAG
, lag5q.quarterlyBasicEPS as QBEPS5LAG
from yahoo_quarterly_fundamental a
left join yahoo_quarterly_fundamental lag1q
on a.ticker = lag1q.ticker and a.asOfDate = last_day(adddate(lag1q.asOfDate, interval 1 quarter))
left join yahoo_quarterly_fundamental lag4q
on a.ticker = lag4q.ticker and a.asOfDate = last_day(adddate(lag4q.asOfDate, interval 4 quarter))
left join yahoo_quarterly_fundamental lag5q
on a.ticker = lag5q.ticker and a.asOfDate = last_day(adddate(lag5q.asOfDate, interval 5 quarter))
where a.ticker = 'AAPL'
)
, step_1 as (
select t.fulldate as Process_Date
, b.earningsEstimate_Avg_next1Q
    , b.revenueEstimate_Avg_next1Q
    , b.updated_dt as Consensus_Datadate
    , ROW_NUMBER() OVER (PARTITION BY t.fulldate ORDER BY b.updated_dt DESC) AS intRow /*support to keep the latest data*/
    , a.* /*Summarized data*/
/*Yahoo data*/
    , main_factor.currentPrice as p0mago
    , lag1.currentPrice as p1mago
    , lag3.currentPrice as p3mago
    , lag6.currentPrice as p6mago
    , lag9.currentPrice as p9mago
	, lag12.currentPrice as p12mago
    , main_factor.fiftyTwoWeekHigh as ph12mago
    , main_factor.targetMedianPrice as tp0mago
    , lag1.targetMedianPrice as tp1mago
    , lag3.targetMedianPrice as tp3mago
    , lag6.targetMedianPrice as tp6mago
    , lag9.targetMedianPrice as tp9mago
    , lag12.targetMedianPrice as tp12mago
    , main_factor.averageDailyVolume3Month
    , main_factor.fiftyDayAverage
    , main_factor.twoHundredDayAverage
, main_factor.beta
, main_factor.beta3Year
, main_factor.sharesOutstanding
, main_factor.floatShares
, main_factor.enterpriseValue
, main_factor.ebitda
, main_factor.enterpriseToEbitda
, main_factor.enterpriseToRevenue
, main_factor.profitMargins
, main_factor.ebitdaMargins
, main_factor.grossMargins
, main_factor.operatingMargins
, main_factor.grossProfits
, main_factor.forwardPE
, main_factor.trailingPE
from date_d t
left join yahoo_fundamental main_factor
on t.fulldate = main_factor.updated_dt and main_factor.ticker in ('AAPL')
left join yahoo_fundamental lag1
on adddate(t.fulldate, interval -1 month) = lag1.updated_dt and main_factor.ticker = lag1.ticker
left join yahoo_fundamental lag3
on adddate(t.last_weekday, interval -3 month) = lag3.updated_dt and main_factor.ticker = lag3.ticker
left join yahoo_fundamental lag6
on adddate(t.last_weekday, interval -6 month) = lag6.updated_dt and main_factor.ticker = lag6.ticker
left join yahoo_fundamental lag9
on adddate(t.last_weekday, interval -9 month) = lag9.updated_dt and main_factor.ticker = lag9.ticker
left join yahoo_fundamental lag12
on adddate(t.last_weekday, interval -12 month) = lag12.updated_dt and main_factor.ticker = lag12.ticker
left join yahoo_quarterly a
on main_factor.ticker = a.ticker and last_day(main_factor.mostRecentQuarter) = a.Quarterly_Repdate
left join yahoo_consensus b
on main_factor.ticker = b.ticker
and last_day(adddate(main_factor.mostRecentQuarter, interval 3 month)) = b.endDate_next1Q
WHERE t.dayofweek = 6 and t.fulldate < current_date() and t.fulldate >= '2021-01-01'
ORDER BY t.fulldate desc
)
select *
from step_1
/*where intRow = 1*/
order by Process_Date desc, intRow, Consensus_Datadate desc