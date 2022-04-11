/* 
This query returns aggregate yellow and red actuations in binned periods for a single detector-phase pair.

Included Periods: Actuations on Yellow, First 4 Seconds of Red, and on Red After 4 Seconds
The 4-second period is the default UDOT uses, it differentiates between "minor" and "severe" violations

A UDOT sponsered study by Avenue Consultants found a 1.5 second average latency from a vehicle 
entering a radar zone to the timestamp of the actuation being recorded by the controller. Therefore,
the timestamps for aggregations are shifted by 1.5 seconds to account for that latency. Users should
keep in mind that 1.5 seconds is the average, the actual distribution varies by unkown factors.
NOTE: the phase events are shifted forward rather than shifting detector events back, because there's less phase events

These hi-res events are used: EventID 8 = Begin Yellow, 9 = End Yellow, 1 = Begin Green (used as proxy for "end red")

The result of this query outputs a "tidy" style long table with a Feature column, with numeric codes as follows:

	1 = Actuations during Yellow
	2 = Actuations during First 4 Seconds of Red (minor violation)
	3 = Actuations After First 4 Seconds of Red (major violation)
	4 = Average Time of Yellow Actuations in Milliseconds (avg elapsed time from start of yellow to when actuations during yellow occured)
	5 = Same as 4, but for first 4 seconds of red
	6 = Same as 4, but for after first 4 seconds of red
	7 = Standard Deviation of 4 (milliseconds, no result if sample size < 2)
	8 = Standard Deviation of 5
	9 = Standard Deviation of 6
	10 = Total Actuations over all periods, including green time.

The intent is that the DeviceID, TimePeriod and Feature column will be used as indexes, and the Values column is the data.
The Feature column will be used for filtering/measures in Power BI
*/

--Set User Variables
DECLARE @TSSU as VARCHAR(5) ='22036';
DECLARE @Phase as INT= 5;
DECLARE @MT as INT= 45;
DECLARE @BinSize AS INT = 15;
DECLARE @start DATETIME ='2022-01-01';
DECLARE @end DATETIME  = '2022-04-07';
DECLARE @Latency FLOAT = 1.5;

	--Run query!--
--use TSSU to look up device id
DECLARE @DeviceID AS INT 
SET @DeviceID= (
SELECT GroupableElements.ID
FROM [MaxView_1.9.0.744].[dbo].[GroupableElements]
WHERE Right(GroupableElements.Number,5) = @TSSU)

--#Yellow: phase yellow on and off events, with start and end timestamps
--Phase event timestamps are shifted by the @Latency variable
SELECT
	DATEADD(second, @Latency, Lag(TimeStamp) OVER (ORDER BY TimeStamp)) as BeginYellow,
	DATEADD(second, @Latency, TimeStamp) as EndYellow,
	Lead(TimeStamp) OVER (ORDER BY TimeStamp) as EndRed,
	EventID,
	Lag(EventID) OVER (ORDER BY TimeStamp) as Previous_EventID,
	Lead(EventID) OVER (ORDER BY TimeStamp) as Next_EventID
INTO #Yellow
FROM ASCEvents
WHERE
	DeviceId=@DeviceID and 
	(EventID = 8 or EventID = 9 or EventID = 1) and 
	Parameter=@Phase and 
	TimeStamp >= @start and 
	TimeStamp < @end

--Filter to the End Yellow events where the previous event was begin yellow and next event is end red(begin green)
--Add Severe_Time column 4 seconds after end yellow
SELECT
	BeginYellow,
	EndYellow,
	DATEADD(second, 4, EndYellow) as Severe_Time,
	EndRed,
	CONVERT(DATE, BeginYellow) as YellowDate
INTO #Filtered_Yellow
FROM #Yellow
WHERE 
	EventID = 9 and 
	Previous_EventID = 8 and
	Next_EventID = 1
DROP TABLE #Yellow


--Actuations: all detector on events for the specified detector, with a rounded timestamp called TimePeriod for joining
--This step takes the longest in the entire query
SELECT
	TimeStamp as ArrivalTime,
	dateadd(minute, datediff(minute,0,TimeStamp)/@BinSize * @BinSize, 0) as TimePeriod,
	CONVERT(DATE,TimeStamp) as ActuationDate
INTO #Actuations
FROM ASCEvents
WHERE
	DeviceID = @DeviceID and 
	EventID = 82 and
	Parameter = @MT and
	TimeStamp >= @start and 
	TimeStamp < @end


-----FINAL OUTPUT-----
--No more temp tables, now final data output from different queries will be unioned together---
--Total: Counts total actuations in each time period. This is used in denominator to calculate percent in final select statement. 
SELECT
	TimePeriod,
	10 as Feature,
	Count(TimePeriod) as "Value"
FROM #Actuations
GROUP BY TimePeriod

UNION ALL
--Yellow2 Actuations Table
SELECT
	TimePeriod,
	Feature,
	"Value"
FROM 
	(SELECT
		TimePeriod,
		COUNT(*) as [1],
		AVG(DATEDIFF(ms, BeginYellow, ArrivalTime)) as [4],
		CONVERT(int, STDEV(DATEDIFF(ms, BeginYellow, ArrivalTime))) as [7]
	FROM #Filtered_Yellow
	JOIN #Actuations
		ON ArrivalTime >= BeginYellow 
		and ArrivalTime < EndYellow
		and ActuationDate = YellowDate
	GROUP BY TimePeriod
	) q
UNPIVOT	
	("Value" FOR Feature IN
		([1], [4], [7])) p

UNION ALL
--Minor Violation Actuations Table
SELECT
	TimePeriod,
	Feature,
	"Value"
FROM 
	(SELECT
		TimePeriod,
		COUNT(*) as [2],
		AVG(DATEDIFF(ms, EndYellow, ArrivalTime)) as [5],
		CONVERT(int, STDEV(DATEDIFF(ms, EndYellow, ArrivalTime))) as [8]
	FROM #Filtered_Yellow
	JOIN #Actuations
		ON ArrivalTime >= EndYellow 
		and ArrivalTime < Severe_Time
		and ActuationDate = YellowDate
	GROUP BY TimePeriod
	) q
UNPIVOT	
	("Value" FOR Feature IN
		([2], [5], [8])) p

UNION ALL
--Severe Violation Actuations Table
SELECT
	TimePeriod,
	Feature,
	"Value"
FROM 
	(SELECT
		TimePeriod,
		COUNT(*) as [3],
		AVG(DATEDIFF(ms, Severe_Time, ArrivalTime)) as [6],
		CONVERT(int, STDEV(DATEDIFF(ms, Severe_Time, ArrivalTime))) as [9]
	FROM #Filtered_Yellow
	JOIN #Actuations
		ON ArrivalTime >= Severe_Time 
		and ArrivalTime < EndRed
		and ActuationDate = YellowDate
	GROUP BY TimePeriod
	) q
UNPIVOT	
	("Value" FOR Feature IN
		([3], [6], [9])) p


DROP TABLE #Filtered_Yellow
DROP TABLE #Actuations