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

--Total: Counts total actuations in each time period. This is used in denominator to calculate percent in final select statement. 
SELECT
	TimePeriod,
	Count(TimePeriod) as TotalActuations
INTO #Total
FROM #Actuations
Group By TimePeriod


--Counts the number of actuations during each period
SELECT
	TimePeriod,
	SUM(CASE WHEN ArrivalTime >= BeginYellow and ArrivalTime < EndYellow THEN 1 ELSE 0 END) as Yellow,
	SUM(CASE WHEN ArrivalTime >= EndYellow and ArrivalTime < Severe_Time THEN 1 ELSE 0 END) as Red_Minor,
	SUM(CASE WHEN ArrivalTime >= Severe_Time and ArrivalTime < EndRed THEN 1 ELSE 0 END) as Red_Severe
INTO #Final
FROM #Filtered_Yellow, #Actuations
WHERE ActuationDate=YellowDate --Add time period to the where clause???
GROUP BY TimePeriod

--Calculate percentages
SELECT
	#Total.TimePeriod, 
	Yellow,
	Red_Minor,
	Red_Severe,
	TotalActuations,
	Yellow * 100 / TotalActuations as Percent_Yellow,
	Red_Minor * 100 / TotalActuations as Percent_Red_Minor,
	Red_Severe * 100 / TotalActuations as Percent_Red_Major
FROM #Total
LEFT JOIN #Final ON #Final.TimePeriod=#Total.TimePeriod
ORDER BY TimePeriod
	

DROP TABLE #Filtered_Yellow
DROP TABLE #Actuations
DROP TABLE #Total
DROP TABLE #Final