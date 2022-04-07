--This is missing events where occupancy was 100 but not actuations? or it's showing just 1 actuation, not sure why.
--i mean it's missing zero volume time periods!. 


--Declare variable, including phase number and MaxTime detector number
DECLARE @TSSU as VARCHAR(5) ='03008';
DECLARE @Det AS INT = 4;
DECLARE @BinSize AS INT = 15
DECLARE @start DATETIME = (getdate()-21) --'2022-03-23 11:00:00'; --or date?
DECLARE @end DATETIME  = getdate()--'2022-03-24';


	--Run query!--
--use TSSU to look up device id
DECLARE @DeviceID AS INT 
SET @DeviceID= (
SELECT GroupableElements.ID
FROM [MaxView_1.9.0.744].[dbo].[GroupableElements]
WHERE Right(GroupableElements.Number,5) = @TSSU)
--select @DeviceID
--Temp tables are created for ALL events, and one each for phase events for the cycle, green, and red. Then joined together to allow grouping.


-----------------CYCLE LENGTH TABLE-------------------------
--This is the actual average cycle length for each period, to be joined to the Volume/Occupancy tables
SELECT
	AVG(Parameter) AS Avg_Cycle_Length,
	dateadd(minute, datediff(minute,0,TimeStamp)/@BinSize * @BinSize, 0) as CycleTime
INTO #Cycle
FROM
	ASCEvents
WHERE
	EventID = 316
	and TimeStamp >= @start
	and TimeStamp < @end
	and DeviceID = @DeviceID
GROUP BY 
	dateadd(minute, datediff(minute,0,TimeStamp)/@BinSize * @BinSize, 0)


----------NOW WORK ON Volume/Occupancy--------------------
-----------------ALL EVENTS TABLE---------------------------
--Create temp table of detector actuations, this includes all events used for the rest of the query
SELECT TimeStamp, EventID
INTO #ALL
FROM ASCEvents 
WHERE EventID IN(81,82) and Parameter = @Det  --detector on, detector off, phase green begin, phase green end, phase red begin
	AND TimeStamp BETWEEN @start AND @end
	AND DeviceID = @DeviceID

--select * from #ALL
-- drop table #ALL
--Add time barriors to #ALL, these are used to separate occupancy into the bins when a detector state is on from one time period to the next
WHILE @start < @end
BEGIN
  INSERT INTO #ALL
  VALUES (@start, 0)
  SET @start = DATEADD(MINUTE, @BinSize, @start)
END

/*
--This method is much faster, but if there are no actuations in a time period it will count occupancy as 100. So maybe just throw out off-peak?
SELECT
TimeStamp,
SUM(MS)/(@BinSize*60*10) AS Occupancy --whole number, decimal option below
--CONVERT(DECIMAL(10,2),SUM(MS))/(@BinSize*60*10) AS Occupancy
FROM 
	(
	SELECT 
		dateadd(minute, datediff(minute,0,TimeStamp)/@BinSize * @BinSize, 0) as TimeStamp, TimeStamp as time, 
		EventID,
		LEAD(EventID) OVER (ORDER BY TimeStamp) AS NextEvent,
		DATEDIFF(MILLISECOND, TimeStamp, LEAD(TimeStamp) OVER (ORDER BY TimeStamp)) AS MS
	From #ALL
	) q
WHERE (EventID=82 or (EventID=0 and (NextEvent=81 or NextEvent=0))) and MS IS NOT NULL
GROUP BY Timestamp
ORDER BY TimeStamp
DROP TABLE #ALL
*/


--Slower Method
-----------------State Table---------------------------
--Add column which tracks the on/off state of the detector to know if it was on or off when each time period starts
select *, 
DATEDIFF(MILLISECOND, TimeStamp, LEAD(TimeStamp) OVER (ORDER BY TimeStamp)) AS MS,
COUNT(CASE WHEN EventID IN(82, 81) THEN 1 END) OVER (ORDER BY timestamp) AS detector_state_group
into #State
from #ALL

-----------------Persist Table---------------------------
--Transform State table to show what the persistant state for detector/red/green are for each event
select *, 
MAX(EventID) OVER (PARTITION BY detector_state_group) AS detector_state
into #Persist
from #State 

-----------------Final Result---------------------------
SELECT 
    dateadd(minute, datediff(minute,0,TimeStamp)/@BinSize * @BinSize, 0) as TimeStamp,
    @TSSU as TSSU,
    @Det as MT,
    SUM(MS)/(@BinSize*60*10) AS Occupancy,
	COUNT(CASE WHEN EventID = 82 THEN 1 ELSE 0 END) * 60 / @BinSize AS Volume
INTO #Final
FROM #Persist
WHERE detector_state = 82
GROUP BY 
	dateadd(minute, datediff(minute,0,TimeStamp)/@BinSize * @BinSize, 0)


-----------------Join Volume/Occupancy with Cycle---------
SELECT TimeStamp, TSSU, MT, Occupancy, Volume, Avg_Cycle_Length
FROM #Final
LEFT JOIN #Cycle ON CycleTime = TimeStamp
ORDER BY TimeStamp

DROP TABLE #ALL
DROP TABLE #Persist
DROP TABLE #State
DROP TABLE #Cycle
DROP TABLE #Final
--select * from #State



