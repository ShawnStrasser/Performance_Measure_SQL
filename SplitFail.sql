-- 
--Calculates split failure for a specified vehicle phase and detector number.
--A split failure is when detector occupancy during green for a given cycle AND the first 5 seconds of red occupancy are both 80% or higher.

--join green start/end table to detector table where detector timestamp column is between start and end timestamps of green table.

--Declare variable, including phase number and MaxTime detector number
DECLARE @TSSU as VARCHAR(5) ='23024';
DECLARE @Phase as INT= 8;
DECLARE @Det AS INT = 24;
DECLARE @BinSize AS INT = 15
DECLARE @start DATETIME ='2021-04-01'; --or date?
DECLARE @end DATETIME  = '2021-05-01';
DECLARE @RedTime INT = 5; --number of seconds after start of red for which red occupancy will be calculated


	--Run query!--
--use TSSU to look up device id
DECLARE @DeviceID AS INT 
SET @DeviceID= (
SELECT GroupableElements.ID
FROM [MaxView_1.9.0.744].[dbo].[GroupableElements]
WHERE Right(GroupableElements.Number,5) = @TSSU)

--Temp tables are created for ALL events, and one each for phase events for the cycle, green, and red. Then joined together to allow grouping.

-----------------ALL EVENTS TABLE---------------------------
--Create temp table of detector actuations and phase events, this includes all events used for the rest of the query
SELECT *
INTO #ALL
FROM ASCEvents 
WHERE ((EventID IN(81,82) and Parameter = @Det) OR (EventID IN(1,7,10) AND Parameter = @Phase)) --detector on, detector off, phase green begin, phase green end, phase red begin
	AND TimeStamp BETWEEN @start AND @end
	AND DeviceID = @DeviceID
--append to temp table events 5 seconds after the phase red events, to mark the end of period where occupancy is calculated
INSERT INTO #ALL (TimeStamp, DeviceId, EventId, Parameter)
SELECT dateadd(second, @RedTime, TimeStamp) as TimeStamp, DeviceID, 11 as EventID, Parameter
FROM #ALL
WHERE EventID = 10



-----------------State Table---------------------------
--Add columns which track the states, that is, whether each event occuring while the detector was on or off, the signal was green, or red
--And track these by cycle number, to be grouped later
select *, 
DATEDIFF(MILLISECOND, TimeStamp, LEAD(TimeStamp) OVER (ORDER BY TimeStamp, EventID DESC)) AS MS,
CASE WHEN EventID = 82 THEN 1 WHEN EventID = 81 THEN 0 END AS DetectorState,
COUNT(CASE WHEN EventID IN(82, 81) THEN 1 END) OVER (ORDER BY timestamp, EventID DESC) AS detector_state_group,
CASE WHEN EventID = 10 THEN 1 WHEN EventID = 11 THEN 0 END AS RedState,
COUNT(CASE WHEN EventID IN(10,11) THEN 1 END) OVER (ORDER BY timestamp, EventID DESC) AS red_state_group,
CASE WHEN EventID = 1 THEN 1 WHEN EventID = 7 THEN 0 END AS GreenState,
COUNT(CASE WHEN EventID IN(1,7) THEN 1 END) OVER (ORDER BY timestamp, EventID DESC) AS green_state_group,
COUNT(CASE WHEN EventID = 1 THEN 1 END) OVER (ORDER BY timestamp, EventID DESC) AS cycle_group
into #State
from #ALL
order by timestamp

-----------------Persist Table---------------------------
--Transform State table to show what the persistant state for detector/red/green are for each event
select TimeStamp, DeviceID, EventID, Parameter, MS,
MAX(DetectorState) OVER (PARTITION BY detector_state_group)   AS detector_state,
MAX(RedState) OVER (PARTITION BY red_state_group)   AS red_state,
MAX(GreenState) OVER (PARTITION BY green_state_group)   AS green_state,
cycle_group
into #Persist
from #State order by TimeStamp

-----------------Group Table---------------------------
--Add up time durations

select 
	MIN([TimeStamp]) AS Cycle_TimeStamp,
	SUM(CASE WHEN green_state = 1 and detector_state = 1 THEN MS ELSE 0 END) * 100 / SUM(CASE WHEN green_state = 1 THEN MS END) AS Green_Occupancy,
	SUM(CASE WHEN red_state = 1 and detector_state = 1 THEN MS ELSE 0 END) * 100 / SUM(CASE WHEN red_state = 1 THEN MS END) AS Red_Occupancy

from #Persist 
where red_state IS NOT NULL and green_state IS NOT NULL
group by cycle_group
order by Cycle_TimeStamp


DROP TABLE #ALL
DROP TABLE #Persist
DROP TABLE #State
