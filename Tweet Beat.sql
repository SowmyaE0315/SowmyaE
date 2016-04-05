IF OBJECT_ID('tempdb..#HASH') IS NOT NULL 
DROP TABLE #HASH
CREATE TABLE #HASH
(
RawString VARCHAR(MAX)
)


INSERT INTO #HASH
VALUES 
('hashtags = [Spark , Apache],        created_at: Thu Mar 24 17:51:10 +0000 2016
hashtags = [Apache, Hadoop, Storm], created_at: Thu Mar 24 17:51:15 +0000 2016
hashtags = [Apache],                created_at: Thu Mar 24 17:51:30 +0000 2016
hashtags = [Flink, Spark],          created_at: Thu Mar 24 17:51:55 +0000 2016
hashtags = [Spark , HBase],         created_at: Thu Mar 24 17:51:58 +0000 2016
hashtags = [Hadoop, Apache],        created_at: Thu Mar 24 17:52:12 +0000 2016
')


--SELECT * FROM #HASH

--DECLARE @IP VARCHAR(MAX)
--SELECT @IP = REPLACE(REPLACE(REPLACE(REPLACE(RawString,'hashtags = ',''),'[',''),'created_at: Thu ',''),' +0000','') FROM #HASH

--SELECT REPLACE(REPLACE(REPLACE(REPLACE(RawString,'hashtags = ',''),'[',''),'created_at: Thu ',''),' +0000','') FROM #HASH

--SELECT * FROM dbo.splitstring(@IP,']')

--Sp_helptext splitstring
--IF OBJECT_ID('Tweets') IS NOT NULL 
--DROP TABLE Tweets 
IF OBJECT_ID('Tempdb..#Tweets') IS NOT NULL 
DROP TABLE #Tweets 
CREATE TABLE #Tweets
( 
Hashtags VARCHAR(MAX),
Created_At DATETIME 
) 



INSERT INTO #Tweets (Hashtags,Created_At)
VALUES 
('Spark , Apache]',        	'3/24/2016 17:51:10'),
('Apache, Hadoop, Storm]', 	'3/24/2016 17:51:15'),
('Apache]',               	'3/24/2016 17:51:30'),
('Flink, Spark]',     	'3/24/2016 17:51:55'),
('Spark , HBase]',         	'3/24/2016 17:51:58'),
('Hadoop, Apache]',     	'3/24/2016 17:52:12')



UPDATE #Tweets 
SET Hashtags = REPLACE(REPLACE(HashTags,' ',''),']','') FROM 
#Tweets 



--Select * from Tweets


--SELECT 
--			* 
--FROM 
--			Tweets 
--WHERE 
--			Created_At BETWEEN (SELECT DATEADD(SS,-60,MAX(Created_at)) FROM Tweets) AND  (SELECT MAX(Created_at) FROM Tweets) 
--ORDER BY	
--			Created_at
IF OBJECT_ID('tempdb..#Hashers') IS NOT NULL 
DROP TABLE #Hashers

CREATE TABLE #Hashers
( 
Identifier INT, 
Hashes VARCHAR(200),
Createdat DATETIME
)


--WHERE 
--			Created_At BETWEEN (SELECT DATEADD(SS,-60,MAX(Created_at)) FROM Tweets) AND  (SELECT MAX(Created_at) FROM Tweets) 			

--SELECT * FROM #Hashers
IF OBJECT_ID('tempdb..#Tweets1') IS NOT NULL 
DROP TABLE #Tweets1
SELECT ROW_NUMBER() OVER (ORDER BY Created_at) AS RN, * INTO #Tweets1 FROM #Tweets 
--SELECT * FROM #Tweets1


DECLARE @Cnt INT,@up INT = 1
SELECT @Cnt = COUNT(1) FROM #Tweets 

DECLARE @SubStr VARCHAR(500),@Identifier INT, @Createdat DATETIME 
WHILE @Cnt > 0
BEGIN 

SELECT @SubStr = Hashtags FROM #Tweets1 WHERE RN = @up
SELECT @Identifier = RN FROM #Tweets1 WHERE RN = @up
SELECT @Createdat = Created_at FROM #Tweets1 WHERE RN = @up
INSERT INTO #Hashers (Identifier,Hashes,Createdat)
SELECT @up AS Identifier,Name ,@Createdat FROM  splitstring(@SubStr,',') 
--WHERE RN = @up
SET @Cnt = @Cnt - 1
SET @up = @up + 1 
END


IF OBJECT_ID('tempdb..#Hashers1') IS NOT NULL 
DROP TABLE #Hashers1
SELECT * INTO #Hashers1 FROM #Hashers
WHERE 
			Createdat BETWEEN (SELECT DATEADD(SS,-60,MAX(Createdat)) FROM #Hashers) AND  (SELECT MAX(Createdat) FROM #Hashers) 


Select H1.*,H.* from #Hashers1 H INNER JOIN 
#Hashers1 H1 On h.hashes = h1.hashes
WHERE h.identifier != h1.identifier
