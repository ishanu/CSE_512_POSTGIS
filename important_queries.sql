select * from rectangles order by latitude2 desc limit 1;
select * from rectangles order by latitude1 asc limit 1;
select * from rectangles where latitude1 >= 41.0628516821
select st_ymax(geom) from rectangles;
select st_ymin(geom) from rectangles;
select max(st_ymax(geom) - st_ymin(geom)) from rectangles;
select count(*) from rectangles;
select count(*) from points;

--check for correct fragmentation point
select count(*) from pointsf1
select count(*) from pointsf2 where geom not in (select geom from pointsf1)
select count(*) from pointsf3 where geom not in (select geom from pointsf2)
select count(*) from pointsf4 where geom not in (select geom from pointsf3)
select * from pointsf1
-73.783424 40.648647

--check for correct fragmentation rectangle
select count(*) from rectsf1
select count(*) from rectsf2 where geom not in (select geom from rectsf1)
select count(*) from rectsf3 where geom not in (select geom from rectsf2)
select count(*) from rectsf4 where geom not in (select geom from rectsf3)
select * from rectsf1


select 

SELECT count(pointsf1.geom) AS totale ,rectsf1.geom
FROM rectsf1
   JOIN pointsf1 ON st_contains(rectsf1.geom,pointsf1.geom)
GROUP BY rectsf1.geom;


SELECT count(pointsf2.geom) AS totale ,rectsf2.geom
FROM rectsf2
   JOIN pointsf2 ON st_contains(rectsf2.geom,pointsf2.geom)
GROUP BY rectsf2.geom;

SELECT  count(pointsf3.geom) AS totale ,rectsf3.geom
FROM rectsf3
   JOIN pointsf3 ON st_contains(rectsf3.geom,pointsf3.geom)
GROUP BY rectsf3.geom;


SELECT  count(pointsf3.geom) AS totale ,rectsf3.geom as rectangle
FROM rectsf3
   JOIN pointsf3 ON st_contains(rectsf3.geom,pointsf3.geom)
GROUP BY rectsf3.geom order by totale asc;

CREATE TABLE output (count bigint, rectangle geometry)


