-- Task One,
--
-- 1.Use Create Table and use LOAD infile which introduced in last lession to insert provided crime2.csv into mySQL database system.
-- 2.create the refined view
--
--
-- Task Two
/*list the TOP crimes of each week day */
select * from
	(select
	OFFENSE_CODE_GROUP,
	DAY_OF_WEEK,
	CNT,
	rank() over (partition by DAY_OF_WEEK order by CNT desc) my_rank
	from
		(select OFFENSE_CODE_GROUP,DAY_OF_WEEK, count(1) CNT
		from crime_refine
		group by 2,1) A
    ) B
where my_rank =1;

/*list top 10 crimes among all 4 years */

select * from
(select
`YEAR`,
OFFENSE_CODE_GROUP,
CNT,
rank() over (partition by `YEAR` order by CNT DESC) my_rank
from
	(select `YEAR`, OFFENSE_CODE_GROUP, count(1) CNT
	from crime_refine
	group by 2,1) A
) B
where my_rank <=10;

/*list most dangerous time of each day in 2017 */


select * from
(	select
	`DATE`,
	`HOUR`,
    OFFENSE_CODE_GROUP
	CNT,
	rank() over ( partition by `DATE` order by CNT desc) my_rank
	from
	(select `DATE`, `HOUR`, OFFENSE_CODE_GROUP, count(1) CNT
	from crime_refine
	where `YEAR` = 2017
	group by 1,2,3 ) A
)B where my_rank = 1;

/*list most dangerous streets of all 4 years */

select * from
  (select
  DISTRICT,
  STREET,
  `YEAR`,
  CNT,
  rank() over (partition by `YEAR` order by CNT desc) my_rank
  from
    (select DISTRICT, STREET, `YEAR`, count(1) CNT
    from crime_refine
    where DISTRICT is not null
    group by 1,2,3 ) A
  ) B
where my_rank = 1;

/*list top crimes for each district of each month in 2018*/

select * from
	(select
	DISTRICT,
	`MONTH`,
	OFFENSE_CODE_GROUP,
	CNT,
	rank() over (partition by DISTRICT, `MONTH` order by CNT desc) my_rank
	from
		(select DISTRICT, `MONTH`, OFFENSE_CODE_GROUP, count(1) CNT
		from crime_refine
		where `YEAR` = 2018
		group by 1,2,3) A
	) B
where my_rank = 1;

/*list the max occurrence duration gap for top 10 crime group */

select
OFFENSE_CODE_GROUP, max(datediff(NEXT_DATE,`DATE`)) GAP
from
	(select
	OFFENSE_CODE_GROUP,
	`DATE`,
	lead(`DATE`, 1) over (partition by OFFENSE_CODE_GROUP order by `DATE`) NEXT_DATE
	from crime_refine) A
group by 1
order by 2 desc
limit 10;

/*list the median crime count of each month in 2017*/

select
distinct `MONTH`,
avg(CNT) over ( partition by `MONTH`) median
from (
	select
	A.`MONTH`,
	A.`DATE`,
	A.CNT,
	row_number() over ( partition by `MONTH` order by CNT) my_row,
	MN_DAYS
	from(
		select
		`MONTH`, `DATE` , count(1) CNT
		from crime_refine
		where `YEAR` = 2017
		group by 1,2) A
	inner join(
		select `MONTH`, count(1) MN_DAYS
		from (
			select
			`MONTH`, `DATE`
			from crime_refine
			where `YEAR` = 2017
			group by 1,2)B
		group by 1) C
	on A.`MONTH` = C.`MONTH`) D
where my_row in( floor((MN_DAYS+1)/2), floor((MN_DAYS+2)/2) );
