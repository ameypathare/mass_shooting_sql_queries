                 -- NOTE - COMMENTS ARE ON THE RIGHT HAND SIDE AND ARE DENOTED BY " -- "

                                                                                 

                     --  ** Data Cleaning **

                                                                                   -- "incidents_public" table
                                                                                   -- Checking Duplicates for "incidents_public" table

WITH incidentspublic AS
  (SELECT *,
          dense_rank() OVER (PARTITION BY incident_id
                             ORDER BY incident_id) AS duplicates
   FROM incidents_public)

SELECT duplicates
FROM incidentspublic
WHERE duplicates > 1


                                                                                  --  Trimming for "incidents_public" table

update  incidents_public
SET city = trim(city)

update  incidents_public
SET county = trim(county)

update  incidents_public
SET state = trim(state)

update  incidents_public
SET firstcod = trim(firstcod)

update  incidents_public
SET firstcod = trim(secondcod)

update  incidents_public
SET type = trim(type)

update  incidents_public
SET situation_type = trim(situation_type)

update  incidents_public
SET location_type =  trim(location_type)

update  incidents_public
SET location =  trim(location)

update  incidents_public
SET narrative =  trim(narrative)





                                                                                       -- "offenders_public" table
                                                                                       -- Checking Duplicates for "offenders_public" table

WITH offenderspublic AS
  (SELECT *,
          dense_rank() OVER (PARTITION BY offender_id
                             ORDER BY offender_id) AS duplicates
   FROM offenders_public)

SELECT duplicates
FROM offenderspublic
WHERE duplicates > 1


                                                                                      --  Trimming for "offenders_public" table

update offenders_public
SET firstname = trim(firstname)

update offenders_public
SET lastname = trim(lastname)

update offenders_public
SET race = trim(race)

update offenders_public
SET sex = trim(sex)

update offenders_public
SET outcome = trim(outcome)

update offenders_public
SET criminal_justice_process = trim(criminal_justice_process)

update offenders_public
SET sentence_type = trim(sentence_type)

update offenders_public
SET sentence_details = trim(sentence_details)



                                                 -- Retrieving characters before "-" Hypen and putting them in "middlename" column "offenders_public" table

update offenders_public
SET middlename = 
                CASE WHEN charindex('-', lastname) > 0 THEN substring(lastname, 1, charindex('-', lastname) - 1)
                ELSE null
                END from offenders_public


                                                -- Deleting characters before "-" Hypen for "lastname" column "offenders_public" table

update offenders_public
SET lastname = 
               right(lastname, len(lastname) - charindex('-', lastname)) 
			   FROM offenders_public




                                                 -- Replacing not applicable with null "offenders_public" table

update offenders_public
SET criminal_justice_process = 
                               nullif(criminal_justice_process, 'not applicable') 
							   FROM offenders_public



DELETE FROM offenders_public                      -- Deleting null values from firstname column                   
WHERE firstname is null





                                                                                    --  "victims_public" table
                                                                                    -- Checking duplicates for "victims_public" table

WITH victimspublic AS
  (SELECT *,
          dense_rank() OVER (PARTITION BY victim_id
                             ORDER BY victim_id) AS duplicates
   FROM victims_public)

SELECT duplicates
FROM victimspublic
WHERE duplicates > 1


alter table victims_public                                                         --  Modifying victim_id column to not null
alter column victim_id int not null

ALTER TABLE victims_public                                                         -- Adding "primary key" to "victim_id" column
ADD CONSTRAINT victim_id PRIMARY KEY (victim_id);


                                                                                   -- Trimming "victims_public" table

update victims_public
SET race = trim(race)

update victims_public
SET sex = trim(sex)

update victims_public
SET vorelationship = trim(vorelationship)





                                                                                   -- "weapons_public" table
                                                                                   -- Checking duplicates for "weapons_public" table
 
WITH weaponspublic AS
  (SELECT *,
          dense_rank() OVER (PARTITION BY weapon_id
                             ORDER BY weapon_id) AS duplicates
   FROM weapons_public)

SELECT duplicates
FROM weaponspublic
WHERE duplicates > 1


alter table weapons_public                                                       --  Modifying weapon_id column to not null
alter column weapon_id int not null

ALTER TABLE weapons_public                                                       --  Adding "primary key" to "weapon_id" column
ADD CONSTRAINT weapon_id PRIMARY KEY (weapon_id);


                                                                                 -- Trimming "weapons_public" table

update weapons_public
SET weapon_type = trim(weapon_type)

update weapons_public
SET gun_class = trim(gun_class)

update weapons_public
SET gun_type = trim(gun_type)



                                                                                -- Capitalizing starting word for "gun_type" column

update weapons_public
SET gun_type = 
              upper(left(gun_type, 1)) + substring(gun_type, 2, len(gun_type)) 
              FROM weapons_public;


                                                                                -- Capitalizing starting word for "weapon_type" column 

update weapons_public
SET weapon_type = 			
              upper(left(weapon_type, 1)) + substring(weapon_type, 2, len(weapon_type)) 
              FROM weapons_public;






--                 ** Answering Questions 

                                                                                -- Total Incidents
SELECT count(incident_id) as total_incidents
FROM incidents_public

                                                                                -- Selecting Top 5  states
SELECT top 5 state, count(incident_id) AS no_of_incidents
FROM incidents_public
GROUP BY state
ORDER BY no_of_incidents DESC
   

                                                                                -- Selecting Top 10 Cities in Top 5  states

SELECT top 10 city,
           count(incident_id) AS no_of_incidents,
           sum(num_victims_killed) AS victims_killed
FROM incidents_public
WHERE state = 'CA'
GROUP BY city
ORDER BY no_of_incidents DESC,
         victims_killed DESC


SELECT top 10 city,
           count(incident_id) AS no_of_incidents,
           sum(num_victims_killed) AS victims_killed
FROM incidents_public
WHERE state = 'TX'
GROUP BY city
ORDER BY no_of_incidents DESC,
         victims_killed DESC


SELECT top 10 city,
           count(incident_id) AS no_of_incidents,
           sum(num_victims_killed) AS victims_killed
FROM incidents_public
WHERE state = 'IL'
GROUP BY city
ORDER BY no_of_incidents DESC,
         victims_killed DESC


SELECT top 10 city,
           count(incident_id) AS no_of_incidents,
           sum(num_victims_killed) AS victims_killed
FROM incidents_public
WHERE state = 'FL'
GROUP BY city
ORDER BY no_of_incidents DESC,
         victims_killed DESC


SELECT top 10 city,
           count(incident_id) AS no_of_incidents,
           sum(num_victims_killed) AS victims_killed
FROM incidents_public
WHERE state = 'OH'
GROUP BY city
ORDER BY no_of_incidents DESC,
         victims_killed DESC




                                                                                -- Top 5 locations in which incidents happen most
SELECT top 5 location,
             count(incident_id) AS incidents
FROM incidents_public
GROUP BY LOCATION
ORDER BY incidents DESC


                                                                               -- Most Top 5 Situations in Top 5 locations
SELECT top 5 situation_type,
           count(situation_type) AS no_of_situations
FROM incidents_public
WHERE LOCATION = 'Residence'
GROUP BY situation_type
ORDER BY no_of_situations DESC


SELECT top 5 situation_type,
           count(situation_type) AS no_of_situations
FROM incidents_public
WHERE LOCATION = 'Open space'
GROUP BY situation_type
ORDER BY no_of_situations DESC


SELECT top 5 situation_type,
           count(situation_type) AS no_of_situations
FROM incidents_public
WHERE LOCATION = 'Commercial/Retail'
GROUP BY situation_type
ORDER BY no_of_situations DESC


SELECT top 5 situation_type,
           count(situation_type) AS no_of_situations
FROM incidents_public
WHERE LOCATION = 'Multiple'
GROUP BY situation_type
ORDER BY no_of_situations DESC


SELECT top 5 situation_type,
           count(situation_type) AS no_of_situations
FROM incidents_public
WHERE LOCATION = 'Bar/Club/Restaurant'
GROUP BY situation_type
ORDER BY no_of_situations DESC



                                                                                -- Incidents Happened with over years
SELECT datepart(YEAR, date) AS years,
       count(incident_id) AS no_of_incidents
FROM incidents_public
GROUP BY datepart(YEAR, date)




                                                                               -- How many offenders were assembled for each incident and in which city
WITH cte AS
  (SELECT op.incident_id,
          op.offender_id,
          ip.city,
          op.firstname,
          op.lastname,
          row_number() OVER (PARTITION BY op.incident_id
                             ORDER BY op.incident_id) AS rownum
   FROM offenders_public op
   JOIN incidents_public ip ON op.incident_id = ip.incident_id
   GROUP BY  op.incident_id, op.offender_id, ip.city, op.firstname, op.lastname 
            )

SELECT cte.incident_id,
       cte.offender_id,
       cte.firstname,
       cte.lastname,
       cte.city,
       cte.rownum AS offender_num,
       count(cte.rownum) over(PARTITION BY cte.incident_id) AS overall_count
FROM cte
GROUP BY cte.incident_id, cte.offender_id, cte.firstname,  cte.lastname, cte.city, cte.rownum
ORDER BY overall_count DESC




                                                                                -- Percentage Of sentences to offenders
WITH cte AS
  (SELECT sentence_type,
          cast(count(sentence_type) AS float) AS numbers
   FROM offenders_public
   WHERE sentence_type IS NOT NULL
   GROUP BY sentence_type)

SELECT sentence_type,
       round(numbers * 100 / (SELECT SUM(numbers) FROM cte), 2) AS criminal_percentage
FROM cte
GROUP BY sentence_type,
         numbers
ORDER BY criminal_percentage DESC




                                                                               -- How many victims were targeted in particular state
SELECT ip.state,
       count(vp.sex) AS victims,
       count(CASE WHEN vp.sex = 'Male' THEN 1   END) AS male,
       count(CASE WHEN vp.sex = 'Female' THEN 0 END) AS female
FROM victims_public   AS vp
JOIN incidents_public AS ip 
ON vp.incident_id = ip.incident_id
GROUP BY ip.state
ORDER BY victims DESC



                                                                               -- How many weapons were being used in incidents
select weapon_type as weapons, count(weapon_type) as numbers
from weapons_public
group by weapon_type
order by numbers desc


                                        -- ** end **
