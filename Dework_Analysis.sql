
--Q1. What percentage of tasks has at least 1 assignee out of all available tasks?


with unnested as 
(
   SELECT
      *,
      unnest(string_to_array(activity, '",')) as activity_unnested 
   FROM
      dework_new
)
,
cleaned_table as 
(
   SELECT
      *,
      to_timestamp(substring(activity_unnested 
   FROM
      '(\w{3} \d{1,2}, \d{4} \d{1,2}:\d{2} [AP]M)'), 'Mon DD, YYYY HH:MI AM') AS timestamp_value 
   FROM
      unnested
)


SELECT
   ROUND(cast((
   SELECT
      COUNT(DISTINCT row_id) as total 
   FROM
      cleaned_table 
   WHERE
      activity_unnested like '%assignee%') as decimal(7, 2)) / cast((
      SELECT
         COUNT(DISTINCT row_id) as total 
      FROM
         cleaned_table) as decimal(7, 2)) * 100, 2) AS Assigned_percentage;









-- Q2. What percentage of tasks were completed?


SELECT
   ROUND(CAST((
   SELECT
      COUNT(DISTINCT row_id) as total 
   FROM
      dework_new 
   WHERE 
      status = 'Done') as decimal(7, 2)) / CAST((
      SELECT
         COUNT(DISTINCT row_id) AS total 
      FROM
         dework_new) as decimal(7, 2)) * 100, 2) AS Completed_percentage;




-- Q3. What is the breakdown of customer participation of tasks BY month 
----       a.	use the following bins ≤ 1, 2- 5, 6-10, a>10 times (how many users participated a unique task this month)



with unnested as 
(
   SELECT
      *,
      unnest(string_to_array(activity, '",')) as activity_unnested 
   FROM
      dework_final
)
,
cleaned_table as 
(
   SELECT
      *,
      to_timestamp(substring(activity_unnested 
   FROM
      '(\w{3} \d{1,2}, \d{4} \d{1,2}:\d{2} [AP]M)'), 'Mon DD, YYYY HH:MI AM') AS timestamp_value 
   FROM
      unnested
)
,
windowed as 
(
   SELECT
      *,
      row_number() OVER(PARTITION BY row_id 
   ORDER BY
      timestamp_value DESC) as row_num 
   FROM
      cleaned_table 
   WHERE
      status = 'Done'
)
,
cal_tasks as 
(
   SELECT
      EXTRACT(MONTH 
   FROM
      DATE(timestamp_value)) as mnth,
      EXTRACT(Year 
   FROM
      DATE(timestamp_value)) as yr,
      assignee,
      COUNT(DISTINCT row_id) as num_tasks 
   FROM
      windowed 
   WHERE
      row_num = 1 
      AND assignee != 'No task assignee...' 
   GROUP BY
      1,
      2,
      3 
   ORDER BY
      4 DESC
)
,
final as 
(
   SELECT
      mnth,
      yr,
      CASE
         WHEN
            num_tasks = 1 
         THEN
            '1' 
         WHEN
            num_tasks between 2 AND 5 
         THEN
            '2-5' 
         WHEN
            num_tasks between 6 AND 10 
         THEN
            '6-10' 
         WHEN
            num_tasks > 10 
         THEN
            '10 or Greater' 
      END
      AS task_bracket 
   FROM
      cal_tasks
)


SELECT
   mnth,
   yr,
   task_bracket,
   COUNT(task_bracket) 
FROM
   final 
GROUP BY
   1,
   2,
   3 
ORDER BY
   1,
   2










-- Q4. What is the Average length of time per task


with unnested as 
(
   SELECT
      *,
      unnest(string_to_array(activity, '",')) as activity_unnested 
   FROM
      dework_new
)
,
cleaned_table as 
(
   SELECT
      *,
      to_timestamp(substring(activity_unnested 
   FROM
      '(\w{3} \d{1,2}, \d{4} \d{1,2}:\d{2} [AP]M)'), 'Mon DD, YYYY HH:MI AM') AS timestamp_value 
   FROM
      unnested
)
,
completed_tasks as 
(
   SELECT
      *,
      CASE
         WHEN
            activity_unnested LIKE '%created this task%' 
         THEN
            'created' 
         WHEN
            activity_unnested LIKE '%changed the status toDone%' 
         THEN
            'completed' 
      END
      as task_status 
   FROM
      cleaned_table 
   WHERE
      status = 'Done' 
      AND timestamp_value is not null
)
, final as
(
   SELECT
      row_id,
      task_title,
      task_status,
      timestamp_value,
      LEAD(timestamp_value) OVER(PARTITION BY row_id, task_title ORDER BY timestamp_value) as completed_timestamp 
   FROM
      completed_tasks 
   WHERE
      task_status in 
      (
         'created',
         'completed'
      )
)
,
cal_daydiff as 
(
   SELECT
      EXTRACT(DAY 
   FROM
      (
         completed_timestamp - timestamp_value
      )
) as time_difference 
   FROM
      final 
   WHERE
      completed_timestamp is not null
)
SELECT
   ROUND(avg(time_difference), 2) AS avg_days_to_completion 
FROM
   cal_daydiff







-- Q5. What is the average number of times the top 10 tasks were completed


SELECT
   round(avg(num_times), 0) 
FROM
   (
      SELECT
         task_title,
         COUNT(DISTINCT row_id) as num_times 
      FROM
         dework_final 
      WHERE
         status = 'Done' 
      GROUP BY
         1 
      ORDER BY
         2 DESC 
         
      LIMIT 10
   )
   sq








--Q6. What are the top 3 Dao's with the most completed tasks


SELECT
   dao,
   COUNT(DISTINCT row_id) as num_times 
FROM
   dework_final 
WHERE
   status = 'Done' 
GROUP BY
   1 
ORDER BY
   2 DESC 
   
LIMIT 3





-- Q7. Who were the top 3 users with the most completed tasks


SELECT
   assignee,
   COUNT(DISTINCT row_id) as num_times 
FROM
   dework_final 
WHERE
   status = 'Done' 
   AND assignee != 'No task assignee...' 
GROUP BY
   1 
ORDER BY
   2 DESC 

LIMIT 3






-- Q8. What is the most completed task?

SELECT
   task_title,
   COUNT(DISTINCT row_id) as num_times 
FROM
   dework_final 
WHERE
   status = 'Done' 
GROUP BY
   1 
ORDER BY
   2 DESC 
   
LIMIT 1






-- Q9. Month to month, what is the COUNT of unique active users?

with unnested as 
(
   SELECT
      *,
      unnest(string_to_array(activity, '",')) as activity_unnested 
   FROM
      dework_final
)
,
cleaned_table as 
(
   SELECT
      *,
      to_timestamp(substring(activity_unnested 
   FROM
      '(\w{3} \d{1,2}, \d{4} \d{1,2}:\d{2} [AP]M)'), 'Mon DD, YYYY HH:MI AM') AS timestamp_value 
   FROM
      unnested
)
SELECT
   EXTRACT(MONTH 
FROM
   DATE(timestamp_value)) as mnth,
   EXTRACT(Year 
FROM
   DATE(timestamp_value)) as yr,
   COUNT(DISTINCT row_id) as num_active_assingees 
FROM
   cleaned_table 
WHERE
   assignee != 'No task assignee...' 
   AND timestamp_value is not null 
GROUP BY
   1,
   2 
ORDER BY
   2,
   1







-- Q9. Month to month, what is the COUNT of unique active DAOs?

with unnested as 
(
   SELECT
      *,
      unnest(string_to_array(activity, '",')) as activity_unnested 
   FROM
      dework_final
)
,
cleaned_table as 
(
   SELECT
      *,
      to_timestamp(substring(activity_unnested 
   FROM
      '(\w{3} \d{1,2}, \d{4} \d{1,2}:\d{2} [AP]M)'), 'Mon DD, YYYY HH:MI AM') AS timestamp_value 
   FROM
      unnested
)
SELECT
   EXTRACT(MONTH 
FROM
   DATE(timestamp_value)) as mnth,
   EXTRACT(Year 
FROM
   DATE(timestamp_value)) as yr,
   COUNT(DISTINCT row_id) as num_active_daos 
FROM
   cleaned_table 
WHERE
   assignee != 'No task assignee...' 
   AND timestamp_value is not null 
GROUP BY
   1,
   2 
ORDER BY
   2,
   1