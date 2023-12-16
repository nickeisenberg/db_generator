-- Find the average saving for the employees that have worked for longer than
-- 5 years

select 
    avg(salary)
from employment
where datediff(timestamp(curdate()), timestamp(start_date)) / 365 > 5
limit 5;
