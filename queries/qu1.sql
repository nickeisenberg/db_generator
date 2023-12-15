-- Find the average salaries of each of the professions

select job, AVG(salary) from employment
group by job;
