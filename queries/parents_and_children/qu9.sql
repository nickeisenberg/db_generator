-- List the names of the family who have the highest combined income, again
-- assuming that if the child works, then he or she earns %30 of the average of
-- their parent's salary.

with 
    fam_sal as (
        select 
            child_id,
            c.first_name,
            c.last_name,
            .3 * (e1.salary + e2.salary) / 2 as c_sal,
            parent1_id,
            e1.salary as p1_sal,
            parent2_id, 
            e2.salary as p2_sal
        from children as c
        left join employment as e1
        on c.parent1_id = e1.parent_id
        left join employment as e2
        on c.parent2_id = e2.parent_id
        where c.is_employed = 1
    ),
    top_p as (
        select
            dense_rank() over ( order by p1_sal + p2_sal + sum(c_sal) desc ) as ranking,
            parent1_id, 
            parent2_id, 
            p1_sal + p2_sal + sum(c_sal) as fam_total
        from fam_sal 
        group by parent1_id, parent2_id
        limit 1
    ),
    top_fam as (
        select
            child_id,
            fs.first_name,
            fs.last_name,
            tp.parent1_id,
            tp.parent2_id
        from top_p as tp
        left join fam_sal as fs
        on tp.parent1_id = fs.parent1_id and tp.parent2_id = fs.parent2_id
    ),
    top_fam_names as (
        select 
            tf.first_name as cf, 
            tf.last_name as cl,
            p1.first_name as p1f, 
            p1.last_name as p1l,
            p2.first_name as p2f, 
            p2.last_name as p2l
        from top_fam as tf
        left join mailing as p1
        on tf.parent1_id = p1.parent_id
        left join mailing as p2
        on tf.parent2_id = p2.parent_id
    )
select cf as first_name, cl as last_name from top_fam_names
union 
select p1f, p1l from top_fam_names
union 
select p2f, p2l from top_fam_names;
