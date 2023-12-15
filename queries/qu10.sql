-- How many parents have children with different partners? There may be a case
--where a parent has a None type parter. If this is the case, cosider "None" as
--a partner.

with 
    unique_pairs as (
        select parent1_id, parent2_id 
        from children 
        group by parent1_id, parent2_id
    ),
    p1_count as (
            select parent1_id, count(parent1_id) as p1c
            from unique_pairs
            group by parent1_id
    ),
    p2_count as (
            select parent2_id, count(parent2_id) as p2c
            from unique_pairs
            group by parent2_id
    ),
    comb_p_count as (
        select * from p1_count
        left join p2_count
        on p1_count.parent1_id = p2_count.parent2_id
    )

select 
    count(coalesce(parent1_id, parent2_id))
from comb_p_count
where
    coalesce(p1c, 0) + coalesce(p2c, 0) = 1
;

