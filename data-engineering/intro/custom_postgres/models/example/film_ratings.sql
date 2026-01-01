with films_with_ratings as (
    select
        f.film_id,
        f.title,
        f.release_date,
        f.rating,
        f.price,
        f.user_rating,
        case
            when f.user_rating >= 4.5 then 'Excellent'
            when f.user_rating >= 3.5 then 'Good'
            when f.user_rating >= 2.5 then 'Average'
            else 'Poor'
        end as rating_category
    from {{ ref('films') }} as f
),

films_with_actors as (
    select
        f.film_id,
        f.title,
        string_agg(a.actor_name, ',') as actors
    from {{ ref('films') }} as f
    left join {{ ref('film_actors') }} as fa
        on f.film_id = fa.film_id
    left join {{ ref('actors') }} as a
        on fa.actor_id = a.actor_id
    group by
        f.film_id,
        f.title
)

select
    fwf.*,
    fwa.actors
from films_with_ratings as fwf
left join films_with_actors as fwa
    on fwf.film_id = fwa.film_id
