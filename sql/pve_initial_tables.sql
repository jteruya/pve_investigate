/* Drop Staging Tables if Exists */
drop table if exists jt.profile_views;
drop table if exists jt.pve_pride_views;
drop table if exists jt.pve_non_pride_views;
drop table if exists jt.pve_pride_views_sequence;
drop table if exists jt.pve_non_pride_views_sequence;

/* Create Staging Table for all Profile View Metrics */
select application_id
     , metadata->>'userid' as user_profile_viewed
     , created as user_profile_viewed_date
     , case
         when extract(hour from created) < 10 then cast((created - interval '1' day) as date)
         else cast(created as date)
       end as user_profile_viewed_date_24hour
     , count(*) as profile_viewed_count
into jt.profile_views
from fact_views
where identifier = 'profile'
and metadata->>'isfollowing' <> 'null'
group by 1,2,3,4
order by count(*) desc;

/* Table: jt.pve_pride_views */
select application_id
     , global_user_id
     , metadata->>'userid' as vieweduserid
     , created as viewdatetime
     , case
         when extract(hour from created) < 10 then cast((created - interval '1' day) as date)
         else cast(created as date)
       end as pvedatetime
into jt.pve_pride_views
from fact_views
where identifier = 'profile'
and application_id = 'bffee970-c8b3-4a2d-89ef-a9c012000abb'
and metadata->>'isfollowing' <> 'null'
and created >= '2015-05-01 00:00:00'
and created <= now();

/* Table: jt.pve_non_pride_views */
select application_id
     , global_user_id
     , metadata->>'userid' as vieweduserid
     , created as viewdatetime
     , case
         when extract(hour from created) < 10 then cast((created - interval '1' day) as date)
         else cast(created as date)
       end as pvedatetime
into jt.pve_non_pride_views
from fact_views
where identifier = 'profile'
and application_id <> 'bffee970-c8b3-4a2d-89ef-a9c012000abb'
and metadata->>'isfollowing' <> 'null'
and created >= '2015-05-01 00:00:00'
and created <= now();

/* Table: jt.pve_pride_views_sequence */
select row_number() over (partition by 1 order by application_id, vieweduserid, global_user_id, pvedatetime) as id
     , dense_rank() over (order by application_id, vieweduserid, global_user_id) as group_id
     , row_number() over (partition by application_id, vieweduserid, global_user_id order by pvedatetime) as vieworder
     , application_id
     , vieweduserid
     , global_user_id
     , pvedatetime
     , case
         when lag(pvedatetime,1) over (partition by application_id, vieweduserid, global_user_id rows between unbounded preceding and current row) is not null then
           pvedatetime - lag(pvedatetime,1) over (partition by application_id, vieweduserid, global_user_id rows between unbounded preceding and current row)
         else 0
       end as day_diff
     , count(*) over (partition by application_id, vieweduserid, global_user_id) as group_cnt
into jt.pve_pride_views_sequence
from (select distinct application_id
           , vieweduserid
           , pvedatetime
           , global_user_id
      from jt.pve_pride_views) a;

/* Table: jt.pve_non_pride_views_sequence */
select row_number() over (partition by 1 order by application_id, vieweduserid, global_user_id, pvedatetime) as id
     , dense_rank() over (order by application_id, vieweduserid, global_user_id) as group_id
     , row_number() over (partition by application_id, vieweduserid, global_user_id order by pvedatetime) as vieworder
     , application_id
     , vieweduserid
     , global_user_id
     , pvedatetime
     , case
         when lag(pvedatetime,1) over (partition by application_id, vieweduserid, global_user_id rows between unbounded preceding and current row) is not null then
           pvedatetime - lag(pvedatetime,1) over (partition by application_id, vieweduserid, global_user_id rows between unbounded preceding and current row)
         else 0
       end as day_diff
     , count(*) over (partition by application_id, vieweduserid, global_user_id) as group_cnt
into jt.pve_non_pride_views_sequence
from (select distinct application_id
           , vieweduserid
           , pvedatetime
           , global_user_id
      from jt.pve_non_pride_views) a;
