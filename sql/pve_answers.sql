/* Percent Pride */
select application_id
     , total_users
     , percentile_cont(0.5) within group (order by total_number_of_users_with_view) as median_total_number_of_users_with_view_per_day
     , percentile_cont(0.5) within group (order by total_pct_of_users_with_view) as median_total_pct_of_users_with_view_per_day     
     , percentile_cont(0.5) within group (order by total_number_of_users_with_3_views) as median_total_number_of_users_with_3_views_per_day
     , percentile_cont(0.5) within group (order by total_pct_of_users_with_3_views) as median_total_pct_of_users_with_3_views_per_day    
from (select application_id
           , pvedatetime
           , total_users
           , count(*) as total_number_of_users_with_view
           , cast(cast(count(*) as decimal(10,4))/cast(total_users as decimal(10,4)) as decimal(10,4)) as total_pct_of_users_with_view
           , count(case when number_of_views_per_day >= 3 then 1 else null end) as total_number_of_users_with_3_views
           , cast(cast(count(case when number_of_views_per_day >= 3 then 1 else null end) as decimal(10,4))/cast(total_users as decimal(10,4)) as decimal(10,4)) as total_pct_of_users_with_3_views
      from (select application_id
                 , pvedatetime
                 , vieweduserid
                 , count(*) as number_of_views_per_day
                 , total_users
            from (select a.vieworder
                       , a.application_id
                       , a.vieweduserid
                       , a.global_user_id
                       , a.pvedatetime
                       , c.total_users
                  from jt.pve_pride_views_sequence a
                  left join jt.pve_pride_views_sequence_new b
                  on a.id = b.id
                  join (select count(*) as total_users
                        from authdb_is_users
                        where lower(applicationid) = 'bffee970-c8b3-4a2d-89ef-a9c012000abb'
                        and isdisabled = 0) c
                  on 1 = 1
                  where b.keep_ind is null or b.keep_ind = 1) x
            group by 1,2,3,5) y
      group by 1,2,3) z
group by 1,2;

/* Percent Everything Else */
select distinct count(*) over (partition by 1) as total_events
     , avg(total_users) over (partition by 1) as avg_number_of_users
     , avg_total_number_of_users_with_view_per_day
     , avg_total_pct_of_users_with_view_per_day
     , avg_total_number_of_users_with_3_views_per_day
     , avg_total_pct_of_users_with_3_views_per_day
from (
select distinct application_id
     , total_users
     , avg(total_number_of_users_with_view) over (partition by 1) as avg_total_number_of_users_with_view_per_day
     , avg(total_pct_of_users_with_view) over (partition by 1) as avg_total_pct_of_users_with_view_per_day
     , avg(total_number_of_users_with_3_views) over (partition by 1) as avg_total_number_of_users_with_3_views_per_day
     , avg(total_pct_of_users_with_3_views) over (partition by 1) as avg_total_pct_of_users_with_3_views_per_day
from (select application_id
           , pvedatetime
           , total_users
           , count(*) as total_number_of_users_with_view
           , cast(cast(count(*) as decimal(10,4))/cast(total_users as decimal(10,4)) as decimal(10,4)) as total_pct_of_users_with_view
           , count(case when number_of_views_per_day >= 3 then 1 else null end) as total_number_of_users_with_3_views
           , cast(cast(count(case when number_of_views_per_day >= 3 then 1 else null end) as decimal(10,4))/cast(total_users as decimal(10,4)) as decimal(10,4)) as total_pct_of_users_with_3_views
      from (select application_id
                 , pvedatetime
                 , vieweduserid
                 , count(*) as number_of_views_per_day
                 , total_users
            from (select a.vieworder
                       , a.application_id
                       , a.vieweduserid
                       , a.global_user_id
                       , a.pvedatetime
                       , c.users as total_users
                  from jt.pve_non_pride_views_sequence a
                  left join jt.pve_non_pride_views_sequence_new b
                  on a.id = b.id
                  join (select applicationid
                             , startdate
                             , enddate
                             , users
                        from jt.tm_eventcubesummary
                        where openevent = 0) c
                  on a.application_id = cast(c.applicationid as varchar)
                  where (b.keep_ind is null or b.keep_ind = 1)
                  and a.pvedatetime >= c.startdate
                  and a.pvedatetime <= c.enddate) x
            group by 1,2,3,5) y
      group by 1,2,3) z
)w;

/* Percent Everything Else by eventtype*/
select distinct 
       case 
         when eventtype is null then 'No Event Type'
         when eventtype = '_Unknown' then 'Unknown'
         else eventtype
       end as eventtype
     , count(*) over (partition by eventtype) as total_events
     , avg(total_users) over (partition by eventtype) as avg_number_of_users
     , avg_total_number_of_users_with_view_per_day
     , avg_total_pct_of_users_with_view_per_day
     , avg_total_number_of_users_with_3_views_per_day
     , avg_total_pct_of_users_with_3_views_per_day
from (
select distinct application_id
     , eventtype
     , total_users
     , avg(total_number_of_users_with_view) over (partition by eventtype) as avg_total_number_of_users_with_view_per_day
     , avg(total_pct_of_users_with_view) over (partition by eventtype) as avg_total_pct_of_users_with_view_per_day
     , avg(total_number_of_users_with_3_views) over (partition by eventtype) as avg_total_number_of_users_with_3_views_per_day
     , avg(total_pct_of_users_with_3_views) over (partition by eventtype) as avg_total_pct_of_users_with_3_views_per_day
from (select application_id
           , eventtype
           , pvedatetime
           , total_users
           , count(*) as total_number_of_users_with_view
           , cast(cast(count(*) as decimal(10,4))/cast(total_users as decimal(10,4)) as decimal(10,4)) as total_pct_of_users_with_view
           , count(case when number_of_views_per_day >= 3 then 1 else null end) as total_number_of_users_with_3_views
           , cast(cast(count(case when number_of_views_per_day >= 3 then 1 else null end) as decimal(10,4))/cast(total_users as decimal(10,4)) as decimal(10,4)) as total_pct_of_users_with_3_views
      from (select application_id
                 , eventtype
                 , pvedatetime
                 , vieweduserid
                 , count(*) as number_of_views_per_day
                 , total_users
            from (select a.vieworder
                       , a.application_id
                       , c.eventtype
                       , a.vieweduserid
                       , a.global_user_id
                       , a.pvedatetime
                       , c.users as total_users
                  from jt.pve_non_pride_views_sequence a
                  left join jt.pve_non_pride_views_sequence_new b
                  on a.id = b.id
                  join (select applicationid
                             , eventtype
                             , startdate
                             , enddate
                             , users
                        from jt.tm_eventcubesummary
                        where openevent = 0) c
                  on a.application_id = cast(c.applicationid as varchar)
                  where (b.keep_ind is null or b.keep_ind = 1)
                  and a.pvedatetime >= c.startdate
                  and a.pvedatetime <= c.enddate) x
            group by 1,2,3,4,6) y
      group by 1,2,3,4) z
)w;
