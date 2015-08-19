drop table if exists jt.pve_email_funnel_delivered;
drop table if exists jt.pve_email_funnel_delivered_once;
drop table if exists jt.pve_email_funnel_delivered_more_than_once;
drop table if exists jt.pve_email_funnel_events_lvl_2;
drop table if exists jt.pve_email_funnel_events_lvl_3;
drop table if exists jt.pve_email_funnel_events_lvl_4;
drop table if exists jt.pve_email_funnel_events_spine;
drop table if exists jt.pve_email_funnel_events_summary;
drop table if exists jt.pve_email_funnel_strange_paths;
drop table if exists jt.pve_email_funnel_fact_analysis;
drop table if exists jt.pve_email_funnel_login_funnel;


/* Table: email_funnel_delivered
   Desciption: Email Delivery Staging Table */
create table jt.pve_email_funnel_delivered as
select a.applicationid
     , b.name
     , b.eventtype
     , a.messageid
     , a.subject
     , a.recipientemail
     , a.senderemail
     , a.eventstatus
     , a.clickurl
     , a.id
     , a.eventtimestamp
from mailgun_events a
join authdb_applications b
on a.applicationid::varchar = lower(b.applicationid)
where a.eventstatus = 'delivered'
and a.subject like 'People are viewing your profile at%'
and a.applicationid <> 'd198da18-e625-45c3-bed9-b53cf38a2fd4'
and a.applicationid <> 'bffee970-c8b3-4a2d-89ef-a9c012000abb';


/* Table: email_funnel_delivered_once
   Description: This contains emails that were only delivered once. */

create table jt.pve_email_funnel_delivered_once as
select applicationid
     , name
     , eventtype
     , recipientemail
     , senderemail
     , messageid
from jt.pve_email_funnel_delivered
group by 1,2,3,4,5,6
having count(*) = 1;

/* Table: email_funnel_delivered_more_than_once
   Description: This contains emails that were delivered more than once - anomaly. */

create table jt.pve_email_funnel_delivered_more_than_once as
select applicationid
     , name
     , eventtype
     , recipientemail
     , senderemail
     , messageid
from jt.pve_email_funnel_delivered
group by 1,2,3,4,5,6
having count(*) > 1;


/* Table: email_funnel_events_lvl_2
   Description: This contains all emails with 'delivered' and/or 'failed' events tracked by mailgun.
*/
create table jt.pve_email_funnel_events_lvl_2 as
select applicationid
     , messageid
     , recipientemail
     , senderemail
     , subject
     , eventstatus
     , eventtimestamp
     , clickurl
     , id
from mailgun_events
where (eventstatus = 'delivered' or eventstatus = 'failed')
and applicationid in (select distinct applicationid from jt.pve_email_funnel_delivered);

/* Table: email_funnel_events_lvl_3
   Description: This contains all emails with 'opened' and/or 'complained' events tracked by mailgun.
*/

create table jt.pve_email_funnel_events_lvl_3 as
select applicationid
     , messageid
     , recipientemail
     , senderemail
     , subject
     , eventstatus
     , eventtimestamp
     , clickurl
     , id
     , case
        when devicetype is null then 'no data'
        else devicetype
       end as devicetype
     , case
        when clienttype is null then 'no data'
        else clienttype
       end as clienttype
     , case
        when clientos is null then 'no data'
        else clientos
       end as clientos
     , case
        when useragent is null then 'no data'
        else useragent
       end as useragent
from mailgun_events
where (eventstatus = 'opened' or eventstatus = 'complained')
and applicationid in (select distinct applicationid from jt.pve_email_funnel_delivered);

/* Table: email_funnel_events_lvl_4
   Description: This contains all email with 'clicked' and/or 'unsubscribed' and/or 'stored' events tracked by mailgun.
*/
create table jt.pve_email_funnel_events_lvl_4 as
select applicationid
     , messageid
     , recipientemail
     , senderemail
     , subject
     , eventstatus
     , eventtimestamp
     , clickurl
     , id
     , case
        when devicetype is null then 'no data'
        else devicetype
       end as devicetype
     , case
        when clienttype is null then 'no data'
        else clienttype
       end as clienttype
     , case
        when clientos is null then 'no data'
        else clientos
       end as clientos
     , case
        when useragent is null then 'no data'
        else useragent
       end as useragent
from mailgun_events
where (eventstatus = 'clicked' or eventstatus = 'unsubscribed' or eventstatus = 'stored')
and applicationid in (select distinct applicationid from jt.pve_email_funnel_delivered);

/* Table: email_funnel_events_spine
   Description: This staging table is a spine table with all of the messageids.
*/

create table jt.pve_email_funnel_events_spine as 
select a.*
from jt.pve_email_funnel_delivered_once a
join (select distinct messageid
      from jt.pve_email_funnel_events_lvl_2
      union
      select distinct messageid
      from jt.pve_email_funnel_events_lvl_3
      union
      select distinct messageid
      from jt.pve_email_funnel_events_lvl_4) b
on a.messageid = b.messageid;



/* Table: email_funnel_events_summary
   Description: This staging table tracks the different states that a message flows through.
*/

create table jt.pve_email_funnel_events_summary as
select a.*
     , case
         when c.messageid is not null then true
         else false
       end as delivered_flag
     , case
         when c.messageid is not null then to_timestamp(c.eventtimestamp/1000)
         else null
       end as delivered_time
     , case
         when d.messageid is not null then true
         else false
       end as failed_flag
     , case
         when d.messageid is not null then to_timestamp(d.eventtimestamp/1000)
         else null
       end as failed_time
     , case
         when e.messageid is not null then true
         else false
       end as opened_flag
     , case
         when e.messageid is not null then to_timestamp(e.eventtimestamp/1000)
         else null
       end as opened_time
     , case
         when f.messageid is not null then true
         else false
       end as complained_flag
     , case
         when f.messageid is not null then to_timestamp(f.eventtimestamp/1000)
         else null
       end as complained_time
     , case
         when g.messageid is not null then true
         else false
       end as clicked_flag
     , case
         when g.messageid is not null then to_timestamp(g.eventtimestamp/1000)
         else null
       end as clicked_time
       
     , case
         when k.messageid is not null then true
         else false
       end as clicked_profile_link_flag
     , case
         when k.messageid is not null then to_timestamp(k.eventtimestamp/1000)
         else null
       end as clicked_profile_link_time       
       
     , case
         when j.messageid is not null then true
         else false
       end as clicked_download_link_flag
     , case
         when j.messageid is not null then to_timestamp(j.eventtimestamp/1000)
         else null
       end as clicked_download_link_time

     , case
         when l.messageid is not null then true
         else false
       end as clicked_unsubscribe_link_flag
     , case
         when l.messageid is not null then to_timestamp(l.eventtimestamp/1000)
         else null
       end as clicked_unsubscribe_link_time

     , case
         when m.messageid is not null then true
         else false
       end as clicked_dd_link_flag
     , case
         when m.messageid is not null then to_timestamp(m.eventtimestamp/1000)
         else null
       end as clicked_dd_link_time

     , case
         when h.messageid is not null then true
         else false
       end as unsubscribed_flag
     , case
         when h.messageid is not null then to_timestamp(i.eventtimestamp/1000)
         else null
       end as unsubscribed_time
     , case
         when i.messageid is not null then true
         else false
       end as stored_flag
     , case
         when i.messageid is not null then to_timestamp(i.eventtimestamp/1000)
         else null
       end as stored_time
from jt.pve_email_funnel_events_spine a
left join (select messageid, min(eventtimestamp) as eventtimestamp, count(*) as cnt from jt.pve_email_funnel_events_lvl_2 where eventstatus = 'delivered' group by 1) c
on a.messageid = c.messageid
left join (select messageid, min(eventtimestamp) as eventtimestamp, count(*) as cnt from jt.pve_email_funnel_events_lvl_2 where eventstatus = 'failed' group by 1) d
on a.messageid = d.messageid
left join (select messageid, min(eventtimestamp) as eventtimestamp, count(*) as cnt from jt.pve_email_funnel_events_lvl_3 where eventstatus = 'opened' group by 1) e
on a.messageid = e.messageid
left join (select messageid, min(eventtimestamp) as eventtimestamp, count(*) as cnt from jt.pve_email_funnel_events_lvl_3 where eventstatus = 'complained' group by 1) f
on a.messageid = f.messageid
left join (select messageid, min(eventtimestamp) as eventtimestamp, count(*) as cnt from jt.pve_email_funnel_events_lvl_4 where eventstatus = 'clicked' group by 1) g
on a.messageid = g.messageid
left join (select messageid, min(eventtimestamp) as eventtimestamp, count(*) as cnt from jt.pve_email_funnel_events_lvl_4 where eventstatus = 'unsubscribed' group by 1) h
on a.messageid = h.messageid
left join (select messageid, min(eventtimestamp) as eventtimestamp, count(*) as cnt from jt.pve_email_funnel_events_lvl_4 where eventstatus = 'stored' group by 1) i
on a.messageid = i.messageid
left join (select messageid, min(eventtimestamp) as eventtimestamp, count(*) as cnt from jt.pve_email_funnel_events_lvl_4 where eventstatus = 'clicked' and clickurl like '%bnc%' group by 1) j
on a.messageid = j.messageid
left join (select messageid, min(eventtimestamp) as eventtimestamp, count(*) as cnt from jt.pve_email_funnel_events_lvl_4 where eventstatus = 'clicked' and clickurl like '%fprofile%' group by 1) k
on a.messageid = k.messageid
left join (select messageid, min(eventtimestamp) as eventtimestamp, count(*) as cnt from jt.pve_email_funnel_events_lvl_4 where eventstatus = 'clicked' and clickurl like '%unsubscribe%' group by 1) l
on a.messageid = l.messageid
left join (select messageid, min(eventtimestamp) as eventtimestamp, count(*) as cnt from jt.pve_email_funnel_events_lvl_4 where eventstatus = 'clicked' and clickurl = 'http://doubledutch.me/' group by 1) m
on a.messageid = m.messageid;



/* Table: email_funnel_strange_paths
   Description: This table contains the emails where anolalous events paths (e.g. "Opened" event but no "Delivered" event) occur.
*/
create table jt.pve_email_funnel_strange_paths as
select applicationid
     , case
         when recipientemail like '%<%>%' then substring(recipientemail from (position('<' in recipientemail) + 1) for (position('>' in recipientemail) - position('<' in recipientemail) - 1))
         else recipientemail
       end as recipientemail
     , messageid
     , delivered_flag
     , failed_flag
     , opened_flag
     , complained_flag
     , clicked_flag
     , unsubscribed_flag
     , stored_flag
     , case
         when ((clicked_flag = true or unsubscribed_flag = true or stored_flag = true) and opened_flag = false)
            then 1
         when ((complained_flag = true or opened_flag = true or clicked_flag = true or unsubscribed_flag = true or stored_flag = true) and delivered_flag = false)
            then 2
         else -1
       end as case_type
from jt.pve_email_funnel_events_summary
where ((complained_flag = true or opened_flag = true or clicked_flag = true or unsubscribed_flag = true or stored_flag = true) and delivered_flag = false)
or ((clicked_flag = true or unsubscribed_flag = true or stored_flag = true) and opened_flag = false);



/* Table: email_funnel_fact_analysis
   Description: This table contains the messages that are not in the strange_paths table.
*/
create table jt.pve_email_funnel_fact_analysis as
select a.applicationid
     , a.eventtype
     , case
         when a.recipientemail like '%<%>%' then substring(a.recipientemail from (position('<' in a.recipientemail) + 1) for (position('>' in a.recipientemail) - position('<' in a.recipientemail) - 1))
         else a.recipientemail
       end as recipientemail
     , a.messageid
     , a.delivered_flag
     , a.delivered_time
     , a.failed_flag
     , a.failed_time
     , case
         when b.case_type = 1 then true
         else a.opened_flag
       end as opened_flag
     , a.opened_time
     , a.complained_flag
     , a.complained_time
     , a.clicked_flag
     , a.clicked_time
     , a.clicked_profile_link_flag
     , a.clicked_profile_link_time
     , a.clicked_download_link_flag
     , a.clicked_download_link_time    
     , a.clicked_unsubscribe_link_flag
     , a.clicked_unsubscribe_link_time     
     , a.clicked_dd_link_flag
     , a.clicked_dd_link_time       
     , a.unsubscribed_flag
     , a.unsubscribed_time
     , a.stored_flag
     , a.stored_time
from jt.pve_email_funnel_events_summary a
left join jt.pve_email_funnel_strange_paths b
on a.messageid = b.messageid and a.applicationid = b.applicationid
where b.applicationid is null
or (b.applicationid is not null and b.case_type <> -1);


drop table jt.pve_email_funnel_login_funnel;

/* Table: email_funnel_login_funnel
   Description: This contains the funnel at the event level
*/
create table jt.pve_email_funnel_login_funnel as
select d.applicationid
     , e.name as eventname
     , d.date as pve_date
     , cast(e.startdate as date)
     , cast(e.enddate as date)
     , u.registered_attendees
     , d.delivered_count as delivered_cnt
     , d.delivered_count::decimal(12,4)/u.registered_attendees::decimal(12,4) as delivered_pct
     , coalesce(d.opened_count,0) as opened_cnt
     , coalesce(d.opened_count,0)::decimal(12,4)/d.delivered_count::decimal(12,4) as opened_pct
     , coalesce(d.clicked_count,0) as clicked_cnt
     , case 
         when coalesce(d.opened_count,0) = 0 then 0
         else coalesce(d.clicked_count,0)::decimal(12,4)/coalesce(d.opened_count,0)::decimal(12,4) 
       end as clicked_pct
     , coalesce(d.clicked_profile_link_count,0) as clicked_profile_cnt
     , case
         when coalesce(d.clicked_count,0) = 0 then 0
         else coalesce(d.clicked_profile_link_count,0)::decimal(12,4)/coalesce(d.clicked_count,0)::decimal(12,4) 
       end as clicked_profile_pct
     , coalesce(d.clicked_download_link_count,0) as clicked_download_count
     , case
         when coalesce(d.clicked_count,0) = 0 then 0
         else coalesce(d.clicked_download_link_count,0)::decimal(12,4)/coalesce(d.clicked_count,0)::decimal(12,4) 
       end as clicked_download_pct     
     , coalesce(d.clicked_unsubscribe_link_count,0) as clicked_unsubscribe_count
     , case
         when coalesce(d.clicked_count,0) = 0 then 0
         else coalesce(d.clicked_unsubscribe_link_count,0)::decimal(12,4)/coalesce(d.clicked_count,0)::decimal(12,4) 
       end as clicked_unsubscribe_pct   
     , coalesce(d.clicked_dd_link_count,0) as clicked_dd_count
     , case
         when coalesce(d.clicked_count,0) = 0 then 0
         else coalesce(d.clicked_dd_link_count,0)::decimal(12,4)/coalesce(d.clicked_count,0)::decimal(12,4) 
       end as clicked_dd_pct  
from (select lower(applicationid) as applicationid
           , count(*) as registered_attendees
      from authdb_is_users
      where lower(applicationid) in (select distinct applicationid::varchar from jt.pve_email_funnel_delivered)
      and isdisabled = 0
      group by 1) u
left join (select applicationid
                , cast(delivered_time as date) as date
                , count(case when delivered_flag = true then 1 else null end) as delivered_count
                , count(case when opened_flag = true then 1 else null end) as opened_count
                , count(case when clicked_flag = true then 1 else null end) as clicked_count
                , count(case when clicked_profile_link_flag = true then 1 else null end) as clicked_profile_link_count
                , count(case when clicked_download_link_flag = true then 1 else null end) as clicked_download_link_count
                , count(case when clicked_unsubscribe_link_flag = true then 1 else null end) as clicked_unsubscribe_link_count   
                , count(case when clicked_dd_link_flag = true then 1 else null end) as clicked_dd_link_count             
           from (select applicationid
                      , recipientemail
                      , delivered_time
                      , delivered_flag
                      , opened_flag
                      , clicked_flag
                      , clicked_profile_link_flag
                      , clicked_download_link_flag
                      , clicked_unsubscribe_link_flag
                      , clicked_dd_link_flag
                 from jt.pve_email_funnel_fact_analysis) x
           group by 1,2) d
on u.applicationid = d.applicationid::varchar
left join authdb_applications e
on d.applicationid::varchar = lower(e.applicationid);

-- Regular Funnel
select applicationid
     , eventname
     , pve_date
     , startdate
     , enddate
     , registered_attendees
     , delivered_cnt
     , delivered_pct
     , opened_cnt
     , opened_pct
     , clicked_cnt
     , clicked_pct
from jt.pve_email_funnel_login_funnel
order by 4,5,1,3;

/*
select case
        when pve_date < startdate then 1
        when pve_date between startdate and enddate then 2
        else 3
       end as group
     , avg(delivered_cnt) as average_delivered_count_per_event_per_day
     , avg(opened_cnt) as average_opened_count_per_event_per_day
     , avg(clicked_cnt) as average_clicked_count_per_event_per_day
     , avg(delivered_pct) as average_delivered_pct_per_event_per_day
     , avg(opened_pct) as average_opened_pct_per_event_per_day
     , avg(clicked_pct) as average_clicked_pct_per_event_per_day
     , percentile_cont(0.5) within group (order by delivered_cnt) as median_delivered_cnt_per_event_per_day
     , percentile_cont(0.5) within group (order by opened_cnt) as median_opened_cnt_per_event_per_day     
     , percentile_cont(0.5) within group (order by clicked_cnt) as median_clicked_cnt_per_event_per_day      
     , percentile_cont(0.5) within group (order by delivered_pct) as median_delivered_pct_per_event_per_day 
     , percentile_cont(0.5) within group (order by opened_pct) as median_opened_pct_per_event_per_day     
     , percentile_cont(0.5) within group (order by clicked_pct) as median_clicked_pct_per_event_per_day
from jt.pve_email_funnel_login_funnel
group by 1;
*/



-- Number of Profile Clicks
select case
        when pve_date < startdate then 1
        when pve_date between startdate and enddate then 2
        else 3
       end as group
     , min(clicked_profile_cnt) as min_clicked_profile_cnt
     , max(clicked_profile_cnt) as max_clicked_profile_cnt
     , avg(clicked_profile_cnt) as average_clicked_profile_cnt
     , percentile_cont(0.5) within group (order by clicked_profile_cnt) as median_clicked_profile_cnt
     , min(clicked_profile_pct) as min_clicked_profile_pct
     , max(clicked_profile_pct) as max_clicked_profile_pct
     , avg(clicked_profile_pct) as average_clicked_profile_pct
     , percentile_cont(0.5) within group (order by clicked_profile_pct) as median_clicked_profile_pct
from jt.pve_email_funnel_login_funnel
where clicked_cnt > 0
group by 1;

-- Number of Download Clicks
select case
        when pve_date < startdate then 1
        when pve_date between startdate and enddate then 2
        else 3
       end as group
     , min(clicked_download_count) as min_clicked_download_count
     , max(clicked_download_count) as max_clicked_download_count
     , avg(clicked_download_count) as average_clicked_download_count
     , percentile_cont(0.5) within group (order by clicked_download_count) as median_clicked_download_count
     , min(clicked_download_pct) as min_clicked_download_pct
     , max(clicked_download_pct) as max_clicked_download_pct
     , avg(clicked_download_pct) as average_clicked_download_pct
     , percentile_cont(0.5) within group (order by clicked_download_pct) as median_clicked_download_pct
from jt.pve_email_funnel_login_funnel
where clicked_cnt > 0
group by 1;

-- Number of Unsubscribe Clicks
select case
        when pve_date < startdate then 1
        when pve_date between startdate and enddate then 2
        else 3
       end as group
     , min(clicked_unsubscribe_count) as min_clicked_unsubscribe_count
     , max(clicked_unsubscribe_count) as max_clicked_unsubscribe_count
     , avg(clicked_unsubscribe_count) as average_clicked_unsubscribe_count
     , percentile_cont(0.5) within group (order by clicked_unsubscribe_count) as median_clicked_unsubscribe_count
     , min(clicked_unsubscribe_pct) as min_clicked_unsubscribe_pct
     , max(clicked_unsubscribe_pct) as max_clicked_unsubscribe_pct
     , avg(clicked_unsubscribe_pct) as average_clicked_unsubscribe_pct
     , percentile_cont(0.5) within group (order by clicked_unsubscribe_pct) as median_clicked_unsubscribe_pct
from jt.pve_email_funnel_login_funnel
where clicked_cnt > 0
group by 1;

-- Number of DD Clicks
select case
        when pve_date < startdate then 1
        when pve_date between startdate and enddate then 2
        else 3
       end as group
     , avg(clicked_profile_pct) as average_clicked_profile_pct  
     , avg(clicked_download_pct) as average_clicked_download_pct
     , avg(clicked_unsubscribe_pct) as average_clicked_unsubscribe_pct
     , avg(clicked_dd_pct) as average_clicked_dd_pct            
     , avg(clicked_profile_cnt) as average_clicked_profile_cnt       
     , avg(clicked_download_count) as average_clicked_download_count
     , avg(clicked_unsubscribe_count) as average_clicked_unsubscribe_count
     , avg(clicked_dd_count) as average_clicked_dd_count          
     , min(clicked_profile_cnt) as min_clicked_profile_cnt
     , max(clicked_profile_cnt) as max_clicked_profile_cnt
     , percentile_cont(0.5) within group (order by clicked_profile_cnt) as median_clicked_profile_cnt
     , min(clicked_profile_pct) as min_clicked_profile_pct
     , max(clicked_profile_pct) as max_clicked_profile_pct
     , percentile_cont(0.5) within group (order by clicked_profile_pct) as median_clicked_profile_pct       
     , min(clicked_download_count) as min_clicked_download_count
     , max(clicked_download_count) as max_clicked_download_count
     , percentile_cont(0.5) within group (order by clicked_download_count) as median_clicked_download_count
     , min(clicked_download_pct) as min_clicked_download_pct
     , max(clicked_download_pct) as max_clicked_download_pct
     , percentile_cont(0.5) within group (order by clicked_download_pct) as median_clicked_download_pct    
     , min(clicked_unsubscribe_count) as min_clicked_unsubscribe_count
     , max(clicked_unsubscribe_count) as max_clicked_unsubscribe_count
     , percentile_cont(0.5) within group (order by clicked_unsubscribe_count) as median_clicked_unsubscribe_count
     , min(clicked_unsubscribe_pct) as min_clicked_unsubscribe_pct
     , max(clicked_unsubscribe_pct) as max_clicked_unsubscribe_pct
     , percentile_cont(0.5) within group (order by clicked_unsubscribe_pct) as median_clicked_unsubscribe_pct       
     , min(clicked_dd_count) as min_clicked_dd_count
     , max(clicked_dd_count) as max_clicked_dd_count
     , percentile_cont(0.5) within group (order by clicked_dd_count) as median_clicked_dd_count
     , min(clicked_dd_pct) as min_clicked_dd_pct
     , max(clicked_dd_pct) as max_clicked_dd_pct
     , percentile_cont(0.5) within group (order by clicked_dd_pct) as median_clicked_dd_pct
from jt.pve_email_funnel_login_funnel
where clicked_cnt > 0
group by 1;

-- Average/Median Number of PVE Deliveries Per Day
select count(*) as number_of_days
     , min(date) as first_day
     , max(date) as last_day
     , avg(delivered_cnt) as average_deliveries_per_day
     , percentile_cont(0.5) within group (order by delivered_cnt) as median_deliveries_per_day
from (
select cast(to_timestamp(eventtimestamp/1000) as date) as date
     , count(*) as delivered_cnt
from mailgun_events a
join authdb_applications b
on a.applicationid::varchar = lower(b.applicationid)
where a.eventstatus = 'delivered'
and a.subject like 'People are viewing your profile at%'
and a.applicationid <> 'd198da18-e625-45c3-bed9-b53cf38a2fd4'
and a.applicationid <> 'bffee970-c8b3-4a2d-89ef-a9c012000abb'
group by 1) a; 


