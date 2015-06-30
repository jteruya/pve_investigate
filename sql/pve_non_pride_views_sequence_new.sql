set schema 'jt';

drop table if exists pve_non_pride_views_sequence_new;

create table pve_non_pride_views_sequence_new (
    id integer
  , group_id integer 
  , vieworder integer
  , day_diff integer
  , keep_ind integer );

\copy pve_non_pride_views_sequence_new from '/home/jteruya/pve_investigate/csv/pve_non_pride_views_sequence_new.csv' delimiter ',';
