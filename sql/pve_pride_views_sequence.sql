\copy (select id, group_id, vieworder, day_diff from jt.pve_pride_views_sequence where group_cnt > 1 order by 1,2,3) to '/home/jteruya/pve_investigate/csv/pve_pride_views_sequence.csv' with csv;
