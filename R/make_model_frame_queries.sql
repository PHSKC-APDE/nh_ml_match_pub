     -- Remember to swap out ptab and dtab
     Select p.id1, p.id2,
     
     -- date of birth comparisons
     IF(l.dob = r.dob, 1, 0) as dob_exact,
     IF(l.dob_clean_year = r.dob_clean_year, 1, 0) as dob_year_exact,
     hamming(substr(cast(l.dob as varchar), 6, 5), substr(cast(r.dob as varchar), 6, 5)) as dob_mdham,
     
     -- Sex/gender
     IF(l.gender = r.gender, 1, 0) as gender_agree,
     
     -- Name  stuff
     jaro_winkler_similarity(r.first_name_noblank, l.first_name_noblank) as first_name_jw,
     jaro_winkler_similarity(r.last_name_noblank, l.last_name_noblank) as last_name_jw,
     jaro_winkler_similarity(r.first_name_noblank, l.last_name_noblank) as name_swap_jw_1,
     jaro_winkler_similarity(r.last_name_noblank, l.first_name_noblank) as name_swap_jw_2,
     IF(l.middle_initial = r.middle_initial, 1, 0) as middle_initial_agree,
     damerau_levenshtein(
        concat(l.first_name_noblank, l.middle_name_noblank, l.last_name_noblank),
        concat(r.first_name_noblank, r.middle_name_noblank, r.last_name_noblank)
     ) as complete_name_withmid_dl,
     damerau_levenshtein(
        concat(l.first_name_noblank, l.last_name_noblank),
        concat(r.first_name_noblank, r.last_name_noblank)
     ) as complete_name_nomid_dl,
     
     -- Social security stuff
     if(len(l.ssn) >=7 AND len(r.ssn)>=7 AND l.ssn = r.ssn, 1, 0) as ssn_full_exact,
     if(right(l.ssn,4) = left(r.ssn,4), 1, 0) as ssn_last4_exact,
     damerau_levenshtein(right(l.ssn,4), right(r.ssn,4)) as ssn_last4_dl,

     -- Data system
     if(l.source_system = r.source_system, 1, 0) as same_datasystem,

     -- Name frequencies
     case fnf1.first_name_noblank_freq > fnf2.first_name_noblank_freq then fnf1.first_name_noblank_freq
     else fnf2.first_name_noblank_freq end as first_name_freq,
     
     case lnf1.last_name_noblank_freq > lnf2.last_name_noblank_freq then lnf1.first_name_noblank_freq
     else lnf2.last_name_noblank_freq end as last_name_freq,

     from ptab
     left join dtab as l on p.id1 = l.main_id
     left join dtab as r on p.id2 = r.main_id
     left join fnftab as fnf1 on l.first_name_noblank = fnf1.first_name_noblank
     left join fnftab as fnf2 on l.first_name_noblank = fnf2.first_name_noblank
     left join lnftab as lnf1 on l.last_name_noblank = lnf1.last_name_noblank
     left join lnftab as lnf2 on l.last_name_noblank = lnf2.last_name_noblank
     