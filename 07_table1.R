
#clear workspace
rm(list=ls())

#install.packages("table1")
library("DBI")
library('parallel')
library('data.table')
library('plyr')
library('dplyr')
library('tidyr')
library('table1')

#in R Studio Server: 
setwd("/db-mnt/databricks/rstudio_collab/ccu013_02_R/atlas")

getwd()

source("05b_outputs_functions_lists.R")
source("00_atlas_functions.R")

cols <- c('N_indiv')


query_sex <- ("SELECT count(distinct(c.NHS_NUMBER_DEID)) as N_indiv, 
          c.sex, 
          mean(c.age_start_date)
          from dsa_391419_j3w9t_collab.ccu013_02_dp_skinny_patient_23_01_2020_phe_cohort_joined_1ymort_eth c
          WHERE c.age_23_01_2020 >= 0 and c.age_23_01_2020 <= 100
          AND c.sex IN (1,2) 
          AND c.IMD_quintile not like ('Unknown%')
          group by 
              c.sex")

df_sex <- DBI::dbGetQuery(con, query_sex)
df_sex$info <- 'sex'
df_sex$N_indiv<- as.numeric(df_sex$N_indiv)


for (i in cols){
  print(i)
  df_sex <- fun_mask_round(df_sex, i)
}



write.csv(df_sex, 'results/v200/table1/pop_sex.csv', row.names = FALSE)

#age_2
query_sex_2 <- ("SELECT count(distinct(c.NHS_NUMBER_DEID)) as N_indiv, 
          c.sex, 
          c.age_band_23_01_2020
          from dsa_391419_j3w9t_collab.ccu013_02_dp_skinny_patient_23_01_2020_phe_cohort_joined_1ymort_eth c
          WHERE c.age_23_01_2020 >= 0 and c.age_23_01_2020 <= 100
          AND c.sex IN (1,2) 
          AND c.IMD_quintile not like ('Unknown%')
          group by 
          c.sex, 
          c.age_band_23_01_2020")

df_sex_2 <- DBI::dbGetQuery(con, query_sex_2)

df_sex_2$N_indiv<- as.numeric(df_sex_2$N_indiv)


for (i in cols){
  print(i)
  df_sex_2 <- fun_mask_round(df_sex_2, i)
}

#round: 

write.csv(df_sex_2, 'results/v200/table1/pop_sex_age.csv', row.names = FALSE)




#age_3 in 5y groups (to expand the above)
query_sex_3 <- ("SELECT count(distinct(c.NHS_NUMBER_DEID)) as N_indiv, 
          c.sex, 
          c.age_band_23_01_2020_5y
          from dsa_391419_j3w9t_collab.ccu013_02_dp_skinny_patient_23_01_2020_phe_cohort_joined_1ymort_eth c
          WHERE c.age_23_01_2020 >= 0 and c.age_23_01_2020 <= 100
          AND c.sex IN (1,2) 
          AND c.IMD_quintile not like ('Unknown%')
          group by 
          c.sex, 
          c.age_band_23_01_2020_5y")

df_sex_3 <- DBI::dbGetQuery(con, query_sex_3)

df_sex_3$N_indiv<- as.numeric(df_sex_3$N_indiv)


for (i in cols){
  print(i)
  df_sex_3 <- fun_mask_round(df_sex_3, i)
}

#round: 

write.csv(df_sex_3, 'results/v200/table1/pop_sex_age_5y.csv', row.names = FALSE)



#imd
query_imd <- ("SELECT count(distinct(c.NHS_NUMBER_DEID)) as N_indiv, 
          c.sex,
          c.IMD_quintile
          from dsa_391419_j3w9t_collab.ccu013_02_dp_skinny_patient_23_01_2020_phe_cohort_joined_1ymort_eth c
          WHERE c.age_23_01_2020 >= 0 and c.age_23_01_2020 <= 100
          AND c.sex IN (1,2) 
          AND c.IMD_quintile not like ('Unknown%')
          group by
          c.sex,
          c.IMD_quintile")

df_imd <- DBI::dbGetQuery(con, query_imd)

#round: 
df_imd$N_indiv<- as.numeric(df_imd$N_indiv)


for (i in cols){
  print(i)
  df_imd <- fun_mask_round(df_imd, i)
}


write.csv(df_imd, 'results/v200/table1/pop_imd.csv', row.names = FALSE)


#c.ethnicity_11_group
query_ethn_11 <- ("SELECT count(distinct(c.NHS_NUMBER_DEID)) as N_indiv, 
           c.sex, 
           c.ethnicity_11_group
          from dsa_391419_j3w9t_collab.ccu013_02_dp_skinny_patient_23_01_2020_phe_cohort_joined_1ymort_eth c
          WHERE c.age_23_01_2020 >= 0 and c.age_23_01_2020 <= 100
          AND c.sex IN (1,2) 
          AND c.IMD_quintile not like ('Unknown%')
          group by c.sex, 
          c.ethnicity_11_group")

df_ethn_11<- DBI::dbGetQuery(con, query_ethn_11)

#round 
df_ethn_11$N_indiv<- as.numeric(df_ethn_11$N_indiv)


for (i in cols){
  print(i)
  df_ethn_11 <- fun_mask_round(df_ethn_11, i)
}


write.csv(df_ethn_11, 'results/v200/table1/pop_ethn_11.csv', row.names = FALSE)

#c.ethnicity_5_group
query_ethn_5 <- ("SELECT count(distinct(c.NHS_NUMBER_DEID)) as N_indiv, 
          c.sex, 
          c.ethnicity_5_group
          from dsa_391419_j3w9t_collab.ccu013_02_dp_skinny_patient_23_01_2020_phe_cohort_joined_1ymort_eth c
          WHERE c.age_23_01_2020 >= 0 and c.age_23_01_2020 <= 100
          AND c.sex IN (1,2) 
          AND c.IMD_quintile not like ('Unknown%')
          group by
          c.sex,
          c.ethnicity_5_group")

df_ethn_5<- DBI::dbGetQuery(con, query_ethn_5)

#round: 
df_ethn_5$N_indiv<- as.numeric(df_ethn_5$N_indiv)


for (i in cols){
  print(i)
  df_ethn_5 <- fun_mask_round(df_ethn_5, i)
}

write.csv(df_ethn_5, 'results/v200/table1/pop_ethn_5.csv', row.names = FALSE)


#c.covid
query_cov <- ("SELECT count(distinct(c.NHS_NUMBER_DEID)) as N_indiv, 
          c.sex, 
          c.covid_case
          from dsa_391419_j3w9t_collab.ccu013_02_dp_skinny_patient_23_01_2020_phe_cohort_joined_1ymort_eth c
          WHERE c.age_23_01_2020 >= 0 and c.age_23_01_2020 <= 100
          AND c.sex IN (1,2) 
          AND c.IMD_quintile not like ('Unknown%')
          group by 
          c.sex, 
          c.covid_case")

df_cov<- DBI::dbGetQuery(con, query_cov)

#round: 
df_cov$N_indiv<- as.numeric(df_cov$N_indiv)


for (i in cols){
  print(i)
  df_cov <- fun_mask_round(df_cov, i)
}


write.csv(df_cov, 'results/v200/table1/pop_cov.csv', row.names = FALSE)

#c.covid
query_cov_2 <- ("SELECT count(distinct(c.NHS_NUMBER_DEID)) as N_indiv, 
          c.sex, 
          c.covid_case, 
          c.type_first_event
          from dsa_391419_j3w9t_collab.ccu013_02_dp_skinny_patient_23_01_2020_phe_cohort_joined_1ymort_eth c
          WHERE c.age_23_01_2020 >= 0 and c.age_23_01_2020 <= 100
          AND c.sex IN (1,2) 
          AND c.IMD_quintile not like ('Unknown%')
          group by c.sex, 
          c.covid_case, 
          c.type_first_event")

df_cov_2<- DBI::dbGetQuery(con, query_cov_2)

#round: 
df_cov_2$N_indiv<- as.numeric(df_cov_2$N_indiv)


for (i in cols){
  print(i)
  df_cov_2 <- fun_mask_round(df_cov_2, i)
}

write.csv(df_cov_2, 'results/v200/table1/pop_cov_2.csv', row.names = FALSE)

#cov severity - CHECK TIME PERIOD: in analysis up to Jan 21 = table has follow up until November - needs to restrict here somehow 
# query <- ("SELECT * 
#           from dsa_391419_j3w9t_collab.ccu013_02_phe_cohort_most_severe_covid_cohort_earliest c
#           limit 100 ")
# 
# df_c <- DBI::dbGetQuery(con, query)

sql = paste0("SELECT count(distinct(d.person_id_deid)) as N_indiv,
    c.sex, 
    d.most_severe
    FROM dsa_391419_j3w9t_collab.ccu013_02_phe_cohort_most_severe_covid_cohort_earliest d
    INNER JOIN dsa_391419_j3w9t_collab.ccu013_02_dp_skinny_patient_23_01_2020_phe_cohort_joined_1ymort_eth c
    ON d.person_id_deid = c.NHS_NUMBER_DEID 
    WHERE c.age_23_01_2020 >= 0 and c.age_23_01_2020 <= 100
    AND c.sex IN (1,2) 
    AND c.IMD_quintile not like ('Unknown%')
    GROUP BY 
    c.sex,
    d.most_severe")

df_cov_3<- DBI::dbGetQuery(con, sql)

#round: 
df_cov_3$N_indiv<- as.numeric(df_cov_3$N_indiv)


for (i in cols){
  print(i)
  df_cov_3 <- fun_mask_round(df_cov_3, i)
}


write.csv(df_cov_3, 'results/v200/table1/pop_cov_3.csv', row.names = FALSE)



#do count from whole followp - above includes first events upto jan2021 even though events considered up to Nov 2021
# query_death_event <- ("select s.person_id_deid, 
# s.most_severe,
# min(s.earliest_most_severe) as earliest_most_severe 
# from dsa_391419_j3w9t_collab.ccu013_02_phe_cohort_most_severe_covid_event_1d s
# group by s.person_id_deid, s.most_severe")
# 
# df_death_event<- DBI::dbGetQuery(con, query_death_event)
# 
# #
# df_death_long$N_indiv<- as.numeric(df_death_long$N_indiv)
# 
# df_death_long_04 <- df_death_event %>% filter(covid_cohort == 'cohort_04')
# length(unique(df_death_long_04$person_id_deid))



#####
#c.death - 1 year (flag)
query_death <- ("SELECT count(distinct(c.NHS_NUMBER_DEID)) as N_indiv, 
          c.sex, 
          c.death_flag
          from dsa_391419_j3w9t_collab.ccu013_02_dp_skinny_patient_23_01_2020_phe_cohort_joined_1ymort_eth c
          WHERE c.age_23_01_2020 >= 0 and c.age_23_01_2020 <= 100
          AND c.sex IN (1,2) 
          AND c.IMD_quintile not like ('Unknown%')
          group by 
          c.sex, 
          c.death_flag")

df_death<- DBI::dbGetQuery(con, query_death)

#
df_death$N_indiv<- as.numeric(df_death$N_indiv)


for (i in cols){
  print(i)
  df_death <- fun_mask_round(df_death, i)
}

write.csv(df_death, 'results/v200/table1/pop_death.csv', row.names = FALSE)



# #death up to 30-11_2021
#c.death -in HR 
query_death <- ("SELECT count(distinct(c.NHS_NUMBER_DEID)) as N_indiv, 
          c.covid_case
          from dsa_391419_j3w9t_collab.ccu013_02_dp_skinny_patient_23_01_2020_phe_cohort_joined_1ymort_eth c
          WHERE c.age_23_01_2020 >= 0 and c.age_23_01_2020 <= 100
          AND c.sex IN (1,2) 
          AND c.IMD_quintile not like ('Unknown%')
          AND c.death_date IS NOT NULL
          and c.death_date <= to_date('2021-11-30', 'yyyy-MM-dd') 
          group by c.covid_case")

df_death<- DBI::dbGetQuery(con, query_death)

#
df_death$N_indiv<- as.numeric(df_death$N_indiv)


for (i in cols){
  print(i)
  df_death <- fun_mask_round(df_death, i)
}

write.csv(df_death, 'results/v200/table1/pop_death_30112021.csv', row.names = FALSE)



#sanity check - comparison with 1 year 

#c.death -in HR 
query_death <- ("SELECT count(distinct(c.NHS_NUMBER_DEID)) as N_indiv, 
          c.covid_case
          from dsa_391419_j3w9t_collab.ccu013_02_dp_skinny_patient_23_01_2020_phe_cohort_joined_1ymort_eth c
          WHERE c.age_23_01_2020 >= 0 and c.age_23_01_2020 <= 100
          AND c.sex IN (1,2) 
          AND c.IMD_quintile not like ('Unknown%')
          AND c.death_date IS NOT NULL
          and c.death_date <= to_date('2021-01-23', 'yyyy-MM-dd') 
          group by c.covid_case")

df_death<- DBI::dbGetQuery(con, query_death)

#
df_death$N_indiv<- as.numeric(df_death$N_indiv)


for (i in cols){
  print(i)
  df_death <- fun_mask_round(df_death, i)
}

write.csv(df_death, 'results/v200/table1/pop_death_23012021.csv', row.names = FALSE)




#####
# #hospitalisations 
##to test- : do in table with only the cohort
query_n_hosp <- ("SELECT count(distinct(c.person_admi_id)) as N_hosp,
          c.person_id_deid_sex
          from dsa_391419_j3w9t_collab.ccu013_phedatav200_admi_unrolled_cohort_sex_dep c
          where c.phecode not like '%ROOT'
          AND c.person_id_deid_sex IN (1,2)
          group by
          c.person_id_deid_sex")

df_hosp_2021<- DBI::dbGetQuery(con, query_n_hosp)

df_hosp_2021$N_hosp<- as.numeric(df_hosp_2021$N_hosp)

cols <- c('N_hosp')
for (i in cols){
   print(i)
   df_hosp_2021 <- fun_mask_round(df_hosp_2021, i)
}

write.csv(df_hosp_2021, 'results/v200/table1/pop_hosp_2021.csv', row.names = FALSE)
# 
#
 #N icd1o codes:
 ##to test- : do in table with only the cohort
query_n_icd10_admi <- ("select 
                        date_band, median(n_icd10_admi) as median_n_icd10,
                        percentile(n_icd10_admi, 0.25) as n_icd10_admi_percentile_25, 
                        percentile(n_icd10_admi, 0.75) as n_icd10_admi_percentile_75
                        from dsa_391419_j3w9t_collab.ccu013_phedatav200_admi_n_icd10
                        group by  date_band")

df_icd10<- DBI::dbGetQuery(con, query_n_icd10_admi)

write.csv(df_icd10, 'results/v200/table1/n_icd10.csv', row.names = FALSE)

###assemble table:






