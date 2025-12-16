

#clear workspace
rm(list=ls())

#install.packages("table1")
library("DBI")
library('parallel')
library('data.table')
library('plyr')
library('dplyr')
library('tidyr')


#in R Studio Server: 
setwd("/db-mnt/databricks/rstudio_collab/ccu013_02_R/atlas")

getwd()

source("00_atlas_functions.R")


cols <- c('N_indiv')




#overall
query_all <- ("SELECT count(distinct(c.NHS_NUMBER_DEID)) as N_indiv, 
          c.hes_apc_phe_record
          from dsa_391419_j3w9t_collab.ccu013_02_dp_skinny_patient_23_01_2020_phe_cohort_joined_1ymort_eth c
          WHERE c.age_23_01_2020 >= 0 and c.age_23_01_2020 <= 100
          AND c.sex IN (1,2) 
          AND c.IMD_quintile not like ('Unknown%')
          group by 
          c.hes_apc_phe_record")

df_all <- DBI::dbGetQuery(con, query_all)

df_all$N_indiv<- as.numeric(df_all$N_indiv)

for (i in cols){
  print(i)
  df_all <- fun_mask_round(df_all, i)
}

#round: 

write.csv(df_all, 'results/v200/table1/pop_all_HES_apc_phe_noHES.csv', row.names = FALSE)


#age_2
query_sex_2 <- ("SELECT count(distinct(c.NHS_NUMBER_DEID)) as N_indiv, 
          c.hes_apc_phe_record,
          c.sex, 
          c.age_band_23_01_2020
          from dsa_391419_j3w9t_collab.ccu013_02_dp_skinny_patient_23_01_2020_phe_cohort_joined_1ymort_eth c
          WHERE c.age_23_01_2020 >= 0 and c.age_23_01_2020 <= 100
          AND c.sex IN (1,2) 
          AND c.IMD_quintile not like ('Unknown%')
          group by 
          c.hes_apc_phe_record,
          c.sex, 
          c.age_band_23_01_2020")

df_sex_2 <- DBI::dbGetQuery(con, query_sex_2)

df_sex_2$N_indiv<- as.numeric(df_sex_2$N_indiv)

df_sex_2 <- arrange(df_sex_2, hes_apc_phe_record, sex, age_band_23_01_2020)

for (i in cols){
  print(i)
  df_sex_2 <- fun_mask_round(df_sex_2, i)
}

#round: 

write.csv(df_sex_2, 'results/v200/table1/pop_sex_age_HES_apc_phe_noHES.csv', row.names = FALSE)


#imd
query_imd <- ("SELECT count(distinct(c.NHS_NUMBER_DEID)) as N_indiv, 
          c.hes_apc_phe_record,
          c.sex,
          c.IMD_quintile
          from dsa_391419_j3w9t_collab.ccu013_02_dp_skinny_patient_23_01_2020_phe_cohort_joined_1ymort_eth c
          WHERE c.age_23_01_2020 >= 0 and c.age_23_01_2020 <= 100
          AND c.sex IN (1,2) 
          AND c.IMD_quintile not like ('Unknown%')
          group by
          c.hes_apc_phe_record,
          c.sex,
          c.IMD_quintile")

df_imd <- DBI::dbGetQuery(con, query_imd)

#round: 
df_imd$N_indiv<- as.numeric(df_imd$N_indiv)


for (i in cols){
  print(i)
  df_imd <- fun_mask_round(df_imd, i)
}


write.csv(df_imd, 'results/v200/table1/pop_imd_HES_apc_phe_noHES.csv', row.names = FALSE)



#c.ethnicity_11_group
query_ethn_11 <- ("SELECT count(distinct(c.NHS_NUMBER_DEID)) as N_indiv, 
           c.hes_apc_phe_record,
           c.sex, 
           c.ethnicity_11_group
          from dsa_391419_j3w9t_collab.ccu013_02_dp_skinny_patient_23_01_2020_phe_cohort_joined_1ymort_eth c
          WHERE c.age_23_01_2020 >= 0 and c.age_23_01_2020 <= 100
          AND c.sex IN (1,2) 
          AND c.IMD_quintile not like ('Unknown%')
          group by c.hes_apc_phe_record,
          c.sex, 
          c.ethnicity_11_group")

df_ethn_11<- DBI::dbGetQuery(con, query_ethn_11)

#round 
df_ethn_11$N_indiv<- as.numeric(df_ethn_11$N_indiv)


for (i in cols){
  print(i)
  df_ethn_11 <- fun_mask_round(df_ethn_11, i)
}


write.csv(df_ethn_11, 'results/v200/table1/pop_ethn_11_HES_apc_phe_noHES.csv', row.names = FALSE)

#c.ethnicity_5_group
query_ethn_5 <- ("SELECT count(distinct(c.NHS_NUMBER_DEID)) as N_indiv, 
          c.hes_apc_phe_record,
          c.sex, 
          c.ethnicity_5_group
          from dsa_391419_j3w9t_collab.ccu013_02_dp_skinny_patient_23_01_2020_phe_cohort_joined_1ymort_eth c
          WHERE c.age_23_01_2020 >= 0 and c.age_23_01_2020 <= 100
          AND c.sex IN (1,2) 
          AND c.IMD_quintile not like ('Unknown%')
          group by
          c.hes_apc_phe_record,
          c.sex,
          c.ethnicity_5_group")

df_ethn_5<- DBI::dbGetQuery(con, query_ethn_5)

#round: 
df_ethn_5$N_indiv<- as.numeric(df_ethn_5$N_indiv)


for (i in cols){
  print(i)
  df_ethn_5 <- fun_mask_round(df_ethn_5, i)
}

write.csv(df_ethn_5, 'results/v200/table1/pop_ethn_5_HES_apc_phe_noHES.csv', row.names = FALSE)


#c.covid
query_cov <- ("SELECT count(distinct(c.NHS_NUMBER_DEID)) as N_indiv, 
          c.hes_apc_phe_record,
          c.sex, 
          c.covid_case
          from dsa_391419_j3w9t_collab.ccu013_02_dp_skinny_patient_23_01_2020_phe_cohort_joined_1ymort_eth c
          WHERE c.age_23_01_2020 >= 0 and c.age_23_01_2020 <= 100
          AND c.sex IN (1,2) 
          AND c.IMD_quintile not like ('Unknown%')
          group by 
          c.hes_apc_phe_record,
          c.sex, 
          c.covid_case")

df_cov<- DBI::dbGetQuery(con, query_cov)

#round: 
df_cov$N_indiv<- as.numeric(df_cov$N_indiv)


for (i in cols){
  print(i)
  df_cov <- fun_mask_round(df_cov, i)
}


write.csv(df_cov, 'results/v200/table1/pop_cov_HES_apc_phe_noHES.csv', row.names = FALSE)



#c.covid most severe:

sql = paste0("SELECT count(distinct(d.person_id_deid)) as N_indiv,
    c.hes_apc_phe_record,
    c.sex, 
    d.most_severe
    FROM dsa_391419_j3w9t_collab.ccu013_02_phe_cohort_most_severe_covid_cohort_earliest d
    INNER JOIN dsa_391419_j3w9t_collab.ccu013_02_dp_skinny_patient_23_01_2020_phe_cohort_joined_1ymort_eth c
    ON d.person_id_deid = c.NHS_NUMBER_DEID 
    WHERE c.age_23_01_2020 >= 0 and c.age_23_01_2020 <= 100
    AND c.sex IN (1,2) 
    AND c.IMD_quintile not like ('Unknown%')
    GROUP BY 
    c.hes_apc_phe_record,
    c.sex,
    d.most_severe")

df_cov_3<- DBI::dbGetQuery(con, sql)

#round: 
df_cov_3$N_indiv<- as.numeric(df_cov_3$N_indiv)


for (i in cols){
  print(i)
  df_cov_3 <- fun_mask_round(df_cov_3, i)
}


write.csv(df_cov_3, 'results/v200/table1/pop_cov_sev_HES_apc_phe_noHES.csv', row.names = FALSE)


#####
#c.death
query_death <- ("SELECT count(distinct(c.NHS_NUMBER_DEID)) as N_indiv, 
          c.hes_apc_phe_record,
          c.sex, 
          c.death_flag
          from dsa_391419_j3w9t_collab.ccu013_02_dp_skinny_patient_23_01_2020_phe_cohort_joined_1ymort_eth c
          WHERE c.age_23_01_2020 >= 0 and c.age_23_01_2020 <= 100
          AND c.sex IN (1,2) 
          AND c.IMD_quintile not like ('Unknown%')
          group by
          c.hes_apc_phe_record,
          c.sex, 
          c.death_flag")

df_death<- DBI::dbGetQuery(con, query_death)

#
df_death$N_indiv<- as.numeric(df_death$N_indiv)


for (i in cols){
  print(i)
  df_death <- fun_mask_round(df_death, i)
}

write.csv(df_death, 'results/v200/table1/pop_death_HES_apc_phe_noHES.csv', row.names = FALSE)

