
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

query_sets <- ("select s.set_identifier, 
               count(distinct(p.person_id_deid)) as N_indiv
               from  dsa_391419_j3w9t_collab.ccu013_phedatav200_unrolled_first_sex_dep p 
               inner join dsa_391419_j3w9t_collab.ccu013_02_dp_skinny_patient_23_01_2020_phe_cohort_joined_1ymort_eth  c
               on p.person_id_deid = c.NHS_NUMBER_DEID
               inner join dsa_391419_j3w9t_collab.ccu013_lkp_sets s
               on p.phecode = s.phecode
               where c.age_23_01_2020 >= 0 and c.age_23_01_2020 <= 100
               and c.sex IN (1, 2)
               and c.IMD_quintile not like ('Unknown') 
               group by s.set_identifier
               order by n_indiv desc")

df_sets<- DBI::dbGetQuery(con, query_sets)

#round: 
df_sets$N_indiv<- as.numeric(df_sets$N_indiv)


#c.covid - ccu013_02_phe_cohort_most_severe_covid_cohort_earliest restricts to first severe events before Jan 21 (follow p Nov 2021)
#case: 
query_cov <- ("SELECT count(distinct(c.NHS_NUMBER_DEID)) as N_indiv, 
          c.covid_case
          from dsa_391419_j3w9t_collab.ccu013_02_dp_skinny_patient_23_01_2020_phe_cohort_joined_1ymort_eth c
          WHERE c.age_23_01_2020 >= 0 and c.age_23_01_2020 <= 100
          AND c.sex IN (1,2) 
          AND c.IMD_quintile not like ('Unknown%')
          group by 
          c.covid_case")

df_cov<- DBI::dbGetQuery(con, query_cov)
df_cov$N_indiv<- as.numeric(df_cov$N_indiv)

df_cov_s <- dplyr::rename(df_cov, set_identifier = covid_case)
df_cov_s$set_identifier <- ifelse(df_cov_s$set_identifier==1, 'covid_case', df_cov_s$set_identifier)
df_cov_s <- df_cov_s %>% filter(set_identifier == 'covid_case')


#severity: 
sql = paste0("SELECT count(distinct(d.person_id_deid)) as N_indiv,
    d.most_severe
    FROM dsa_391419_j3w9t_collab.ccu013_02_phe_cohort_most_severe_covid_cohort_earliest d
    INNER JOIN dsa_391419_j3w9t_collab.ccu013_02_dp_skinny_patient_23_01_2020_phe_cohort_joined_1ymort_eth c
    ON d.person_id_deid = c.NHS_NUMBER_DEID 
    WHERE c.age_23_01_2020 >= 0 and c.age_23_01_2020 <= 100
    AND c.sex IN (1,2) 
    AND c.IMD_quintile not like ('Unknown%')
    GROUP BY 
    d.most_severe")

df_cov_3<- DBI::dbGetQuery(con, sql)
df_cov_3$N_indiv<- as.numeric(df_cov_3$N_indiv)

df_cov_3s <- dplyr::rename(df_cov_3, set_identifier = most_severe)

df_cov_3s$set_identifier <- ifelse(df_cov_3s$set_identifier=='01_phenotype', 'cohort_01', df_cov_3s$set_identifier)
df_cov_3s$set_identifier <- ifelse(df_cov_3s$set_identifier=='02_phenotype', 'cohort_02', df_cov_3s$set_identifier)
df_cov_3s$set_identifier <- ifelse(df_cov_3s$set_identifier=='03_phenotype', 'cohort_03', df_cov_3s$set_identifier)
df_cov_3s$set_identifier <- ifelse(df_cov_3s$set_identifier=='04_phenotype', 'cohort_04', df_cov_3s$set_identifier)


#combine: 
df_sets_all <- rbind(df_sets, 
                     df_cov_s, 
                     df_cov_3s)


#round: 
df_sets_all$N_indiv<- as.numeric(df_sets_all$N_indiv)


for (i in cols){
  print(i)
  df_sets_all <- fun_mask_round(df_sets_all, i)
}


write.csv(df_sets_all, 'results/v200/table1/table1_sets.csv', row.names = FALSE)
