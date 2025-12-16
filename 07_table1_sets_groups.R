
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

query_sets <- ("select s.group_identifier, 
               count(distinct(p.person_id_deid)) as N_indiv
               from  dsa_391419_j3w9t_collab.ccu013_phedatav200_unrolled_first_sex_dep p 
               inner join dsa_391419_j3w9t_collab.ccu013_02_dp_skinny_patient_23_01_2020_phe_cohort_joined_1ymort_eth  c
               on p.person_id_deid = c.NHS_NUMBER_DEID
               inner join dsa_391419_j3w9t_collab.ccu013_lkp_sets_groups s
               on p.phecode = s.phecode
               where c.age_23_01_2020 >= 0 and c.age_23_01_2020 <= 100
               and c.sex IN (1, 2)
               and c.IMD_quintile not like ('Unknown') 
               group by s.group_identifier
               order by n_indiv desc")

df_sets<- DBI::dbGetQuery(con, query_sets)

#round: 
df_sets$N_indiv<- as.numeric(df_sets$N_indiv)


for (i in cols){
  print(i)
  df_sets <- fun_mask_round(df_sets, i)
}


write.csv(df_sets, 'results/v200/table1/table1_sets_groups.csv', row.names = FALSE)

