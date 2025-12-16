


#clear workspace
rm(list=ls())

library("DBI")
library('parallel')
library('survival')
library('data.table')
library('dplyr')
library('tidyr')



#in R Studio Server: 
setwd("/db-mnt/databricks/rstudio_collab/ccu013_02_R/atlas")

getwd()

#con 

source("00_atlas_MM_functions.R")
source("05b_outputs_functions_lists.R")
########
#functions used here:

#n cases and n unique admissions before baseline - grouped by phecode 
n_n_baseline <- ("SELECT * 
          from dsa_391419_j3w9t_collab.ccu013_phedatav200_n_n_admi_baseline c")

df_n_n_baseline <- DBI::dbGetQuery(con, n_n_baseline)

#########################
#n days per admission 
n_days_dur <- ("select *
             from dsa_391419_j3w9t_collab.ccu013_phedatav200_admi_median_duration_before_baseline")

df_n_days_dur <- DBI::dbGetQuery(con, n_days_dur)
# 
# #########################
# #n admissions after baseline (to calculate risk of hospitalisations) - work in progress
# admi_1y_after_baseline <- ("SELECT * 
#           from dsa_391419_j3w9t_collab.ccu013_phedatav200_admi_n_1y_afterbaseline c")
# 
# df_admi_1y_after_baseline <- DBI::dbGetQuery(con, admi_1y_after_baseline)
# 

#combine in one table: 

df_n_n_baseline <- merge(df_n_n_baseline, df_n_days_dur, by = 'phecode')
#df_n_n_baseline_after <- merge(df_n_n_baseline, df_admi_1y_after_baseline, by = 'phecode')
#df_n_n_baseline_after$absrisk_hosp1y <- (df_n_n_baseline_after$n_individuals/df_n_n_baseline_after$n_patients) * 100


#cast list:

# sql_cast = paste0("SELECT * FROM dsa_391419_j3w9t_collab.ccu013_lkp_phev200_cast")
# cast <- DBI::dbGetQuery(con, sql_cast)
# cast_2 <- c('X696_41', 'X252_1_1', 'X278_1_1', 'X557_1_1', 'XK862', 'X295_1', 
#             'X531_2_5', 'X531_3_5', 'X603_0_4', 'XREMAPI839', 'X601_8_3', 
#             'X158_0_6', 'X159_0_1', 'X164_0_9', 'X187_8_3', 'X208_0_8', 'X227_0_1', 
#             'X365_0', 'X366_2_1', 'X378_0_4', 'XUNMELTH209', 'X323_2', 'X332_0', 
#             'X334_1_4', 'X359_1_1', 'ORPHA010', 'X180_3_8', 'X182_0_3', 'X184_2_16', 
#             'X614_32_1', 'X380_1_4', 'X381_1_4', 'XREMAPJ350', 'X709_2', 'X594_2_1', 
#             'XREMAPI890')
# cast_phe <- c(cast$phecode, cast_2)

#select nodes: 
nodes <- get_nodes_phecodes_from_table('ccu013_phedatav200_unrolled_first_sex_dep')
  

df_n_cast  <- df_n_n_baseline %>% filter(phecode %in% nodes)


cols <- c('n_patients_before_baseline', 'n_admissions_before_baseline')
  
  
df_n_cast$n_patients_before_baseline<- as.numeric(df_n_cast$n_patients_before_baseline)
df_n_cast$n_admissions_before_baseline<- as.numeric(df_n_cast$n_admissions_before_baseline)


for (i in cols){
  print(i)
  df_n_cast <- fun_mask_round(df_n_cast, i)
}

write.csv(df_n_cast, 'results/v200/table1/n_hosp_nodes.csv', row.names = FALSE)
