
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
library('glue')

#in R Studio Server: 
setwd("/db-mnt/databricks/rstudio_collab/ccu013_02_R/atlas")

getwd()

source("05b_outputs_functions_lists.R")
source("00_atlas_functions.R")
source("00_atlas_MM_functions.R")


#This script calculates counts of N people per set using the table with most up to date sets, 
#including data driven set: derived using the main analysis results, 
#and appends all counts together as single output


#query to see sets and table most up to date:
#check table: 
sql = paste0("SELECT * 
      FROM dsa_391419_j3w9t.dsa_391419_j3w9t_collab.ccu013_lkp_sets_comprehensive_supersets_v4 ")

sets_check <- DBI::dbGetQuery(con, sql)


#see table with sets: 
sql = paste0("SELECT count(distinct(group_identifier)), count(distinct(set_type)) 
      FROM dsa_391419_j3w9t.dsa_391419_j3w9t_collab.ccu013_lkp_sets_comprehensive_supersets_v4 d")

sets_n_check <- DBI::dbGetQuery(con, sql)



#N in skinny table to get denominator, for output: 
sql_deno = paste0("SELECT count(distinct(s.NHS_NUMBER_DEID)) from dsa_391419_j3w9t_collab.ccu013_02_dp_skinny_patient_23_01_2020_phe_cohort_joined_1ymort_eth s
                   where s.SEX IN (1,2) 
                   AND s.IMD_quintile not like ('Unknown%')
                   AND s.age_23_01_2020 >= 0 and s.age_23_01_2020 <= 100")

df_deno_n <- DBI::dbGetQuery(con, sql_deno)

#deno to use:
deno_n <- df_deno_n$`count(DISTINCT NHS_NUMBER_DEID)`


####################################################################################
#1. query count N people per set (single set as defined in the table imported)
####################################################################################

query_sets <- ("select s.group_identifier as group_identifier,
                s.set_type as set_type,
               count(distinct(p.person_id_deid)) as N_indiv, 
               count(distinct(s.phecode)) as N_dis
               from  dsa_391419_j3w9t_collab.ccu013_phedatav200_unrolled_first_sex_dep p 
               inner join dsa_391419_j3w9t_collab.ccu013_02_dp_skinny_patient_23_01_2020_phe_cohort_joined_1ymort_eth  c
               on p.person_id_deid = c.NHS_NUMBER_DEID
               inner join dsa_391419_j3w9t.dsa_391419_j3w9t_collab.ccu013_lkp_sets_comprehensive_supersets_v4 s
               on p.phecode = s.phecode
               where c.age_23_01_2020 >= 0 and c.age_23_01_2020 <= 100
               and c.sex IN (1, 2)
               and c.IMD_quintile not like ('Unknown') 
               group by s.group_identifier, s.set_type
               order by N_indiv desc")

df_sets<- DBI::dbGetQuery(con, query_sets)





########################
#2. creating datadriven sets: 
########################

#diseases freq < 500 per Mill-using results
main_res <- read.csv("results/v200/main_May28_phe.csv")

#subset phecodes nodes and all n
phenotypes_nodes_dis <- get_nodes_phecodes_from_table('ccu013_phedatav200_unrolled_first_sex_dep')
phe_nodes <- phenotypes_nodes_dis[phenotypes_nodes_dis %in% main_res$cohort]

#select res nodes for these regardless of their n
main_res_nodes <- main_res %>% filter(cohort %in% phe_nodes)

#select diseases with pop_freq <= 500 per Mill (N <=  )
main_res_nodes_rare <- main_res_nodes %>% filter(n_cohort <= 28475)

#create set in same format as the others
df_rare_data_driven <- data.frame(matrix(nrow = nrow(main_res_nodes_rare), ncol=4))
colnames(df_rare_data_driven) <- c('phecode', 'set_identifier', 'group_identifier', 'set_type')

df_rare_data_driven$set_identifier <- 'RARE_DISEASES_DATA_DRIVEN'
df_rare_data_driven$group_identifier <- 'RARE_DISEASES_DATA_DRIVEN'
df_rare_data_driven$set_type <- 'data_driven'
df_rare_data_driven$phecode <- main_res_nodes_rare$cohort



query_data_driven_set <- function(df_data_driven_set){

  #'description:
  #'function that queries to get count of N people using a data driven set given as input 
  #'(data driven input df set should have the same structure as the other sets in databricks table (imported csv))
  
  #@param df_data_driven df (that includes set descriptor and phecodes)
   
  #'usage:
  #' query_data_driven_set(df_data_driven)
  
  
  
# group identifier: 
identifier <- df_data_driven_set$group_identifier[1]
print(identifier)

# set_type: 
set_type <- df_data_driven_set$set_type[1]
print(set_type)

  
# phecodes in set 
set_phe <- df_data_driven_set$phecode
N_dis <- length(unique(set_phe))
print(N_dis)

#sql query here
print(paste0("Extracting data for set ", identifier))
# 
# # query set data driven: 
sql = glue_sql("select {identifier*} as group_identifier,
               {set_type*} as set_type,
               count(distinct(p.person_id_deid)) as N_indiv, 
               {N_dis*} as N_dis
               from  dsa_391419_j3w9t_collab.ccu013_phedatav200_unrolled_first_sex_dep p
               inner join dsa_391419_j3w9t_collab.ccu013_02_dp_skinny_patient_23_01_2020_phe_cohort_joined_1ymort_eth  c
               on p.person_id_deid = c.NHS_NUMBER_DEID
               where  c.age_23_01_2020 >= 0 and c.age_23_01_2020 <= 100
               and c.sex IN (1, 2)
               and c.IMD_quintile not like ('Unknown')
               and p.phecode in ({set_phe*})", .con = con)

#for testing 
# sql = glue_sql("select {identifier*} as group_identifier,
#                count(distinct(p.person_id_deid)) as N_indiv
#                from  dsa_391419_j3w9t_collab.ccu013_phedatav200_unrolled_first_sex_dep p
#                inner join dsa_391419_j3w9t_collab.ccu013_02_dp_skinny_patient_23_01_2020_phe_cohort_joined_1ymort_eth  c
#                on p.person_id_deid = c.NHS_NUMBER_DEID", .con = con)



print(sql)

#get counts
df <- DBI::dbGetQuery(con, sql)


}



#call for this set: 
df_sets_rare<- query_data_driven_set(df_rare_data_driven)



#combine results from sets predefined and datafdriven: 
df_all_sets <- rbind(df_sets, df_sets_rare)


#######################
#prepare to export: 
#######################

cols <- c('N_indiv', 'deno_n')

#round: 
df_all_sets$N_indiv<- as.numeric(df_all_sets$N_indiv)
df_all_sets$N_dis<- as.numeric(df_all_sets$N_dis)


#calculate percentage of total denominator before rounding: 
df_all_sets$proportion_total_deno <- ''


#deno_n: n in pop
df_all_sets <- df_all_sets %>%
  mutate(deno_n  = deno_n)

df_all_sets$proportion_total_deno <- df_all_sets$N_indiv / df_all_sets$deno_n * 100


#call function to round and mask counts 
for (i in cols){
  print(i)
  df_all_sets <- fun_mask_round(df_all_sets, i)
}


df_all_sets <- arrange(df_all_sets, desc(N_indiv_m_r))


#remove this to export as needs rounding
#df_all_sets$N_dis <- NULL

df_all_sets <-df_all_sets %>% filter(group_identifier != 'SUPERSET_NOT_IN_SUPERSET')

write.csv(df_all_sets, 'results/v200/table1/table1_sets_v4_supersets_data_driven_v4.csv', row.names = FALSE)


#################################################################################################################
# test with other sets of phecodes (catalogue or small set for testing
#- done in query separately) - not part of the results 


#create set in same format as the others
df_test_data_driven <- data.frame(matrix(nrow = nrow(main_res_nodes), ncol=4))
colnames(df_test_data_driven) <- c('phecode', 'set_identifier', 'group_identifier', 'set_type')

df_test_data_driven$set_identifier <- 'TEST_DATA_DRIVEN'
df_test_data_driven$group_identifier <- 'TEST_DATA_DRIVEN'
df_test_data_driven$set_type <- 'test_data_driven'
df_test_data_driven$phecode <- main_res_nodes$cohort

#N of N in hes_phe (pyramid) is more general - does not restrict to nodes and does not exclude age, imd unknow etc 
df_test<- query_data_driven_set(df_test_data_driven)

######


#with one phecode matches N_index_dis 
sql = glue_sql("select
                count(distinct(p.person_id_deid)) as N_indiv
                from  dsa_391419_j3w9t_collab.ccu013_phedatav200_unrolled_first_sex_dep p
                inner join dsa_391419_j3w9t_collab.ccu013_02_dp_skinny_patient_23_01_2020_phe_cohort_joined_1ymort_eth  c
                on p.person_id_deid = c.NHS_NUMBER_DEID
                where  c.age_23_01_2020 >= 0 and c.age_23_01_2020 <= 100
                and c.sex IN (1, 2)
                and c.IMD_quintile not like ('Unknown') 
                and p.phecode in ('X250_11')", .con = con)
 
df <- DBI::dbGetQuery(con, sql)


#example small sets:
example <- sets_check %>% filter(group_identifier == 'DET_SOCIAL_EXCLUSION')
View(example)


g2_set_phe<- example$phecode
length(g2_set_phe) #matches N_dis

#with one phecode matches N_index_dis 
sql = glue_sql("select
                count(distinct(p.person_id_deid)) as N_indiv,
                count(distinct(p.phecode)) as N_dis
                from  dsa_391419_j3w9t_collab.ccu013_phedatav200_unrolled_first_sex_dep p
                inner join dsa_391419_j3w9t_collab.ccu013_02_dp_skinny_patient_23_01_2020_phe_cohort_joined_1ymort_eth  c
                on p.person_id_deid = c.NHS_NUMBER_DEID
                where  c.age_23_01_2020 >= 0 and c.age_23_01_2020 <= 100
                and c.sex IN (1, 2)
                and c.IMD_quintile not like ('Unknown') 
                and p.phecode in ({g2_set_phe*})", .con = con)

df <- DBI::dbGetQuery(con, sql)


######
#check list that is intersection between 2 sets - org and det
pair_intersection_phe<- c('X261_2_1', 'X262_0_9', 'XREMAPE45X')
length(pair_intersection_phe) #matches N_dis

#with one phecode matches N_index_dis 
sql = glue_sql("select
                count(distinct(p.person_id_deid)) as N_indiv,
                count(distinct(p.phecode)) as N_dis
                from  dsa_391419_j3w9t_collab.ccu013_phedatav200_unrolled_first_sex_dep p
                inner join dsa_391419_j3w9t_collab.ccu013_02_dp_skinny_patient_23_01_2020_phe_cohort_joined_1ymort_eth  c
                on p.person_id_deid = c.NHS_NUMBER_DEID
                where  c.age_23_01_2020 >= 0 and c.age_23_01_2020 <= 100
                and c.sex IN (1, 2)
                and c.IMD_quintile not like ('Unknown') 
                and p.phecode in ({pair_intersection_phe*})", .con = con)

df <- DBI::dbGetQuery(con, sql)
