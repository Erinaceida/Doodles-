
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
library('stringr')

#in R Studio Server: 
setwd("/db-mnt/databricks/rstudio_collab/ccu013_02_R/atlas")

getwd()

source("05b_outputs_functions_lists.R")
source("00_atlas_functions.R")
source("00_atlas_MM_functions.R")


#this script have queries to count N people in combination of sets in 2 ways: 

#1:people with co-occurring sets (supersets and other sets)

#2: intersection of 2 sets (organ vs determinants)


#test: 
# query to see sets and table most up to date:
# check table: 
sql = paste0("SELECT * 
     FROM dsa_391419_j3w9t.dsa_391419_j3w9t_collab.ccu013_lkp_sets_comprehensive_supersets_v4 d
    limit 5")

sets_check <- DBI::dbGetQuery(con, sql)

#each set and its type:
sql = paste0("SELECT distinct(group_identifier), d.set_type
      FROM dsa_391419_j3w9t.dsa_391419_j3w9t_collab.ccu013_lkp_sets_comprehensive_supersets_v4 d")

sets_unique <- DBI::dbGetQuery(con, sql)

#count unique sets
sql = paste0("SELECT count(distinct(group_identifier)), count(distinct(set_type)) 
      FROM dsa_391419_j3w9t.dsa_391419_j3w9t_collab.ccu013_lkp_sets_comprehensive_supersets_v4 d")

sets_n <- DBI::dbGetQuery(con, sql)


###########################################################################################
##query to count number of people with phecodes in co-occuring sets (given as input)
##########################################################################################

pairs_of_sets_cooc <- function(g1, g2){
  
  #'description:
  #'function that obtain the number of people in both sets given as input
  #'sets should exist and defined as such in the table used inside the function
  
  #'param g1 string
  #'param g2 string
  
  #'usage:
  #'pairs_of_sets_cooc('UK_COVID_SHIELDED_03_2020_SINGLES', 'SUPERSET_GENETIC')
  
  query_cooc_sets = glue_sql("SELECT count(distinct(t1.person_id_deid)) as N_indiv, {g1*} as s1, {g2*} as s2
                 from dsa_391419_j3w9t_collab.ccu013_phedatav200_unrolled_first_sex_dep t1,
                 dsa_391419_j3w9t.dsa_391419_j3w9t_collab.ccu013_lkp_sets_comprehensive_supersets_v4 t3
                 where t1.phecode = t3.phecode 
                 and t3.group_identifier = {g2*} 
                 and t1.person_id_deid in 
                 (select t2.person_id_deid
                  from dsa_391419_j3w9t_collab.ccu013_phedatav200_unrolled_first_sex_dep t2
                  INNER JOIN dsa_391419_j3w9t_collab.ccu013_02_dp_skinny_patient_23_01_2020_phe_cohort_joined_1ymort_eth s
                  ON t2.person_id_deid = s.NHS_NUMBER_DEID
                  inner join dsa_391419_j3w9t.dsa_391419_j3w9t_collab.ccu013_lkp_sets_comprehensive_supersets_v4 s2
                  on t2.phecode = s2.phecode
                  where s.SEX IN (1,2) 
                  AND s.IMD_quintile not like ('Unknown%')
                  AND s.age_23_01_2020 >= 0 and s.age_23_01_2020 <= 100
                  and s2.group_identifier =  {g1*}  
                  GROUP BY t2.person_id_deid, t2.phecode, s.dob
                  )", .con = con)
  
  
  df_sets<- DBI::dbGetQuery(con, query_cooc_sets)
  
  return(df_sets)
  
  
}

#test1 <- pairs_of_sets_cooc('SUPERSET_MUSCL', 'SUPERSET_MUSCL')
#test <- pairs_of_sets_cooc('SUPERSET_MUSCL', 'SUPERSET_GENETIC')
#test3 <- pairs_of_sets_cooc('UK_COVID_SHIELDED_03_2020_SINGLES', 'UK_COVID_SHIELDED_03_2020_SINGLES')
#test4 <- pairs_of_sets_cooc('UK_COVID_SHIELDED_03_2020_SINGLES', 'SUPERSET_GENETIC')

##create all possible pairs of sets in lkp table 
#create dataframe with all pairs in the group of sets/supersets we want to test
supersets <- c('SUPERSET_INFLAMMATION', 'SUPERSET_DIGESTIVE',
               'DET_STRUCTURAL', 'SUPERSET_MUSCL', 
               'SUPERSET_CVD', 'SUPERSET_INFECTION',
               'ORGAN_BRAIN_MENTAL', 'SUPERSET_NERVOUS', 
               'NEOPLASM_NON_MALIG', 'SUPERSET_CANCER',
               'SUPERSET_GENETIC')


# superset_pairs <- combn(supersets, 2)

# define combinations of 2
df_all_pairs_supersets <- expand_grid(supersets, supersets)
colnames(df_all_pairs_supersets) <- c("s1", "s2")


#expand with pairs of both the same - equals to people in that set:- done for testing purposes and completeness 
df_all_pairs_supersets_same <- data.frame(matrix(nrow = length(supersets), ncol=2))
colnames(df_all_pairs_supersets_same) <- c("s1", "s2")

df_all_pairs_supersets_same$s1 <- supersets
df_all_pairs_supersets_same$s2 <- supersets

df_all_pairs_supersets <- rbind(df_all_pairs_supersets, df_all_pairs_supersets_same)



#create datafrme for outputs: 
df_all_pairs_supersets_counts <- data.frame()

for (i in 1:nrow(df_all_pairs_supersets)){
  s1 <- df_all_pairs_supersets$s1[i]
  s2 <- df_all_pairs_supersets$s2[i]
  
  print(c(s1, s2))
  
  
  temp_out <- pairs_of_sets_cooc(s1, s2)
  
  df_all_pairs_supersets_counts <- rbind(df_all_pairs_supersets_counts, temp_out)
  
}


########################
#coo-occ sets when one set is data driven:
########################
pairs_of_sets_cooc_data_driven <- function(g1, g2_data_driven, g2_set_phe){
  
  #'description:
  #'function to obtain the number of people in both sets given as input, when one set is defined data driven
  #'one set should exist and defined as such in the table used inside the function
  #'the data driven set is defined separately 
  
  #'param g1 string
  #'param g2_data_driven string
  #'param g2_set_phe string (list of phecodes in the data driven set)
  
  #'usage:
  #'pairs_of_sets_cooc_data_driven('SUPERSET_GENETIC', 'RARE_DISEASES_DATA_DRIVEN',
  #'                               df_rare_data_driven$phecode )
  
  query_cooc_sets = glue_sql("SELECT count(distinct(t1.person_id_deid)) as N_indiv, {g1*} as s1, {g2_data_driven*} as s2
                  from dsa_391419_j3w9t_collab.ccu013_phedatav200_unrolled_first_sex_dep t1 
                  where t1.phecode in ({g2_set_phe*}) 
                  and t1.person_id_deid in 
                  (select t2.person_id_deid
                   from dsa_391419_j3w9t_collab.ccu013_phedatav200_unrolled_first_sex_dep t2
                   INNER JOIN dsa_391419_j3w9t_collab.ccu013_02_dp_skinny_patient_23_01_2020_phe_cohort_joined_1ymort_eth s
                   ON t2.person_id_deid = s.NHS_NUMBER_DEID
                   inner join dsa_391419_j3w9t.dsa_391419_j3w9t_collab.ccu013_lkp_sets_comprehensive_supersets_v4 s2
                   on t2.phecode = s2.phecode
                   where s.SEX IN (1,2) 
                   AND s.IMD_quintile not like ('Unknown%')
                   AND s.age_23_01_2020 >= 0 and s.age_23_01_2020 <= 100
                   and s2.group_identifier = {g1*}  
                   GROUP BY t2.person_id_deid, t2.phecode, s.dob
                   )", .con = con)
  
  #single set: 
  #query_cooc_sets = glue_sql("select count(distinct(t2.person_id_deid))
  #                from dsa_391419_j3w9t_collab.ccu013_phedatav200_unrolled_first_sex_dep t2
  #                INNER JOIN dsa_391419_j3w9t_collab.ccu013_02_dp_skinny_patient_23_01_2020_phe_cohort_joined_1ymort_eth s
  #                ON t2.person_id_deid = s.NHS_NUMBER_DEID
  #                inner join dsa_391419_j3w9t.dsa_391419_j3w9t_collab.ccu013_lkp_sets_comprehensive_supersets_v4 s2
  #                on t2.phecode = s2.phecode
  #                where s.SEX IN (1,2) 
  #                AND s.IMD_quintile not like ('Unknown%')
  #                AND s.age_23_01_2020 >= 0 and s.age_23_01_2020 <= 100
  #                and s2.group_identifier = {g1*}  
  #                GROUP BY t2.person_id_deid, t2.phecode, s.dob", .con = con)
  
  
  df_sets<- DBI::dbGetQuery(con, query_cooc_sets)
  
  return(df_sets)
  
  
}



########################
#coo-occ sets when two sets are data driven:
########################
pairs_of_sets_cooc_two_data_driven <- function(g1_data_driven, g1_set_phe, g2_data_driven, g2_set_phe){
  
  
  #'description:
  #'function to obtain the number of people in both sets given as input, when two sets are defined in a data driven way
  #'the data driven sets are defined separately prior to their use in this function 
  
  #'param g1_data_driven - string
  #'param g1_set_phe - string 
  #'param g2_data_driven - string
  #'param g2_set_phe - string
  
  #'usage: pairs_of_sets_cooc_two_data_driven('RARE_DISEASES_DATA_DRIVEN', df_rare_data_driven$phecode,
  #'                                          'RARE_DISEASES_DATA_DRIVEN', df_rare_data_driven$phecode)

  
  query_cooc_sets = glue_sql("SELECT count(distinct(t1.person_id_deid)) as N_indiv, {g1_data_driven*} as s1, {g2_data_driven*} as s2
                  from dsa_391419_j3w9t_collab.ccu013_phedatav200_unrolled_first_sex_dep t1 
                  where t1.phecode in ({g2_set_phe*}) 
                  and t1.person_id_deid in 
                  (select t2.person_id_deid
                   from dsa_391419_j3w9t_collab.ccu013_phedatav200_unrolled_first_sex_dep t2
                   INNER JOIN dsa_391419_j3w9t_collab.ccu013_02_dp_skinny_patient_23_01_2020_phe_cohort_joined_1ymort_eth s
                   ON t2.person_id_deid = s.NHS_NUMBER_DEID
                   inner join dsa_391419_j3w9t.dsa_391419_j3w9t_collab.ccu013_lkp_sets_comprehensive_supersets_v4 s2
                   on t2.phecode = s2.phecode
                   where s.SEX IN (1,2) 
                   AND s.IMD_quintile not like ('Unknown%')
                   AND s.age_23_01_2020 >= 0 and s.age_23_01_2020 <= 100
                   and s2.phecode in ({g1_set_phe*})  
                   GROUP BY t2.person_id_deid, t2.phecode, s.dob
                   )", .con = con)
  
  #single set: 
  #query_cooc_sets = glue_sql("select count(distinct(t2.person_id_deid))
  #                from dsa_391419_j3w9t_collab.ccu013_phedatav200_unrolled_first_sex_dep t2
  #                INNER JOIN dsa_391419_j3w9t_collab.ccu013_02_dp_skinny_patient_23_01_2020_phe_cohort_joined_1ymort_eth s
  #                ON t2.person_id_deid = s.NHS_NUMBER_DEID
  #                inner join dsa_391419_j3w9t.dsa_391419_j3w9t_collab.ccu013_lkp_sets_comprehensive_supersets_v4 s2
  #                on t2.phecode = s2.phecode
  #                where s.SEX IN (1,2) 
  #                AND s.IMD_quintile not like ('Unknown%')
  #                AND s.age_23_01_2020 >= 0 and s.age_23_01_2020 <= 100
  #                and s2.group_identifier = {g1*}  
  #                GROUP BY t2.person_id_deid, t2.phecode, s.dob", .con = con)
  
  
  df_sets<- DBI::dbGetQuery(con, query_cooc_sets)
  
  return(df_sets)
  
  
}

########################
#creating datadriven sets: 
########################

#diseases freq < 500 per Mill-using results
main_res <- read.csv("results/v200/main_May28_phe.csv")

#subset phecodes nodes and all n
phenotypes_nodes_dis <- get_nodes_phecodes_from_table('ccu013_phedatav200_unrolled_first_sex_dep')
phe_nodes <- phenotypes_nodes_dis[phenotypes_nodes_dis %in% main_res$cohort]

#select res nodes for these reagrdless of their n
main_res_nodes <- main_res %>% filter(cohort %in% phe_nodes)

#select diseases with pop_freq > 500 per Mill (N > 28475)
main_res_nodes_rare <- main_res_nodes %>% filter(n_cohort <= 28475)


#create set in same format as the others
df_rare_data_driven <- data.frame(matrix(nrow = nrow(main_res_nodes_rare), ncol=4))
colnames(df_rare_data_driven) <- c('phecode', 'set_identifier', 'group_identifier', 'set_type')

df_rare_data_driven$set_identifier <- 'RARE_DISEASES_DATA_DRIVEN'
df_rare_data_driven$group_identifier <- 'RARE_DISEASES_DATA_DRIVEN'
df_rare_data_driven$set_type <- 'data_driven'
df_rare_data_driven$phecode <- main_res_nodes_rare$cohort



#test:
#test_datadriven <- pairs_of_sets_cooc_data_driven('SUPERSET_INFLAMMATION', 'RARE_DISEASES_DATA_DRIVEN', df_rare_data_driven$phecode )
#test_datadriven_2 <- pairs_of_sets_cooc_data_driven('RARE_DISEASES_DATA_DRIVEN', 'RARE_DISEASES_DATA_DRIVEN', df_rare_data_driven$phecode )


#create pairs using data_driven_set to then rbind results below!
superset_pairs_data_driven <-  CJ(supersets, 'RARE_DISEASES_DATA_DRIVEN',  unique = TRUE)
superset_pairs_data_driven <- rename(superset_pairs_data_driven, 's1'= supersets, 's2' = V2)

#placeholder
#superset_pairs_data_driven_mirr <-  CJ('RARE_DISEASES_DATA_DRIVEN',supersets,  unique = TRUE)
#superset_pairs_data_driven_mirr <- rename(superset_pairs_data_driven_mirr, 's1'= V1, 's2' = supersets)

#superset_pairs_data_driven <- rbind(superset_pairs_data_driven, 
#                                    superset_pairs_data_driven_mirr)


df_all_pairs_supersets_data_driven <- data.frame(matrix(nrow = nrow(superset_pairs_data_driven), ncol=2))
colnames(df_all_pairs_supersets_data_driven) <- c("s1", "s2")

df_all_pairs_supersets_data_driven$s1 <- superset_pairs_data_driven$s1
df_all_pairs_supersets_data_driven$s2 <- superset_pairs_data_driven$s2

#create datafrme for outputs: 
df_all_pairs_supersets_data_driven_counts <- data.frame()

for (i in 1:nrow(df_all_pairs_supersets_data_driven)){
  s1 <- df_all_pairs_supersets_data_driven$s1[i]
  s2 <- df_all_pairs_supersets_data_driven$s2[i]
  
  print(c(s1, s2))
  
  temp_out <- pairs_of_sets_cooc_data_driven(s1, 'RARE_DISEASES_DATA_DRIVEN', df_rare_data_driven$phecode )
  
  df_all_pairs_supersets_data_driven_counts <- rbind(df_all_pairs_supersets_data_driven_counts, temp_out)
  
  
}



#sets wo are datadriven: 
cooc_rare_same_data_driven <- pairs_of_sets_cooc_two_data_driven('RARE_DISEASES_DATA_DRIVEN', df_rare_data_driven$phecode, 'RARE_DISEASES_DATA_DRIVEN', df_rare_data_driven$phecode)



#append coo-cocc sets and coocsets datadriven: 
df_all_pairs_supersets_counts_all<- rbind(df_all_pairs_supersets_data_driven_counts, df_all_pairs_supersets_counts)
df_all_pairs_supersets_counts_all<- rbind(df_all_pairs_supersets_counts_all, cooc_rare_same_data_driven)

df_all_pairs_supersets_counts_all$pair_type <- 'cooccurring_sets'

#round and save 
df_all_pairs_supersets_counts_all$N_indiv <- as.numeric(df_all_pairs_supersets_counts_all$N_indiv)


cols <- c('N_indiv')

for (i in cols){
  print(i)
  df_all_pairs_supersets_counts_all <- fun_mask_round(df_all_pairs_supersets_counts_all, i)
}


#arrange
df_all_pairs_supersets_counts_all <- arrange(df_all_pairs_supersets_counts_all, desc(N_indiv_m_r))

#remove this to export if not wanted
#df_all_pairs_intersect_counts$N_dis <- NULL


write.csv(df_all_pairs_supersets_counts_all, 'results/v200/table1/table1_sets_supersets_cooccurring_v4_all.csv', row.names = FALSE)

# #########
# #test query:
# query_test = paste0("select 
#                count(distinct(p.person_id_deid)) as N_indiv
#                from  dsa_391419_j3w9t_collab.ccu013_phedatav200_unrolled_first_sex_dep p 
#                inner join dsa_391419_j3w9t_collab.ccu013_02_dp_skinny_patient_23_01_2020_phe_cohort_joined_1ymort_eth  c
#                on p.person_id_deid = c.NHS_NUMBER_DEID
#                where c.age_23_01_2020 >= 0 and c.age_23_01_2020 <= 100
#                and c.sex IN (1, 2)
#                and c.IMD_quintile not like ('Unknown')
#                and s.phecode IN (X286_4, X287_0_1, X287_0_3, X289_5_2, X656_4)
#                ")
# 
# test_qu<- DBI::dbGetQuery(con, query_test)


################################################################################################
#count unique people in phecodes that are in both sets (in the intersection between two sets), using input sets 
##doing a self-join to check 2 sets 
################################################################################################

pairs_of_sets_intersections <- function(g1, g2){
  
  #'description: function to obtain number of people observed in diseases in the intersection of two sets given as input
  #
  
  #'param g1 string
  #'param g2 string
  
  
  #'usage: pairs_of_sets_intersections_test('INFECTION_OTHER', 'ORGAN_SYSTEMIC')

  query_sets = paste0("select '", g1,"' as s1, 
                '", g2,"' as s2,
               count(distinct(p.person_id_deid)) as N_indiv, 
               count(distinct(s.phecode)) as N_dis
               from  dsa_391419_j3w9t_collab.ccu013_phedatav200_unrolled_first_sex_dep p 
               inner join dsa_391419_j3w9t_collab.ccu013_02_dp_skinny_patient_23_01_2020_phe_cohort_joined_1ymort_eth  c
               on p.person_id_deid = c.NHS_NUMBER_DEID
               inner join dsa_391419_j3w9t.dsa_391419_j3w9t_collab.ccu013_lkp_sets_comprehensive_supersets_v4 s
               on p.phecode = s.phecode
               inner join dsa_391419_j3w9t.dsa_391419_j3w9t_collab.ccu013_lkp_sets_comprehensive_supersets_v4 s2
               on s.phecode = s2.phecode
               where c.age_23_01_2020 >= 0 and c.age_23_01_2020 <= 100
               and c.sex IN (1, 2)
               and c.IMD_quintile not like ('Unknown')
               and s.group_identifier = '", g1,"' 
               and s2.group_identifier = '", g2,"'
               ")
  
  df_sets<- DBI::dbGetQuery(con, query_sets)
  

}

################################
#organs and determinants
################################
organs <-sets_unique %>% filter(str_starts(group_identifier, 'ORGAN_'))
n_organ_sets <- length(unique(organs$group_identifier))

determinants  <-sets_unique %>% filter(set_type == 'Determinants')
n_determinants_sets <- length(unique(determinants$group_identifier))

#create df with sets pairs:
df_all_pairs_sets_o_d <- data.frame(matrix(nrow = n_organ_sets*n_determinants_sets, ncol=2))
colnames(df_all_pairs_sets_o_d) <- c("s1", "s2")
#df_all_pairs_sets_o_d$s1 <-organs$group_identifier    
#df_all_pairs_sets_o_d


#combine each organ set with all determinatn set and create all possible combinations between organs and determinants: 
df_all_pairs_sets_o_d <- CJ(organs$group_identifier, determinants$group_identifier, unique = TRUE)
df_all_pairs_sets_o_d<- rename(df_all_pairs_sets_o_d,  's1' = V1, 's2' = V2)


#to parallel easier:
run_pairs_of_sets_intersections <- function(i, df_all_pairs_sets_o_d){
  
  s1 <- df_all_pairs_sets_o_d$s1[i]
  s2 <- df_all_pairs_sets_o_d$s2[i]
  
  #print(s1)
  #print(s2)
  
  temp_out <- pairs_of_sets_intersections(s1, s2)
  
  
}


print(paste0("START: ", Sys.Date(), " ", Sys.time() ))
all_pairs_intersect_counts <-mclapply(1:nrow(df_all_pairs_sets_o_d), function(i) {run_pairs_of_sets_intersections(i, df_all_pairs_sets_o_d)}, mc.cores = 6)
df_all_pairs_intersect_counts <-bind_rows(all_pairs_intersect_counts)
print(paste0("end: ", Sys.Date(), " ", Sys.time() ))

#round:
df_all_pairs_intersect_counts$N_indiv <- as.numeric(df_all_pairs_intersect_counts$N_indiv)


cols <- c('N_indiv')

for (i in cols){
  print(i)
  df_all_pairs_intersect_counts <- fun_mask_round(df_all_pairs_intersect_counts, i)
}



df_all_pairs_intersect_counts <- arrange(df_all_pairs_intersect_counts, desc(N_indiv_m_r))

#remove this to export as needs rounding
#df_all_pairs_intersect_counts$N_dis <- NULL

df_all_pairs_intersect_counts$pair_type <- 'intersection'

write.csv(df_all_pairs_intersect_counts, 'results/v200/table1/table1_sets_pairs_intersections_v4.csv', row.names = FALSE)



############
#test: 
#############

pairs_of_sets_intersections_test <- function(g1, g2){
  
  query_sets = paste0("select '", g1,"' as s1, 
                '", g2,"' as s2,
               count(distinct(p.person_id_deid)) as N_indiv
               from  dsa_391419_j3w9t_collab.ccu013_phedatav200_unrolled_first_sex_dep p 
               inner join dsa_391419_j3w9t_collab.ccu013_02_dp_skinny_patient_23_01_2020_phe_cohort_joined_1ymort_eth  c
               on p.person_id_deid = c.NHS_NUMBER_DEID
               inner join dsa_391419_j3w9t.dsa_391419_j3w9t_collab.ccu013_lkp_sets_comprehensive_supersets_v4 s
               on p.phecode = s.phecode
               inner join dsa_391419_j3w9t.dsa_391419_j3w9t_collab.ccu013_lkp_sets_comprehensive_supersets_v4 s2
               on s.phecode = s2.phecode
               where c.age_23_01_2020 >= 0 and c.age_23_01_2020 <= 100
               and c.sex IN (1, 2)
               and c.IMD_quintile not like ('Unknown')
               and s.group_identifier = '", g1,"' 
               and s2.group_identifier = '", g2,"'
               ")
  
  df_sets<- DBI::dbGetQuery(con, query_sets)
  
  
}


test1 <- pairs_of_sets_intersections_test('ORGAN_BLOOD', 'INFECTION_OTHER')
test2 <- pairs_of_sets_intersections_test('INFECTION_OTHER', 'ORGAN_BLOOD')
test3 <- pairs_of_sets_intersections_test('ORGAN_SYSTEMIC', 'INFECTION_OTHER')
test4 <- pairs_of_sets_intersections_test('INFECTION_OTHER', 'ORGAN_SYSTEMIC')