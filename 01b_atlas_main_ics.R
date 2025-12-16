

#clear workspace
rm(list=ls())

library("DBI")
library('parallel')
library('survival')
library('data.table')
library('plyr')
library('dplyr')
library('tidyr')
library('dsrTest')


#in R Studio Server: 
setwd("/db-mnt/databricks/rstudio_collab/ccu013_02_R/atlas")

getwd()


source("00_atlas_functions.R")
source("00_atlas_MM_functions.R")
source("05b_outputs_functions_lists.R")

########


#count per sex and age band, variable and level, for denos in standard prevalence 
get_age_sex_bands_ICS_deno_from_table <- function(tablename, variable, level){
  
  #'description:
  #count N people age and sex 
  
  #@param tablename - string
  #@param variable - string - column name
  #@param level - string - ICD code 
  
  
  #'usage:
  #' get_age_sex_bands_ICS_deno_from_table('ccu013_02_gp_ICS_map_cohort', 'ICB_Code', i ) 
  #' 
  #' 
  #' 
  sqlden = paste0("
  SELECT c.age_band_23_01_2020, c.sex, count(distinct(i.NHS_NUMBER_DEID)) as n_indiv 
  FROM dars_nic_391419_j3w9t_collab.", tablename, " i
  INNER JOIN  dars_nic_391419_j3w9t_collab.ccu013_02_dp_skinny_patient_23_01_2020_phe_cohort_joined_1ymort c
  ON c.NHS_NUMBER_DEID = i.NHS_NUMBER_DEID WHERE ", variable, " ='", level,  
                  "'AND c.IMD_quintile not like ('Unknown%')
  AND c.sex in (1, 2)
  AND c.age_23_01_2020 >= 0 and c.age_23_01_2020 <= 100
  group by c.age_band_23_01_2020, c.sex
  order by c.age_band_23_01_2020, c.sex")
  counts <- DBI::dbGetQuery(con, sqlden)
  return(counts)
}







run_ics <- function(p, tablename, ics_deno){
  
  #'description:
  ##function to run per phecode (disease)
  
  #@param p: phecode
  #@param tablename: events table from databricks 
  #@param ics_deno: denominator in ics
  #output df_res (containing relevant estimates per disease)
  
  
  #'usage:
  #' run_ics(i, 'ccu013_phedatav200_unrolled_first_sex_dep', ics_deno)
  #' 
  
  #ncol must be same length as length colnames
  df_res <- data.frame(matrix(nrow = 1, ncol=6))
  colnames(df_res) <- c("cohort", "sex_flag","n_cohort", 
                        "prev_pop", "ci_left_prev_pop", "ci_right_prev_pop")
  
  #colnames(df_res) <- 'phecode'
  df_res$cohort <- p
  print("creating df")
  #print(df_res)
  
  
  if (main_phe_ics){
    
    # Get data
    print(paste0("Extracting data for ", p))
    
    sql = paste0("SELECT ph.person_id_deid, 
    MIN(ph.date) as first_date, 
    c.ethnicity, 
    c.IMD_quintile, 
    c.sex, 
    c.covid_case, 
    c.death_date, 
    c.start_date, 
    c.end_date, 
    c.age_start_date, 
    c.dob,
    c.age_band_23_01_2020,
    c.death_flag,
    datediff(MIN(ph.date), date(dob))/365.2425 as age,
    i.PRACTICE, 
    i.ICB_Code,
    i.DATE
    FROM dsa_391419_j3w9t_collab.", tablename, " ph
    INNER JOIN  dars_nic_391419_j3w9t_collab.ccu013_02_dp_skinny_patient_23_01_2020_phe_cohort_joined_1ymort c
    ON ph.person_id_deid = c.NHS_NUMBER_DEID 
    INNER  JOIN dars_nic_391419_j3w9t_collab.ccu013_02_gp_ICS_map_cohort i
    ON i.NHS_NUMBER_DEID = c.NHS_NUMBER_DEID
    WHERE ph.phecode ='", p,"'
    AND c.age_23_01_2020 >= 0 and c.age_23_01_2020 <= 100
    AND c.sex IN (1,2) 
    AND c.IMD_quintile not like ('Unknown%')
    GROUP BY ph.person_id_deid, 
               c.ethnicity, 
               c.IMD_quintile, 
               c.sex, 
               c.covid_case, 
               c.death_date, 
               c.start_date, 
               c.end_date, 
               c.age_start_date, 
               c.dob,
               c.age_band_23_01_2020,
               c.death_flag, 
               i.PRACTICE, 
               i.ICB_Code, 
               i.DATE
               HAVING MIN(ph.date) <= to_date('2020-01-23', 'yyyy-MM-dd') 
               AND MIN(ph.date) >= date(c.dob)")
    
    
    #find sex in phe_dict for this phecode 
    sql_sex = paste0("SELECT sex as sex_flag
            FROM dsa_391419_j3w9t_collab.ccu013_lkp_phev200_phedict d 
                                    WHERE d.phecode ='", p,"'")
    
    df_sex_flag <- DBI::dbGetQuery(con, sql_sex)
    
    
    
    
    df_res$sex_flag <- df_sex_flag[1,]
    #print(df_res)
    
  }else if (main_cov_ics){

    sql = paste0("SELECT d.person_id_deid, 
    MIN(d.earliest_most_severe) as first_date, 
    c.ethnicity, 
    c.IMD_quintile, 
    c.sex, 
    c.covid_case, 
    c.death_date, 
    c.start_date, 
    c.end_date, 
    c.age_start_date, 
    c.dob,
    c.age_band_23_01_2020,
    c.death_flag,
    datediff(MIN(d.earliest_most_severe), date(dob))/365.2425 as age,
    i.PRACTICE, 
    i.ICB_Code,
    i.DATE
    FROM dsa_391419_j3w9t_collab.", tablename, " d
    INNER JOIN  dars_nic_391419_j3w9t_collab.ccu013_02_dp_skinny_patient_23_01_2020_phe_cohort_joined_1ymort c
    ON d.person_id_deid = c.NHS_NUMBER_DEID 
    INNER  JOIN dars_nic_391419_j3w9t_collab.ccu013_02_gp_ICS_map_cohort i
    ON i.NHS_NUMBER_DEID = c.NHS_NUMBER_DEID
    WHERE d.covid_cohort ='", p,"'
    AND c.age_23_01_2020 >= 0 and c.age_23_01_2020 <= 100
    AND c.sex IN (1,2) 
    AND c.IMD_quintile not like ('Unknown%')
    GROUP BY d.person_id_deid, 
               d.earliest_most_severe,
               c.ethnicity, 
               c.IMD_quintile, 
               c.sex, 
               c.covid_case, 
               c.death_date, 
               c.start_date, 
               c.end_date, 
               c.age_start_date, 
               c.dob,
               c.age_band_23_01_2020,
               c.death_flag, 
               i.PRACTICE, 
               i.ICB_Code, 
               i.DATE
               HAVING MIN(d.earliest_most_severe) <= to_date('2021-01-23', 'yyyy-MM-dd') 
               AND MIN(d.earliest_most_severe) >= date(c.dob)")
    
    
    sex_flag <- 0 
    
    df_res$sex_flag <- sex_flag 
    
  }

  
  
  df <- DBI::dbGetQuery(con, sql)

  #based on sex_flag
  #lkp: male = 1 and female = 2 
  #aligned with TRE gdppr and our skinny
  
  if(df_res$sex_flag =='1'){
    
    print('male specific cohort')
    
    #subset from events using sex in skinny
    df <- subset(df, (sex == '1'))
    
  }else if(df_res$sex_flag =='2'){
    
    print('female specific cohort')
    
    #subset from events using sex in skinny
    df <- subset(df, (sex == '2'))
  
  }else{
    
    print('no sex specific')
    
  }
  
  
  # There are no cases, skip, return empty df_res
  if (nrow(df) == 1){
    df_res$n_cohort <- 1
    print(paste0("Skip due to lack of cases ", p))
    return(df_res)
  } else {
    
    
    # nphecode in age and sex bands - will write 20 columns in output df (df_res)
    df_res$n_cohort=length(unique(df[,'person_id_deid']))
    
    prev_scale <- 10^6
    

    deno_prev_pop <- den_pop
    
    #add nphecode for sanity 
    df_res$prev_pop <- (df_res$n_cohort/deno_prev_pop) * prev_scale
    print(df_res$prev_pop )
    print(prev_scale)
    
    df_res$deno_pop <- deno_prev_pop
    
    #ci
    ci_prev_pop <- ci_prop(df_res$prev_pop, prev_scale, deno_prev_pop )
    df_res$ci_left_prev_pop <- ci_prev_pop[1]
    df_res$ci_right_prev_pop <- ci_prev_pop[2]
    
    #integrate ICS
    ics_all <- unique(ics_deno$ICB_CODE)
    
    print(ics_all)
    
    for (i in ics_all){
      
      print(i)
      #select rows unique patients in this ICS 
      temp_df <- df %>% filter(ICB_Code == i)
      
      #select deno of this ics
      temp_ics_deno <- ics_deno %>% filter(ICB_CODE == i)
      
      
      # total deno ics
      tot_icd_deno <- sum(temp_ics_deno$n_indiv)
      
      print(tot_icd_deno)
      
      if (length(unique(temp_df$person_id_deid)) > 0){
        
        #save cols for every ICS
        col_coh <- paste('n_cohort_', i, sep = '')
        df_res[[col_coh]]<-length(unique(temp_df$person_id_deid))
        
        col_deno <- paste('deno_', i, sep = '')
        df_res[[col_deno]]<- tot_icd_deno
        
        #calculate prevalence in the ics
        prev_ics = (length(unique(temp_df$person_id_deid))/tot_icd_deno) * prev_scale
        
        col_prev <- paste('prev_ics_', i, sep = '')
        df_res[[col_prev]]<- prev_ics
        
        ci_ics <- ci_prop(df_res[[col_prev]], prev_scale,tot_icd_deno )
        
        col_ci_left <- paste('ci_left_', i, sep= '')
        df_res[[col_ci_left]]<- ci_ics[1]
        
        col_ci_right <- paste('ci_right_', i, sep= '')
        df_res[[col_ci_right]]<- ci_ics[2]

        print(df_res)
        
        #do stand prev:select aage and sex bands counts for this ICS
        temp_counts_ICB <- counts_ICB_Code %>% filter(ICB_Code==i)
        
        df_res_temp <- data.frame(matrix(nrow = 1, ncol=22))
        colnames(df_res_temp) <- c("cohort", "ICS", 
        "B0_9_male","B0_9_female", "B10_19_male", "B10_19_female", "B20_29_male", "B20_29_female", "B30_39_male", "B30_39_female", 
        "B40_49_male", "B40_49_female", "B50_59_male", "B50_59_female", "B60_69_male", "B60_69_female", "B70_79_male", "B70_79_female", 
        "B80_89_male", "B80_89_female", "B90p_male", "B90p_female")
        
         
        
        #stanprev 
        df_res_temp <- fun_stand_prev_pop(temp_df, 
                                        temp_counts_ICB, 
                                        esp_10y, 
                                        df_res_temp, 
                                        prev_scale) 
        
        
        
        col_stand_pop_ics <- paste('prev_', i, '_stand',sep = '') 
        df_res[[col_stand_pop_ics]]<- df_res_temp$prev_pop_stand
        
        col_ci_left_ics <- paste('ci_left_prev_', i, '_stand', sep = '')
        df_res[[col_ci_left_ics]]<- df_res_temp$ci_left_prev_pop_stand
        
        col_ci_right_ics <- paste('ci_right_prev_', i, '_stand', sep = '')
        df_res[[col_ci_right_ics]]<- df_res_temp$ci_right_prev_pop_stand
        
        
        #to do: delete all age bands!
        
        
      } else {
        #if no cases
        #save cols for every ICS
        col_coh <- paste('n_cohort_', i, sep = '')
        df_res[[col_coh]]<-0
        
        col_deno <- paste('deno_', i, sep = '')
        df_res[[col_deno]]<- tot_icd_deno
        
        #calculate prevalence in the ics
        prev_ics = 0
        
        col_prev <- paste('prev_ics_', i, sep = '')
        df_res[[col_prev]]<- prev_ics
        
        col_ci_left <- paste('ci_left_', i, sep= '')
        df_res[[col_ci_left]]<-NA
        
        col_ci_right <- paste('ci_right_', i, sep= '')
        df_res[[col_ci_right]]<-NA
        
        #stand
        col_stand_pop_ics <- paste('prev_', i, '_stand',sep = '') 
        df_res[[col_stand_pop_ics]]<- NA
        
        col_ci_left_ics <- paste('ci_left_prev_', i, '_stand', sep = '')
        df_res[[col_ci_left_ics]]<- NA
        
        col_ci_right_ics <- paste('ci_right_prev_', i, '_stand', sep = '')
        df_res[[col_ci_right_ics]]<- NA
        
      }
      
    }
    
    return(df_res)
    
  }
  
}



####call functions

den_sex_all <- get_denominator_from_table('ccu013_02_dp_skinny_patient_23_01_2020_phe_cohort_joined_1ymort', 'sex')
den_male <- filter(den_sex_all, sex==1)$n_indiv
den_female <- filter(den_sex_all, sex==2)$n_indiv

den_pop <- sum(den_sex_all$n_indiv)

den_ethn_all <- get_denominator_from_table('ccu013_02_dp_skinny_patient_23_01_2020_phe_cohort_joined_1ymort', 'ethnicity')
den_ethn_male <- get_sex_specific_denominator_from_table('ccu013_02_dp_skinny_patient_23_01_2020_phe_cohort_joined_1ymort', 'ethnicity', '1')
den_ethn_female <- get_sex_specific_denominator_from_table('ccu013_02_dp_skinny_patient_23_01_2020_phe_cohort_joined_1ymort', 'ethnicity', '2')


den_imd_all <- get_denominator_from_table('ccu013_02_dp_skinny_patient_23_01_2020_phe_cohort_joined_1ymort', 'IMD_quintile')
den_imd_male <- get_sex_specific_denominator_from_table('ccu013_02_dp_skinny_patient_23_01_2020_phe_cohort_joined_1ymort', 'IMD_quintile', '1')
den_imd_female <- get_sex_specific_denominator_from_table('ccu013_02_dp_skinny_patient_23_01_2020_phe_cohort_joined_1ymort', 'IMD_quintile', '2')

ics_deno <- dbGetQuery(con, "SELECT * FROM dars_nic_391419_j3w9t_collab.ccu013_02_gp_ICS_map_cohort_deno")

print(paste0("Strata specific and general denominators identified"))

###
#unique ICS
df_ics <- dbGetQuery(con, "SELECT distinct(ICB_Code) 
                  FROM dars_nic_391419_j3w9t_collab.ccu013_02_gp_ICS_map_cohort")

#allICS 
list_ics <- df_ics$ICB_Code

#counts per ICS , age and sex band
#age band per ics
counts_ICB_Code <- data.frame()
for (i in list_ics){
  print (i)
  df_temp <- get_age_sex_bands_ICS_deno_from_table('ccu013_02_gp_ICS_map_cohort', 'ICB_Code', i ) 
  df_temp$ICB_Code <- i
  counts_ICB_Code <- rbind(counts_ICB_Code, df_temp)
}


#standpop
esp_10y <- dbGetQuery(con, "SELECT * FROM dars_nic_391419_j3w9t_collab.ccu013_lkp_esp_10y")
esp_10y <- arrange(esp_10y, AgeGroup, Sex)
esp_10y$ESP <- as.numeric(esp_10y$ESP)
#####
#call functions

#phenotypes <- get_phecodes_from_table('ccu013_phedatav200_unrolled_first')
#phenotypes <- get_phecodes_sex_specific_from_table('ccu013_phedatav200_unrolled_first')
phenotypes <- get_nodes_phecodes_from_table('ccu013_phedatav200_unrolled_first_sex_dep')
print(paste0("Got ", length(phenotypes), " phenotypes"))

#exclude XROOT
phe_exclude <- 'XROOT'
phenotypes <- phenotypes[!phenotypes %in% phe_exclude]

main_res <- read.csv("results/v200/main_May28_phe.csv")
main_res_n10 <- main_res %>% filter(n_cohort>=10)
phenotypes <- phenotypes[phenotypes %in% main_res_n10$cohort]

cov_cohorts <- c('cohort_01', 'cohort_02', 'cohort_03', 'cohort_04')

#################################
#only one can be TRUE
main_phe_ics <- TRUE
main_cov_ics <- FALSE
#################################

print(paste0("Querying and running models"))

print(paste0("START: ", Sys.Date(), " ", Sys.time()))

if (main_phe_ics){
  
  str_out <- 'phe'
  print(str_out)
  
  results <- mclapply(phenotypes[1:10], function(i) {run_ics(i, 'ccu013_phedatav200_unrolled_first_sex_dep', ics_deno)}, mc.cores = 8)
  #results <- lapply(phenotypes[1], function(i) {run_ics(i, 'ccu013_phedatav200_unrolled_first_sex_dep', ics_deno)})
  
} else if (main_cov_ics){
  
  str_out <- 'cov'
  print(str_out)
  
  results <- lapply(cov_cohorts, function(i) {run_ics(i, 'ccu013_02_phe_cohort_most_severe_covid_cohort_earliest', ics_deno)})
  
}

print(paste0("END: ", Sys.Date(), " ", Sys.time() )) 

output_all <-bind_rows(results)
write.csv(output_all, paste0("results/v200/main_May28_ICS_8_", str_out, ".csv"), row.names = FALSE)

#export: 
#read results: 
# ics_all <- rbind()

ics_all <- fread("results/v200/main_May28_ICS.csv")

ics_all_sm <- ics_all %>% select(!starts_with('deno'))

#cols to pass through the function
cols <- colnames(ics_all_sm %>% select(starts_with('n_')))

for (i in cols){
  print(i)
  ics_all_sm <- fun_mask_round(ics_all_sm, i)
}


ics_all_sm_stand  <- ics_all_sm %>% select(contains('stand'))
ics_cohort  <- ics_all[,c('cohort')]
ics_export <- cbind(ics_cohort, ics_all_sm_stand)

df_ics_export <- data.frame(matrix(nrow = nrow(ics_all_sm), ncol=1))
df_ics_export$cohort <- ics_export$ics_cohort
#df_ics_export$matrix.nrow...nrow.ics_all_sm...ncol...1. <- NULL 

#subset columns per icd to concatenate together 
for (i in list_ics){
  
  print(i)
  
  temp_df <- ics_export %>% select(contains(i))
  
  temp_df <- round(temp_df, 2)
  
  print(colnames(temp_df))
  
  cols <- colnames(temp_df)
  
  newcol <- paste0(cols[1], ',', cols[2], ',', cols[3])
  
  temp <- unite(temp_df, newcol)

  print(head(temp))
  
  temp <- dplyr::rename(temp, !!i := newcol) 
  
  df_ics_export <- cbind(temp, df_ics_export)

}




df_ics_export <- merge(df_ics_export, ics_all[,c('cohort', 'n_cohort')], by = 'cohort')
df_ics_export <- df_ics_export %>% relocate(cohort)

df_ics_export <- df_ics_export %>% filter(n_cohort>100)
df_ics_export$n_cohort <- NULL

df_ics_export_1 <- df_ics_export[1:29]

split_into_1k_new(df_ics_export_1,5, 'ICS_stand', '_May28_')

#second set: 
df_ics_export_2 <- df_ics_export[30:44]
split_into_1k_new(df_ics_export_2, 5, 'ICS_stand_set2', '_May28_')

ics_all_counts <- ics_all %>% filter(cohort %in% df_ics_export$cohort)
ics_all_counts <- ics_all_counts %>% select(starts_with('n_cohort_Q'))

for (i in colnames(ics_all_counts)){
  print(i)
  ics_all_counts <- fun_mask_round(ics_all_counts, i)
}

###
#export N per ICS
###

ics_all_sm_n  <- ics_all_sm %>% select(starts_with('n_'))
ics_cohort  <- ics_all[,c('cohort')]
ics_export_2 <- cbind(ics_cohort, ics_all_sm_n)

df_ics_export_2 <- ics_export_2 %>% filter(n_cohort_m_r>100)
df_ics_export_2$n_cohort_m_r <- NULL

#remove 
names(df_ics_export_2) <- gsub(pattern = "_m_r", replacement = "", x = names(df_ics_export_2))

#function to concatenate 2 columns - counts of ics and their names; returns concat column
concat_n_ICS <- function(pair_df){
  name_newcol <- paste0(colnames(pair_df)[1], ',', colnames(pair_df)[2]) 
  pair_df$newcol  <- paste0(pair_df[[1]], ',', pair_df[[2]])
  pair_df <- dplyr::rename(pair_df, !!name_newcol := newcol) 
  
  print(head(pair_df))
  
  pair_df[[1]] <- NULL
  pair_df[[1]] <- NULL 
  
  print(head(pair_df))
  
  return(pair_df)
}


pair1 <- df_ics_export_2[,2:3]
temp_p1 <- concat_n_ICS(pair1)

pair2<- df_ics_export_2[,4:5]
temp_p2 <- concat_n_ICS(pair2)

pair3<- df_ics_export_2[,6:7]
temp_p3 <- concat_n_ICS(pair3)

pair4<- df_ics_export_2[,8:9]
temp_p4 <- concat_n_ICS(pair4)

pair5<- df_ics_export_2[,10:11]
temp_p5 <- concat_n_ICS(pair5)

pair6<- df_ics_export_2[,12:13]
temp_p6 <- concat_n_ICS(pair6)

pair7<- df_ics_export_2[,14:15]
temp_p7 <- concat_n_ICS(pair7)

pair8<- df_ics_export_2[,16:17]
temp_p8 <- concat_n_ICS(pair8)

pair9<- df_ics_export_2[,18:19]
temp_p9 <- concat_n_ICS(pair9)

pair10<- df_ics_export_2[,20:21]
temp_p10 <- concat_n_ICS(pair10)

pair11<- df_ics_export_2[,22:23]
temp_p11 <- concat_n_ICS(pair11)

pair12<- df_ics_export_2[,24:25]
temp_p12 <- concat_n_ICS(pair12)

pair13<- df_ics_export_2[,26:27]
temp_p13 <- concat_n_ICS(pair13)

pair14<- df_ics_export_2[,28:29]
temp_p14 <- concat_n_ICS(pair14)

pair15<- df_ics_export_2[,30:31]
temp_p15 <- concat_n_ICS(pair15)

pair16<- df_ics_export_2[,32:33]
temp_p16 <- concat_n_ICS(pair16)

pair17<- df_ics_export_2[,34:35]
temp_p17 <- concat_n_ICS(pair17)

pair18<- df_ics_export_2[,36:37]
temp_p18 <- concat_n_ICS(pair18)

pair19<- df_ics_export_2[,38:39]
temp_p19 <- concat_n_ICS(pair19)

pair20<- df_ics_export_2[,40:41]
temp_p20 <- concat_n_ICS(pair20)

pair21<- df_ics_export_2[,42:43]
temp_p21 <- concat_n_ICS(pair21)


df_export_ics_n_1 <- cbind(df_ics_export_2$cohort, temp_p1, temp_p2, temp_p3, temp_p4, temp_p5, 
                     temp_p6, temp_p7, temp_p8, temp_p9, temp_p10, 
                     temp_p11, temp_p12, temp_p13, temp_p14, temp_p15, 
                     temp_p16, temp_p17, temp_p18, temp_p19, temp_p20, 
                     temp_p21)

df_export_ics_n_1 <- df_export_ics_n_1 %>% rename(index_dis = V1)

split_into_1k_new(df_export_ics_n_1, 5, 'ICS_n', '_May28_')

