
#ccu013_02


#clear workspace
rm(list=ls())

library('odbc')
library("DBI")
library('parallel')
library('survival')
library('data.table')
library('plyr')
library('dplyr')
library('tidyr')
library('dsrTest')



#in R Studio Desktop project git folder:
#setwd("D:/PhotonUser/My Files/Home Folder/ccu013_02_R/atlas")
#con <- DBI::dbConnect(odbc::odbc(), "databricks",PWD = "")


#in R Studio Server: 
setwd("/db-mnt/databricks/rstudio_collab/ccu013_02_R/atlas")
#con: R>Connection>New script, then and add token 

getwd()


source("00_atlas_functions.R")
source("05b_outputs_functions_lists.R")
source("00_atlas_MM_functions.R")
#########

#see functions above



run_main <- function(p, tablename){
  
  #'description:
  #'main analysis function to run per phecode (disease)
  #'function queries databricks and save info in df
  #'then run analyses
  #'output df_res (containing relevant estimates per disease)
  
  #@param p - string (phecode)
  #@param tablename - string (events table from databricks) 
  
  #'usage:
  #'run_main('X401_1', 'ccu013_phedatav200_unrolled_first_sex_dep')
  #'
  
  
  
  #ncol must be same length as length colnames
  df_res <- data.frame(matrix(nrow = 1, ncol=255))
  colnames(df_res) <- c("cohort", "sex_flag","n_cohort", 
                        "F0_9_male","F0_9_female", "F10_19_male", "F10_19_female", "F20_29_male", "F20_29_female", "F30_39_male", "F30_39_female", 
                        "F40_49_male", "F40_49_female", "F50_59_male", "F50_59_female", "F60_69_male", "F60_69_female", "F70_79_male", "F70_79_female", 
                        "F80_89_male", "F80_89_female", "F90p_male", "F90p_female",
                        "B0_9_male","B0_9_female", "B10_19_male", "B10_19_female", "B20_29_male", "B20_29_female", "B30_39_male", "B30_39_female", 
                        "B40_49_male", "B40_49_female", "B50_59_male", "B50_59_female", "B60_69_male", "B60_69_female", "B70_79_male", "B70_79_female", 
                        "B80_89_male", "B80_89_female", "B90p_male", "B90p_female",
                        "prev_pop", "ci_left_prev_pop", "ci_right_prev_pop", 
                        "mean_age","Q25", "Q75", "median_age",
                        "IQR_age", "age_under1month_perc", "age_under1_perc", "age_between1019_perc","age_under18_perc",
                        "mean_age_baseline", "IQR_age_baseline", "median_age_baseline",
                        "mean_length_from_first_record", "median_length_from_first_record",
                        "n_females", "prev_fem", "ci_left_prev_fem", "ci_right_prev_fem", "mean_age_f",
                        "n_males", "prev_male", "ci_left_prev_male", "ci_right_prev_male","mean_age_m",
                        "prev_sex_ratio", "ci_left_prev_sex_ratio", "ci_right_prev_sex_ratio", 
                        "n_5_asi_ab", "prev_5_asi_ab","ci_left_prev_5_asi_ab","ci_right_prev_5_asi_ab",
                        "n_5_bla_bb", "prev_5_bla_bb","ci_left_prev_5_bla_bb","ci_right_prev_5_bla_bb",
                        "n_5_mix",  "prev_5_mix","ci_left_prev_5_mix","ci_right_prev_5_mix",
                        "n_5_oth_eth", "prev_5_oth_eth", "ci_left_prev_5_oth_eth", "ci_right_prev_5_oth_eth",
                        "n_5_unkn",  "prev_5_unkn","ci_left_prev_5_unkn", "ci_right_prev_5_unkn", 
                        "n_5_whi",  "prev_5_whi","ci_left_prev_5_whi","ci_right_prev_5_whi",
                        "prev_ethn_ratio_5_asi_ab_whi", "ci_left_prev_ethn_ratio_5_asi_ab_whi","ci_right_prev_ethn_ratio_5_asi_ab_whi", 
                        "prev_ethn_ratio_5_bla_bb_whi", "ci_left_prev_ethn_ratio_5_bla_bb_whi","ci_right_prev_ethn_ratio_5_bla_bb_whi",
                        "n_11_bangl", "prev_11_bangl","ci_left_prev_11_bangl","ci_right_prev_11_bangl",
                        "n_11_bla_af", "prev_11_bla_af","ci_left_prev_11_bla_af","ci_right_prev_11_bla_af",
                        "n_11_bla_ca", "prev_11_bla_ca","ci_left_prev_11_bla_ca","ci_right_prev_11_bla_ca",
                        "n_11_chi",  "prev_11_chi","ci_left_prev_11_chi","ci_right_prev_11_chi",
                        "n_11_ind",  "prev_11_ind","ci_left_prev_11_ind","ci_right_prev_11_ind",
                        "n_11_mix",  "prev_11_mix","ci_left_prev_11_mix","ci_right_prev_11_mix",
                        "n_11_pak",  "prev_11_pak","ci_left_prev_11_pak","ci_right_prev_11_pak",
                        "n_11_oth_asi", "prev_11_oth_asi", "ci_left_prev_11_oth_asi", "ci_right_prev_11_oth_asi",
                        "n_11_oth_bl", "prev_11_oth_bl", "ci_left_prev_11_oth_bl", "ci_right_prev_11_oth_bl",
                        "n_11_oth_eth", "prev_11_oth_eth", "ci_left_prev_11_oth_eth", "ci_right_prev_11_oth_eth",
                        "n_11_unkn",  "prev_11_unkn","ci_left_prev_11_unkn", "ci_right_prev_11_unkn", 
                        "n_11_whi",  "prev_11_whi","ci_left_prev_11_whi","ci_right_prev_11_whi",
                        "prev_ethn_ratio_11_bangl_whi", "ci_left_prev_ethn_ratio_11_bangl_whi","ci_right_prev_ethn_ratio_11_bangl_whi", 
                        "prev_ethn_ratio_11_chi_whi", "ci_left_prev_ethn_ratio_11_chi_whi","ci_right_prev_ethn_ratio_11_chi_whi",
                        "prev_ethn_ratio_11_ind_whi", "ci_left_prev_ethn_ratio_11_ind_whi","ci_right_prev_ethn_ratio_11_ind_whi",
                        "prev_ethn_ratio_11_pak_whi", "ci_left_prev_ethn_ratio_11_pak_whi","ci_right_prev_ethn_ratio_11_pak_whi",
                        "prev_ethn_ratio_11_bla_af_whi", "ci_left_prev_ethn_ratio_11_bla_af_whi","ci_right_prev_ethn_ratio_11_bla_af_whi",
                        "prev_ethn_ratio_11_bla_ca_whi", "ci_left_prev_ethn_ratio_11_bla_ca_whi","ci_right_prev_ethn_ratio_11_bla_ca_whi",
                        "n_imd1", "prev_imd1", "ci_left_prev_imd1", "ci_right_prev_imd1", 
                        "n_imd2", "prev_imd2", "ci_left_prev_imd2", "ci_right_prev_imd2",
                        "n_imd3","prev_imd3","ci_left_prev_imd3", "ci_right_prev_imd3",
                        "n_imd4","prev_imd4","ci_left_prev_imd4", "ci_right_prev_imd4",
                        "n_imd5","prev_imd5","ci_left_prev_imd5","ci_right_prev_imd5",
                        "prev_imd_ratio", "ci_left_prev_imd_ratio", "ci_right_prev_imd_ratio",
                        "covid_cases", "covid_cases_male", "covid_cases_fem","controls", "covid_cases_percent",
                        "nevents", "beta","se", "pval", "HR","ci_left","ci_right", "median_survivaltime",
                        "HR_n_covid_controls_noout", "HR_n_covid_controls_out",
                        "HR_n_covid_case_noout", "HR_n_covid_case_out",
                        "median_survtime_covid_control_noout", "median_survtime_covid_control_out",
                        "median_survtime_covid_case_noout", "median_survtime_covid_case_out",
                        "median_survtime_covid_control_noout_Q25", "median_survtime_covid_control_out_Q25", 
                        "median_survtime_covid_case_noout_Q25", "median_survtime_covid_case_out_Q25", 
                        "median_survtime_covid_control_noout_Q75", "median_survtime_covid_control_out_Q75", 
                        "median_survtime_covid_case_noout_Q75", "median_survtime_covid_case_out_Q75",
                        "nevents_unadj_mod", "beta_unadj_mod","se_unadj_mod", 
                        "HR_unadj_mod_n_covid_controls_noout", "HR_unadj_mod_n_covid_controls_out", 
                        "HR_unadj_mod_n_covid_case_noout", "HR_unadj_mod_n_covid_case_out",
                        "median_survtime_covid_control_noout_unadj", "median_survtime_covid_control_out_unadj",
                        "median_survtime_covid_case_noout_unadj", "median_survtime_covid_case_out_unadj",
                        "median_survtime_covid_control_noout_unadj_Q25", "median_survtime_covid_control_out_unadj_Q25", 
                        "median_survtime_covid_case_noout_unadj_Q25", "median_survtime_covid_case_out_unadj_Q25", 
                        "median_survtime_covid_control_noout_unadj_Q75", "median_survtime_covid_control_out_unadj_Q75", 
                        "median_survtime_covid_case_noout_unadj_Q75", "median_survtime_covid_case_out_unadj_Q75",
                        "pval_unadj_mod", "HR_unadj_mod", "ci_left_unadj_mod","ci_right_unadj_mod", "median_survivaltime_unadj_mod",
                        "SMR_prev", "ci_left_SMR_prev", "ci_right_SMR_prev", 
                        "obs_deaths_1y",
                        "SMR_prev_hosp", "ci_left_SMR_prev_hosp", "ci_right_SMR_prev_hosp", 
                        "abs_mort_1y", "ci_left_abs_mort_1y", "ci_right_abs_mort_1y")
  
  #45 extra columns related to stand prevalence added below (all end *_stand)
  #output %>% select(contains("stand"))
  
  
  #colnames(df_res) <- 'phecode'
  df_res$cohort <- p
  print(p)
  
  # create df_res for results
  #df_res <- data.frame(matrix(nrow = 1))
  #colnames(df_res) <- 'phecode'
  #df_res$phecode <- p
  
  
  if (main_phe){
    
    # Get data
  print(paste0("Extracting data for phecode ", p))
    

  sql = paste0("SELECT ph.person_id_deid, 
    MIN(ph.date) as first_date, 
    c.ethn_5,
    c.ethn_11,
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
    datediff(MIN(ph.date), date(dob))/365.2425 as age
    FROM dsa_391419_j3w9t_collab.", tablename, " ph
    INNER JOIN  dsa_391419_j3w9t_collab.ccu013_02_dp_skinny_patient_23_01_2020_phe_cohort_joined_1ymort_eth c
    ON ph.person_id_deid = c.NHS_NUMBER_DEID 
    WHERE ph.phecode ='", p,"'
    AND c.age_23_01_2020 >= 0 and c.age_23_01_2020 <= 100
    AND c.sex IN (1,2) 
    AND c.IMD_quintile not like ('Unknown%')
    GROUP BY ph.person_id_deid, 
               c.ethn_5,
               c.ethn_11,
               c.IMD_quintile, 
               c.sex, 
               c.covid_case, 
               c.death_date, 
               c.start_date, 
               c.end_date, 
               c.age_start_date, 
               c.dob,
               c.age_band_23_01_2020,
               c.death_flag
               HAVING MIN(ph.date) <= to_date('2020-01-23', 'yyyy-MM-dd') 
               AND MIN(ph.date) >= date(c.dob)")

  
  #find sex in phe_dict for this phecode 
  
  sql_sex = paste0("SELECT sex as sex_flag
            FROM dsa_391419_j3w9t_collab.ccu013_lkp_phev200_phedict d 
                                    WHERE d.phecode ='", p,"'")
  
  df_sex_flag <- DBI::dbGetQuery(con, sql_sex)
  
  df_res$sex_flag <- df_sex_flag[1,]
  
  print('done')

  } else if (main_cov_cohorts){

  sql = paste0("SELECT d.person_id_deid, 
      MIN(d.earliest_most_severe) as first_date,
      c.ethn_5,
      c.ethn_11,
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
      datediff(MIN(d.earliest_most_severe), date(dob))/365.2425 as age
      FROM dsa_391419_j3w9t_collab.", tablename, " d
      INNER JOIN dsa_391419_j3w9t_collab.ccu013_02_dp_skinny_patient_23_01_2020_phe_cohort_joined_1ymort_eth c
      ON d.person_id_deid = c.NHS_NUMBER_DEID 
      WHERE d.covid_cohort ='", p,"'
      AND c.age_23_01_2020 >= 0 and c.age_23_01_2020 <= 100
      AND c.sex IN (1,2) 
      AND c.IMD_quintile not like ('Unknown%')
      GROUP BY d.person_id_deid,
                d.earliest_most_severe,
                c.ethn_5,
                c.ethn_11,
                c.IMD_quintile, 
                c.sex, 
                c.covid_case, 
                c.death_date, 
                c.start_date, 
                c.end_date, 
                c.age_start_date, 
                c.dob,
                c.age_band_23_01_2020,
                c.death_flag
                HAVING MIN(d.earliest_most_severe) <= to_date('2021-01-23', 'yyyy-MM-dd') 
                AND MIN(d.earliest_most_severe) >= date(c.dob)")

  sex_flag <- 0 
  
  df_res$sex_flag <- sex_flag 

  }
  
  
  
  df <- DBI::dbGetQuery(con, sql)
  
  
  #subset events if sex-dependent 
  if(df_res$sex_flag =='1'){
    
    #print('male phecode')
    
    #subset from events using sex in skinny
    df <- subset(df, (sex == '1'))
    
  }else if(df_res$sex_flag =='2'){
    
    #print('female phecode')
    
    #subset from events using sex in skinny
    df <- subset(df, (sex == '2'))
  }
  
  # There are no cases, skip, return empty df_res
  if (nrow(df) < 10){
    df_res$n_cohort <- length(unique(df[,'person_id_deid']))
    print(paste0("Skip due to lack of cases ", p))
    return(df_res)
  } else {
    
    
    #counts at first event: 
    #this adds counts per band to df_res 
    df_res <- fun_n_cohort_age_sex_bands_first_date(df, 
                                                    counts, 
                                                    df_res)
    
    #fill with values
    df_res$controls = nrow(subset(df,covid_case == 0))
    df_res$covid_cases = nrow(subset(df,covid_case == 1))
    
    df_res$covid_cases_male = nrow(subset(df, (covid_case == 1 & sex == 1)))
    df_res$covid_cases_fem = nrow(subset(df, (covid_case == 1 & sex == 2)))
      
    df_res$n_cohort=length(unique(df[,'person_id_deid']))
    df_res$covid_cases_percent = (df_res$covid_cases/df_res$n_cohort) * 100

    prev_scale <- 10^6
    

    ##THIS FOR ALL in prev_pop regardless of sex_flag
    deno_prev_pop <- den_pop


    #print(deno_prev_pop)
    #print(den_white)
    #print(den_imd5)
    #print(esp_10y)

    #calculate general prevalence (using sex_flag and denos)
    df_res <- fun_prev_pop(df_res, 
                              deno_prev_pop, 
                              prev_scale)
    
    
    # #calculate standprevalence
    df_res <- fun_stand_prev_pop(df,
                                    counts,
                                    esp_10y,
                                    df_res,
                                    prev_scale)
    
    #other estimates
    df_res$n_males <- nrow(subset(df, sex == 1))
    df_res$n_females <-  nrow(subset(df, sex == 2))
    df_res$mean_age = mean(df$age)
    #df_res$min_age <- min(df$age)
    
    df_res$mean_age_m = mean(df$age[df$sex==1])
    df_res$mean_age_f = mean(df$age[df$sex==2])
    
    df_res$median_age = median(df$age)
    df_res$IQR_age = IQR(df$age)
    df_res$age_under18_perc = (length(which(df$age <= 18))/df_res$n_cohort)*100
    df_res$age_between1019_perc = (length(which(df$age <= 19 & df$age >= 10))/df_res$n_cohort)*100
    df_res$age_under1_perc = (length(which(df$age <=  1))/df_res$n_cohort)*100
    #df_res$age_under2months_perc = (length(which(df$age <=  0.167))/df_res$n_cohort)*100
    df_res$age_under1month_perc = (length(which(df$age <=  0.084))/df_res$n_cohort)*100
    
    #mean age at study start (baseline)
    df$age_start_date <- as.numeric(df$age_start_date)
    df_res$mean_age_baseline <- mean(df$age_start_date)
    df_res$IQR_age_baseline <-IQR(df$age_start_date) 
    #median age at study start (baseline)
    df_res$median_age_baseline<- median(df$age_start_date)
      
    #
    df_res$Q25 <- unname(quantile(df$age))[2]
    df_res$Q75 <- unname(quantile(df$age))[4]
    
    #age_diff
    df_res$mean_length_from_first_record <- mean(df$age_start_date - df$age)
    df_res$median_length_from_first_record <- median(df$age_start_date - df$age)
    
    #prev in females or males 
    df_res$prev_fem <- (df_res$n_females/den_female) * prev_scale
    df_res$prev_male <- (df_res$n_males/den_male) * prev_scale
    
    #df_res$den_fem <- den_female
    #df_res$den_male <- den_male
    
    #CI
    ci_prev_fem <- ci_prop(df_res$prev_fem, prev_scale, den_female)
    df_res$ci_left_prev_fem <- ci_prev_fem[1]
    df_res$ci_right_prev_fem <- ci_prev_fem[2]
    
    ci_prev_male <- ci_prop(df_res$prev_male, prev_scale, den_male)
    df_res$ci_left_prev_male <- ci_prev_male[1]
    df_res$ci_right_prev_male <- ci_prev_male[2]
    
    
    #prev sex ratio
    df_res$prev_sex_ratio <- (df_res$prev_fem/df_res$prev_male)
    
    ci_prev_sex_ratio <- ci_prev_ratio(df_res$prev_sex_ratio, 
                                          df_res$n_females, den_female, 
                                          df_res$n_males, den_male)
    
    df_res$ci_left_prev_sex_ratio <- ci_prev_sex_ratio[1]
    df_res$ci_right_prev_sex_ratio <- ci_prev_sex_ratio[2]
    
    
    #stand_prev_sex_ratio
    df_res <- fun_stand_prev_pop_strata(df,
                                           "sex",
                                           strata = c('1','2'),
                                           counts_sex,
                                           esp10y,
                                           df_res,
                                           prev_scale)
    
    ######imd
    #IMD: 
    #get counts in phecode pop. 
    #1 most deprived
    df_res$n_imd1 <- nrow(subset(df, IMD_quintile == '1'))
    df_res$n_imd2 <- nrow(subset(df, IMD_quintile == '2'))
    df_res$n_imd3 <- nrow(subset(df, IMD_quintile == '3'))
    df_res$n_imd4 <- nrow(subset(df, IMD_quintile == '4'))
    df_res$n_imd5 <- nrow(subset(df, IMD_quintile == '5'))
    
    
    #call function to calculate prev per imd quintile, and the prev ratio
    df_res <- fun_prev_imd(df_res, 
                              den_imd1 = den_imd1, 
                              den_imd2 = den_imd2, 
                              den_imd3 = den_imd3, 
                              den_imd4 = den_imd4, 
                              den_imd5 = den_imd5, 
                              prev_scale)
    
    
    # #stand_prev_per_strata: IMD
    df_res <- fun_stand_prev_pop_strata(df,
                                           "IMD_quintile",
                                           strata = c('1','2','3','4','5'),
                                           counts_IMD_quintile,
                                           esp10y,
                                           df_res,
                                           prev_scale)

    
    ######ethnicity
    #count in phecode pop
    # df_res$n_asian <- nrow(subset(df, ethnicity == 'Asian'))
    # df_res$n_black <- nrow(subset(df, ethnicity == 'Black'))
    # df_res$n_white <- nrow(subset(df, ethnicity == 'White'))
    # df_res$n_other <- nrow(subset(df, ethnicity == 'Other'))
    # df_res$n_unknown <- nrow(subset(df, ethnicity == 'Unknown'))
    
    #5
    df_res$n_5_asi_ab <- nrow(subset(df, ethn_5 == 'asi_ab'))
    df_res$n_5_bla_bb <- nrow(subset(df, ethn_5 == 'bla_bb'))
    df_res$n_5_mix <- nrow(subset(df, ethn_5 == 'mix'))
    df_res$n_5_oth_eth <- nrow(subset(df, ethn_5 == 'oth_eth'))
    df_res$n_5_unkn <- nrow(subset(df, ethn_5 == 'unkn'))
    df_res$n_5_whi <- nrow(subset(df, ethn_5 == 'whi'))
    
    #11
    df_res$n_11_bangl <- nrow(subset(df, ethn_11 == 'bangl'))
    df_res$n_11_bla_af <- nrow(subset(df, ethn_11 == 'bla_af'))
    df_res$n_11_bla_ca <- nrow(subset(df, ethn_11 == 'bla_ca'))
    df_res$n_11_chi <- nrow(subset(df, ethn_11 == 'chi'))
    df_res$n_11_ind <- nrow(subset(df, ethn_11 == 'ind'))
    df_res$n_11_mix <- nrow(subset(df, ethn_11 == 'mix'))
    df_res$n_11_pak <- nrow(subset(df, ethn_11 == 'pak'))
    df_res$n_11_oth_asi <- nrow(subset(df, ethn_11 == 'oth_asi'))
    df_res$n_11_oth_bl <- nrow(subset(df, ethn_11 == 'oth_bl'))
    df_res$n_11_oth_eth <- nrow(subset(df, ethn_11 == 'oth_eth'))
    df_res$n_11_unkn <- nrow(subset(df, ethn_11 == 'unkn'))
    df_res$n_11_whi <- nrow(subset(df, ethn_11 == 'whi'))
    
    
    #function to calculate prev etc
    
    #gen groups
    # df_res <- fun_prev_ethn(df_res, 
    #                  den_asian=den_asian, 
    #                  den_black = den_black,
    #                  den_other=den_other, 
    #                  den_unknown = den_unknown, 
    #                  den_white = den_white, 
    #                  prev_scale)
    # 
    
    # #unadjust prev:
    # df_res <- fun_prev_eth(df_res, c(den_asian, den_black, den_white, den_other, den_unknown), 'ethnicity', prev_scale)
    # 
    # 
    # 
    # #stand_prev:
    # df_res <- fun_stand_prev_pop_strata(df,
    #                                        "ethnicity",
    #                                        strata = c('White','Black','Asian'), #'Unknown','Other'
    #                                        counts_ethnicity,
    #                                        esp10y,
    #                                        df_res,
    #                                        prev_scale)

    
    
    #######
    
    #5- unadjusted: 
    df_res <- fun_prev_eth(df_res, c(den_5_asi_ab, den_5_bla_bb, den_5_mix, den_5_oth_eth, den_5_unkn, den_5_whi), 'ethn_5', prev_scale)


    #5 - stand:
    df_res <- fun_stand_prev_pop_strata(df,
                                        "ethn_5",
                                        strata = c('whi','bla_bb', 'asi_ab'), 
                                        counts_ethn_5,
                                        esp10y,
                                        df_res,
                                        prev_scale)
    #####################
    #11 unadjusted:

    df_res <- fun_prev_eth(df_res,
                          c(den_11_bangl, den_11_bla_af, den_11_bla_ca, 
                            den_11_chi, den_11_ind,
                           den_11_mix, den_11_pak, den_11_oth_asi, 
                           den_11_oth_bla,
                           den_11_oth_eth, den_11_unkn, den_11_whi),
                           'ethn_11',
                           prev_scale)
    
    #11 - stand:
    df_res <- fun_stand_prev_pop_strata(df,
                                        "ethn_11",
                                        strata = c('whi','bangl', 'chi', 'ind', 'pak', 'bla_af', 'bla_ca'), 
                                        counts_ethn_11,
                                        esp10y,
                                        df_res,
                                        prev_scale)
    
    
    #calculate SMR 
    #use sex_flag to update gen_pop (sex dependent)
    #sex_flag <- df_res$sex_flag
    SMRoutput<- get_SMR(df_genpop, df, df_res$sex_flag)
    
    df_res$SMR_prev <- SMRoutput[1]
    df_res$ci_left_SMR_prev <- SMRoutput[2]
    df_res$ci_right_SMR_prev <- SMRoutput[3]
    df_res$obs_deaths_1y <- SMRoutput[4]
    
    
    #calculate SMR_prev_hosp (relative to N and deaths of individuals in hospital anytime)
    SMRoutput_hosp<- get_SMR(df_genpop_hosp, df, df_res$sex_flag)
    
    df_res$SMR_prev_hosp <- SMRoutput_hosp[1]
    df_res$ci_left_SMR_prev_hosp <- SMRoutput_hosp[2]
    df_res$ci_right_SMR_prev_hosp <- SMRoutput_hosp[3]
    #df_res$obs_deaths_1y_hosp <- SMRoutput_hosp[4]
    
    
    #1 year absolute mortality
    #n deaths/ N patients
    #df_res$n_deaths_1y <- sum(df$death_flag)
    df_res$abs_mort_1y <- (df_res$obs_deaths_1y/df_res$n_cohort) * 100
    #ci
    ci_prev_abs_mort_1y <- ci_prop(df_res$abs_mort_1y, 100, df_res$n_cohort)
    df_res$ci_left_abs_mort_1y <- ci_prev_abs_mort_1y[1]
    df_res$ci_right_abs_mort_1y <- ci_prev_abs_mort_1y[2]
    

    
    print(paste0('abs mortality:' , df_res$abs_mort_1y))
    
    print(paste0('SMR:' , df_res$SMR_prev))
    
    print(paste0('SMR_hosp:' , df_res$SMR_prev_hosp))
  
    if (df_res$cohort %in% cov_cohorts){
      
      return(df_res)

    } else {

      print('doing HR')
      
      #prepare for analysis
      df$covid_case <- as.factor(df$covid_case)
      df$sex <- as.factor(df$sex)
      df$ethn_5 <- as.factor(df$ethn_5)
      df$IMD <- as.factor(df$IMD_quintile)
      
      
      #keep here for now - can move to table in databricks
      df$survivaltime <- as.Date(df$end_date)-as.Date(df$start_date)
      #print(df$survivaltime)

      #anycause mortality - if death date before before end_date of any cause
      df$outcome <- ifelse(df$death_date <= df$end_date, 1, 0)
      # not died: 0
      df$outcome[is.na(df$outcome)]<- 0
      
      print(paste0('n unique outcomes: ', length(unique(df$outcome))))
      print(paste0('unique outcomes: ', unique(df$outcome)))
      
      #print(paste0("Running model for ", p))
      
      
      #HR unadj: 
      if (sum(df$outcome) > 10){
        
          m_unadj <- coxph(formula =  Surv(survivaltime, outcome) ~  covid_case, data = df)
          
          #print(summary(m_unadj))
          
          df_res$beta_unadj_mod = coef(summary(m_unadj))[1,1]
          df_res$se_unadj_mod = coef(summary(m_unadj))[1,3]
          df_res$pval_unadj_mod = coef(summary(m_unadj))[1,5]
          df_res$HR_unadj_mod = exp(df_res$beta_unadj_mod)
          df_res$ci_left_unadj_mod = exp(confint.default(m_unadj)[1,1])
          df_res$ci_right_unadj_mod = exp(confint.default(m_unadj)[1,2])
          df_res$nevents_unadj_mod = m_unadj$nevent

          df_res$median_survivaltime_unadj_mod <- median(as.numeric(df$survivaltime)/365.2425)
          #print(df_res$median_survivaltime_unadj_mod)
          
      
          #N people covid cases or control per model 
          df_temp_n_outcome  <- df %>% group_by(covid_case, outcome) %>% count()
          df_res$HR_unadj_mod_n_covid_controls_noout <- df_temp_n_outcome$n[1]
          df_res$HR_unadj_mod_n_covid_controls_out <- df_temp_n_outcome$n[2]
          df_res$HR_unadj_mod_n_covid_case_noout <- df_temp_n_outcome$n[3]
          df_res$HR_unadj_mod_n_covid_case_out <- df_temp_n_outcome$n[4]
          
          
          
          #surv time by exposure status
          df_temp <- df %>% group_by(covid_case, outcome) %>% summarise(median = median(survivaltime, na.rm = TRUE), .groups = 'drop_last')
          df_res$median_survtime_covid_control_noout_unadj <- as.numeric(df_temp$median[1])/365.2425 
          df_res$median_survtime_covid_control_out_unadj <- as.numeric(df_temp$median[2])/365.2425
          df_res$median_survtime_covid_case_noout_unadj <- as.numeric(df_temp$median[3])/365.2425 
          df_res$median_survtime_covid_case_out_unadj <- as.numeric(df_temp$median[4])/365.2425
          
          
          df_temp <- df %>% group_by(covid_case, outcome) %>% summarise(Q25 = unname(quantile(survivaltime))[2], .groups = 'drop_last')
          df_res$median_survtime_covid_control_noout_unadj_Q25 <- as.numeric(df_temp$Q25[1])/365.2425 
          df_res$median_survtime_covid_control_out_unadj_Q25 <- as.numeric(df_temp$Q25[2])/365.2425
          df_res$median_survtime_covid_case_noout_unadj_Q25 <- as.numeric(df_temp$Q25[3])/365.2425 
          df_res$median_survtime_covid_case_out_unadj_Q25 <- as.numeric(df_temp$Q25[4])/365.2425
          
          
          df_temp <- df %>% group_by(covid_case, outcome) %>% summarise(Q75 = unname(quantile(survivaltime))[4], .groups = 'drop_last')
          df_res$median_survtime_covid_control_noout_unadj_Q75 <- as.numeric(df_temp$Q75[1])/365.2425 
          df_res$median_survtime_covid_control_out_unadj_Q75 <- as.numeric(df_temp$Q75[2])/365.2425
          df_res$median_survtime_covid_case_noout_unadj_Q75 <- as.numeric(df_temp$Q75[3])/365.2425 
          df_res$median_survtime_covid_case_out_unadj_Q75 <- as.numeric(df_temp$Q75[4])/365.2425
            
      }
      
      
      
      
      #create flag to see if data in any level of ethnicity or imd - for adjusted model 
      v_eth <- c(df_res$n_5_asi_ab, df_res$n_5_bla_bb, df_res$n_5_whi, df_res$n_5_oth_eth, df_res$n_5_unkn, df_res$n_5_mix)
      #length(v_eth[v_eth >=1])
      
      v_imd <- c(df_res$n_imd1, df_res$n_imd2, df_res$n_imd3, df_res$n_imd4, df_res$n_imd5)
      #if there is 1 case in at least one level in the variable
      #length(v_imd[v_imd >= 1])

      #keep same n events as unadjusted for consistency but restrict to min numb events after
      if (sum(df$outcome) > 10) {
      # if (sum(df$outcome) > 10 & 
      #     length(v_imd[v_imd >= 1]) >= 1 & 
      #     length(v_eth[v_eth >=1]) >= 1 ){
        
        #then check if no males or females (sex_dependent phenotypes)
        if (df_res$n_males == 0 | df_res$n_females == 0){
          
          #m <- coxph(formula =  Surv(survivaltime, outcome) ~  covid_case + age_start_date + ethn_5 + IMD, data = df)
          m <- coxph(formula =  Surv(survivaltime, outcome) ~  covid_case + age_start_date, data = df)
          
        } else {
          
          #m <- coxph(formula =  Surv(survivaltime, outcome) ~  covid_case + sex + age_start_date + ethn_5 + IMD, data = df)
          m <- coxph(formula =  Surv(survivaltime, outcome) ~  covid_case + sex + age_start_date, data = df)
          
        }
        
      } else {
        #not enough events
        df_res$nevents <-sum(df$outcome)
        return(df_res) 
      }
      
      
      if (exists("m")){
        #print(summary(m))
        if (dim(coef(summary(m)))[1] >= 2){
          df_res$beta = coef(summary(m))[1,1]
          df_res$se = coef(summary(m))[1,3]
          df_res$pval = coef(summary(m))[1,5]
          df_res$HR = exp(df_res$beta)
          df_res$ci_left = exp(confint.default(m)[1,1])
          df_res$ci_right = exp(confint.default(m)[1,2])
          df_res$nevents = m$nevent
          
          df_res$median_survivaltime <- median(as.numeric(df$survivaltime)/365.2425)
          
          #N people covid cases or control per model 
          df_temp_n_outcome  <- df %>% group_by(covid_case, outcome) %>% count()
          df_res$HR_n_covid_controls_noout <- df_temp_n_outcome$n[1]
          df_res$HR_n_covid_controls_out <- df_temp_n_outcome$n[2]
          df_res$HR_n_covid_case_noout <- df_temp_n_outcome$n[3]
          df_res$HR_n_covid_case_out <- df_temp_n_outcome$n[4]
          
          
          
          #surv time by exposure status
          df_temp <- df %>% group_by(covid_case, outcome) %>% summarise(median = median(survivaltime, na.rm = TRUE), .groups = 'drop_last')
          df_res$median_survtime_covid_control_noout <- as.numeric(df_temp$median[1])/365.2425 
          df_res$median_survtime_covid_control_out <- as.numeric(df_temp$median[2])/365.2425
          df_res$median_survtime_covid_case_noout <- as.numeric(df_temp$median[3])/365.2425 
          df_res$median_survtime_covid_case_out <- as.numeric(df_temp$median[4])/365.2425
          
          
          df_temp <- df %>% group_by(covid_case, outcome) %>% summarise(Q25 = unname(quantile(survivaltime))[2], .groups = 'drop_last')
          df_res$median_survtime_covid_control_noout_Q25 <- as.numeric(df_temp$Q25[1])/365.2425 
          df_res$median_survtime_covid_control_out_Q25 <- as.numeric(df_temp$Q25[2])/365.2425
          df_res$median_survtime_covid_case_noout_Q25 <- as.numeric(df_temp$Q25[3])/365.2425 
          df_res$median_survtime_covid_case_out_Q25 <- as.numeric(df_temp$Q25[4])/365.2425
          
          
          df_temp<- df %>% group_by(covid_case, outcome) %>% summarise(Q75 = unname(quantile(survivaltime))[4], .groups = 'drop_last')
          df_res$median_survtime_covid_control_noout_Q75 <- as.numeric(df_temp$Q75[1])/365.2425 
          df_res$median_survtime_covid_control_out_Q75 <- as.numeric(df_temp$Q75[2])/365.2425
          df_res$median_survtime_covid_case_noout_Q75 <- as.numeric(df_temp$Q75[3])/365.2425 
          df_res$median_survtime_covid_case_out_Q75 <- as.numeric(df_temp$Q75[4])/365.2425
          
          
          #print(df_res$median_survivaltime)
          return(df_res)
        } 
      }

    } 
    
    
  }
  
  
}


## get glob and call function #################


#####################################################
#get strata spec denos - for crude strat prev
#####################################################

#prep: get strata specific denominators from skinny joined table 
#sex
den_sex_all <- get_denominator_from_table('ccu013_02_dp_skinny_patient_23_01_2020_phe_cohort_joined_1ymort_eth', 'sex')
den_male <- filter(den_sex_all, sex==1)$n_indiv
den_female <- filter(den_sex_all, sex==2)$n_indiv

den_pop <- den_male + den_female

# #ethnicity: 
# den_ethn_all <- get_denominator_from_table('ccu013_02_dp_skinny_patient_23_01_2020_phe_cohort_joined_1ymort_eth', 'ethnicity')
# 
# den_asian <- filter(den_ethn_all, ethnicity=='Asian')$n_indiv
# den_black <- filter(den_ethn_all, ethnicity=='Black')$n_indiv
# den_other <- filter(den_ethn_all, ethnicity=='Other')$n_indiv
# den_unknown <- filter(den_ethn_all, ethnicity=='Unknown')$n_indiv
# den_white <- filter(den_ethn_all, ethnicity=='White')$n_indiv

#ethnicity new - general: 
den_ethn_all_5 <- get_denominator_from_table('ccu013_02_dp_skinny_patient_23_01_2020_phe_cohort_joined_1ymort_eth', 'ethn_5')

den_5_asi_ab <- filter(den_ethn_all_5, ethn_5 =='asi_ab')$n_indiv
den_5_bla_bb <- filter(den_ethn_all_5, ethn_5 =='bla_bb')$n_indiv
den_5_mix <- filter(den_ethn_all_5, ethn_5 =='mix')$n_indiv
den_5_oth_eth <- filter(den_ethn_all_5, ethn_5 =='oth_eth')$n_indiv
den_5_unkn <- filter(den_ethn_all_5, ethn_5 =='unkn')$n_indiv
den_5_whi <- filter(den_ethn_all_5, ethn_5 =='whi')$n_indiv

#ethnicity - 11 granular: 
den_ethn_all_11 <- get_denominator_from_table('ccu013_02_dp_skinny_patient_23_01_2020_phe_cohort_joined_1ymort_eth', 'ethn_11')

den_11_bangl <- filter(den_ethn_all_11, ethn_11=='bangl')$n_indiv
den_11_bla_af <- filter(den_ethn_all_11, ethn_11=='bla_af')$n_indiv
den_11_bla_ca <- filter(den_ethn_all_11, ethn_11=='bla_ca')$n_indiv
den_11_chi <- filter(den_ethn_all_11, ethn_11=='chi')$n_indiv
den_11_ind <- filter(den_ethn_all_11, ethn_11=='ind')$n_indiv
den_11_mix <- filter(den_ethn_all_11, ethn_11=='mix')$n_indiv
den_11_pak <- filter(den_ethn_all_11, ethn_11=='pak')$n_indiv
den_11_oth_asi <- filter(den_ethn_all_11, ethn_11=='oth_asi')$n_indiv
den_11_oth_bl <- filter(den_ethn_all_11, ethn_11=='oth_bl')$n_indiv
den_11_oth_eth <- filter(den_ethn_all_11, ethn_11=='oth_eth')$n_indiv
den_11_unkn <- filter(den_ethn_all_11, ethn_11=='unkn')$n_indiv
den_11_whi <- filter(den_ethn_all_11, ethn_11=='whi')$n_indiv


#imd: 
den_imd_all <- get_denominator_from_table('ccu013_02_dp_skinny_patient_23_01_2020_phe_cohort_joined_1ymort_eth', 'IMD_quintile')

den_imd1 <- filter(den_imd_all, IMD_quintile=='1')$n_indiv
den_imd2 <- filter(den_imd_all, IMD_quintile=='2')$n_indiv
den_imd3 <- filter(den_imd_all, IMD_quintile=='3')$n_indiv
den_imd4 <- filter(den_imd_all, IMD_quintile=='4')$n_indiv
den_imd5 <- filter(den_imd_all, IMD_quintile=='5')$n_indiv



print(paste0("Strata specific and general denominators identified"))


##########################################################################
#get table with N counts and N deaths by strata - for SMR
##########################################################################

#this needs to be done only once. 
#Then it is combined with ndeaths in each phecode in the function get_SMR 
counts <- dbGetQuery(con, "SELECT * FROM dars_nic_391419_j3w9t_collab.ccu013_02_n_count_genpop_stratas")
deaths <- dbGetQuery(con, "SELECT * FROM dars_nic_391419_j3w9t_collab.ccu013_02_death_count_genpop_stratas")

counts <- dplyr::arrange(counts, age_band_23_01_2020, sex)
deaths <- dplyr::arrange(deaths, age_band_23_01_2020, sex)

df_genpop <- merge(counts, deaths, by = c('age_band_23_01_2020', 'sex'))
df_genpop <- dplyr::rename(df_genpop,n_strata_genpop=n_indiv.x, n_deaths_genpop=n_indiv.y)

#this is used in get_SMR 
#with the ndeaths in the phecode (from query)
df_genpop$rate_genpop <- (df_genpop$n_deaths/df_genpop$n_strata) 

print(paste0("Counts in general population for SMR"))

######
#same for counts restricted to population who have been in hospital  
#Then it is combined with ndeaths in each phecode in the function get_SMR 
counts_hosp <- dbGetQuery(con, "SELECT * FROM dars_nic_391419_j3w9t_collab.ccu013_02_n_count_genpop_stratas_hosp")
deaths_hosp <- dbGetQuery(con, "SELECT * FROM dars_nic_391419_j3w9t_collab.ccu013_02_death_count_genpop_stratas_hosp")

counts_hosp <- dplyr::arrange(counts_hosp, age_band_23_01_2020, sex)
deaths_hosp <- dplyr::arrange(deaths_hosp, age_band_23_01_2020, sex)

df_genpop_hosp <- merge(counts_hosp, deaths_hosp, by = c('age_band_23_01_2020', 'sex'))
df_genpop_hosp <- dplyr::rename(df_genpop_hosp,n_strata_genpop=n_indiv.x, n_deaths_genpop=n_indiv.y)

#this is used in get_SMR 
#with the ndeaths in the phecode (from query)
df_genpop_hosp$rate_genpop <- (df_genpop_hosp$n_deaths/df_genpop_hosp$n_strata) 

print(paste0("Counts in general population for SMR - hosp only"))


#####################################################
#read standpop - for stand prev (gen and stratified)
#####################################################

esp_10y <- dbGetQuery(con, "SELECT * FROM dars_nic_391419_j3w9t_collab.ccu013_lkp_esp_10y")
esp_10y <- arrange(esp_10y, AgeGroup, Sex)
esp_10y$ESP <- as.numeric(esp_10y$ESP)
#esp_10y$ESP_w <- (esp_10y$ESP/sum(esp_10y$ESP)) * 100
esp_10y$ESP_w <- (esp_10y$ESP/sum(esp_10y$ESP))

##############################################################
# age and sex specif denom for standard prev 
##############################################################

#imd 
counts_IMD_quintile <- data.frame()
for (i in 1:5){
  #print (i)
  df_temp <- get_age_sex_bands_deno_from_table('ccu013_02_dp_skinny_patient_23_01_2020_phe_cohort_joined_1ymort_eth', 'IMD_quintile', i ) 
  df_temp$IMD_quintile <- i
  counts_IMD_quintile <- rbind(counts_IMD_quintile, df_temp)
}


# #ethnicity
# counts_ethnicity<- data.frame()
# for (i in c('White', 'Black', 'Asian', 'Other', 'Unknown')){
#   #print (i)
#   df_temp <- get_age_sex_bands_deno_from_table('ccu013_02_dp_skinny_patient_23_01_2020_phe_cohort_joined_1ymort_eth', 'ethnicity', i ) 
#   df_temp$ethnicity <- i
#   counts_ethnicity <- rbind(counts_ethnicity, df_temp)
# }


#ethn_5
counts_ethn_5 <- data.frame()
for (i in c('whi', 'bla_bb', 'asi_ab', 'mix', 'oth_eth', 'unkn')){
  #print (i)
  df_temp <- get_age_sex_bands_deno_from_table('ccu013_02_dp_skinny_patient_23_01_2020_phe_cohort_joined_1ymort_eth', 'ethn_5', i ) 
  df_temp$ethn_5 <- i
  counts_ethn_5 <- rbind(counts_ethn_5, df_temp)
}


#ethn_11
counts_ethn_11 <- data.frame()
for (i in c('whi',  'bla_af', 'bla_ca', 'bangl', 'chi', 'ind', 'pak', 'mix', 'oth_asi', 'oth_bl', 'oth_eth', 'unkn')){
  #print (i)
  df_temp <- get_age_sex_bands_deno_from_table('ccu013_02_dp_skinny_patient_23_01_2020_phe_cohort_joined_1ymort_eth', 'ethn_11', i ) 
  df_temp$ethn_11 <- i
  counts_ethn_11 <- rbind(counts_ethn_11, df_temp)
}



##count sex and age band per strata, for stand prevalence 
#sex
#rearrange so 
counts_sex <- data.frame()
for (i in 1:2){
  #print (i)
  df_temp <- get_age_sex_bands_deno_from_table('ccu013_02_dp_skinny_patient_23_01_2020_phe_cohort_joined_1ymort_eth', 'sex', i ) 
  #df_temp$sex <- i
  counts_sex <- rbind(counts_sex, df_temp)
  #extra steps in this case to build up table with 20 rows as in imd, ethn
  #see fun_stand_prev_ph_pop_strata - for sex and also consistency with the rest 
  
}

counts_sex <- counts_sex %>% arrange(age_band_23_01_2020, sex)
counts_sex$sex_dummy <- 1
  
counts_sex_2 <- counts_sex
counts_sex_2$sex_dummy <- 2
counts_sex <- rbind(counts_sex, counts_sex_2)


##########################################################
#get phecodes:
##########################################################

#find phecodes in event table
phenotypes <- get_nodes_phecodes_from_table('ccu013_phedatav200_unrolled_first_sex_dep')
#phenotypes_all <- get_phecodes_from_table('ccu013_phedatav200_unrolled_first')
print(paste0("Got ", length(phenotypes), " phenotypes"))
#phenotypes <- get_phecodes_sex_specific_from_table('ccu013_phedatav200_unrolled_first')
  
#exclude XROOT - should not go through analysis (gets full catalogue)
phe_exclude <- 'XROOT'
phenotypes <- phenotypes[!phenotypes %in% phe_exclude]

###This code commented below is used for testing ######
#res1 <- run_phecode("X180_1", 'ccu013_phedatav5_unrolled_first')
pheno_sm <- c()

main_HR <- fread('results/v200/main_May28_b_phe.csv')

main_HR <-  main_HR %>% filter(!is.na(HR))

phenotypes_HR <- main_HR$cohort

cov_cohorts <- c('cohort_01', 'cohort_02', 'cohort_03', 'cohort_04')
#################################
#only one can be TRUE
basic_pipeline <- FALSE
main_phe <- TRUE
main_cov_cohorts <- FALSE

#################################



print(paste0("Querying and running models"))
if (basic_pipeline){
  print('basic pipeline')
  #results <- mclapply(phenotypes, function(i) {run_phecode_basic(i, 'ccu013_phedatav5_unrolled_first_sex_dep')})
  results <- lapply(phenotypes, function(i) {run_phecode_basic(i, 'ccu013_phedatav5_unrolled_first_sex_dep')})
}else if (main_phe) {
  
  
  
  print(paste0("START: ", Sys.Date(), " ", Sys.time() ))
  
  str_out <- 'phe'

  print(str_out)

  results <- mclapply(phenotypes_HR, function(i) {run_main(i, 'ccu013_phedatav200_unrolled_first_sex_dep')}, mc.cores = 6)
  #results <- lapply(phenotypes[1:10], function(i) {run_main(i, 'ccu013_phedatav200_unrolled_first_sex_dep')})
  
  print(paste0("END: ", Sys.Date(), " ", Sys.time() )) 
  
  
  output_all <-bind_rows(results)
  
  write.csv(output_all, paste0("results/v200/main_May28d_19_", str_out, ".csv"), row.names = FALSE)
  
  
} else if (main_cov_cohorts){
  
  str_out <- 'cov'
  
  print(str_out)
  
  results <- mclapply(cov_cohorts, function(i) {run_main(i, 'ccu013_02_phe_cohort_most_severe_covid_cohort_earliest')}, mc.cores = 3)
  #results <- lapply(cov_cohorts, function(i) {run_main(i, 'ccu013_02_phe_cohort_most_severe_covid_cohort_earliest')})
}



# output_all <-bind_rows(results)
# 
# write.csv(output_all, paste0("results/v200/main_May28c_3_", str_out, ".csv"), row.names = FALSE)


###############
#merge - outputs
###############

f <- list.files("results/v200/")

f_list <- f[grep("main_May28d", f)]

#output 2 (20 on)
f_list <- f_list

all_f <- data.frame()


#subset from f_list the remaining outputs (e.g. to export pilot, or merge cov and phe )
#for (i in f_list[1:3]){
for (i in f_list){
  print(i)
  temp_i <- read.csv(paste0( "/db-mnt/databricks/rstudio_collab/ccu013_02_R/atlas/results/v200/", i))
  all_f <- rbind(all_f, temp_i)
  
}

#all_f_test <- head(all_f, 1)
all_f <- unique(all_f)

write.csv(all_f, "results/v200/main_May28d_phe_cov.csv", row.names = FALSE)



####################
#prepare to export d:
####################
##


all_f <- all_f %>% filter(n_cohort >= 10)


all_f <- distinct(all_f)


duplicates <- all_f %>%
  group_by(cohort) %>%
  filter(n() > 1) %>%
  ungroup()



all_f <-
  all_f %>% 
  group_by(cohort) %>% 
  filter(row_number()==1)



View(all_f[c("cohort", "covid_cases", "covid_cases_male", "covid_cases_fem","controls", "covid_cases_percent",
         "nevents", "beta","se", "pval", "HR","ci_left","ci_right", "median_survivaltime",
         "HR_n_covid_controls_noout", "HR_n_covid_controls_out",
         "HR_n_covid_case_noout", "HR_n_covid_case_out",
         "median_survtime_covid_control_noout", "median_survtime_covid_control_out",
         "median_survtime_covid_case_noout", "median_survtime_covid_case_out",
         "median_survtime_covid_control_noout_Q25", "median_survtime_covid_control_out_Q25", 
         "median_survtime_covid_case_noout_Q25", "median_survtime_covid_case_out_Q25", 
         "median_survtime_covid_control_noout_Q75", "median_survtime_covid_control_out_Q75", 
         "median_survtime_covid_case_noout_Q75", "median_survtime_covid_case_out_Q75",
         "nevents_unadj_mod", "beta_unadj_mod","se_unadj_mod", 
         "HR_unadj_mod_n_covid_controls_noout", "HR_unadj_mod_n_covid_controls_out", 
         "HR_unadj_mod_n_covid_case_noout", "HR_unadj_mod_n_covid_case_out",
         "median_survtime_covid_control_noout_unadj", "median_survtime_covid_control_out_unadj",
         "median_survtime_covid_case_noout_unadj", "median_survtime_covid_case_out_unadj",
         "median_survtime_covid_control_noout_unadj_Q25", "median_survtime_covid_control_out_unadj_Q25", 
         "median_survtime_covid_case_noout_unadj_Q25", "median_survtime_covid_case_out_unadj_Q25", 
         "median_survtime_covid_control_noout_unadj_Q75", "median_survtime_covid_control_out_unadj_Q75", 
         "median_survtime_covid_case_noout_unadj_Q75", "median_survtime_covid_case_out_unadj_Q75",
         "pval_unadj_mod", "HR_unadj_mod", "ci_left_unadj_mod","ci_right_unadj_mod", "median_survivaltime_unadj_mod")])


all_f_sm <- all_f %>% select("cohort", "covid_cases_percent",
                 "HR",
                 "median_survtime_covid_case_noout",
                 "median_survtime_covid_case_noout_Q25",
                 "median_survtime_covid_case_noout_Q75",
                 "median_survtime_covid_case_out",
                 "median_survtime_covid_case_out_Q25", 
                 "median_survtime_covid_case_out_Q75",
                 "median_survtime_covid_control_noout", 
                 "median_survtime_covid_control_noout_Q25",
                 "median_survtime_covid_control_noout_Q75",
                 "median_survtime_covid_control_out",
                 "median_survtime_covid_control_out_Q25", 
                 "median_survtime_covid_control_out_Q75",
                 "median_survtime_covid_case_noout_unadj",
                 "median_survtime_covid_case_noout_unadj_Q25",
                 "median_survtime_covid_case_noout_unadj_Q75",
                 "median_survtime_covid_case_out_unadj",
                 "median_survtime_covid_case_out_unadj_Q25",
                 "median_survtime_covid_case_out_unadj_Q75",
                 "median_survtime_covid_control_noout_unadj", 
                 "median_survtime_covid_control_noout_unadj_Q25",
                 "median_survtime_covid_control_noout_unadj_Q75",
                 "median_survtime_covid_control_out_unadj",
                 "median_survtime_covid_control_out_unadj_Q25", 
                 "median_survtime_covid_control_out_unadj_Q75", 
                 "HR_unadj_mod")




write.csv(all_f_sm, "results/v200/export/main_May28d_phe_export.csv", row.names = FALSE)


####################
#prepare to export:
####################


all_f <- all_f %>% filter(n_cohort >= 10)

# #cols with counts to pass through the function

#start with n 
cols_n <-  all_f %>% select(starts_with('n')) %>% colnames()

#start with B (age bands)
cols_B <-  all_f %>% select(starts_with('B')) %>% colnames()
cols_F <-  all_f %>% select(starts_with('F')) %>% colnames()
cols_o <-  all_f %>% select(starts_with('ob')) %>% colnames()
cols_c <-  all_f %>% select(starts_with('con')) %>% colnames()
cols_co <-  all_f %>% select(starts_with('cov')) %>% colnames()

cols <- c(cols_n, cols_B, cols_F, cols_o, cols_c, cols_co)

#exclude- no counts
excl <- c("beta", "beta_unadj_mod", "covid_cases_percent")

cols <- cols[!cols %in% excl]


all_f$nevents_unadj_mod <- as.numeric(all_f$nevents_unadj_mod)


#then round: 
for (i in cols){
  all_f <- fun_mask_round(all_f, i)
}

#set all cols - except cohort- to NA if n < 10
all_f <- all_f %>% mutate(across(.cols=!starts_with('cohort'), ~ifelse(n_cohort_m_r >= 10,., NA )))


#export all dis updates: 
all_f_sm <- all_f %>% select(c('cohort', 'n_cohort_m_r', 
                               starts_with('F'),
                               'median_length_from_first_record', 'mean_length_from_first_record', 
                        'HR',  'beta', 'se', 'pval', 'ci_left', 'ci_right', 'median_survivaltime',
                        'HR_unadj_mod',
                        'median_survivaltime_unadj_mod',
                        'nevents_m_r',
                        'covid_cases_percent'))


all_f_sm$cohort_n_cohort <- paste0(all_f_sm$cohort, ',', all_f_sm$n_cohort_m_r, ',')
all_f_sm$cohort<- NULL
all_f_sm$n_cohort_m_r<- NULL
all_f_sm$covid_cases_percent_nevents <- paste0(round(all_f_sm$covid_cases_percent, 4), ',', all_f_sm$nevents_m_r)
all_f_sm$covid_cases_percent <- NULL 
all_f_sm$nevents_m_r <- NULL 
all_f_sm$HR_ci_left_ci_right <- paste0(round(all_f_sm$ci_left, 4), ',', round(all_f_sm$ci_right, 4))
all_f_sm$ci_left <- NULL
all_f_sm$ci_right <- NULL
all_f_sm$beta_se <- paste0(round(all_f_sm$beta, 4), ',', round(all_f_sm$se, 4))
all_f_sm$beta <- NULL
all_f_sm$se <- NULL
all_f_sm$median_survivaltime_adj_unadj  <- paste0(round(all_f_sm$median_survivaltime,4), ',', round(all_f_sm$median_survivaltime_unadj_mod, 4))
all_f_sm$median_survivaltime <- NULL
all_f_sm$median_survivaltime_unadj_mod <- NULL
all_f_sm$mean_median_length_from_first_record <- paste0(round(all_f_sm$mean_length_from_first_record,4), ',', round(all_f_sm$median_length_from_first_record, 4))
all_f_sm$mean_length_from_first_record <- NULL
all_f_sm$median_length_from_first_record <- NULL

#export small set as pilot list or all except those -examples: 
# sql_cast = paste0("SELECT * FROM dsa_391419_j3w9t_collab.ccu013_lkp_phev200_cast")
# cast <- DBI::dbGetQuery(con, sql_cast)
# all_f_sm <- all_f %>% filter(!cohort %in% cast)

all_f_sm <- all_f_sm %>% relocate(cohort_n_cohort, covid_cases_percent_nevents)

# split: saves files in results/export (see path in function split_into_1k) 
split_into_1k_new(all_f_sm, 6, 'main', '_May28b_')



#########################################################
##export main -c - survival time in covid cases 
##########################################################

all_f <- all_f %>% filter(n_cohort >= 10)

# #cols with counts to pass through the function

#start with n 
all_f_s <-  all_f %>% select(cohort, HR, pval,
                            median_survivaltime_covid_case,
                            median_survivaltime_covid_control,
                            HR_unadj_mod,pval_unadj_mod,
                            median_survivaltime_covid_case_unadj,
                            median_survivaltime_covid_control_unadj)


write.csv(all_f_s, row.names = FALSE, "results/v200/export/main_May28c_phe.csv")


#########################################################
##export counts (age and sex strata) - old
##########################################################

#save with no rounding:
write.csv(counts, row.names = FALSE, "atlas_main_September12_counts.csv")
#with rounding
counts$n_indiv <- as.numeric(counts$n_indiv)
counts_col <- 'n_indiv'

for (i in counts_col){
  
  counts <- fun_mask_round(counts, i)
}



write.csv(counts, row.names = FALSE, "atlas_main_September12_counts_m_r.csv")






#####
#history for outputs exports- keep here for records for now-   (to be deleted when needed)

#############
# #reading results not needed if run whole pipeline including code to export 
# output_all <- read.csv("~/dars_nic_391419_j3w9t_collab/CCU013_02/atlas/results/v5/main_Feb03.csv")
# 
# #add column for denominators - 
# #TO DO: move to script to save during analysis
# output_all$den_pop <- den_pop
# output_all$den_pop[output_all$sex_flag == "1"] <- den_female
# output_all$den_pop[output_all$sex_flag == "2"] <- den_male
# output_all$den_pop <- as.numeric(output_all$den_pop)
# 
# #test_deno <-output_all %>% select(den_pop, sex_flag)
# 
# 
# 
# #function
# fun_mask_round<- function(df_in, 
#                           col_in){
#   
#   ######mask values under 10###
#   #output col
#   col_m <- paste(col_in, '_m', sep = '')
#   
#   #mask in col_in and save in col_m in the same df  
#   #df_in[[col_m]]<- ifelse(df_in[[col_in]] < 10, NA, df_in[[col_in]])
#   #asign -99 if n > 0 and < 10
#   df_in[[col_m]]<- ifelse(df_in[[col_in]] > 0 & df_in[[col_in]] < 10, -99, df_in[[col_in]])
#   
#   #print(class(df_in[[col_m]]))
#   
#   #####round to nearest multiple of 5#####  
#   #output col
#   col_m_r <- paste(col_m, '_r', sep = '')
#   
#   #col_m needs to be numeric 
#   df_in[[col_m_r]]<- round_any(df_in[[col_m]], 5)
#   
#   #drop input and interm m columns   
#   df_in[[col_in]]<- NULL
#   df_in[[col_m]]<- NULL
#   
#   #move new columns to front
#   df_in <- df_in %>% relocate(ends_with("_m_r"))
#   
#   return(df_in)
#   
# }
# 
# #cols to pass through the function
# cols <- c('nphecode', 
#           'B0_9_male', 'B0_9_female',
#           'B10_19_male', 'B10_19_female','B20_29_male', 'B20_29_female',
#           'B30_39_male', 'B30_39_female', 'B40_49_male', 'B40_49_female', 
#           'B50_59_male', 'B50_59_female', 'B60_69_male', 'B60_69_female', 
#           'B70_79_male', 'B70_79_female', 'B80_89_male', 'B80_89_female',
#           'B90p_male', 'B90p_female', 
#           'n_ph_males', 'n_ph_females',
#           'n_ph_asian', 'n_ph_black', 'n_ph_white',
#           'n_ph_other', 'n_ph_unknown', 
#           'n_ph_imd1', 'n_ph_imd2', 'n_ph_imd3', 
#           'n_ph_imd4', 'n_ph_imd5', 
#           'covid_cases', 'covid_cases_male', 'covid_cases_fem',
#           'controls', 'nevents', 'obs_deaths_1y', 
#           'den_pop')
# 
# 
# for (i in cols){
#   output_all <- fun_mask_round(output_all, i)
# }
# 
# 
# 
# #test_na <- test %>% filter(is.na(nphecode))
# 
# #test <- output_all %>% select(nphecode, nphecode_m_r, den_pop, den_pop_m_r, 
# #                             X0.9_male, X0.9_male_m_r, 
# #                              n_ph_imd1, n_ph_imd1_m_r, 
# #                              n_ph_imd2, n_ph_imd2_m_r,
# #                              n_ph_imd3, n_ph_imd3_m_r, 
# #                              n_ph_imd4, n_ph_imd4_m_r, 
# #                              n_ph_imd5, n_ph_imd5_m_r, 
# #                              nevents,nevents_m_r, 
# #                              obs_deaths_1y, obs_deaths_1y,
# #                              covid_cases, covid_cases_m_r,
# #                              controls, controls_m_r)
# 
# #break in small files of 1000 rows aprox to export 
# nrows <- nrow(output_all)
# ncsv <- 7
# 
# output_all$group <- 1:nrows %% ncsv + 1
# output_list <- split(output_all, list(output_all$group))
# 
# for (group in names(output_list)){
#   
#   write.csv(output_list[[group]], row.names = FALSE, paste0("results/v5/export/main_Feb03_m_r_", group, ".csv"))
# }
# 
# 


