

###########functions


get_phecodes_from_table <- function(tablename){
  
  #'description:
  #obtain unique phecodes present in a table that contains the column 'phecode'
  #joins with phe_dictionary
  
  #@param tablename -string
  
  #'usage:
  #' get_phecodes_from_table(tablename)
  
  sql = paste0("
SELECT DISTINCT(p.phecode)
FROM dsa_391419_j3w9t_collab.", tablename, " p, dsa_391419_j3w9t_collab.ccu013_lkp_phev200_phedict l 
WHERE l.phecode = p.phecode 
")
  
  phenotypes <- DBI::dbGetQuery(con, sql)
  phenotypes <- phenotypes$phecode
  return(phenotypes)
}



get_phecodes_sex_specific_from_table <- function(tablename){
  
  #'description:
  # obtain unique phecodes that are sex specific, present in a table - that contains the column 'phecode'
  #'
  
  #@param tablename -string
  
  #'usage:
  #' get_phecodes_sex_specific_from_table(tablename)
  
  sql = paste0("
SELECT DISTINCT(p.phecode)
FROM dsa_391419_j3w9t_collab.", tablename, " p, dsa_391419_j3w9t_collab.ccu013_lkp_phev200_phedict l 
WHERE l.phecode = p.phecode 
AND SEX in (1, 2)
")
  
  phenotypes <- DBI::dbGetQuery(con, sql)
  phenotypes <- phenotypes$phecode
  return(phenotypes)
}




get_age_sex_bands_deno_from_table <- function(tablename, variable, level){
  
  #'description:
  #'function that queries DB to get count of N people stratified by age and sex 
  #'using a variable and level of the variable as input
  
  #@param tablename -string 
  #@param variable - string 
  #@param i - string
  
  #'usage:
  #'get_age_sex_bands_deno_from_table('ccu013_02_dp_skinny_patient_23_01_2020_phe_cohort_joined_1ymort_eth', 'sex', i ) 
   
  
  sqlden = paste0("
  SELECT age_band_23_01_2020, sex, count(distinct(NHS_NUMBER_DEID)) as n_indiv 
  FROM dsa_391419_j3w9t_collab.", tablename, 
                  " WHERE ", variable, " ='", level,  
                  "'AND IMD_quintile not like ('Unknown%')
  AND sex in (1, 2)
  AND age_23_01_2020 >= 0 and age_23_01_2020 <= 100
  group by age_band_23_01_2020, sex
  order by age_band_23_01_2020, sex")
  counts <- DBI::dbGetQuery(con, sqlden)
  return(counts)
}



fun_stand_prev_pop <- function(df,
                                  counts, #gen_pop deno (also used as template cols in fun_nphecode)
                                  stand_pop, 
                                  df_res,
                                  prev_scale){
  
  #'description:
  #'function to calculate standardized prevalence 
  # calls internally other functions: fun_n_cohort_age_sex_bands
  
  
  #@param df  dataframe with info in phenotype  - from query - to calculate counts per age and sex bands 
  #@param counts  - df -  from gen_pop (n in age sex strata in pop, deno for strata specific prevalence)
  #@param stand_pop - df - standard population (ESP)
  #@param df_res - output df -  results will be written there 
  #@param factor to scale the prev -
  
  #'usage:
  #'df_res <- fun_stand_prev_pop(df,
  #                              counts,
  #                              esp_10y,
  #                              df_res,
  #                              prev_scale)
  #

  
  
  #this adds counts per band to df_res 
  df_res <- fun_n_cohort_age_sex_bands(df, 
                                       counts, 
                                       df_res)
  
  #print(df_res) 

  #now wrangling to use this
  bands <-df_res %>% select(matches("^B[0-9]"))
  bands_t <- transpose(bands)
  bands_t$N_cohort <- bands_t$V1
  bands_t$V1 <- NULL
  bands_t$N_cohort <- as.numeric(bands_t$N_cohort)
  
  #counts gen pop - deno for band specif prev
  counts <- arrange(counts, age_band_23_01_2020, sex)
  counts$n_indiv <- as.numeric(counts$n_indiv)
  
  ###calculate age_sex_specific 
  pops <- cbind(bands_t, counts, esp_10y)
  pops$sex_age_band_23_01_2020<- paste(pops$age_band_23_01_2020, pops$sex)
  pops  <- pops %>% relocate(sex_age_band_23_01_2020)
  pops$age_band_23_01_2020 <-NULL
  pops$Sex<- NULL
  pops$sex <- NULL 
  pops$AgeGroup <- NULL 
  
  #replace with 0 cells with na 
  pops <- pops %>% mutate(N_cohort = ifelse(is.na(N_cohort), 0, N_cohort))
  
  
  #print(pops$N_cohort)
  #print(pops$n_indiv)
  #print(pops$ESP)
  
  #this calculates the standard prev and 95% ci
  output = dsrTest(pops$N_cohort, 
                   pops$n_indiv, 
                   pops$ESP, 
                   conf.level = 0.95, 
                   mult = prev_scale, 
                   method = "dobson")
  
  
  df_res$prev_pop_stand <- output$estimate 
  df_res$ci_left_prev_pop_stand <- output$conf.int[1] 
  df_res$ci_right_prev_pop_stand <- output$conf.int[2] 

  #full calculation - same estimate as with package
  #pops$age_sex_specific_in_genpop <- pops$N_cohort/pops$n_indiv
  #pops$expected_cases_in_gen_pop <- pops$age_sex_specific_in_genpop*pops$ESP
  #df_res$stand_prev_cohort_pop_calc <- sum(pops$expected_cases_in_gen_pop, na.rm = TRUE)/sum(pops$ESP, na.rm = TRUE) * prev_scale
  
  return(df_res)
  
}


fun_stand_prev_pop_strata <- function(df,
                                         var,
                                         strata, #vector (e.g. c('1','2'), c('1','2', '3', '4', '5'), c('white', 'black', 'asian'))
                                         counts,
                                         esp10y,
                                         df_res,
                                         prev_scale){
  
  
  #'description:
  #'function that calculates standardarised prevalence for a stratum
  #'
  #@param df - dataframe info about phenotype 
  #@param var - string - variable (col in df) to consider for stratification
  #@param strata - vector (e.g. c('1','2'), c('1','2', '3', '4', '5'), c('white', 'black', 'asian'))
  #@param counts - dataframe - from gen_pop (n in each age sex strata in pop, deno for strata specific prevalence)
  #@param stand_pop - df - standard population (ESP)
  #@param df_res -output df, results will be written there 
  #@param prev_scale: factor to scale prev, should be aligned with other functions
  
  
  #'usage:
  #'df_res <- fun_stand_prev_pop_strata(df,
  #                                    "sex",
  #                                     strata = c('1','2'),
  #                                     counts_sex,
  #                                     esp10y,
  #                                     df_res,
  #                                     prev_scale)
  # 
  
  # print(var)
  
  for (st in strata){
    
    #print(st)
    #print(strata)
    #get rows of strata
    df_str <- df %>% filter(df[[var]] == st)  
    #print(df_str)
    
    
    #print(counts)
    
    #counts_st <- counts %>% filter(counts[[var]]== st)
    
    if (var == 'sex'){
      
      #build with dummy_sex so table is same size as other strata, 20 rows
      counts_st <- counts %>% filter(sex_dummy == st)
      # to keep deno in one sex (sex-specific prev) but structure of table with all age and sex strata 
      counts_st$n_indiv[counts_st$sex != st] <- 0
      
    } else {
      counts_st <- counts %>% filter(counts[[var]]== st)
    }
    
    
    #print(counts_st)
    
    df_res_temp <-df_res
    
    #this adds counts per band to df_res
    #counts_st purpose is only to have template of 20 age sex bands
    df_res_temp <- fun_n_cohort_age_sex_bands(df_str, 
                                              counts_st, 
                                              df_res_temp)
    
    #now wrangling to use this
    bands <-df_res_temp %>% select(matches("^B[0-9]"))
    bands_t <- transpose(bands)
    bands_t$N_cohort <- bands_t$V1
    bands_t$V1 <- NULL
    bands_t$N_ph <- as.numeric(bands_t$N_cohort)
    
    #counts gen pop - deno for band specif prev
    counts_st <- arrange(counts_st, age_band_23_01_2020, sex)
    counts_st$n_indiv <- as.numeric(counts_st$n_indiv)
    
    ###calculate age_sex_specific 
    pops <- cbind(bands_t, counts_st, esp_10y)
    pops$sex_age_band_23_01_2020<- paste(pops$age_band_23_01_2020, pops$sex)
    pops  <- pops %>% relocate(sex_age_band_23_01_2020)
    pops$age_band_23_01_2020 <-NULL
    pops$Sex<- NULL
    pops$sex <- NULL 
    pops$AgeGroup <- NULL 
    
    
    #replace with 0 cells with na 
    pops <- pops %>% mutate(N_cohort = ifelse(is.na(N_cohort), 0, N_cohort))
    pops <- pops %>% mutate(n_indiv = ifelse(is.na(n_indiv), 0, n_indiv))
    
    
    if (var == 'sex'){
      
      #to get stand pop of sex strata - and do age correction only 
      pops <- pops %>% filter(n_indiv != 0)
      
    }
    
    
    #print(pops)
    
    
    #this calculates the standard prev and 95% ci
    output = dsrTest(pops$N_cohort, 
                     pops$n_indiv, 
                     pops$ESP, 
                     conf.level = 0.95, 
                     mult = prev_scale, 
                     method = "dobson")
    
    
    #save stand prev and 95CI interv 
    col_estimate <- paste('prev_', var, '_', st, '_stand',sep = '')
    df_res[[col_estimate]] <-output$estimate 
    #print(col_estimate)
    
    col_ci_left <- paste('ci_left_prev_', var, '_', st, '_stand', sep = '')
    df_res[[col_ci_left]] <-output$conf.int[1] 
    #print(col_ci_left)
    
    col_ci_right <- paste('ci_right_prev_', var, '_', st, '_stand', sep = '')
    df_res[[col_ci_right]] <-output$conf.int[2] 
    #print(col_ci_right)
    
    #print('finish ci, saved in df_res')
    
    #print(pops)
    
    #save num and deno for CI fo the ratios 
    #pops$age_sex_specific_in_genpop <- pops$N_cohort/pops$n_indiv
    #N in band weighted with ESP_w (for num) 
    #pops$expected_cases <- pops$N_ph*pops$ESP_w

    #save num and deno of stand prev to construct 95% of the prev ratio next
 
    #deno : sum total adding age and sex specific band in the strata 
    col_estimate_den <- paste('prev_', var, '_', st,'_stand_den',  sep = '')
    df_res[[col_estimate_den]] <-sum(pops$n_indiv, na.rm = TRUE)
    
    #num: 
    col_estimate_num <- paste('prev_', var, '_', st,'_stand_num',  sep = '')
    #df_res[[col_estimate_num]] <- sum(pops$expected_cases, na.rm = TRUE) 
    df_res[[col_estimate_num]] <- (df_res[[col_estimate]]/prev_scale) * df_res[[col_estimate_den]]
    
  }
  
  #calculate the prev ratios depending on var
  if (var == 'sex'){
    
    df_res$prev_sex_ratio_stand <- df_res$prev_sex_2_stand/df_res$prev_sex_1_stand
    
    #CI
    ci_prev_sex_ratio_stand <- ci_prev_ratio(df_res$prev_sex_ratio_stand,
                                                df_res$prev_sex_2_stand_num, df_res$prev_sex_2_stand_den,
                                                df_res$prev_sex_1_stand_num, df_res$prev_sex_1_stand_den)
    
    df_res$ci_left_prev_sex_ratio_stand <- ci_prev_sex_ratio_stand[1]
    df_res$ci_right_prev_sex_ratio_stand <- ci_prev_sex_ratio_stand[2]
    
    
    
  } else if (var == 'IMD_quintile'){
    
    df_res$prev_imd_ratio_stand <- df_res$prev_IMD_quintile_1_stand/df_res$prev_IMD_quintile_5_stand
    
    #CI
    ci_prev_imd_ratio_stand <- ci_prev_ratio(df_res$prev_imd_ratio_stand,
                                                df_res$prev_IMD_quintile_1_stand_num, df_res$prev_IMD_quintile_1_stand_den,
                                                df_res$prev_IMD_quintile_5_stand_num, df_res$prev_IMD_quintile_5_stand_den)
    
    df_res$ci_left_prev_imd_ratio_stand <- ci_prev_imd_ratio_stand[1]
    df_res$ci_right_prev_imd_ratio_stand <- ci_prev_imd_ratio_stand[2]
    
    
  } else if(var == 'ethnicity'){
    
    #B vs W
    df_res$prev_ethn_ratio_B_W_stand <- df_res$prev_ethnicity_Black_stand/df_res$prev_ethnicity_White_stand
    
    
    ci_prev_ethn_ratio_B_W_stand <- ci_prev_ratio(df_res$prev_ethn_ratio_B_W_stand,
                                                     df_res$prev_ethnicity_Black_stand_num, df_res$prev_ethnicity_Black_stand_den,
                                                     df_res$prev_ethnicity_White_stand_num, df_res$prev_ethnicity_White_stand_den)
    
    df_res$ci_left_prev_ethn_ratio_B_W_stand <- ci_prev_ethn_ratio_B_W_stand[1]
    df_res$ci_right_prev_ethn_ratio_B_W_stand <- ci_prev_ethn_ratio_B_W_stand[2]
    
    
    
    #A vs W
    df_res$prev_ethn_ratio_A_W_stand <- df_res$prev_ethnicity_Asian_stand/df_res$prev_ethnicity_White_stand
    
    ci_prev_ethn_ratio_A_W_stand <- ci_prev_ratio(df_res$prev_ethn_ratio_A_W_stand,
                                                     df_res$prev_ethnicity_Asian_stand_num, df_res$prev_ethnicity_Asian_stand_den,
                                                     df_res$prev_ethnicity_White_stand_num, df_res$prev_ethnicity_White_stand_den)
    
    df_res$ci_left_prev_ethn_ratio_A_W_stand <- ci_prev_ethn_ratio_A_W_stand[1]
    df_res$ci_right_prev_ethn_ratio_A_W_stand <- ci_prev_ethn_ratio_A_W_stand[2]
    
    
    
  }else if(var == 'ethn_5'){
    
    
    #B vs W
    df_res$prev_ethn_ratio_5_bla_bb_whi_stand <- df_res[['prev_ethn_5_bla_bb_stand']]/df_res[['prev_ethn_5_whi_stand']]
    
    
    ci_prev_ethn_ratio_5_bla_bb_whi_stand <- ci_prev_ratio(df_res$prev_ethn_ratio_5_bla_bb_whi_stand,
                                                   df_res[['prev_ethn_5_bla_bb_stand_num']], df_res[['prev_ethn_5_bla_bb_stand_den']],
                                                   df_res[['prev_ethn_5_whi_stand_num']], df_res[['prev_ethn_5_whi_stand_den']])
    
    df_res$ci_left_prev_ethn_ratio_5_bla_bb_whi_stand <- ci_prev_ethn_ratio_5_bla_bb_whi_stand[1]
    df_res$ci_right_prev_ethn_ratio_5_bla_bb_whi_stand <- ci_prev_ethn_ratio_5_bla_bb_whi_stand[2]
    
    
    #A vs W
    df_res$prev_ethn_ratio_5_asi_ab_whi_stand <- df_res[['prev_ethn_5_asi_ab_stand']]/df_res[['prev_ethn_5_whi_stand']]
    
    ci_prev_ethn_ratio_5_asi_ab_whi_stand <- ci_prev_ratio(df_res$prev_ethn_ratio_5_asi_ab_whi_stand,
                                                  df_res[['prev_ethn_5_asi_ab_stand_num']], df_res[['prev_ethn_5_asi_ab_stand_den']],
                                                  df_res[['prev_ethn_5_whi_stand_num']], df_res[['prev_ethn_5_whi_stand_den']])
    
    df_res$ci_left_prev_ethn_ratio_5_asi_ab_whi_stand <- ci_prev_ethn_ratio_5_asi_ab_whi_stand[1]
    df_res$ci_right_prev_ethn_ratio_5_asi_ab_whi_stand <- ci_prev_ethn_ratio_5_asi_ab_whi_stand[2]
    
    
    
  }else if(var == 'ethn_11'){
    
    #To DO: edit names in skinny t
    
    #Blangladeshi vs W
    df_res$prev_ethn_ratio_11_bangl_whi_stand <- df_res[['prev_ethn_11_bangl_stand']]/df_res[['prev_ethn_11_whi_stand']]
    
    
    ci_prev_ethn_ratio_11_bangl_whi_stand <- ci_prev_ratio(df_res$prev_ethn_ratio_11_bangl_whi_stand,
                                                    df_res[['prev_ethn_11_bangl_stand_num']], df_res[['prev_ethn_11_bangl_stand_den']],
                                                    df_res[['prev_ethn_11_whi_stand_num']], df_res[['prev_ethn_11_whi_stand_den']])
    
    df_res$ci_left_prev_ethn_ratio_11_bangl_whi_stand <- ci_prev_ethn_ratio_11_bangl_whi_stand[1]
    df_res$ci_right_prev_ethn_ratio_11_bangl_whi_stand <- ci_prev_ethn_ratio_11_bangl_whi_stand[2]
    
    
    #Chinese vs W
    df_res$prev_ethn_ratio_11_chi_whi_stand <- df_res[['prev_ethn_11_chi_stand']]/df_res[['prev_ethn_11_whi_stand']]

    ci_prev_ethn_ratio_11_chi_whi_stand <- ci_prev_ratio(df_res$prev_ethn_ratio_11_chi_whi_stand,
                                                    df_res[['prev_ethn_11_chi_stand_num']], df_res[['prev_ethn_11_chi_stand_den']],
                                                    df_res[['prev_ethn_11_whi_stand_num']], df_res[['prev_ethn_11_whi_stand_den']])

    df_res$ci_left_prev_ethn_ratio_11_chi_whi_stand <- ci_prev_ethn_ratio_11_chi_whi_stand[1]
    df_res$ci_right_prev_ethn_ratio_11_chi_whi_stand <- ci_prev_ethn_ratio_11_chi_whi_stand[2]
    
    
    #Indian vs W
    df_res$prev_ethn_ratio_11_ind_whi_stand <- df_res[['prev_ethn_11_ind_stand']]/df_res[['prev_ethn_11_whi_stand']]

    ci_prev_ethn_ratio_11_ind_whi_stand <- ci_prev_ratio(df_res$prev_ethn_ratio_11_ind_whi_stand,
                                                    df_res[['prev_ethn_11_ind_stand_num']], df_res[['prev_ethn_11_ind_stand_den']],
                                                    df_res[['prev_ethn_11_whi_stand_num']], df_res[['prev_ethn_11_whi_stand_den']])

    df_res$ci_left_prev_ethn_ratio_11_ind_whi_stand <- ci_prev_ethn_ratio_11_ind_whi_stand[1]
    df_res$ci_right_prev_ethn_ratio_11_ind_whi_stand <- ci_prev_ethn_ratio_11_ind_whi_stand[2]


    #Pakistani vs W
    df_res$prev_ethn_ratio_11_pak_whi_stand <- df_res[['prev_ethn_11_pak_stand']]/df_res[['prev_ethn_11_whi_stand']]

    ci_prev_ethn_ratio_11_pak_whi_stand <- ci_prev_ratio(df_res$prev_ethn_ratio_11_pak_whi_stand,
                                                    df_res[['prev_ethn_11_pak_stand_num']], df_res[['prev_ethn_11_pak_stand_den']],
                                                    df_res[['prev_ethn_11_whi_stand_num']], df_res[['prev_ethn_11_whi_stand_den']])

    df_res$ci_left_prev_ethn_ratio_11_pak_whi_stand <- ci_prev_ethn_ratio_11_pak_whi_stand[1]
    df_res$ci_right_prev_ethn_ratio_11_pak_whi_stand <- ci_prev_ethn_ratio_11_pak_whi_stand[2]


    #Black Afr vs W
    df_res$prev_ethn_ratio_11_bla_af_whi_stand <- df_res[['prev_ethn_11_bla_af_stand']]/df_res[['prev_ethn_11_whi_stand']]

    ci_prev_ethn_ratio_11_bla_af_whi_stand <- ci_prev_ratio(df_res$prev_ethn_ratio_11_bla_af_whi_stand,
                                                    df_res[['prev_ethn_11_bla_af_stand_num']], df_res[['prev_ethn_11_bla_af_stand_den']],
                                                    df_res[['prev_ethn_11_whi_stand_num']], df_res[['prev_ethn_11_whi_stand_den']])

    df_res$ci_left_prev_ethn_ratio_11_bla_af_whi_stand <- ci_prev_ethn_ratio_11_bla_af_whi_stand[1]
    df_res$ci_right_prev_ethn_ratio_11_bla_af_whi_stand <- ci_prev_ethn_ratio_11_bla_af_whi_stand[2]

    

    #Black Carib vs W
    df_res$prev_ethn_ratio_11_bla_ca_whi_stand <- df_res[['prev_ethn_11_bla_ca_stand']]/df_res[['prev_ethn_11_whi_stand']]

    ci_prev_ethn_ratio_11_bla_ca_whi_stand <- ci_prev_ratio(df_res$prev_ethn_ratio_11_bla_ca_whi_stand,
                                                    df_res[['prev_ethn_11_bla_ca_stand_num']], df_res[['prev_ethn_11_bla_ca_stand_den']],
                                                    df_res[['prev_ethn_11_whi_stand_num']], df_res[['prev_ethn_11_whi_stand_den']])

    df_res$ci_left_prev_ethn_ratio_11_bla_ca_whi_stand <- ci_prev_ethn_ratio_11_bla_ca_whi_stand[1]
    df_res$ci_right_prev_ethn_ratio_11_bla_ca_whi_stand <- ci_prev_ethn_ratio_11_bla_ca_whi_stand[2]

  }
  
  #delete these - temp dummy cols to calculate ratio 95% CI
  cols_to_delete_1 <- colnames(df_res %>% select(ends_with("num")))
  cols_to_delete_2 <- colnames(df_res %>% select(ends_with("den")))
  
  df_res <-df_res[!(colnames(df_res) %in% cols_to_delete_1)]
  df_res <-df_res[!(colnames(df_res) %in% cols_to_delete_2)]
  
  
  #output %>% select(contains("stand"))
  
  return(df_res)
  
  
}



fun_n_cohort_age_sex_bands <- function(df_input, 
                                       df_template_allstrata, 
                                       df_output){
  
  
  #'description:
  #'function to get counts of nphecode (n cases per phecode) per age and sex age bands at baseline
  #'
  
  #@param df_input : df (from query)
  #@param df_template_allstrata : counts (df with all strata possible; using only the structure from counts (gen_pop) as example)
  #@param df_output: df where to save the columns 
  
  #'usage:
  #'   df_res <- fun_n_cohort_age_sex_bands(df, 
  #'                                       counts, 
  #'                                       df_res)
  #' 
  
  
  df_dt <-data.table(df_input)
  df_studypop_n <- df_dt[,.(n_strata_studypop=.N), by=.(age_band_23_01_2020, sex)]
  
  
  
  df_studypop_n <- arrange(df_studypop_n, age_band_23_01_2020, sex)
  df_studypop_n[is.na(df_studypop_n)] <- 0
  
  #use 'counts' gen_pop as template with all strata
  df_studypop_n <- merge(df_template_allstrata, df_studypop_n, by = c('age_band_23_01_2020', 'sex'), all=T)
  
  
  
  #skinny: 1 = male
  df_studypop_n$sex_label <- ifelse(df_studypop_n$sex == '1', 'male', 'female')
  df_studypop_n$n_indiv <- NULL
  df_studypop_n$sex <- NULL
  
  
  
  #pivot and save as separate columns 
  df_count <- df_studypop_n %>%
    pivot_wider(names_from = c(age_band_23_01_2020, sex_label), 
                values_from = n_strata_studypop, 
                names_sep="_")   
  
  #replace na with 0 
  df_count[is.na(df_count)] <- 0
  
  #Temp code
  df_output$B0_9_male <- df_count$`0-9_male` 
  
  #print(df_count$`0-9_male`)
  
  df_output$B0_9_female <- df_count$`0-9_female`
  
  #df_output$`10-19_male` <- df_res$`10-19_male` 
  df_output$B10_19_male <- df_count$`10-19_male` 
  df_output$B10_19_female <- df_count$`10-19_female`
  
  df_output$B20_29_male <- df_count$`20-29_male` 
  df_output$B20_29_female <- df_count$`20-29_female`
  
  df_output$B30_39_male <- df_count$`30-39_male` 
  df_output$B30_39_female <- df_count$`30-39_female`
  
  df_output$B40_49_male <- df_count$`40-49_male` 
  df_output$B40_49_female <- df_count$`40-49_female`
  
  df_output$B50_59_male <- df_count$`50-59_male` 
  df_output$B50_59_female <- df_count$`50-59_female`
  
  df_output$B60_69_male <- df_count$`60-69_male` 
  df_output$B60_69_female <- df_count$`60-69_female`
  
  df_output$B70_79_male <- df_count$`70-79_male` 
  df_output$B70_79_female <- df_count$`70-79_female`
  
  df_output$B80_89_male <- df_count$`80-89_male` 
  df_output$B80_89_female <- df_count$`80-89_female`
  
  df_output$B90p_male <- df_count$`90+_male` 
  df_output$B90p_female <- df_count$`90+_female`
  
  
  return(df_output)
}




fun_n_cohort_age_sex_bands_first_date <- function(df_input, 
                                                  df_template_allstrata, 
                                                  df_output){
  
  
  #'description:
  #'function to get counts of nphecode (n cases per phecode) per age and sex age bands at baseline at first date of phenotype
  #'
  
  #@param df_input : df (from query)
  #@param df_template_allstrata : counts (df with all strata possible; using only the structure from counts (gen_pop) as example)
  #@param df_output: df where to save the columns 
  
  #'usage:
  #'   df_res <- fun_n_cohort_age_sex_bands_first_date(df, 
  #'                                       counts, 
  #'                                       df_res)
  #' 
  
  
  df_dt <-data.table(df_input)
  
  df_dt$age_band_first_date <- factor(cut(df_dt$age, breaks = c(0, 10, 20, 30 , 40, 50, 60, 70, 80, 90, 105), 
                                          labels = c("0-9","10-19", "20-29", "30-39", "40-49", "50-59", "60-69", "70-79", "80-89", "90+")))
  
  #make age_band_first_date: 
  
  df_studypop_n <- df_dt[,.(n_strata_studypop=.N), by=.(age_band_first_date, sex)]
  
  
  
  df_studypop_n <- arrange(df_studypop_n, age_band_first_date, sex)
  df_studypop_n[is.na(df_studypop_n)] <- 0
  
  #use 'counts' gen_pop as template with all strata
  df_studypop_n <- merge( df_template_allstrata, df_studypop_n, 
                          by.x = c('age_band_23_01_2020', 'sex'), 
                          by.y = c('age_band_first_date', 'sex'), all=T)
  
  
  
  #skinny: 1 = male
  df_studypop_n$sex_label <- ifelse(df_studypop_n$sex == '1', 'male', 'female')
  df_studypop_n$n_indiv <- NULL
  df_studypop_n$sex <- NULL
  
  
  
  #pivot and save as separate columns 
  df_count <- df_studypop_n %>%
    pivot_wider(names_from = c(age_band_23_01_2020, sex_label), 
                values_from = n_strata_studypop, 
                names_sep="_")   
  
  #replace na with 0 
  df_count[is.na(df_count)] <- 0
  
  #Temp code
  df_output$F0_9_male <- df_count$`0-9_male` 
  df_output$F0_9_female <- df_count$`0-9_female`
  
  df_output$F10_19_male <- df_count$`10-19_male` 
  df_output$F10_19_female <- df_count$`10-19_female`
  
  df_output$F20_29_male <- df_count$`20-29_male` 
  df_output$F20_29_female <- df_count$`20-29_female`
  
  df_output$F30_39_male <- df_count$`30-39_male` 
  df_output$F30_39_female <- df_count$`30-39_female`
  
  df_output$F40_49_male <- df_count$`40-49_male` 
  df_output$F40_49_female <- df_count$`40-49_female`
  
  df_output$F50_59_male <- df_count$`50-59_male` 
  df_output$F50_59_female <- df_count$`50-59_female`
  
  df_output$F60_69_male <- df_count$`60-69_male` 
  df_output$F60_69_female <- df_count$`60-69_female`
  
  df_output$F70_79_male <- df_count$`70-79_male` 
  df_output$F70_79_female <- df_count$`70-79_female`
  
  df_output$F80_89_male <- df_count$`80-89_male` 
  df_output$F80_89_female <- df_count$`80-89_female`
  
  df_output$F90p_male <- df_count$`90+_male` 
  df_output$F90p_female <- df_count$`90+_female`
  
  
  return(df_output)
}



fun_prev_pop <- function(df_input, 
                            deno_prev_pop, 
                            prev_scale){
  
 
  #'description:
  #function to calculate prevalence of disease (phecode) and 95% CI
  
  #@param df_input - dataframe - input df_res
  #@param deno_prev_ph_pop - numeric - deno to consider
  #@param prev scale - numeric -factor to scale prevalence (10^x)
  
  
  #'usage:
  #'    df_res <- fun_prev_pop(df_res, 
  #'             deno_prev_pop, 
  #'             prev_scale)
  
  
  
  df_input$prev_pop <- (df_input$n_cohort/deno_prev_pop) * prev_scale
  
  #CI
  ci_prev_pop <- ci_prop(df_input$prev_pop, 
                            prev_scale, 
                            deno_prev_pop)
  #prev_pop CI
  df_input$ci_left_prev_pop <- ci_prev_pop[1]
  df_input$ci_right_prev_pop <- ci_prev_pop[2]
  
  #df_input$den_pop <-deno_prev_pop 
  
  return(df_input)
  
}



fun_prev_imd <- function(df_input, 
                            den_imd1, 
                            den_imd2, 
                            den_imd3, 
                            den_imd4, 
                            den_imd5, 
                            prev_scale){
  
  
  #'description:
  ##function to calculate prev per imd strata, 95%ci and prev_ph_imd_ratio and 95% ci
  #@param df_input (df_res (where results are saved - that also contains relevant columns used inside the function) like count of cases per imd strata)
  #@param den_imd1 etc; - deno per strata (can depend if disease is sex related)- adjusted in main code
  #@param prev scale: factor to scale prevalence (10^x)
  
  #'usage:
  #'    df_res <- fun_prev_imd(df_res, 
  #'                           den_imd1 = den_imd1, 
  #'                           den_imd2 = den_imd2, 
  #'                           den_imd3 = den_imd3, 
  #'                           den_imd4 = den_imd4, 
  #'                           den_imd5 = den_imd5, 
  #'                           prev_scale)
  #' #' 
  
  df_input$prev_imd1 <- (df_input$n_imd1/den_imd1) * prev_scale
  df_input$prev_imd2 <- (df_input$n_imd2/den_imd2) * prev_scale
  df_input$prev_imd3 <- (df_input$n_imd3/den_imd3) * prev_scale
  df_input$prev_imd4 <- (df_input$n_imd4/den_imd4) * prev_scale
  df_input$prev_imd5 <- (df_input$n_imd5/den_imd5) * prev_scale
  
  #CI
  ci_prev_imd1 <- ci_prop(df_input$prev_imd1, prev_scale, den_imd1)
  df_input$ci_left_prev_imd1 <- ci_prev_imd1[1]
  df_input$ci_right_prev_imd1 <- ci_prev_imd1[2]
  #df_input$den_imd1 <- den_imd1
  
  ci_prev_imd2 <- ci_prop(df_input$prev_imd2, prev_scale, den_imd2)
  df_input$ci_left_prev_imd2 <- ci_prev_imd2[1]
  df_input$ci_right_prev_imd2 <- ci_prev_imd2[2]
  #df_input$den_imd2 <- den_imd2
  
  ci_prev_imd3 <- ci_prop(df_input$prev_imd3, prev_scale, den_imd3)
  df_input$ci_left_prev_imd3 <- ci_prev_imd3[1]
  df_input$ci_right_prev_imd3 <- ci_prev_imd3[2]
  #df_input$den_imd3 <- den_imd3
  
  ci_prev_imd4 <- ci_prop(df_input$prev_imd4, prev_scale, den_imd4)
  df_input$ci_left_prev_imd4 <- ci_prev_imd4[1]
  df_input$ci_right_prev_imd4 <- ci_prev_imd4[2]
  #df_input$den_imd4 <- den_imd4
  
  ci_prev_imd5 <- ci_prop(df_input$prev_imd5, prev_scale, den_imd5)
  df_input$ci_left_prev_imd5 <- ci_prev_imd5[1]
  df_input$ci_right_prev_imd5 <- ci_prev_imd5[2]
  #df_input$den_imd5 <- den_imd5
  
  #prev imd ratio: most deprived / least deprived
  df_input$prev_imd_ratio <-  df_input$prev_imd1/df_input$prev_imd5
  
  ci_prev_imd_ratio <- ci_prev_ratio(df_input$prev_imd_ratio, 
                                        df_input$n_imd1, den_imd1, 
                                        df_input$n_imd5, den_imd5)
  
  df_input$ci_left_prev_imd_ratio <- ci_prev_imd_ratio[1]
  df_input$ci_right_prev_imd_ratio <- ci_prev_imd_ratio[2]
  
  return(df_input)
  
}



fun_prev_ethn_old <- function(df_input, 
                             den_asian, 
                             den_black, 
                             den_other,
                             den_unknown,
                             den_white, 
                             prev_scale){
  
  
  #'description:
  ##function to calculate prev per ethnicity strata, 95%ci and prev_ph_eth_ratio and 95% ci
  
  #@param df_input - df_res (where results are saved -#that also contains relevant columns used inside the function, 
  #like count of cases per eth strata)
  #@param den_asian etc; deno per strata (can depend if disease is sex related)- adjusted in main code
  #@param prev scale -  factor to scale prevalence (10^x)
  
  #'usage:
  #' 
  #' 
  
  df_input$prev_asian <- (df_input$n_asian/den_asian) * prev_scale
  #df_input$den_asian <- den_asian
  
  df_input$prev_black <- (df_input$n_black/den_black) * prev_scale
  #df_input$den_black <- den_black
  
  df_input$prev_white <- (df_input$n_white/den_white) * prev_scale
  #df_input$den_white <- den_white
  
  df_input$prev_unknown <- (df_input$n_unknown/den_unknown) * prev_scale
  #df_input$den_unknown <- den_unknown
  
  df_input$prev_other <- (df_input$n_other/den_other) * prev_scale
  #df_input$den_other <- den_other
  
  #CI
  ci_prev_asian <- ci_prop(df_input$prev_asian, prev_scale, den_asian)
  df_input$ci_left_prev_asian <- ci_prev_asian[1]
  df_input$ci_right_prev_asian <- ci_prev_asian[2]
  
  ci_prev_black <- ci_prop(df_input$prev_black, prev_scale, den_black)
  df_input$ci_left_prev_black <- ci_prev_black[1]
  df_input$ci_right_prev_black <- ci_prev_black[2]
  
  ci_prev_white <- ci_prop(df_input$prev_white, prev_scale, den_white)
  df_input$ci_left_prev_white <- ci_prev_white[1]
  df_input$ci_right_prev_white <- ci_prev_white[2]
  
  ci_prev_other <- ci_prop(df_input$prev_other, prev_scale, den_other)
  df_input$ci_left_prev_other <- ci_prev_other[1]
  df_input$ci_right_prev_other <- ci_prev_other[2]
  
  ci_prev_unknown <- ci_prop(df_input$prev_unknown, prev_scale, den_unknown)
  df_input$ci_left_prev_unknown <- ci_prev_unknown[1]
  df_input$ci_right_prev_unknown <- ci_prev_unknown[2]
  
  
  #prev ethn ratio asian vs white 
  df_input$prev_ethn_ratio_A_W <-  df_input$prev_asian/df_input$prev_white
  
  #CI
  ci_prev_ethn_ratio_A_W <- ci_prev_ratio(df_input$prev_ethn_ratio_A_W, 
                                             df_input$n_asian, den_asian, 
                                             df_input$n_white, den_white)
  
  df_input$ci_left_prev_ethn_ratio_A_W <- ci_prev_ethn_ratio_A_W[1]
  df_input$ci_right_prev_ethn_ratio_A_W <- ci_prev_ethn_ratio_A_W[2]
  
  
  #prev eth ratio B vs white
  df_input$prev_ethn_ratio_B_W <-  df_input$prev_black/df_input$prev_white
  
  
  #CI
  ci_prev_ethn_ratio_B_W <- ci_prev_ratio(df_input$prev_ethn_ratio_B_W, 
                                             df_input$n_black, den_black, 
                                             df_input$n_white, den_white)
  
  df_input$ci_left_prev_ethn_ratio_B_W <- ci_prev_ethn_ratio_B_W[1]
  df_input$ci_right_prev_ethn_ratio_B_W <- ci_prev_ethn_ratio_B_W[2]
  
  
  return(df_input)
  
}



#function to calculate prev and ci to use in multipe strata - OLD FUNCTION
prev_str_obs <- function(df_input, #df_res where cols exist to write new cols too
                         st,       #strata (string) to use in names
                         n_num,    #num for this strata to get from df_input
                         n_den,    #den from df_den_eth
                         prev_scale){
  
  #prev_asian
  col_estimate <- paste('prev_', st ,sep = '')
  
  df_input[[col_estimate]] <-(df_input[[n_num]]/n_den) * prev_scale
  
  #CI
  col_ci_left <- paste('ci_left_prev_', st ,sep = '')
  col_ci_right <- paste('ci_right_prev_', st ,sep = '')
  
  ci_prev <- ci_prop(df_input[[col_estimate]], prev_scale, n_den)
  
  df_input[[col_ci_left]] <- ci_prev[1]
  df_input[[col_ci_right]] <- ci_prev[2]
  
  return(df_input)
  
  
}




fun_prev_eth <- function(df_input,   # df_res
                          list_den_ethn,  # den from den_ethn_all, den_ethn_all_5, den_ethn_all_11
                          ethn_granular,  # ethnicity, ethn_5, ethn_11- flag to use
                          prev_scale){    # 10*6
  
  
  
  #'description:
  ##function to calculate prev per ethnicity strata, 95%ci and prev_ph_eth_ratio and 95% ci
  
  #@param df_input:df_res (where results are saved - that also contains relevant columns used inside the function, like count of cases per eth strata)
  #@param den_iasian etc; deno per strata (can depend if disease is sex related)- adjusted in main code
  #@param prev scale: factor to scale prevalence (10^x)
  
  #'usage:
  #'df_res <- fun_prev_eth(df_res, c(den_5_asi_ab, den_5_bla_bb, den_5_mix, den_5_oth_eth, den_5_unkn, den_5_whi), 'ethn_5', prev_scale)
  
  
  
  if (ethn_granular == 'ethnicity'){
    
    
    df_input <- prev_str_obs(df_input, 'asian', 'n_asian', den_asian, prev_scale)
    df_input <- prev_str_obs(df_input, 'black', 'n_black', den_black, prev_scale)
    df_input <- prev_str_obs(df_input, 'other', 'n_other', den_other, prev_scale)
    df_input <- prev_str_obs(df_input, 'unknown', 'n_unknown', den_unknown, prev_scale)
    df_input <- prev_str_obs(df_input, 'white', 'n_white', den_white, prev_scale)
    
    
    #ratios: 
    
    #prev ethn ratio asian vs white 
    df_input$prev_ethn_ratio_A_W <-  df_input$prev_asian/df_input$prev_white
    
    #CI
    ci_prev_ethn_ratio_A_W <- ci_prev_ratio(df_input$prev_ethn_ratio_A_W, 
                                            df_input$n_asian, den_asian, 
                                            df_input$n_white, den_white)
    
    df_input$ci_left_prev_ethn_ratio_A_W <- ci_prev_ethn_ratio_A_W[1]
    df_input$ci_right_prev_ethn_ratio_A_W <- ci_prev_ethn_ratio_A_W[2]
    
    
    #prev eth ratio B vs white
    df_input$prev_ethn_ratio_B_W <-  df_input$prev_black/df_input$prev_white
    
    
    #CI
    ci_prev_ethn_ratio_B_W <- ci_prev_ratio(df_input$prev_ethn_ratio_B_W, 
                                            df_input$n_black, den_black, 
                                            df_input$n_white, den_white)
    
    df_input$ci_left_prev_ethn_ratio_B_W <- ci_prev_ethn_ratio_B_W[1]
    df_input$ci_right_prev_ethn_ratio_B_W <- ci_prev_ethn_ratio_B_W[2]
    
    
  }else if (ethn_granular == 'ethn_5'){
    
    
    df_input <- prev_str_obs(df_input, '5_asi_ab', 'n_5_asi_ab', den_5_asi_ab, prev_scale)
    df_input <- prev_str_obs(df_input, '5_bla_bb', 'n_5_bla_bb', den_5_bla_bb, prev_scale)
    df_input <- prev_str_obs(df_input, '5_mix', 'n_5_mix', den_5_mix, prev_scale)
    df_input <- prev_str_obs(df_input, '5_oth_eth', 'n_5_oth_eth', den_5_oth_eth, prev_scale)
    df_input <- prev_str_obs(df_input, '5_unkn', 'n_5_unkn', den_5_unkn, prev_scale)
    df_input <- prev_str_obs(df_input, '5_whi', 'n_5_whi', den_5_whi, prev_scale)
    
    #ratios: 
    
    #prev ethn ratio asian vs white 
    df_input$prev_ethn_ratio_5_asi_ab_whi <-  df_input$prev_5_asi_ab/df_input$prev_5_whi
    
    #CI
    ci_prev_ethn_ratio_5_asi_ab_whi <- ci_prev_ratio(df_input$prev_ethn_ratio_5_asi_ab_whi, 
                                            df_input$n_5_asi_ab, den_5_asi_ab, 
                                            df_input$n_5_whi, den_5_whi)
    
    df_input$ci_left_prev_ethn_ratio_5_asi_ab_whi <- ci_prev_ethn_ratio_5_asi_ab_whi[1]
    df_input$ci_right_prev_ethn_ratio_5_asi_ab_whi <- ci_prev_ethn_ratio_5_asi_ab_whi[2]
    

    #prev eth ratio B vs white
    df_input$prev_ethn_ratio_5_bla_bb_whi <-  df_input$prev_5_bla_bb/df_input$prev_5_whi
    
    
    #CI
    ci_prev_ethn_ratio_5_bla_bb_whi <- ci_prev_ratio(df_input$prev_ethn_ratio_5_bla_bb_whi, 
                                            df_input$n_5_bla_bb, den_5_bla_bb, 
                                            df_input$n_5_whi, den_5_whi)
    
    df_input$ci_left_prev_ethn_ratio_5_bla_bb_whi <- ci_prev_ethn_ratio_5_bla_bb_whi[1]
    df_input$ci_right_prev_ethn_ratio_5_bla_bb_whi <- ci_prev_ethn_ratio_5_bla_bb_whi[2]
    
    
    
  }else if (ethn_granular == 'ethn_11'){

    
    df_input <- prev_str_obs(df_input, '11_bangl', 'n_11_bangl', den_11_bangl, prev_scale)
    df_input <- prev_str_obs(df_input, '11_bla_af', 'n_11_bla_af', den_11_bla_af, prev_scale)
    df_input <- prev_str_obs(df_input, '11_bla_ca', 'n_11_bla_ca', den_11_bla_ca, prev_scale)
    df_input <- prev_str_obs(df_input, '11_chi', 'n_11_chi', den_11_chi, prev_scale)
    df_input <- prev_str_obs(df_input, '11_ind', 'n_11_ind', den_11_ind, prev_scale)
    df_input <- prev_str_obs(df_input, '11_mix', 'n_11_mix', den_11_mix, prev_scale)
    df_input <- prev_str_obs(df_input, '11_pak', 'n_11_pak', den_11_pak, prev_scale)
    df_input <- prev_str_obs(df_input, '11_oth_asi', 'n_11_oth_asi', den_11_oth_asi, prev_scale)
    df_input <- prev_str_obs(df_input, '11_oth_bl', 'n_11_oth_bl', den_11_oth_bl, prev_scale)
    df_input <- prev_str_obs(df_input, '11_oth_eth', 'n_11_oth_eth', den_11_oth_eth, prev_scale)
    df_input <- prev_str_obs(df_input, '11_unkn', 'n_11_unkn', den_11_unkn, prev_scale)
    df_input <- prev_str_obs(df_input, '11_whi', 'n_11_whi', den_11_whi, prev_scale)
    
    
    #ratios:
    #Blangladeshi vs W
    df_input$prev_ethn_ratio_11_bangl_whi <- df_input[['prev_11_bangl']]/df_input[['prev_11_whi']]
    
    
    ci_prev_ethn_ratio_11_bangl_whi <- ci_prev_ratio(df_input$prev_ethn_ratio_11_bangl_whi,
                                                   df_input[['n_11_bangl']], den_11_bangl,
                                                   df_input[['n_11_whi']], den_11_whi)
    
    df_input$ci_left_prev_ethn_ratio_11_bangl_whi <- ci_prev_ethn_ratio_11_bangl_whi[1]
    df_input$ci_right_prev_ethn_ratio_11_bangl_whi <- ci_prev_ethn_ratio_11_bangl_whi[2]
    
    
    #Chinese vs W
    df_input$prev_ethn_ratio_11_chi_whi <- df_input[['prev_11_chi']]/df_input[['prev_11_whi']]
    
    ci_prev_ethn_ratio_11_chi_whi <- ci_prev_ratio(df_input$prev_ethn_ratio_11_chi_whi,
                                                  df_input[['n_11_chi']], den_11_chi,
                                                  df_input[['n_11_whi']], den_11_whi)
    
    df_input$ci_left_prev_ethn_ratio_11_chi_whi <- ci_prev_ethn_ratio_11_chi_whi[1]
    df_input$ci_right_prev_ethn_ratio_11_chi_whi <- ci_prev_ethn_ratio_11_chi_whi[2]
    
    
    #Indian vs W
    df_input$prev_ethn_ratio_11_ind_whi<- df_input[['prev_11_ind']]/df_input[['prev_11_whi']]
    
    ci_prev_ethn_ratio_11_ind_whi <- ci_prev_ratio(df_input$prev_ethn_ratio_11_ind_whi,
                                                    df_input[['n_11_ind']], den_11_ind,
                                                    df_input[['n_11_whi']], den_11_whi)
    
    df_input$ci_left_prev_ethn_ratio_11_ind_whi <- ci_prev_ethn_ratio_11_ind_whi[1]
    df_input$ci_right_prev_ethn_ratio_11_ind_whi <- ci_prev_ethn_ratio_11_ind_whi[2]
    
    
    #Pakistani vs W
    df_input$prev_ethn_ratio_11_pak_whi <- df_input[['prev_11_pak']]/df_input[['prev_11_whi']]
    
    ci_prev_ethn_ratio_11_pak_whi <- ci_prev_ratio(df_input$prev_ethn_ratio_11_pak_whi,
                                                    df_input[['n_11_pak']], den_11_pak,
                                                    df_input[['n_11_whi']], den_11_whi)
    
    df_input$ci_left_prev_ethn_ratio_11_pak_whi <- ci_prev_ethn_ratio_11_pak_whi[1]
    df_input$ci_right_prev_ethn_ratio_11_pak_whi <- ci_prev_ethn_ratio_11_pak_whi[2]
    
    
    #Black Afr vs W
    df_input$prev_ethn_ratio_11_bla_af_whi <- df_input[['prev_11_bla_af']]/df_input[['prev_11_whi']]
    
    ci_prev_ethn_ratio_11_bla_af_whi <- ci_prev_ratio(df_input$prev_ethn_ratio_11_bla_af_whi,
                                                        df_input[['n_11_bla_af']], den_11_bla_af,
                                                        df_input[['n_11_whi']], den_11_whi)
    
    df_input$ci_left_prev_ethn_ratio_11_bla_af_whi <- ci_prev_ethn_ratio_11_bla_af_whi[1]
    df_input$ci_right_prev_ethn_ratio_11_bla_af_whi <- ci_prev_ethn_ratio_11_bla_af_whi[2]
    
    
    
    #Black Carib vs W
    df_input$prev_ethn_ratio_11_bla_ca_whi <- df_input[['prev_11_bla_ca']]/df_input[['prev_11_whi']]
    
    ci_prev_ethn_ratio_11_bla_ca_whi <- ci_prev_ratio(df_input$prev_ethn_ratio_11_bla_ca_whi,
                                                         df_input[['n_11_bla_ca']], den_11_bla_ca,
                                                         df_input[['n_11_whi']], den_11_whi)
    
    df_input$ci_left_prev_ethn_ratio_11_bla_ca_whi <- ci_prev_ethn_ratio_11_bla_ca_whi[1]
    df_input$ci_right_prev_ethn_ratio_11_bla_ca_whi <- ci_prev_ethn_ratio_11_bla_ca_whi[2]
    
    
    
    
  }
  
  
  return(df_input)
  
}



get_SMR <- function(df_genpop, df, sex_flag){

  
  #'description:
  #'function to calculate SMR (standardised mortality ratio) and 95% ci
  #'returns: SMR, ci_left, ci_right, obs_deaths
  #'
  #@param df_genpop -dataframe - (counts per age and sex group in gen_pop)
  #@param df - phecode data (count per age and sex band)
  #@param sex_flag - df_res$sex_flag[1]

  
  #'usage:
  #' get_SMR(df_genpop, df, df_res$sex_flag)
  #' 
  #'   
  
  df_dt <-data.table(df)
  
  #count N in each strata 
  df_studypop_n <- df_dt[,.(n_strata_studypop=.N), by=.(age_band_23_01_2020, sex)]
  
  #count deaths in each strata - these are the observed death 
  df_studypop <- df_dt[,.(n_deaths_studypop=sum(death_flag)), by=.(age_band_23_01_2020, sex)]
  df_studypop <- dplyr::arrange(df_studypop, age_band_23_01_2020, sex)
  
  #merge count n in phecode and n deaths in phecode in age bands
  df_studypop <- merge(df_studypop_n, df_studypop, by = c('age_band_23_01_2020', 'sex'))
  
  #merge genpop with studypop(phecode) - it has death rate
  SMR_df <- merge(df_genpop, df_studypop, by = c('age_band_23_01_2020', 'sex'))
  
  #not needed - to keep with same 56m deno and events in sex-dep diseases
  #update pop if phenotype is sex-dependent (denominators, and events)
  # if(sex_flag =='1'){
  # 
  #   print('male phecode')
  # 
  #   #subset from events using sex in skinny
  #   SMR_df <- subset(SMR_df, (sex == '1'))
  # 
  # }else if(sex_flag =='2'){
  # 
  #   print('female phecode')
  # 
  #   #subset from events using sex in skinny
  #   SMR_df <- subset(SMR_df, (sex == '2'))
  # }

  #print(SMR_df)
  
  #calculate expected deaths as N in phecode in age and sex band * death rate in genpop 
  SMR_df$n_exp_deaths_studypop <- SMR_df$n_strata_studypop * SMR_df$rate_genpop
  
  #SMR as sum of observed deaths in study pop / sum of expected deaths in study pop 
  SMR <- sum(SMR_df$n_deaths_studypop)/sum(SMR_df$n_exp_deaths_studypop)
  
  obs_deaths <-sum(SMR_df$n_deaths_studypop)
  
  ci_left_SMR <- SMR - 1.96*(sqrt(sum(SMR_df$n_deaths_studypop))/sum(SMR_df$n_exp_deaths_studypop))
  ci_right_SMR <- SMR + 1.96*(sqrt(sum(SMR_df$n_deaths_studypop))/sum(SMR_df$n_exp_deaths_studypop))
  
  SMR_res <- c(SMR, ci_left_SMR,ci_right_SMR, obs_deaths)
  
  return(SMR_res)
  
}



get_denominator_from_table <- function(tablename, variable){
  
  #'description:
  #'gets total N unique deids in table
  
  #@param tablename - databricks table as input 
  #@param variable  - for counts
  
  #'usage:
  #' get_denominator_from_table('ccu013_02_dp_skinny_patient_23_01_2020_phe_cohort_joined_1ymort_eth', 'sex')
  #' 
  #' 
  #' 
  sqlden = paste0("
  SELECT ", variable, " , count(distinct(NHS_NUMBER_DEID)) as n_indiv 
  FROM dsa_391419_j3w9t_collab.", tablename, " WHERE SEX IN (1,2) 
  AND IMD_quintile not like ('Unknown%')
  AND age_23_01_2020 >= 0 and age_23_01_2020 <= 100
  GROUP BY ", variable, " ORDER BY ", variable )
  counts <- DBI::dbGetQuery(con, sqlden)
  return(counts)
  
}



get_sex_specific_denominator_from_table <- function(tablename, variable, sex_skinny){
  
  #'description:
  #'gets total N unique deids in table by sex
  
  #@param tablename - databricks table as input 
  #@param variable  - for counts
  #@param sex_skinny - string - #variable for sex_specific count 
  
  #'usage:
  #' get_denominator_from_table('ccu013_02_dp_skinny_patient_23_01_2020_phe_cohort_joined_1ymort_eth', 'sex')
  #' 
  #' 
  
  sqlden = paste0("
  SELECT ", variable, " , count(distinct(NHS_NUMBER_DEID)) as n_indiv 
  FROM dars_nic_391419_j3w9t_collab.", tablename, " WHERE SEX = '", sex_skinny, "'
  AND IMD_quintile not like ('Unknown%')
  AND age_23_01_2020 >= 0 and age_23_01_2020 <= 100
  GROUP BY ", variable, " ORDER BY ", variable )
  counts <- DBI::dbGetQuery(con, sqlden)
  return(counts)
  
}



fun_mask_round <- function(df_in, 
                          col_in){
  
  #'description:
  #'#function to mask and round counts before exporting
  
  #@param df_in: input df 
  #@param col_in: cols from the df_in to process through the function (round and mask) - #in the df this can only be counts 
    
  #'usage:
  #'  fun_mask_round(counts, 'n_indiv')
  #' 
  
  ######mask values under 10###
  #output col
  col_m <- paste(col_in, '_m', sep = '')
  
  #mask in col_in and save in col_m in the same df  
  #df_in[[col_m]]<- ifelse(df_in[[col_in]] < 10, NA, df_in[[col_in]])
  #asign -99 if n > 0 and < 10
  df_in[[col_m]]<- ifelse(df_in[[col_in]] > 0 & df_in[[col_in]] < 10, -9, df_in[[col_in]])
  
  #print(class(df_in[[col_m]]))
  
  #####round to nearest multiple of 5#####  
  #output col
  col_m_r <- paste(col_m, '_r', sep = '')
  
  #col_m needs to be numeric 
  df_in[[col_m_r]]<- round_any(df_in[[col_m]], 5)
  
  #drop input and interm m columns   
  df_in[[col_in]]<- NULL
  df_in[[col_m]]<- NULL
  
  #move new columns to front
  df_in <- df_in %>% relocate(ends_with("_m_r"))
  
  return(df_in)
  
}


#
ci_prop <- function(estimate, estimate_escale, denominator){
  
  
  #'description:
  #ci for prevalence as proportion
  
  #@param  estimate
  #@param estiamte scale
  #@param denominator 
  
  #'usage:
  #'  ci_prop(df_res$prev_fem, prev_scale, den_female)
  #' 
  #' 
  ci_left <- estimate - (1.96 * sqrt((estimate * (estimate_escale-estimate))/denominator))
  ci_right <- estimate + (1.96 * sqrt((estimate * (estimate_escale-estimate))/denominator))
  
  ci <- c(ci_left, ci_right)
  return(ci)
  
}




ci_prev_ratio <- function(prev_ratio, x1, n1, x2, n2){
  
  
  #'description:
  ##function to calculate CI of prev ratio 
  
  #@param #prev_ratio: numeric value prev_ratio (obtained as prev1/prev2)
  #@param x1: cases 1 
  #@param n1: deno 1
  #@param x2: cases 2
  #@param n2: deno 2
  
  #'usage:
  #'    ci_prev_sex_ratio <- ci_prev_ratio(df_res$prev_sex_ratio, 
  #'                                      df_res$n_females, den_female, 
  #'                                       df_res$n_males, den_male)
  #' 
  
  ci_left <- prev_ratio * exp(-1.96*sqrt((1/x1) - (1/n1) + (1/x2) - (1/n2)))
  ci_right <- prev_ratio * exp(1.96*sqrt((1/x1) - (1/n1) + (1/x2) - (1/n2)))
  
  ci <- c(ci_left, ci_right)
  return(ci)
  
}



#in MM (main analysis has other in 01_atlas_main) - OLD FUNCTION
prev_ratio_ci_vect <- function(df, prev_ratio, x1, n1, x2, n2, ci_left, ci_right){
  
  #init
  df[[ci_left]] <- ''
  df[[ci_right]] <- ''
  
  #calc
  df[[ci_left]] <- df[[prev_ratio]] * exp(-1.96*sqrt((1/df[[x1]]) - (1/df[[n1]]) + (1/df[[x2]]) - (1/df[[n2]])))          
  df[[ci_right]] <- df[[prev_ratio]] * exp(1.96*sqrt((1/df[[x1]]) - (1/df[[n1]]) + (1/df[[x2]]) - (1/df[[n2]])))
  
  return(df)
}


run_phecode_basic <- function(p, tablename){
  
  #'description:
  #'#function similar structure as run_phecode 
  
  #@param df_data_driven df (that includes set descriptor and phecodes) - #but simple, only to run basic count - for testing
  #@param p - string -  phecode 
  #@param tablename - string table of events in databricks
  #sure query etc are aligned with run_phecode
  
  #'usage:
  #' run_phecode_basic('X401_1', 'ccu013_phedatav5_unrolled_first_sex_dep')}
  #' 
  
  # Get data
  print(paste0("Extracting data for ", p))
  #print(paste0("sex flag is ", sex_flag))
  
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
    datediff(MIN(ph.date), date(dob))/365.2425 as age
    FROM dars_nic_391419_j3w9t_collab.", tablename, " ph
    INNER JOIN  dars_nic_391419_j3w9t_collab.ccu013_02_dp_skinny_patient_23_01_2020_phe_cohort_joined_1ymort c
    ON ph.person_id_deid = c.NHS_NUMBER_DEID 
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
               c.death_flag
               HAVING MIN(ph.date) <= to_date('2020-01-23', 'yyyy-MM-dd') 
               AND MIN(ph.date) >= date(c.dob)")
  
  
  df <- DBI::dbGetQuery(con, sql)
  
  #find sex in phe_dict for this phecode 
  print(paste0("Extracting sex for ", p))
  
  sql_sex = paste0("SELECT sex as sex_flag
            FROM dars_nic_391419_j3w9t_collab.ccu013_lkp_phev5_phedict d 
                                    WHERE d.phecode ='", p,"'")
  
  df_sex_flag <- DBI::dbGetQuery(con, sql_sex)
  
  #print(paste0("both queries done for ", p))
  
  #ncol must be same length as length colnames
  df_res <- data.frame(matrix(nrow = 1, ncol=5))
  colnames(df_res) <- c("cohort", "sex_flag", "n_cohort",
                        "n_males", "n_females")
  
  #colnames(df_res) <- 'phecode'
  df_res$cohort <- p
  print("creating df")
  df_res$sex_flag <- df_sex_flag[1,]
  
  #TEMP sex _flag from lkp: 
  #sex_flag lkp in dict: 1 female, 2 male
  #we will have new version to be aligned with skinny (1 male; 2 female)
  if(df_res$sex_flag =='1'){
    
    print('male phecode')
    df <- subset(df, (sex == '1'))
    
  }else if(df_res$sex_flag =='2'){
    
    print('female phecode')
    df <- subset(df, (sex == '2'))
  }
  
  # There are no cases, skip, return empty df_res
  if (nrow(df) == 1){
    df_res$n_cohort <- 1
    print(paste0("Skip due to lack of cases ", p))
    return(df_res)
  } else {
    
    #if sex_flag is 'both' do not filter
    df_res$n_cohort=length(unique(df[,'person_id_deid']))
    
    df_res$n_males <- nrow(subset(df, sex == 1))
    df_res$n_females <-  nrow(subset(df, sex == 2))
    
    return(df_res)
    
  }
  
}




