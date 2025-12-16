## Atlas for health in NHS SDE
## Project ccu013_02
This directory contains the code developed in the SDE. It has been run to generate patient level estimates. Code and estimates were then requested for safe export of the environment and were approved by NHS England. 


### Databricks: 
Notebooks written using SQL, python, PySpark. 

Code for processing: 

* Disease Catalogue and HES data 
* Cohort  
* ICS
* Prescriptions 
* Sets
* Denominator samples 


**Disease Catalogue and HES data:** 

* 0001a_Load_New_Lookups_Improved_Map_v200
* 0002_Pivot_HES_APC
* 0003_Map_HES_to_phecodes_v200
* 0004_Explode_Phecodes_v200


**Cohort:**

* 000_Define_covid_cohort.ipynb
* 001_Create_skinny_covid.ipynb
* 001bb_create_covid_most_severe_event
* 002_Create_counts_strata_SMR_export.py
* 010_ethnicity_granular_prodDates.ipynb
* 010_standpop.ipynb

**ICS:**

* 007_ICS_gp.ipynb

**Medications:**

* 008_med_gp_bnf.ipynb

**Hospitalisations:**

* 006c_counts_hospitalisations_create_tables.ipynb
* 006c_counts_hospitalisations_results.ipynb

**Sets:**

* 013_sets.ipynb

**Samples:**

* 014_denominators_run_random.ipynb


### RStudio: 
R Scripts and functions, including SQL code.

Code for analyses of patient level data in various domains

* Epidemiological analyses
* Geography  
* Multimorbidity 
* Medications
* Table 1 
* Hospitalisations 
* Disease sets 
* Denominator samples 


**Epidemiological estimates:** 

* 01_atlas_main.R
* 00_atlas_functions.R

**Geography:**

* 01b_atlas_main_ics.R

**Multimorbidity:**

**Medications:**

**Table 1:**
* 07_table1.R
* 07_table1_HES_apc_noHES_apc.R
* 07_table1_HES_apc_phe_noHES.R
* 07_table1_HES_phe_noHES.R


**Hospitalisations:**

* 07_admissions.R

**Sets:**

* 07_table1_sets.R
* 07_table1_sets_groups.R
* 07_table1_sets_groups_supersets_pairs.R
* 07_table1_sets_groups_supersets.R
