library(dplyr)
library(RMySQL)
library(DBI)
library(data.table)
library(dummies)
require(ggplot2)
library(zoo)


# ---------------------------------------------------------------- SELECT DIABETIC PATIENTS

# load / merge tables
quest_MDV <- fread("Idata/1_DATA_MDV.csv", sep = ";")
quest_MED <- fread("Idata/1_DATA_MED.csv", sep = ";")
paraclin <- fread("Idata/2_DATA_PARACLIN.txt", header = TRUE, sep = ";")
pop_info <- fread("Idata/0_DATA_POP.csv", sep = ";")
  
# identify diab patients from MDV
diab_patients_MDV <- quest_MDV %>% filter((AQ_DIABETE_Trait_n == 1) | (AQ_DIABETE_Inject_n == 1) | ((AQ_DIABETE_Consulte_n == 1) & (AQ_DIABETE_DitMed_n == 1)))
diab_patients_MDV_wt <- quest_MDV %>% filter((AQ_DIABETE_Trait_n == 1) | (AQ_DIABETE_Inject_n == 1)) %>% select(PROJ_NCONSTANCES)
MDV_wo_trait <- quest_MDV %>% filter((!(AQ_DIABETE_Trait_n == 1) | is.na(AQ_DIABETE_Trait_n)) & (!(AQ_DIABETE_Inject_n == 1) | is.na(AQ_DIABETE_Inject_n == 1)))
diab_patients_MDV_wo_trait <- MDV_wo_trait %>% filter((AQ_DIABETE_Consulte_n == 1) & (AQ_DIABETE_DitMed_n == 1))
diab_patients_MDV_consulte_wo_diag <- MDV_wo_trait %>% filter((AQ_DIABETE_Consulte_n == 1) & (!(AQ_DIABETE_DitMed_n == 1) | is.na(AQ_DIABETE_DitMed_n))) %>% select(PROJ_NCONSTANCES)
diab_patients_MDV_diag_wo_consult <- MDV_wo_trait %>% filter((AQ_DIABETE_DitMed_n == 1) & (!(AQ_DIABETE_Consulte_n == 1) | is.na(AQ_DIABETE_Consulte_n))) %>% select(PROJ_NCONSTANCES)
remove(MDV_wo_trait)

diab_patients_MDV_type1 <- diab_patients_MDV %>% filter((AQ_DIABETE_Age_n < 45) & (AQ_DIABETE_Inject_n == 1) & (AQ_DIABETE_InjectAge_n - AQ_DIABETE_Age_n < 2)) %>% select(PROJ_NCONSTANCES)
diab_patients_MDV_type2 <-  diab_patients_MDV %>% filter(!(PROJ_NCONSTANCES %in% diab_patients_MDV_type1$PROJ_NCONSTANCES)) %>% select(PROJ_NCONSTANCES)


# identify patients from MED
diab_patients_MED_type1 <- quest_MED %>% filter(AQ_MED_EndDiabet1 == 1) %>% select(PROJ_NCONSTANCES)
diab_patients_MED_type2 <- quest_MED %>% filter(AQ_MED_EndDiabet2 == 1) %>% select(PROJ_NCONSTANCES)


# overlaps bewteen questionnaires
diab_patients_commun_type1 <- inner_join(diab_patients_MDV_type1, diab_patients_MED_type1)
diab_patients_commun_type2 <- inner_join(diab_patients_MDV_type2, diab_patients_MED_type2)


# identify volonteers with a high glycemia 
diab_patients_glyc <- paraclin %>% filter(PARACL_BIO_Glyc > 7) %>% select(PROJ_NCONSTANCES)
diab_patients_glyc_patients <- diab_patients_glyc %>% filter(PROJ_NCONSTANCES %in% diab_patients_MDV$PROJ_NCONSTANCES)
diab_patients_glyc_notpatients <- diab_patients_glyc %>% filter(!(PROJ_NCONSTANCES %in% diab_patients_MDV$PROJ_NCONSTANCES))


# regroup all potential patients in a table
all_diab_patients_MDV <- quest_MDV %>% filter((AQ_DIABETE_Trait_n == 1) | (AQ_DIABETE_Inject_n == 1) | (AQ_DIABETE_Consulte_n == 1) | (AQ_DIABETE_DitMed_n == 1)) %>% select(PROJ_NCONSTANCES)
all_diab_patients <- unique(do.call("rbind", list(all_diab_patients_MDV, diab_patients_commun_type1, diab_patients_commun_type2, diab_patients_glyc)))



# ---------------------------------------------------------------- COLLECT DATA FROM SNIIRAM

for (i in 2010:2013) {
      consult_diab <- readRDS(paste0("Odata/consult_",i,".rds")) %>%
                        filter(ID %in% all_diab_patients$PROJ_NCONSTANCES)
      med_diab <- readRDS(paste0("Odata/med_",i,".rds")) %>%
                        filter(ID %in% all_diab_patients$PROJ_NCONSTANCES)
      actbio_diab <- readRDS(paste0("Odata/actbio_",i,".rds")) %>%
                        filter(ID %in% all_diab_patients$PROJ_NCONSTANCES)
      assign(paste0("events_diab_",i), rbind(consult_diab, med_diab, actbio_diab))
}
rm(consult_diab)
rm(med_diab)
rm(actbio_diab)


# join 2010-2013 tables
events_diab_2010_2013 <- rbind(events_diab_2010, events_diab_2011, events_diab_2012, events_diab_2013)
rm(events_diab_2010)
rm(events_diab_2011)
rm(events_diab_2012)
rm(events_diab_2013)


# indiv table
indiv_diab_2013 <- readRDS("Odata/indiv_2013.rds") %>%
      filter(NUMERO_ENQ %in% all_diab_patients$PROJ_NCONSTANCES)

# save tables
saveRDS(events_diab_2010_2013, file = "Odata/events_diab_2010_2013.rds")
saveRDS(indiv_diab_2013, file = "Odata/indiv_diab_2013.rds")





# ---------------------------------------------------------------- BUILD POPULATION SPECIFIC TABLE

# load tables
events_diab_2010_2013 <- readRDS("Odata/events_diab_2010_2013.rds")


# filtre_ATC <- function(x) {ifelse(grepl("^A10", x), substr(x, 1, 5), substr(x, 1, 3) )}
# pha_prs_diab_1$cat <- lapply(pha_prs_diab_1$PHA_ATC_C07, filtre_ATC)


# clean health_events table
events_diab_2010_2013 <- filter(events_diab_2010_2013, cat!='med_NA', cat!='med_') # à enlever quand corrigé dans SniiramEventsTable
events_diab_2010_2013$date <- as.Date(events_diab_2010_2013$date, "%Y-%m-%d")
events_diab_2010_2013 <- events_diab_2010_2013 %>% mutate(year=format(date, "%Y"))
events_diab_2010_2013$quarter <- as.yearqtr(events_diab_2010_2013$date, format = "%Y-%m-%d")


# ---------------------------------------------------------------- DATA ANALYSIS: INCIDENCES




# construction de la table d'incidence

quest_MDV_age_sex <- left_join(quest_MDV, pop_info)
quest_MDV_age_sex$diab_situation <- "not_sick"
quest_MDV_age_sex$diab_situation[which(quest_MDV_age_sex$PROJ_NCONSTANCES %in% diab_patients_MDV_w_trait$PROJ_NCONSTANCES)] <- "diag_trait"
quest_MDV_age_sex$diab_situation[which(quest_MDV_age_sex$PROJ_NCONSTANCES %in% diab_patients_MDV_wo_trait$PROJ_NCONSTANCES)] <- "diag_notrait"
quest_MDV_age_sex$diab_situation[which(quest_MDV_age_sex$PROJ_NCONSTANCES %in% diab_patients_glyc_notpatients$PROJ_NCONSTANCES)] <- "notdiag"
quest_MDV_age_sex$diab_situation <- as.factor(quest_MDV_age_sex$diab_situation)


quest_MDV_age_sex$clas_age <- cut(floor(quest_MDV_age_sex$FM_IncluAge), breaks = c(18,50,60,100), right = FALSE)
levels(quest_MDV_age_sex$clas_age) <- c('29-50 ans','50-60 ans', '60 ans et plus')
quest_MDV_age_sex$FM_Sexe <- as.factor(quest_MDV_age_sex$FM_Sexe)


all <- TDB(quest_MDV_age_sex, quest_MDV_age_sex$diab_situation, quest_MDV_age_sex$FM_Sexe, quest_MDV_age_sex$clas_age, c("N diag_notrait", "% diag_notrait", "N diag_trait", "% diag_trait", "N not_sick", "% not_sick", "N notdiag", "% notdiag"))
all_t <- tbl_char(all, c("", "", "N diag_notrait", "% diag_notrait", "N diag_trait", "% diag_trait", "N not_sick", "% not_sick", "N notdiag", "% notdiag"), c('29-50 ans','50-60 ans', '60 ans et plus', 'ensemble'))
p <- graph(all, 4)


all <- TDB(iab_table, iab_table$diab_situation, iab_table$FM_Sexe, quest_MDV_age_sex$clas_age, c("N diag_notrait", "% diag_notrait", "N diag_trait", "% diag_trait", "N not_sick", "% not_sick", "N notdiag", "% notdiag"))
all_t <- tbl_char(all, c("", "", "N diag_notrait", "% diag_notrait", "N diag_trait", "% diag_trait", "N not_sick", "% not_sick", "N notdiag", "% notdiag"), c('29-50 ans','50-60 ans', '60 ans et plus', 'ensemble'))
p <- graph(all, 4)


# ----------------------------------------------------------------END


str(diab_table$diab_situation, list.len = 999)
summary(quest_MDV_age_sex)
table(quest_MDV_age_sex$diab_situation)




# diab_table <- filter(diab_table, years_since_diag >=0, years_since_diag < 99)



all_t_df <- data.frame(all_t)
ggplot(data  = all_t_df, aes(x=Employer, y=value, fill=factor(variable)))






