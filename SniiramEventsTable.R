library(dplyr)
library(RMySQL)
library(DBI)
library(data.table)
library(dummies)
require(ggplot2)
library(zoo)
library(bit64)
library(reshape2)
# ---------------------------------------------------------------- COLLECT DATA FROM SNIIRAM

# set the connexion with SQL database
con <- dbConnect(MySQL(), user="root", password="R0m@1n", host="127.0.0.1")

# import data in R
for (i in 2010:2013){
i = 2010
      dcir_prs <- as.data.table(dbGetQuery(con, paste0("SELECT    NUMERO_ENQ, 
                                                                  EXE_SOI_DTD, 
                                                                  EXE_SOI_DTF, 
                                                                  PFS_EXE_NUM, 
                                                                  PFS_PRE_NUM, 
                                                                  PRS_NAT_REF, 
                                                                  ORG_CLE_NUM, 
                                                                  ETB_PRE_FIN, 
                                                                  PRE_PRE_DTD, 
                                                                  PSE_SPE_COD,
                                                                  BEN_AMA_COD,
                                                                  BEN_SEX_COD,
                                                                  PRS_ACT_QTE, 
                                                                  key_jointure
                                                            FROM CONSTANCES.c_s",i,"_dcir_prs
                                                            WHERE BSE_PRS_NAT = PRS_NAT_REF and CPL_PRS_NAT = 0;"))) 
                                                            # on ne prend que les actes de base
        
      dcir_pha <- as.data.table(dbGetQuery(con, paste0("SELECT    PHA_ACT_QSN, 
                                                                  PHA_PRS_C13, 
                                                                  key_jointure
                                                            FROM CONSTANCES.c_s",i,"_dcir_pha;")))
        
      dcir_bio <- as.data.table(dbGetQuery(con, paste0("SELECT    BIO_PRS_IDE, 
                                                                  BIO_ACT_QSN, 
                                                                  key_jointure
                                                            FROM CONSTANCES.c_s",i,"_dcir_bio;")))
        
      dcir_ete <- as.data.table(dbGetQuery(con, paste0("SELECT    ETB_EXE_FIN, 
                                                                  ETE_GHS_NUM,
                                                                  ETE_IND_TAA,
                                                                  key_jointure
                                                            FROM CONSTANCES.c_s",i,"_dcir_ete;")))
      
      pmsi_mco_c <- as.data.table(dbGetQuery(con, paste0("SELECT  NUMERO_ENQ,
                                                                  ENT_DAT,
                                                                  SOR_DAT,
                                                                  key_jointure
                                                            FROM CONSTANCES.c_f",i,"_pmsimco_c;")))

      pmsi_mco_b <- as.data.table(dbGetQuery(con, paste0("SELECT  DGN_PAL, 
                                                                  DGN_REL,
                                                                  key_jointure
                                                         FROM CONSTANCES.c_f",i,"_pmsimco_b;")))

      pmsi_mco_d <- as.data.table(dbGetQuery(con, paste0("SELECT  ASS_DGN,
                                                                  key_jointure
                                                         FROM CONSTANCES.c_f",i,"_pmsimco_d;")))
      
      pmsi_mco_um <- as.data.table(dbGetQuery(con, paste0("SELECT DGN_PAL,
                                                                  DGN_REL,
                                                                  key_jointure
                                                         FROM CONSTANCES.c_f",i,"_pmsimco_um;")))
        
        
      # ---------------------------------------------------------------- MERGE TABLES
         
      # joining data
      setkey(dcir_prs, "key_jointure")
      setkey(dcir_pha, "key_jointure")
      setkey(dcir_bio, "key_jointure")
      setkey(dcir_ete, "key_jointure")
      setkey(pmsi_mco_c, "key_jointure")
      setkey(pmsi_mco_b, "key_jointure")
      setkey(pmsi_mco_d, "key_jointure")
      setkey(pmsi_mco_um, "key_jointure")
      prs_ete <- merge(dcir_prs, dcir_ete, all.x = TRUE)
      pha <- merge(dcir_pha, prs_ete, all.x = TRUE)
      bio <- merge(dcir_bio, prs_ete, all.x = TRUE)
      mco_b <- merge(pmsi_mco_b, pmsi_mco_c, all.x = TRUE)
      mco_d <- merge(pmsi_mco_d, pmsi_mco_c, all.x = TRUE)
      mco_um <- merge(pmsi_mco_um, pmsi_mco_c, all.x = TRUE)

      
      
      
      # ---------------------------------------------------------------- CREATE CONSULT AND INDIV TABLES TABLES    
      
      # cleaning tables
      # prs
      prs <- prs_ete %>% # length = 7127791
                  filter((ETE_GHS_NUM == 0) | is.na(ETE_GHS_NUM), (ETE_IND_TAA != 1) | is.na(ETE_IND_TAA)) %>%          
                        # On élimine tout les infos contenues dans les séjours (des hopitaux privés) 
                        # et les infos issues des consultations ou actes externes des hopitaux publics
                  select(-ETE_GHS_NUM, -ETE_IND_TAA, -key_jointure, -BEN_AMA_COD, -BEN_SEX_COD)
                        # length = 6994020 (1728837)
      cols <- colnames(prs) 
      agg_cols <- cols[-which(cols == "PRS_ACT_QTE")]   
                        # "NUMERO_ENQ"  "EXE_SOI_DTD" "EXE_SOI_DTF" "PFS_EXE_NUM" "PFS_PRE_NUM" "PRS_NAT_REF" 
                        # "ORG_CLE_NUM" "ETB_PRE_FIN" "PRE_PRE_DTD" "ETB_EXE_FIN"
      prs <- prs[, .(PRS_ACT_QTE = sum(PRS_ACT_QTE)), by=agg_cols] # length = 6133669 (1566213)
      agg_cols <- agg_cols[-which(cols == "PSE_SPE_COD")]
      prs <- prs[, .(PSE_SPE_COD = paste(PSE_SPE_COD, collapse = "_")), by=agg_cols]
      prs_bugs <- prs %>% filter(grepl("_", PSE_SPE_COD))
      
        # -- consultations medecins (au moins une consultation dans la journée de ce spécialiste)
        consult <- prs %>% 
                  filter(PSE_SPE_COD > 0, PSE_SPE_COD < 99, PRS_ACT_QTE > 0) %>% # length = 1565689
                  select(NUMERO_ENQ, EXE_SOI_DTD, PSE_SPE_COD) %>%
                  unique() %>%   
                        # ces deux lignes sélectionne un seul événement de spécialité par jour (avec une quantité positive)
                  rename(ID = NUMERO_ENQ, date = EXE_SOI_DTD, cat = PSE_SPE_COD) # length = 1497456
        consult$cat <- paste0("spe_", consult$cat)
        saveRDS(consult, file = paste0("Odata/consult_",i,".rds"))
       
        # -- individus (a priori l'info est mieux dans Constances)
        indiv <- prs_ete %>%
                  select(NUMERO_ENQ, BEN_AMA_COD, BEN_SEX_COD) %>%
                  rename(ID = NUMERO_ENQ) %>%
                  filter((BEN_SEX_COD == 1) | (BEN_SEX_COD == 2)) %>%
                  filter((BEN_AMA_COD > 10) & (BEN_AMA_COD < 100))
        indiv <- indiv[, .(age = max(BEN_AMA_COD), sex = round(mean(BEN_SEX_COD))), by = ID] 
                        # length = 188017 (sur 188250 différents NUM_ENQ)
        saveRDS(indiv, file = paste0("Odata/indiv_",i,".rds"))
        
        
        
        
        
      # ---------------------------------------------------------------- CREATE MED TABLE 
        
      # pha
      pha <- pha %>% # length = 4131787
                  filter((ETE_GHS_NUM == 0) | is.na(ETE_GHS_NUM), (ETE_IND_TAA != 1) | is.na(ETE_IND_TAA)) %>% 
                        # cette partie ne filtre aucune ligne (cf expertise de M/J)
                  select(-ETE_GHS_NUM, -ETE_IND_TAA, -key_jointure, -BEN_AMA_COD, - BEN_SEX_COD, -PRS_ACT_QTE, -PSE_SPE_COD)
      cols <- colnames(pha)
      agg_cols <- cols[-which(cols == "PHA_ACT_QSN")] 
                        # "PHA_PRS_C13" "NUMERO_ENQ"  "EXE_SOI_DTD" "EXE_SOI_DTF" "PFS_EXE_NUM" "PFS_PRE_NUM" 
                        # "PRS_NAT_REF" "ORG_CLE_NUM" "ETB_PRE_FIN" "PRE_PRE_DTD" "ETB_EXE_FIN"
      pha <- pha[, .(PHA_ACT_QSN = sum(PHA_ACT_QSN)), by=agg_cols] # length = 3970038
        
        
        # -- medicaments (au moins un achat d'un médicament de la classe ATC dans la journée)
        cod_med <- fread('Idata/cod_med.csv') %>% select(PHA_CIP_C13, PHA_ATC_C07)
        setkey(pha, "PHA_PRS_C13")
        setkey(cod_med, "PHA_CIP_C13")
        med <- merge(pha, cod_med, by.x = "PHA_PRS_C13", by.y = "PHA_CIP_C13" , all.x = TRUE)
        med <- med %>% 
                  filter(PHA_ACT_QSN > 0) %>% # length = 3969224
                  filter(!is.na(PHA_ATC_C07), PHA_ATC_C07 !="") %>% # length = 3902548
                  select(NUMERO_ENQ, EXE_SOI_DTD, PHA_ATC_C07) %>%
                  unique() %>%  
                        # ces deux lignes sélectionne un seul événement par jour (avec une quantité positive - 
                        # en ayant tenu compte des régularisation et des annulations)
                  rename(ID = NUMERO_ENQ, date = EXE_SOI_DTD, cat = PHA_ATC_C07) # length = 3816839
        med$cat <- paste0("med_", med$cat)
        saveRDS(med, file = paste0("Odata/med_",i,".rds"))
        
        
        
        
        
      # ---------------------------------------------------------------- CREATE BIO TABLE  
      
      # bio
      bio <- bio %>% # length = 2110882
                  filter((ETE_GHS_NUM == 0) | is.na(ETE_GHS_NUM), (ETE_IND_TAA != 1) | is.na(ETE_IND_TAA)) %>% 
                  select(-ETE_GHS_NUM, -ETE_IND_TAA, -key_jointure, -BEN_AMA_COD, -BEN_SEX_COD, -PRS_ACT_QTE, -PSE_SPE_COD) 
                        # length = 2012174
      cols <- colnames(bio)
      agg_cols <- cols[-which(cols == "BIO_ACT_QSN")] 
                        # "BIO_PRS_IDE" "NUMERO_ENQ"  "EXE_SOI_DTD" "EXE_SOI_DTF" "PFS_EXE_NUM" "PFS_PRE_NUM" 
                        # "PRS_NAT_REF" "ORG_CLE_NUM" "ETB_PRE_FIN" "PRE_PRE_DTD" "ETB_EXE_FIN"
      bio <- bio[, .(BIO_ACT_QSN = sum(BIO_ACT_QSN)), by=agg_cols] 
                        # length = 1975948
        
        
        # -- actbio (au moins un acte bio de ce code dans la journée) >>>>>>>>>>>>>> ajouter les descriptions des codes bio
        actbio <- bio %>% 
                    filter(BIO_ACT_QSN > 0) %>% # length = 1975267
                    select(NUMERO_ENQ, EXE_SOI_DTD, BIO_PRS_IDE) %>% 
                    unique() %>% # ces deux lignes sélectionne un seul événement par jour (avec une quantité positive)
                    rename(ID = NUMERO_ENQ, date = EXE_SOI_DTD, cat = BIO_PRS_IDE) # length = 1963562
        actbio$cat <- paste0("bio_", actbio$cat)
        saveRDS(actbio, file = paste0("Odata/actbio_",i,".rds"))
      
        
        
        
        
      # ---------------------------------------------------------------- CREATE HOSPIT TABLE  
        
      # hospit # faire une expertise des dates pour le pmsi (ces dates sont apparues récemment dans les extractions)
      mco_b <- mco_b %>%
                  select(-key_jointure) %>%
                  melt(id = c("NUMERO_ENQ", "ENT_DAT", "SOR_DAT")) %>%
                  select(-variable) %>% # on met ensemble le diagnostic principal et relié dans la meme colonne
                  unique() %>% # il ne rester plus que les colonnes "NUMERO_ENQ", "ENT_DAT", "SOR_DAT" et "value"
                  rename(ID = NUMERO_ENQ, cat = value, date_d = ENT_DAT, date_f = SOR_DAT)
      setcolorder(mco_b, c("ID", "cat", "date_d", "date_f"))
      
      mco_d <- mco_d %>%
            select(-key_jointure) %>% # il ne rester plus que les colonnes "NUMERO_ENQ", "ENT_DAT", "SOR_DAT" et "ASS_DGN"
            unique() %>%
            rename(ID = NUMERO_ENQ, cat = ASS_DGN, date_d = ENT_DAT, date_f = SOR_DAT)
      setcolorder(mco_d, c("ID", "cat", "date_d", "date_f"))
      
      mco_um <- mco_um %>%
            select(-key_jointure) %>%
            melt(id = c("NUMERO_ENQ", "ENT_DAT", "SOR_DAT")) %>% # idem mco_b
            select(-variable) %>%
            unique() %>%
            rename(ID = NUMERO_ENQ, cat = value, date_d = ENT_DAT, date_f = SOR_DAT)
      setcolorder(mco_um, c("ID", "cat", "date_d", "date_f"))
      
      hospit <- rbind(mco_b, mco_d, mco_um) %>% filter(cat != "") %>% unique() # on ne récuppérer tous les diagnostic distincts
      saveRDS(hospit, file = paste0("Odata/hospit",i,".rds"))
}


# deconnect 
dbDisconnect(con)

# save tables
# for (i in 2010:2013) {
#       save(get(paste0("consult_", i)), file = paste0("Odata/consult_",i,".Rda"))
#       save(get(paste0("indiv_", i)), file = paste0("Odata/indiv_",i,".Rda"))
#       save(get(paste0("med_", i)), file = paste0("Odata/med_",i,".Rda"))
#       save(get(paste0("actbio_", i)), file = paste0("Odata/actbio_",i,".Rda"))
# }

