setwd("~/Desktop/CONSTANCES/Romain-Elie/data")
library(dplyr)
library(dummies)
require(ggplot2)
library(zoo)
library(TraMineR)


# load and merge tables
health_events_2010 <- read.csv("health_events_2010.csv")
health_events_2011 <- read.csv("health_events_2011.csv")
health_events_2012 <- read.csv("health_events_2012.csv")
health_events_2013 <- read.csv("health_events_2013.csv")
health_events <- rbind(health_events_2010,health_events_2011, health_events_2012,health_events_2013)
health_events <- health_events %>% select(-quant, -date_f)
health_events$date_d <- as.Date(health_events$date_d, "%Y-%m-%d") 
health_events$mois <- format(health_events$date_d, "%m")
health_events$trimestre <- as.yearqtr(health_events$date_d, format = "%Y-%m-%d")


# keep only med_A10 events + unique per quarter
health_eventsA10 <- health_events %>% filter(grepl("^med_A10", health_events$cat)) %>% select(NUMERO_ENQ, cat, trimestre) %>% distinct %>% arrange(cat)

# agregate events per quarter
health_eventsA10agg <- aggregate(health_eventsA10$cat, by=list(NUMERO_ENQ = health_eventsA10$NUMERO_ENQ, trimestre = health_eventsA10$trimestre), FUN = function(x) paste(x, collapse = "_"))
health_eventsA10agg <- rename(health_eventsA10agg, agg_cat = x)

# select only patients that have an empty first quarter
health_eventsA10agg_NOTemptystartpatients <- health_eventsA10agg %>% filter(grepl("2010 Q1", health_eventsA10agg$trimestre))
health_eventsA10agg_emptystartpatients <- health_eventsA10agg %>% filter(!(health_eventsA10agg$NUMERO_ENQ %in% health_eventsA10agg_NOTemptystartpatients$NUMERO_ENQ))


# prepare sequences table
prepare_seq_table <- function(dataframe, n_cat){

    # count distinct categories
    assign("df", dataframe) 
    sapply(df , function(x) length(unique(x)))
    cat_count <- count(df, agg_cat) %>% arrange(-n)
    df$agg_cat[!(df$agg_cat %in% cat_count$agg_cat[1:n_cat])] = "other"
    sapply(df , function(x) length(unique(x)))
    
    #Transformer la variable trimestre en colonne
    dummy_df= dummy.data.frame(df, names="trimestre")
    colnames(dummy_df) <- gsub("^trimestre", "", colnames(dummy_df))
    dummy_colnames <- grep("^20", colnames(dummy_df), value=TRUE)
    for (i in 1:nrow(dummy_df)) {
      for (col in dummy_colnames) {
        if (dummy_df[[col]][i] == 1) {
          dummy_df[[col]][i] = dummy_df$agg_cat[i]
          }
        else {
          dummy_df[[col]][i]=''
        }
      }
    }
    dummy_df <-  dummy_df %>% select(-agg_cat)
    dummy_df <- aggregate(.~NUMERO_ENQ, data = dummy_df, FUN = function(x) paste(x, collapse = ""))
    for (col in dummy_colnames) {
      dummy_df[[col]] <- sub("^$", "empty", dummy_df[[col]])
      }
  return(list(df, dummy_df, cat_count))
}
  
list_df <- prepare_seq_table(health_eventsA10agg, 10)
health_eventsA10agg_10 <- list_df[[1]]
health_eventsA10agg_seq <- list_df[[2]]
cat_count <- list_df[[3]]
list_df_emptystart <- prepare_seq_table(health_eventsA10agg_emptystartpatients, 10)
health_eventsA10agg_emptystartpatients_10 <- list_df_emptystart[[1]]
health_eventsA10agg_emptystartpatients_seq <- list_df_emptystart[[2]]

# reinitializing sequences for empty start patients health_eventsA10agg_emptystartpatients_seq
reinitialize <- function(dataframe){
  assign("df", dataframe) 
  for (i in 1:nrow(df)) {
    timer = 1
    while((timer < ncol(df)) && (health_eventsA10agg_emptystartpatients_seq[i,2] == "empty")) {
      for (j in 3:ncol(df)) {
        df[i,j-1] <- df[i,j]
      }
      df[i,ncol(df)] <- NA
    timer <- timer + 1
    }
  }
}
health_eventsA10agg_emptystartpatients_seq_reinitialized <- reinitialize(health_eventsA10agg_emptystartpatients_seq)

# create the sequence
build_seq <- function(df1, df2) {
    seq.alphabet <- c(sort(unique(df1$agg_cat)), "empty")
    seq.labels <- c(sort(unique(df1$agg_cat)), "empty")
    seq.scodes <- c(sort(unique(df1$agg_cat)), "empty")
    seq.seq <- seqdef(df2, 2:length(df2), alphabet = seq.alphabet, states = seq.scodes, labels = seq.labels, xtstep = 1)
return(seq.seq)
}

seq.health_eventsA10agg <- build_seq(health_eventsA10agg_10, health_eventsA10agg_seq)
seq.health_eventsA10agg_emptystartpatients <- build_seq(health_eventsA10agg_emptystartpatients_10, health_eventsA10agg_emptystartpatients_seq)

seq.seq <- seq.health_eventsA10agg


# plot the sequences
par(mfrow = c(2, 2))
seqiplot(seq.seq, withlegend = FALSE, border = NA)
seqIplot(seq.seq, sortv = "from.start", withlegend = FALSE)
seqIplot(seq.seq, sortv = "from.end", withlegend = FALSE)
#seqfplot(seq.seq, withlegend = FALSE, border = NA)
seqlegend(seq.seq)

# describe the sequences
par(mfrow = c(2, 2))
seqdplot(seq.seq, withlegend = FALSE, border = NA)
seqHtplot(seq.seq)
seqmsplot(seq.seq, withlegend = FALSE, border = NA)
seqmtplot(seq.seq, withlegend = FALSE)

# clustering and typology
dist.om1 <- seqdist(seq.seq, method = "OM", indel = 1, sm = "TRATE")
clusterward1 <- agnes(dist.om1, diss = TRUE, method = "ward")
plot(clusterward1, which.plot = 2)
cl1.4 <- cutree(clusterward1, k = 6)
cl1.4fac <- factor(cl1.4, labels = paste("Type", 1:6))
seqIplot(seq.seq, group = cl1.4fac, sortv = "from.start", withlegend = FALSE)
par(mfrow = c(1, 1))
seqlegend(seq.seq)

# Extracting cluster ID and merging with patients ID
ID_patients_cluster <- data.frame(health_eventsA10agg_seq$NUMERO_ENQ, cl1.4)
colnames(ID_patients_cluster) <- list("ID_patient", "ID_cluster")
