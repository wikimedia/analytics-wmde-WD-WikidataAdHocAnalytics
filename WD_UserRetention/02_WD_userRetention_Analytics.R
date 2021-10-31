### ---------------------------------------------------------------------------
### --- 01_WD_userRetention_Analytics.R
### --- Author: Goran S. Milovanovic, Data Scientist, WMDE
### --- Developed under the contract between:
### --- Goran Milovanovic PR Data Kolektiv and WMDE.
### --- Contact: goran.milovanovic_ext@wikimedia.de
### --- October 2021.
### --- Phabricator reference: https://phabricator.wikimedia.org/T282563
### ---------------------------------------------------------------------------
### --- COMMENT:
### --- R ETL procedures for the Wikidata User Retention project
### ---------------------------------------------------------------------------
### ---------------------------------------------------------------------------
### --- LICENSE:
### ---------------------------------------------------------------------------
### --- GPL v2
### --- This file is part of the Wikidata User Retention (WUR) project.
### ---
### --- WUR is free software: you can redistribute it and/or modify
### --- it under the terms of the GNU General Public License as published by
### --- the Free Software Foundation, either version 2 of the License, or
### --- (at your option) any later version.
### ---
### --- WUR is distributed in the hope that it will be useful,
### --- but WITHOUT ANY WARRANTY; without even the implied warranty of
### --- MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
### --- GNU General Public License for more details.
### ---
### --- You should have received a copy of the GNU General Public License
### --- along with WUR If not, see <http://www.gnu.org/licenses/>.
### ---------------------------------------------------------------------------
### ---------------------------------------------------
### --- Inactivity Criteria:
### --- https://phabricator.wikimedia.org/T282563#7107757
# - Definitions:
# - Q1. When do we say that a user has left Wikidata?
# - A. (1) We look at the procession of months since user registration, 
# - each month coded as active (1) or inactive (0); 
# - A. (2) We look if the user sequence of active months ends in 
# - "00000$"; if it does, we pronounce the user to be inactive.
# - Q2. How do we measure for how long has the user been active before
# - leaving Wikidata, given that any user can have interspersed 
# - periods of activity and inactivity?
#   A2. We count all active months for a user before it was declared 
# - that the user has stopped editing. Motivation: that is how we align a 
# - (a) a user who has edited for six months, *each month*, and left, 
# - i.e. 111111000..., and (b) a user who has edited here and there 
# - for six months and then left, e.g. 101001000....
# - Q3. What about the users who are still active 
# - (in the sense of not obeying to the definition given in A1 )?
# - A3. We say that their measure of the length of activity is simply 
# - the count of their active months, and thus the measures 
# - in A3 and A2 are comparable.
# - N.B. All registered users that have never edited at all 
# - are filtered out.
### ---------------------------------------------------

### ---------------------------------------------------
### --- Inactivity Criteria:
### --- https://phabricator.wikimedia.org/T282563#7124389
# - ... specifically, we consider an editor to be "dead" or 
# - inactive if he did not make any edits for a certain period 
# - of time. Here we set the threshold of inactivity to be 
# - 5 months, since it reflects WMF’s concern as demonstrated 
# - in the recent Wikipedia Participation Challenge.
# - N.B. Also incorporating Adam Wight's observation on
# - regex: https://phabricator.wikimedia.org/T282563#7110253
### ---------------------------------------------------

### --- Setup

library(tidyverse)
library(data.table)
library(ggrepel)
library(snowfall)
library(MatrixModels)
library(xgboost)
library(pROC)
dataDir <- '_data/'
analyticsDir <- '_analytics/'

### --- Dataset

dataSet <- fread(paste0(dataDir, "WD_UserRetention.csv"))
dataSet$V1 <- NULL
dim(dataSet)

# - keep complete observations only
dataSet <- dataSet[complete.cases(dataSet), ]
dim(dataSet)

# - search for "NULL" (string)
w1 <- which(dataSet$registrationYM == "NULL")
w2 <- which(dataSet$revisionYM == "NULL")
if ((length(w1) == 0) | (length(w2) == 0)) {
  w <- c(w1, w2)
  removeId <- unique(dataSet$userId[w])
  w <- which(dataSet$userId %in% removeId)
  dataSet <- dataSet[-w, ]
}
dim(dataSet)

# - add time units: months
minRegMonth <- min(dataSet$registrationYM)
maxRevMonth <- max(dataSet$revisionYM)
yearSpan <- seq(as.numeric(substr(minRegMonth, 1, 4)), 
                as.numeric(substr(maxRevMonth, 1, 4)),
                by = 1)
monthSpan <- c("01", "02", "03", "04", "05", "06",
               "07", "08", "09", "10", "11", "12")
ymSpan <- lapply(yearSpan, function(x) {
  paste0(x, "-", monthSpan)
})
ymSpan <- unlist(ymSpan)
ymSpan <- ymSpan[ymSpan >= minRegMonth]
ymSpan <- ymSpan[ymSpan <= maxRevMonth]

### --- Reactivations dataset

# - uniqueUserId
uniqueUserId <- unique(dataSet$userId)

# - cluster
sfInit(parallel = T, cpus = 23)
sfExport('dataSet')
sfExport('ymSpan')
sfLibrary(dplyr)
sfLibrary(stringr)

# - execute
reactivations <- sfClusterApplyLB(uniqueUserId, function(x) {
  wUserId <- which(dataSet$userId == x)
  active <- dataSet[wUserId, ] %>% 
    dplyr::arrange(revisionYM)
  registrationYMId <- unique(active$registrationYM)
  minRegMonthId <- min(active$registrationYM)
  ymSpanId <- ymSpan[ymSpan >= minRegMonthId]
  ymSpanId <- data.frame(revisionYM = ymSpanId, 
                         stringsAsFactors = F)
  active <- dplyr::left_join(ymSpanId, 
                             active,
                             by = "revisionYM")
  active$registrationYM <- registrationYMId
  active$userId <- x
  active$revisions[is.na(active$revisions)] <- 0
  # - positions:
  active$revPosition <- 1:dim(active)[1]
  # - active months (def: revisions > 0)
  activeMonths <- as.numeric(active$revisions > 0)
  # - sumActiveMonths
  sumActiveMonths <- sum(activeMonths)
  # - leftWikidata
  activeMonths <- paste(activeMonths, collapse = "")
  leftWikidata <- grepl("00000$", activeMonths)
  # - number of reactivations
  activeMonths <- gsub("^0+", "", activeMonths)
  reactiveN <- length(
    str_extract_all(activeMonths,
                    "000001+")[[1]]
  )
  # - output
  d <- data.frame(userId = x, 
                  reactivationsN = reactiveN,
                  totalActiveMonths = activeMonths,
                  leftWikidata = leftWikidata)
  return(d)
})

# - cluster shutdown
sfStop()

# - collect result
reactivations <- rbindlist(reactivations)

# - sumActiveMonths
reactivations$sumActiveMonths <- 
  sapply(reactivations$totalActiveMonths, function(x) {
    length(str_extract_all(x, "1")[[1]])
  })
# - add totalMonths
reactivations$totalMonths <- nchar(reactivations$totalActiveMonths)
# - p(ActiveMonth)
reactivations$pActiveMonth <- 
  reactivations$sumActiveMonths/reactivations$totalMonths
# - reactivations_complete.csv
write.csv(reactivations, 
          paste0(dataDir, "reactivations_complete.csv"))
# - Power-Law dataset: from complete account ages
WD_userRevisionsFrequency <- reactivations %>% 
  select(totalMonths) %>% 
  group_by(totalMonths) %>% 
  summarize(Frequency = n()) %>% 
  arrange(desc(Frequency))
write.csv(WD_userRevisionsFrequency, 
          paste0(dataDir, "WD_userRevisionsFrequency.csv"))

# - filter out all users with less than six (6) months of engagement
reactivations <- reactivations %>% 
  filter(totalMonths >= 6)
# - Power-Law dataset: from complete account ages
WD_userRevisionsFrequency6 <- reactivations %>% 
  select(totalMonths) %>% 
  group_by(totalMonths) %>% 
  summarize(Frequency = n()) %>% 
  arrange(desc(Frequency))
write.csv(WD_userRevisionsFrequency6, 
          paste0(dataDir, "WD_userRevisionsFrequency_6.csv"))

# - add user revisions to reactivations
numRevs <- dataSet %>% 
  select(userId, revisions) %>% 
  group_by(userId) %>% 
  summarise(numRevisions = sum(revisions))
reactivations <- left_join(reactivations,
                           numRevs,
                           by = "userId")
rm(numRevs)

# - mean and medium length of inactive periods, per userId
inactiveLength <- lapply(reactivations$totalActiveMonths, function(x) {
  inact <- nchar(str_extract_all(x, "0+")[[1]])
  mean_inact <- mean(inact)
  med_inact <- median(inact)
  return(data.frame(mean_inact,
                    med_inact)
         )
  })
inactiveLength <- rbindlist(inactiveLength)
reactivations <- cbind(reactivations,
                       inactiveLength)
rm(inactiveLength)

### --- Number of Revisions in Talk Namespaces
talkRevisions <- fread(paste0(dataDir, "WD_UserRetention_TalkRevisions.csv"))
talkRevisions$V1 <- NULL
reactivations <- left_join(reactivations, 
                           talkRevisions,
                           by = "userId")
rm(talkRevisions)
reactivations$talkrevisions[is.na(reactivations$talkrevisions)] <- 0

# - extract long "0"-tailed cases from reactivations, for exact length
w <- which(
  grepl("00000+$", reactivations$totalActiveMonths)
)
lSeq <- reactivations[w, c('userId', 'totalActiveMonths')]
lSeq$totalActiveMonths <- str_extract(lSeq$totalActiveMonths,
                                      "[1|0]*1+00000")
w <- which(reactivations$userId %in% lSeq$userId)
reactivations$totalActiveMonths[w] <- lSeq$totalActiveMonths

### --- Store reactivations
write.csv(reactivations, 
          paste0(dataDir, "reactivations_analytics.csv"))

### -- Clean up
rm(dataSet); gc()

### ------------------------------------------------------------------
### --- Results and visualizations

## -- The Probability to Leave Wikidata
pLeaveWD <- table(reactivations$leftWikidata)
pLeaveWD <- pLeaveWD/sum(pLeaveWD)
print(paste0("The probability to stop editing Wikidata is: ", 
             round(pLeaveWD[2], 2)))

## -- Lindy Effect

# - lindyFrame by sumActiveMonths
lindyFrame <- reactivations %>% 
  select(sumActiveMonths, leftWikidata) %>% 
  group_by(sumActiveMonths) %>% 
  summarise(pLeftWikidata = sum(leftWikidata)/n(),
            count = n())
# - visualize Lindy: sumActiveMonths vs pLeftWikidata 
ggplot(data = lindyFrame, 
       aes(x = sumActiveMonths,
           y = pLeftWikidata, 
           label = count)) + 
  geom_path(color = "blue", size = .25) + 
  geom_point(color = "blue", size = 1.5) + 
  geom_point(color = "white", size = 1) + 
  geom_text_repel(size = 2, 
                  max.overlaps = 20) + 
  xlab("Total active months") + ylab("P(Leave Wikidata)") + 
  ggtitle("Total Active Months vs P(Leave Wikidata)") + 
  theme_bw() + 
  theme(panel.border = element_blank()) + 
  theme(plot.title = element_text(hjust = .5))

# - lindyFrame by totalMonths
lindyFrame <- reactivations %>% 
  select(totalMonths, leftWikidata) %>% 
  group_by(totalMonths) %>% 
  summarise(pLeftWikidata = sum(leftWikidata)/n(),
            count = n())
# - visualize Lindy: totalMonths vs pLeftWikidata 
ggplot(data = lindyFrame, 
       aes(x = totalMonths,
           y = pLeftWikidata, 
           label = count)) + 
  geom_path(color = "blue", size = .25) + 
  geom_point(color = "blue", size = 1.5) + 
  geom_point(color = "white", size = 1) + 
  geom_text_repel(size = 2, 
                  max.overlaps = 20) + 
  xlab("Total Months") + ylab("P(Leave Wikidata)") + 
  ggtitle("Lindy Effect:\nTotal Months vs P(Leave Wikidata)") +
  theme_bw() + 
  theme(panel.border = element_blank())

# - lindyFrame by pActiveMonths
lindyFrame <- reactivations %>% 
  select(pActiveMonth, leftWikidata) %>% 
  mutate(pInts = cut(pActiveMonth, breaks = 100)) %>%
  group_by(pInts) %>% 
  summarise(pLeftWikidata = sum(leftWikidata)/n(),
            count = n())
# - visualize Lindy: pInts vs pLeftWikidata 
every_nth = function(n) {
  return(function(x) {x[c(TRUE, rep(FALSE, n - 1))]})
}
ggplot(data = lindyFrame, 
       aes(x = pInts,
           y = pLeftWikidata, 
           label = count)) + 
  geom_path(color = "blue", size = .25, group = 1) + 
  geom_point(color = "blue", size = 1.5) + 
  geom_point(color = "white", size = 1) + 
  geom_text_repel(size = 2, 
                  max.overlaps = 20) + 
  scale_x_discrete(breaks = every_nth(n = 3)) +
  xlab("P(Active Month)") + ylab("P(Leave Wikidata)") + 
  ggtitle("P(Active Month) vs P(Leave Wikidata)") +
  theme_bw() + 
  theme(panel.border = element_blank()) + 
  theme(axis.text.x = element_text(angle = 90)) + 
  theme(plot.title = element_text(hjust = .5))

### --- Left Wikidata vs Number of Revisions

# - plot: the revision distributions, left vs not left Wikidata
ggplot(reactivations, 
       aes(x = log(numRevisions), 
           group = leftWikidata,
           color = leftWikidata,
           fill = leftWikidata)) + 
  geom_density(alpha = .35, size = .25)  + 
  xlab("log(Revisions)") + ylab("Density") + 
  scale_color_manual(values = c('darkorange', 'deepskyblue')) + 
  scale_fill_manual(values = c('darkorange', 'deepskyblue')) +
  labs(fill = "Left Wikidata", 
       color = "Left Wikidata") +  
  ggtitle("Number of Revisions vs Left Wikidata)") +
  theme_bw() + 
  theme(panel.border = element_blank())

summary(reactivations$numRevisions[reactivations$leftWikidata])
summary(reactivations$numRevisions[!reactivations$leftWikidata])

### --- The Distribution of Length of Inactivity Periods

# - plot: the mean length of inactivity in months, left vs not left Wikidata
ggplot(reactivations, 
       aes(x = mean_inact, 
           group = leftWikidata,
           color = leftWikidata,
           fill = leftWikidata)) + 
  geom_density(alpha = .35, size = .25)  + 
  xlab("Mean length of inactivity (months)") + ylab("Density") + 
  scale_color_manual(values = c('darkorange', 'deepskyblue')) + 
  scale_fill_manual(values = c('darkorange', 'deepskyblue')) +
  labs(fill = "Left Wikidata", 
       color = "Left Wikidata") +  
  ggtitle("Mean length of inactive periods vs Left Wikidata)") +
  theme_bw() + 
  theme(panel.border = element_blank())

summary(reactivations$mean_inact[reactivations$leftWikidata])
summary(reactivations$mean_inact[!reactivations$leftWikidata])


# - plot: the median length of inactivity in months, left vs not left Wikidata
ggplot(reactivations, 
       aes(x = med_inact, 
           group = leftWikidata,
           color = leftWikidata,
           fill = leftWikidata)) + 
  geom_density(alpha = .35, size = .25)  + 
  xlab("Median length of inactivity (months)") + ylab("Density") + 
  scale_color_manual(values = c('darkorange', 'deepskyblue')) + 
  scale_fill_manual(values = c('darkorange', 'deepskyblue')) +
  labs(fill = "Left Wikidata", 
       color = "Left Wikidata") +  
  ggtitle("Median length of inactive periods vs Left Wikidata)") +
  theme_bw() + 
  theme(panel.border = element_blank())

summary(reactivations$med_inact[reactivations$leftWikidata])
summary(reactivations$med_inact[!reactivations$leftWikidata])

# - reactivations$talkrevisions vs Left Wikidata
ggplot(reactivations, 
       aes(x = log(talkrevisions), 
           group = leftWikidata,
           color = leftWikidata,
           fill = leftWikidata)) + 
  geom_density(alpha = .35, size = .25)  + 
  xlab("log(Revisions in Talk Namespaces)") + ylab("Density") + 
  scale_color_manual(values = c('darkorange', 'deepskyblue')) + 
  scale_fill_manual(values = c('darkorange', 'deepskyblue')) +
  labs(fill = "Left Wikidata", 
       color = "Left Wikidata") +  
  ggtitle("Revisions in Talk Namespaces vs Left Wikidata)") +
  theme_bw() + 
  theme(panel.border = element_blank())

summary(reactivations$talkrevisions[reactivations$leftWikidata])
summary(reactivations$talkrevisions[!reactivations$leftWikidata])

### --- Lindy effect: the power-law estimation
### --- R.V. The Wikidata Account Age

# - dataset: powerLawSet
powerLawSet <- reactivations %>% 
  select(totalActiveMonths)
powerLawSet$id <- 1:dim(powerLawSet)[1]

# - account ages:
powerLawSet$age <- nchar(powerLawSet$totalActiveMonths)

# - boostrap_p: estimate xmin
plEstimate <- displ$new(powerLawSet$age)
xminEstimate <- estimate_xmin(plEstimate)
plEstimate$setXmin(xminEstimate)
plEstimate$xmin
plEstimate$pars
estimate_pars(plEstimate)$pars
bs_p = bootstrap_p(plEstimate, no_of_sims = 1000, threads = 23)
bs_p$p

# - boostrap_p: empirical xmin
plEstimate <- displ$new(powerLawSet$age)
parsEstimate <- estimate_pars(plEstimate)$pars
plEstimate$setPars(parsEstimate)
plEstimate$xmin
plEstimate$pars
bs_p = bootstrap_p(plEstimate, no_of_sims = 1000, threads = 23)
bs_p$p

### --- Lindy effect: the power-law estimation
### --- R.V. The Number of Active Months in Wikidata Editing

# - dataset: powerLawSet
powerLawSet <- reactivations %>% 
  select(totalActiveMonths)
powerLawSet$id <- 1:dim(powerLawSet)[1]

# - totalActiveMonths:
totalAM_Freq <- unname(sapply(powerLawSet$totalActiveMonths,
                              function(x) {
                                length(str_extract_all(x, "1")[[1]])
                                }))

# - boostrap_p: estimate xmin
plEstimate <- displ$new(totalAM_Freq)
xminEstimate <- estimate_xmin(plEstimate)
plEstimate$setXmin(xminEstimate)
plEstimate$xmin
plEstimate$pars
estimate_pars(plEstimate)$pars
bs_p = bootstrap_p(plEstimate, no_of_sims = 1000, threads = 23)
bs_p$p

# - boostrap_p: empirical xmin
plEstimate <- displ$new(totalAM_Freq)
parsEstimate <- estimate_pars(plEstimate)$pars
plEstimate$setPars(parsEstimate)
plEstimate$xmin
plEstimate$pars
bs_p = bootstrap_p(plEstimate, no_of_sims = 1000, threads = 23)
bs_p$p


# - plots
ageFreq <- as.numeric(table(powerLawSet$age))
ageFreqFrame <- data.frame(freq = sort(ageFreq, decreasing = TRUE))
ageFreqFrame$rank <- 1:dim(ageFreqFrame)[1]
ggplot(ageFreqFrame, 
       aes(x = log(rank),
           y = log(freq))) + 
  geom_smooth(method = "lm", color = "red", size = .25) +
  geom_point(size = 1.5, color = "darkblue") + 
  geom_point(size = 1, color = "white") + 
  xlab("log(Rank)") + ylab("log(Frequency)") + 
  ggtitle("Rank-Frequency: Account Ages") +
  theme_bw() + 
  theme(panel.border = element_blank())

totalAM_Freq_Table <- table(totalAM_Freq)
totalAM_Freq_Table <- data.frame(freq = sort(totalAM_Freq_Table, 
                                             decreasing = TRUE))
totalAM_Freq_Table$rank <- 1:dim(totalAM_Freq_Table)[1]
colnames(totalAM_Freq_Table) <- c("Obs", "freq", "rank")
ggplot(totalAM_Freq_Table, 
       aes(x = log(rank),
           y = log(freq))) + 
  geom_smooth(method = "lm", color = "red", size = .25) +
  geom_point(size = 1.5, color = "darkblue") + 
  geom_point(size = 1, color = "white") + 
  xlab("log(Rank)") + ylab("log(Frequency)") + 
  ggtitle("Rank-Frequency: Active Months") +
  theme_bw() + 
  theme(panel.border = element_blank())



### ------------------------------------------------------
### --- Predicting: left Wikidata vs Active
### ------------------------------------------------------

# - the dataset
reactivations <- 
  fread(paste0(dataDir, "reactivations_analytics.csv"))

# - model dataset
modelFrame <- reactivations %>% 
  select(totalActiveMonths, 
         reactivationsN,
         sumActiveMonths,
         numRevisions,
         talkrevisions,
         leftWikidata)

### --- data clean-up
# - 1. Remove the last month from reactivations$totalActiveMonths
# - N.B. At prediction time, "00000$" (5 inactive months) 
# - would be already considered as: left Wikidata
# - so, we focus on everything up to "0000$" (4 inactive months)

modelFrame$totalActiveMonths <- substr(modelFrame$totalActiveMonths, 
                                       1, 
                                       nchar(modelFrame$totalActiveMonths)-1)

### --- feature engineering from modelFrame$totalActiveMonths
# - 1. account age
modelFrame$accountAge <- nchar(modelFrame$totalActiveMonths)

# - 2. sumActiveMonths
modelFrame$sumActiveMonths <- gsub("0", "", modelFrame$totalActiveMonths)
modelFrame$sumActiveMonths <- nchar(modelFrame$sumActiveMonths)

# - 3. Shannon Diversity Index (H)
modelFrame$pActiveMonth <- 
  modelFrame$sumActiveMonths/modelFrame$accountAge

modelFrame$H <- -(modelFrame$pActiveMonth*log(modelFrame$pActiveMonth) +
                     (1-modelFrame$pActiveMonth)*log(1-modelFrame$pActiveMonth)
)
modelFrame$H[is.nan(modelFrame$H)] <- 1
# - normalize Entropy for Shannon Diversity Index
modelFrame$H <- modelFrame$H/log(2)

# - 4. Mean and Median length of inactivity
inactiveLength <- lapply(modelFrame$totalActiveMonths, function(x) {
  inact <- nchar(str_extract_all(x, "0+")[[1]])
  mean_inact <- mean(inact)
  med_inact <- median(inact)
  return(data.frame(mean_inact,
                    med_inact)
  )
})
inactiveLength <- rbindlist(inactiveLength)
modelFrame <- cbind(modelFrame,
                       inactiveLength)
rm(inactiveLength)

# - 5. Average revisions per month
modelFrame$averageRevisionsPerMonth <- 
  modelFrame$numRevisions/modelFrame$accountAge

# - 6. Average revisions in Talk Namespaces per month
modelFrame$averageTalkRevisionsPerMonth <- 
  modelFrame$talkrevisions/modelFrame$accountAge

# - final modelFrame
modelFrame$totalActiveMonths <- NULL
modelFrame$leftWikidata <- as.numeric(modelFrame$leftWikidata)

# - prepare for XGBoost
print("-----------------------------------------------------------------------")
print("Split into train and test data sets now and prepare data for XGBoost.")

# - split: train vs test
modelFrame$SPLIT <- round(runif(dim(modelFrame)[1], 0, 1))
trainFrame <- modelFrame[modelFrame$SPLIT == 0, ]
testFrame <- modelFrame[modelFrame$SPLIT == 1, ]

# - test matrices 
leftWD_test <- testFrame$leftWikidata
testFrame$leftWikidata <- NULL

# - remove SPLIT
trainFrame$SPLIT <- NULL
testFrame$SPLIT <- NULL

print("testFrame as.matrix now.")
testFrame <- as.matrix(testFrame)

# - to {xgboost} DMatrix
print("testFrame as xgb.DMatrix now.")
testFrame <- xgb.DMatrix(testFrame, label = leftWD_test)
print(paste0("Dimesion of testFrame: ", 
             paste(dim(testFrame), collapse = ", ", sep = "")))
print("Preparation complete. Model w. XGBoost now.")

print("-----------------------------------------------------------------------")
print("Set CV tree parameters:")
# - nthread
cv_nthread <- 23
print(paste0("Using: ", cv_nthread,  ' cores.'))
# - max_delta_step
cv_max_delta_step <- 1
print(paste0("Using: ", cv_max_delta_step,  ' max_delta_step.'))
# - colsample_bytree
cv_colsample_bytree <- .75
print(paste0("Using: ", cv_colsample_bytree,  ' colsample_bytree.'))
# - eta
cv_eta <- c(.1, .2, .5)
print(paste0(paste0("Set eta as: "), 
             paste(cv_eta, collapse = ", ", sep = "")))
# - nrounds
cv_nrounds <- c(10000, 10000, 10000, 10000)
print(paste0(paste0("Set n_rounds as: "), 
             paste(cv_nrounds, collapse = ", ", sep = ""), 
             " - NOTE: respective of eta."))  # - max_depth
cv_max_depth <- c(5, 10)
print(paste0(paste0("Set max_depth as: "), 
             paste(cv_max_depth, collapse = ", ", sep = "")))
# - sub_sample
cv_sub_sample <- seq(.25, .5, .75)
print(paste0(paste0("Set sub_sample as: "), 
             paste(cv_sub_sample, collapse = ", ", sep = "")))

# - downsampling
# - t <- table(trainFrame$leftWikidata)
# - t[2]/10
# - use only 10% of negative examples
# - downsampling factor
downsample_factor = 20000
p_choice <- downsample_factor/sum(trainFrame$leftWikidata == 1)
print(paste0("p_choice is: ", p_choice))
print(paste0("downsample_factor is: ", downsample_factor))
# - downsample
trainFrame$select <- 0
trainFrame$select[trainFrame$leftWikidata == 0] <- 1
trainFrame$select[trainFrame$leftWikidata == 1] <- 
  rbinom(sum(trainFrame$leftWikidata == 1), 1, p = p_choice)
table(trainFrame$select[trainFrame$leftWikidata == 1])
table(trainFrame$leftWikidata[trainFrame$select == 1])
trainFrame <- trainFrame[trainFrame$select == 1, ]
leftWD_train <- trainFrame$leftWikidata
trainFrame$leftWikidata <- NULL
trainFrame$select <- NULL
print("trainFrame as.matrix now.")
trainFrame <- as.matrix(trainFrame)
print("trainFrame as xgb.DMatrix now.")
trainFrame <- xgb.DMatrix(trainFrame, label = leftWD_train)
print(paste0("Dimesion of trainFrame: ", 
             paste(dim(trainFrame), collapse = ", ", sep = "")))

print("-----------------------------------------------------------------------")
print("RUN XGBOOST w. LOGISTIC LOSS CROSS-VALIDATION NOW:")
print("-----------------------------------------------------------------------")
print("Prepare list to store data now.")
cv_list_length <- length(cv_eta)*length(cv_max_depth)*length(cv_sub_sample)
print(paste0("Will estimate ", cv_list_length, " models."))
cv_list <- vector(mode = "list", length = cv_list_length)
print("-----------------------------------------------------------------------")
print("FIRE:.")
print("-----------------------------------------------------------------------")
# - count models
num_models <- 0
for(cv_eta_i in 1:length(cv_eta)) {
  
  for (cv_max_depth_i in 1:length(cv_max_depth)) {
    
    for (cv_sub_sample_i in 1:length(cv_sub_sample)) {
      num_models <- num_models + 1
      print("-----------------------------------------------------------------------")
      print(paste0("Running model: " , num_models, "/", cv_list_length, "."))
      print("XGBooost at: ")
      print(paste(
        paste0("eta = ", cv_eta[cv_eta_i]),
        paste0("max_depth = ", cv_max_depth[cv_max_depth_i]),
        paste0("sub_sample = ", cv_sub_sample[cv_sub_sample_i]),
        collapse = "", sep = "; "))
      print(paste0("Training begins: ", Sys.time()))
      print(paste0("n_rounds will be: ", cv_nrounds[cv_eta_i]))
      t1 <- Sys.time()
      res_boost <- xgb.train(
        data = trainFrame,
        watchlist = list(validation = testFrame), 
        params = list(booster = "gbtree",
                      nthread = cv_nthread,
                      eta = cv_eta[cv_eta_i], 
                      max_depth = cv_max_depth[cv_max_depth_i], 
                      subsample = cv_sub_sample[cv_sub_sample_i], 
                      max_delta_step = cv_max_delta_step,
                      colsample_bytree = cv_colsample_bytree,
                      scale_pos_weight = downsample_factor,
                      objective = "binary:logistic"),
        nrounds = cv_nrounds[cv_eta_i],
        verbose = 0,
        print_every_n = 0,
        eval_metric = "logloss",
        early_stopping_rounds = NULL,
        maximize = NULL,
        save_period = NULL,
        save_name = NULL,
        xgb_model = NULL
      )
      print(paste0("Training ends: ", Sys.time()))
      training_time <- difftime(Sys.time(), t1, units = "mins")
      print(paste0("XGBoost CV took: ", training_time))
      print("-----------------------------------------------------------------------")
      # - feature importance
      print("Compute feature importance now.")
      importance <- xgb.importance(feature_names = colnames(trainFrame), 
                                   model = res_boost)
      # - ROC analysis
      print("ROC analysis now.")
      predTest <- predict(res_boost, testFrame)
      accTest_05 <- mean(as.numeric(predTest > .5) == leftWD_test)
      print(paste0("Accuracy at decision boundary of .5: ", round(accTest_05, 8)))
      predFrameTest <- data.frame(prediction = predTest,
                                  observed = leftWD_test)
      
      rocFrameTest <- data.frame(prediction = predTest > .5,
                                  observed = leftWD_test)
      TPR_05 <- sum(rocFrameTest$prediction == 1 & leftWD_test == 1)/sum(leftWD_test == 1)
      print(paste0("TPR (Hit) at decision boundary of .5: ", round(TPR_05, 8)))
      FPR_05 <- sum(rocFrameTest$prediction == 1 & leftWD_test == 0)/sum(leftWD_test == 0)
      print(paste0("FPR (FA) at decision boundary of .5: ", round(FPR_05, 8)))
      TNR_05 <- sum(rocFrameTest$prediction == 0 & leftWD_test == 0)/sum(leftWD_test == 0)
      print(paste0("TNR (CR) at decision boundary of .5: ", round(TNR_05, 8)))
      FNR_05 <- sum(rocFrameTest$prediction == 0 & leftWD_test == 1)/sum(leftWD_test == 1)
      print(paste0("FNR (Miss) at decision boundary of .5: ", round(FNR_05, 8)))
      
      # - compute AUC
      roc_obj <- roc(predFrameTest, 
                     response = observed, 
                     predictor = prediction,
                     ci = T)
      model_auc <- as.numeric(roc_obj$ci) 
      names(model_auc) <- c('AUC_CI_lower', 'AUC', 'AUC_CI_upper')
      # - compute accuracy across decision boundary == seq(.1, .9, .1) 
      model_acc <- sapply(seq(.1, .9, .1), function(x) {
        mean(as.numeric(predTest > x) == leftWD_test)
      })
      names(model_acc) <- paste("ACC_db_", seq(.1, .9, .1))
      # - Model outputs
      print("ROC analysis now.")
      model_params <- unlist(res_boost$params)
      model_n_features <- res_boost$nfeatures
      names(model_n_features) <- 'num_feats'
      model_n_iters <- res_boost$niter
      names(model_n_iters) <- 'num_iters'
      # - log validation_logloss
      validation_logloss <- tail(res_boost$evaluation_log$validation_logloss, 1)
      best_iter <- which.min(res_boost$evaluation_log$validation_logloss)
      validation_logloss_best_iter <- res_boost$evaluation_log$validation_logloss[best_iter]
      model_validation_logloss <- c(validation_logloss, 
                                    best_iter, 
                                    validation_logloss_best_iter)
      names(model_validation_logloss) <- c('validation_logloss', 
                                           'best_iter', 
                                           'validation_logloss_best_iter')
      # - Model log
      cv_list[[num_models]]$logFrame <- cbind(
        as.data.frame(t(model_auc)),
        as.data.frame(t(model_acc)),
        as.data.frame(t(model_params)),
        as.data.frame(t(model_n_features)),
        as.data.frame(t(model_n_iters)),
        as.data.frame(t(model_validation_logloss))
      )
      # - Model feature importance
      cv_list[[num_models]]$featureImportance <- importance
      # - Model predictive frame
      cv_list[[num_models]]$predFrame <- predFrameTest
      # - done
      print("DONE.")
      print("-----------------------------------------------------------------------")
      
      
    } # - END cv_sub_sample_i loop
    
  } # - END cv_max_depth loop
  
} # - END cv_eta loop

# - store dataset results
saveRDS(cv_list, 
        paste0(analyticsDir, "WD_UserRetention_xbgoostCV.Rds"))

### --- Model Selection
# - auc
auc <- lapply(cv_list, function(x) {
  x[[1]]
})
auc <- rbindlist(auc)
wauc <- which.max(auc$AUC)
auc[wauc, ]
# - roc 
predFrameTest <- cv_list[[wauc]]$predFrame
roc_obj <- roc(predFrameTest, 
               response = observed, 
               predictor = prediction,
               ci = T)
model_auc <- as.numeric(roc_obj$ci) 
names(model_auc) <- c('AUC_CI_lower', 'AUC', 'AUC_CI_upper')

predictClass <- function(decBoundary, decData) {
  
  # - predict class
  predClass <- ifelse(decData$prediction >= decBoundary, 1, 0)
  
  # - ROC analysis
  tpr <- sum(decData$observed == 1 & predClass == 1)
  tpr <- tpr/sum(decData$observed == 1)
  fnr <- sum(decData$observed == 1 & predClass == 0)
  fnr <- fnr/sum(decData$observed == 1)
  fpr <- sum(decData$observed == 0 & predClass == 1)
  fpr <- fpr/sum(decData$observed == 0)
  tnr <- sum(decData$observed == 0 & predClass == 0)
  tnr <- tnr/sum(decData$observed == 0)
  roc <- c(decBoundary, tpr, fnr, fpr, tnr)
  names(roc) <- c('decBoundary', 'tpr', 'fnr', 'fpr', 'tnr')
  return(roc)
}
decBoundaries <- seq(.000001, .999999, by = .001)
ROC_results <- lapply(decBoundaries, function(x) {
  as.data.frame(t(predictClass(x, predFrameTest)))
})
ROC_results <- rbindlist(ROC_results)
head(ROC_results)
ROC_results$diff <- ROC_results$tpr - ROC_results$fpr

### --- a posteriori analysis
w <- which.max(ROC_results$diff)
model <- ROC_results[w, ]
a_priori = .92
p_predictedL = model$tpr * apriori + model$fpr * (1 - a_priori)
p_leave_predictedL <- model$tpr * apriori / p_predictedL
print(p_leave_predictedL)

### --- feature importance
fi <- cv_list[[1]]$featureImportance
fi$Feature <- factor(fi$Feature, 
                     levels = fi$Feature[order(-fi$Gain)])
ggplot(data = fi, 
       aes(x = Feature, 
           y = Gain, 
           label = round(Gain, 2))) +
  geom_path(color = "red", group = 1, size = .25) + 
  geom_point(color = "red", size = 1.5) + 
  geom_point(color = "white", size = 1) +
  geom_text_repel(size = 2.5) +
  ggtitle("Feature Importance") +
  xlab("Features") + ylab("Gain (XGBoost model)") + 
  theme_bw() + 
  theme(plot.title = element_text(hjust = .5)) + 
  theme(axis.text.x = element_text(angle = 90, 
                                   hjust=0.95,
                                   vjust=0.2)) + 
  theme(panel.border = element_blank())
