
### --- project: WMDE Technical Wishlist Survey 20222
### --- https://phabricator.wikimedia.org/T294094

### --- setup

# - env
renv::load()

# - lib
library(WMDEData)

# - dirTree
dataDir <- paste0(getwd(), 
                  "/_data/")
analyticsDir <- paste0(getwd(),
                       "/_analytics/")

# Sys.setenv(http_proxy = "http://webproxy.eqiad.wmnet:8080",
#            https_proxy = "http://webproxy.eqiad.wmnet:8080")

### ------------------------------------------------------
### --- 1.New editors
### ------------------------------------------------------

### ----------------------------------------------------------------
### --- etl phase

### --- determine wmf.mediawiki_history snapshot
# - Kerberos init
WMDEData::kerberos_init(kerberosUser = "analytics-privatedata")
# - Define query
queryFile <- paste0(dataDir, 
                    "snapshot_Query.hql")
hiveQLquery <- "SHOW PARTITIONS wmf.mediawiki_history;"
write(hiveQLquery, queryFile)
# - Run HiveQL query
filename <- "snapshot.tsv"
WMDEData::kerberos_runHiveQL(kerberosUser = "analytics-privatedata",
                             query = queryFile,
                             localPath = dataDir,
                             localFilename = filename)
mwhistorySnapshot <- read.table(paste0(dataDir, filename),
                                sep = "\t",
                                stringsAsFactors = FALSE)
mwhistorySnapshot <- tail(mwhistorySnapshot$V1, 1)
mwhistorySnapshot <- 
  stringr::str_extract(mwhistorySnapshot,
                       "[[:digit:]]+-[[:digit:]]+") 

### --- ETL
# - Kerberos init
WMDEData::kerberos_init()
# - Query
query <- paste0("SELECT 
                  event_user_text, 
                  event_user_is_bot_by, 
                  page_namespace
                FROM wmf.mediawiki_history 
                WHERE (
                  event_entity = 'revision' AND 
                  event_type = 'create' AND 
                  wiki_db = 'dewiki' AND 
                  event_user_is_anonymous = FALSE AND 
                  page_is_redirect = FALSE AND 
                  revision_is_deleted_by_page_deletion = FALSE AND
                  event_user_registration_timestamp > '2021-07-01 00:00:00.0' 	AND 
                  snapshot = '", mwhistorySnapshot, "');"
)
# - prepare to run HiveQL query
queryFile <- "TW_Survey_2021_Query1_NewEditors.hql"
filename <- "TW_Survey_2021_Query1_NewEditors.tsv"
write(query, 
      paste0(dataDir, queryFile))
# - run HiveQL query
WMDEData::kerberos_runHiveQL(kerberosUser = "analytics-privatedata",
                             query = paste0(dataDir, queryFile),
                             localPath = dataDir,
                             localFilename = filename)

### ----------------------------------------------------------------
### --- load and process

dataSet <- data.table::fread(paste0(dataDir, filename))
w_bot <- which(grepl("bot|name", dataSet$event_user_is_bot_by))
if (length(w_bot) > 0) {
  dataSet <- dataSet[-w_bot, ]
}
dataSet$event_user_is_bot_by <- NULL
colnames(dataSet) <- c("username", "namespace")
totalRevs <- dataSet %>% 
  dplyr::group_by(username) %>% 
  dplyr::summarise(revisions = dplyr::n())
nsmainRevs <- dataSet %>% 
  dplyr::filter(namespace == 0) %>% 
  dplyr::group_by(username) %>% 
  dplyr::summarise(ns0_revisions = dplyr::n())
totalRevs <- totalRevs %>% 
  dplyr::left_join(nsmainRevs, 
                   by = "username")
totalRevs <- dplyr::arrange(totalRevs, desc(ns0_revisions))
filename <- gsub("\\.tsv", "\\.csv", filename)
write.csv(totalRevs, 
          paste0(analyticsDir, filename))


### ------------------------------------------------------
### --- 3. Editors who hardly participate in discussions
### ------------------------------------------------------

### ----------------------------------------------------------------
### --- etl phase

### --- determine wmf.mediawiki_history snapshot
# - Kerberos init
WMDEData::kerberos_init(kerberosUser = "analytics-privatedata")
# - Define query
queryFile <- paste0(dataDir, 
                    "snapshot_Query.hql")
hiveQLquery <- "SHOW PARTITIONS wmf.mediawiki_history;"
write(hiveQLquery, queryFile)
# - Run HiveQL query
filename <- "snapshot.tsv"
WMDEData::kerberos_runHiveQL(kerberosUser = "analytics-privatedata",
                             query = queryFile,
                             localPath = dataDir,
                             localFilename = filename)
mwhistorySnapshot <- read.table(paste0(dataDir, filename),
                                sep = "\t",
                                stringsAsFactors = FALSE)
mwhistorySnapshot <- tail(mwhistorySnapshot$V1, 1)
mwhistorySnapshot <- 
  stringr::str_extract(mwhistorySnapshot,
                       "[[:digit:]]+-[[:digit:]]+") 

### --- ETL
# - Kerberos init
WMDEData::kerberos_init()
# - Query
query <- paste0("SELECT 
                  COUNT(*) as count,
                  event_user_id,
                  event_user_text, 
                  event_user_is_bot_by, 
                  page_namespace AS ns_rev_count
                FROM wmf.mediawiki_history 
                WHERE (
                  event_entity = 'revision' AND 
                  event_type = 'create' AND 
                  wiki_db = 'dewiki' AND 
                  event_user_is_anonymous = FALSE AND 
                  page_is_redirect = FALSE AND 
                  revision_is_deleted_by_page_deletion = FALSE AND
                  snapshot = '", mwhistorySnapshot, "') 
                GROUP BY page_namespace, event_user_is_bot_by, event_user_id, event_user_text;"
)
# - prepare to run HiveQL query
queryFile <- "TW_Survey_2021_Query3_Discussions.hql"
filename <- "TW_Survey_2021_Query3_Discussions.tsv"
write(query, 
      paste0(dataDir, queryFile))
# - run HiveQL query
WMDEData::kerberos_runHiveQL(kerberosUser = "analytics-privatedata",
                             query = paste0(dataDir, queryFile),
                             localPath = dataDir,
                             localFilename = filename)

### - Eligible users: edited in 2020.
  # - Kerberos init
  WMDEData::kerberos_init()
  # - Query
  query <- paste0("SELECT 
                    event_user_id,
                    event_user_text                
                  FROM wmf.mediawiki_history 
                WHERE (
                  event_entity = 'revision' AND 
                  event_type = 'create' AND 
                  wiki_db = 'dewiki' AND 
                  event_user_is_anonymous = FALSE AND 
                  page_is_redirect = FALSE AND 
                  revision_is_deleted_by_page_deletion = FALSE AND 
                  substring(event_timestamp, 1, 4) = '2020' AND 
                  snapshot = '", mwhistorySnapshot, "');"
  )
  # - prepare to run HiveQL query
  queryFile <- "TW_Survey_2021_Query3_Discussions_EUsers.hql"
  filename <- "TW_Survey_2021_Query3_Discussions_EUsers.tsv"
  write(query, 
        paste0(dataDir, queryFile))
  # - run HiveQL query
  WMDEData::kerberos_runHiveQL(kerberosUser = "analytics-privatedata",
                               query = paste0(dataDir, queryFile),
                               localPath = dataDir,
                               localFilename = filename)

### ----------------------------------------------------------------
### --- load and process

dataSet <- data.table::fread(paste0(dataDir, 
                                    "TW_Survey_2021_Query3_Discussions.tsv"))
head(dataSet)
colnames(dataSet) <- c("count", "user_id", "username", "bot_flag", "namespace")

# - filter users who did not edit in 2020
users2020Set <- 
  data.table::fread(paste0(dataDir,
                           "TW_Survey_2021_Query3_Discussions_EUsers.tsv"))
colnames(users2020Set) <- c("user_id", "username")
sel_users <- unique(users2020Set$user_id)
length(sel_users)
w <- which(dataSet$user_id %in% sel_users)
length(w)
dataSet <- dataSet[w, ]
dim(dataSet)

# - continue processing
dataSet <- dplyr::filter(dataSet, 
                         !grepl("name|group", dataSet$bot_flag))
dim(dataSet)
sel_ns <- c(0, 1, 3)
dataSet <- dataSet %>% 
  dplyr::filter(namespace %in% sel_ns)
dim(dataSet)
dataSet$bot_flag <- NULL
dataSet$user <- paste0(dataSet$user_id, 
                      "_us_", 
                      dataSet$username)
dataSet$user_id <- NULL
dataSet$username <- NULL
dataSet <- dataSet %>% 
  tidyr::pivot_wider(id_cols = user, 
                     names_from = namespace, 
                     values_from = count, 
                     values_fill = 0)
colnames(dataSet)[2:4] <- paste0("ns_", colnames(dataSet)[2:4])
dataSet$user <- gsub("^[[:digit:]]+_us_", "", dataSet$user)
colnames(dataSet)[1] <- "username"
dataSet$main <- dataSet$ns_0
dataSet$talk <- dataSet$ns_1 + dataSet$ns_3
dataSet$ns_0 <- NULL
dataSet$ns_1 <- NULL
dataSet$ns_3 <- NULL
dataSet$ratio <- dataSet$main/dataSet$talk
dataSet <- dplyr::arrange(dataSet, desc(ratio, main))
filename <- gsub("\\.tsv", "\\.csv", filename)
dim(dataSet)
write.csv(dataSet, 
          paste0(analyticsDir,
                 "TW_Survey_2021_Query3_Discussions.csv"))


### ------------------------------------------------------
### --- 4. I have a list of usernames and I want to check 
### --- how active they were after participation in 
### --- the 2019 survey.
### ------------------------------------------------------

library(lubridate)

### --- start date
startTimestamp <- '20200101000000'
# - get user ids
rev_user <- read.csv(paste0(dataDir, 
                            "Survey.csv"), 
                     stringsAsFactors = FALSE)
rev_user <- rev_user$users

dataSet <- data.frame(actor_user = character(length = length(rev_user)), 
                      actor_username = rev_user, 
                      stringsAsFactors = FALSE)

# - iterate over rev_user
for (i in 1:length(rev_user)) {
  
  # - SQL query
  sqlQuery <- paste0(
    "\"SELECT user_id FROM user WHERE user_name = \'", 
    rev_user[i], "';\"")
  ### --- output filename
  filename <- paste(dataDir,'userEdits', "_", i, ".tsv", sep = "")
  ### --- execute sql script:
  sqlLogInPre <- paste0('/usr/local/bin/analytics-mysql dewiki -e ')
  sqlInput <- paste(sqlQuery,
                    " > ",
                    filename,
                    sep = "")
  # - command:
  sqlCommand <- paste(sqlLogInPre, sqlInput)
  system(command = sqlCommand, wait = TRUE)
  # - report
  print(paste0("DONE: user_name: ", i, "."))
  
  # - grab username
  userid <- data.table::fread(filename)
  userid <- userid$user_id
  if(!is.null(userid)) {
    dataSet$actor_user[i] <- userid 
  } else {
    dataSet$actor_user[i] <- NA
  }
  
  # - SQL query
  sqlQuery <- paste0(
    "\"SELECT actor.actor_id, actor.actor_user, revision_actor_temp.revactor_timestamp FROM actor LEFT JOIN revision_actor_temp ON (actor.actor_id = revision_actor_temp.revactor_actor) WHERE (revision_actor_temp.revactor_timestamp >= ", 
    startTimestamp, " AND actor.actor_user = ", 
    userid, ");\"");
  ### --- output filename
  filename <- paste(dataDir,'userEdits', "_", i, ".tsv", sep = "")
  ### --- execute sql script:
  sqlLogInPre <- paste0('/usr/local/bin/analytics-mysql dewiki -e ')
  sqlInput <- paste(sqlQuery,
                    " > ",
                    filename,
                    sep = "")
  # - command:
  sqlCommand <- paste(sqlLogInPre, sqlInput)
  system(command = sqlCommand, wait = TRUE)
  # - report
  print(paste0("DONE: user ", i, "."))
}
### --- END run SQL scripts
# - load user edits:
lF <- list.files(dataDir)
lF <- lF[grepl("^userEdits_", lF)]
userEdits <- lapply(paste0(dataDir, lF), data.table::fread)
userEdits <- data.table::rbindlist(userEdits)
userEdits <- userEdits %>% 
  dplyr::select(-actor_id, -revactor_timestamp) %>% 
  dplyr::group_by(actor_user) %>% 
  dplyr::summarise(revisions = dplyr::n())
userEdits$actor_user <- as.character(userEdits$actor_user)
dataSet <- dataSet %>% 
  dplyr::left_join(userEdits, 
                   by = "actor_user")

dim(dataSet)
rev_user <- read.csv(paste0(dataDir, 
                            "Survey.csv"), 
                     stringsAsFactors = FALSE)
rev_user <- rev_user %>% 
  dplyr::left_join(dataSet, 
                   by = c("users" = "actor_username"))

head(rev_user)
rev_user$actor_user <- NULL
rev_user <- rev_user %>% 
  dplyr::arrange(desc(rev_user))

# - store user edits:
write.csv(rev_user, 
          paste0(analyticsDir, 
                 'TW_Survey_2021_Query4_SurveyRespondentRevisions.csv'))



ds <- read.csv("_analytics/TW_Survey_2021_Query4_SurveyRespondentRevisions.csv", 
               header = TRUE,
               stringsAsFactors = FALSE,
               check.names = FALSE, 
               row.names = 1)
ds[is.na(ds)] <- 0
ds <- dplyr::arrange(ds, dplyr::desc(revisions))
write.csv(ds, 
          "_analytics/TW_Survey_2021_Query4_SurveyRespondentRevisions.csv")


### ------------------------------------------------------
### --- 2. Casual Editors
### ------------------------------------------------------

### ----------------------------------------------------------------
### --- etl phase

### --- determine wmf.mediawiki_history snapshot
# - Kerberos init
WMDEData::kerberos_init(kerberosUser = "analytics-privatedata")
# - Define query
queryFile <- paste0(dataDir, 
                    "snapshot_Query.hql")
hiveQLquery <- "SHOW PARTITIONS wmf.mediawiki_history;"
write(hiveQLquery, queryFile)
# - Run HiveQL query
filename <- "snapshot.tsv"
WMDEData::kerberos_runHiveQL(kerberosUser = "analytics-privatedata",
                             query = queryFile,
                             localPath = dataDir,
                             localFilename = filename)
mwhistorySnapshot <- read.table(paste0(dataDir, filename),
                                sep = "\t",
                                stringsAsFactors = FALSE)
mwhistorySnapshot <- tail(mwhistorySnapshot$V1, 1)
mwhistorySnapshot <- 
  stringr::str_extract(mwhistorySnapshot,
                       "[[:digit:]]+-[[:digit:]]+") 

### --- ETL
# - Kerberos init
WMDEData::kerberos_init()
# - Query
query <- paste0("SELECT 
                  event_user_id AS user_id,
                  event_user_text AS username,
                  substring(event_timestamp, 1, 4) AS year,
                  substring(event_timestamp, 6, 2) AS month,
                  substring(event_timestamp, 9, 2) AS day, 
                  COUNT(*) as count 
                FROM wmf.mediawiki_history 
                WHERE (
                  event_entity = 'revision' AND 
                  event_type = 'create' AND 
                  wiki_db = 'dewiki' AND 
                  event_user_is_anonymous = FALSE AND 
                  NOT ARRAY_CONTAINS(user_is_bot_by, 'name') AND 
                  NOT ARRAY_CONTAINS(user_is_bot_by, 'group') AND 
                  NOT ARRAY_CONTAINS(user_is_bot_by_historical, 'name') AND 
                  NOT ARRAY_CONTAINS(user_is_bot_by_historical, 'group') AND 
                  page_namespace = 0 AND 
                  page_is_redirect = FALSE AND 
                  revision_is_deleted_by_page_deletion = FALSE AND
                  event_timestamp > '2019-01-01 00:00:00.0' AND 
                  snapshot = '", mwhistorySnapshot, "') 
                GROUP BY 
                  event_user_id, 
                  event_user_text, 
                  substring(event_timestamp, 1, 4), 
                  substring(event_timestamp, 6, 2),  
                  substring(event_timestamp, 9, 2);"
)
# - prepare to run HiveQL query
queryFile <- "TW_Survey_2021_Query2_CasualEditors.hql"
filename <- "TW_Survey_2021_Query2_CasualEditors.tsv"
write(query, 
      paste0(dataDir, queryFile))
# - run HiveQL query
WMDEData::kerberos_runHiveQL(kerberosUser = "analytics-privatedata",
                             query = paste0(dataDir, queryFile),
                             localPath = dataDir,
                             localFilename = filename)

### --- Analytics

# - load
dataSet <- data.table::fread(
  paste0(
    dataDir,
    "TW_Survey_2021_Query2_CasualEditors.tsv")
  )
head(dataSet)
dataSet$user <- paste0(dataSet$user_id,
                       "_____", 
                       dataSet$username)
dataSet$user_id <- NULL
dataSet$username <- NULL
head(dataSet)
dim(dataSet)

# - remove users who were not active in:
# - 2019, 2020, AND 2021
active_users <- dataSet %>% 
  dplyr::select(user, year) %>% 
  dplyr::group_by(user) %>% 
  dplyr::summarise(years = length(unique(year))) %>% 
  dplyr::filter(years == 3)
head(active_users)
w <- which(dataSet$user %in% active_users$user)
length(w)
dataSet <- dataSet[w, ]
dim(dataSet)

# - total user edits
userEditsFrame <- dataSet %>% 
  dplyr::select(user, count) %>% 
  dplyr::group_by(user) %>% 
  dplyr::summarise(total_edits = sum(count)) %>% 
  dplyr::arrange(dplyr::desc(total_edits))
head(userEditsFrame)

# - mean distance between active days
# - add time units: months
all_days <- 
  seq(as.Date("2019/1/1"), as.Date("2021/10/31"), by = "day")
all_days <- data.frame(date = as.character(all_days), 
                       date_id = 1:length(all_days), 
                       stringsAsFactors = FALSE)
head(all_days)
dataSet$month <- ifelse(nchar(dataSet$month) == 1,
                        paste0("0", dataSet$month),
                        dataSet$month)
dataSet$day <- ifelse(nchar(dataSet$day) == 1,
                      paste0("0", dataSet$day),
                      dataSet$day)
dataSet$date <- paste(dataSet$year,
                      dataSet$month,
                      dataSet$day,
                      sep = "-")
head(dataSet)
dataSet <- dataSet %>% 
  dplyr::left_join(all_days, 
                   by = "date")
head(dataSet)
dataSet <- dataSet %>% 
  dplyr::select(user, date_id)
dataSet <- dataSet %>% 
  dplyr::arrange(user, date_id)
head(dataSet)
length(unique(dataSet$user))


library(snowfall)
snowfall::sfInit(cpus = 30, parallel = TRUE)
snowfall::sfExport("dataSet")
snowfall::sfLibrary(dplyr)
edit_frequency <- snowfall::sfLapply(unique(dataSet$user),
                                     function(x) {
                                       w <- which(dataSet$user == x)
                                       d <- dataSet$date_id[w]
                                       mean_dfs <- mean(diff(d))
                                       total_edit_days = length(d)
                                       return(
                                         data.frame(
                                           user = x,
                                           total_edit_days = total_edit_days,
                                           mean_dfs = mean_dfs,
                                           stringsAsFactors = FALSE
                                           )
                                         )
                                       })
snowfall::sfStop()
edit_frequency <- data.table::rbindlist(edit_frequency)
edit_frequency <- edit_frequency %>% 
  dplyr::arrange(mean_dfs)
head(edit_frequency)
edit_frequency <- edit_frequency %>% 
  dplyr::left_join(userEditsFrame, 
                   by = "user")
head(edit_frequency)
table(edit_frequency$mean_dfs)
edit_frequency <- edit_frequency %>% 
  dplyr::filter(mean_dfs >= 2)
head(edit_frequency)
dim(edit_frequency)
edit_frequency$user <- 
  gsub("^[[:digit:]]+_____", 
       "",
       edit_frequency$user)
head(edit_frequency)
edit_frequency$percent_days <- 
  edit_frequency$total_edit_days/dim(all_days)[1]
head(edit_frequency)
edit_frequency <- edit_frequency %>% 
  dplyr::select(user, 
                percent_days,
                mean_dfs,
                total_edits)
head(edit_frequency)
edit_frequency <- edit_frequency %>% 
  dplyr::arrange(mean_dfs)
head(edit_frequency)
write.csv(edit_frequency, 
          "TW_Survey_2021_Query2_CasualUsers.csv")




