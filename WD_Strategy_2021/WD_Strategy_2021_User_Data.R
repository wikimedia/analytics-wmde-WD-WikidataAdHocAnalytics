
### ----------------------------------------------------------------
### --- project: Wikidata Strategy Data and Insights
### --- USER oriented
### --- Reference Phabricator tickets:
### ----------------------------------------------------------------
# - Provide Wikidata user behavior data to categorize users by hand
# - https://phabricator.wikimedia.org/T287667
# - Number and proportion of bot edits per projects
# - https://phabricator.wikimedia.org/T289810

### ----------------------------------------------------------------
### --- setup

# - env
renv::load()

# - lib
library(WMDEData)

# - proxy
WMDEData::set_proxy()

# - dirTree
workDir <- paste0(getwd(), "/")
dataDir <- paste0(workDir, 
                  "_data/")
analyticsDir <- paste0(workDir,
                       "_analytics/")


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

### --- Select year and month
year <- "2021"
month <- "08"

### --- ETL
# - from wmf.mediawiki_history:
# - username - a signed NDA document with the WMF is needed for this
# - last edit 
# - (plus active editor status derived from that -- 
# - edited at all last 30 days)
# - user edits per namespace (pagenamespace)
# - bot flag
# - revision tags
# - cumulative edit counts

# - Kerberos init
WMDEData::kerberos_init()
# - Query
query <- paste0("SELECT 
                  event_user_id, 
                  event_user_text, 
                  event_user_revision_count, 
                  event_user_is_bot_by, 
                  page_namespace, 
                  revision_tags,
                  event_timestamp 
                FROM wmf.mediawiki_history 
                WHERE (
                  event_entity = 'revision' AND 
                  event_type = 'create' AND 
                  wiki_db = 'wikidatawiki' AND 
                  event_user_is_anonymous = FALSE AND 
                  page_is_redirect = FALSE AND 
                  revision_is_deleted_by_page_deletion = FALSE AND 
                  snapshot = '", 
                  mwhistorySnapshot, "' AND 
                  substring(event_timestamp, 1, 4) = '", year, "' AND 
                  substring(event_timestamp, 6, 2) = '", month, "');"
)
# - prepare to run HiveQL query
queryFile <- "WD_strategyETL_USERS.hql"
filename <- "WD_strategyETL_USERS.tsv"
write(query, 
      paste0(dataDir, queryFile))
# - run HiveQL query
WMDEData::kerberos_runHiveQL(kerberosUser = "analytics-privatedata",
                             query = paste0(dataDir, queryFile),
                             localPath = dataDir,
                             localFilename = filename)

### ----------------------------------------------------------------
### --- Wrangle datasets

### --- The fundamental dataset
dataSet <- data.table::fread(paste0(dataDir, filename))
colnames(dataSet) <- c("user_id",
                       "user_name",
                       "revision_count",
                       "bot_by",
                       "namespace",
                       "revision_tags",
                       "revision_timestamp")

### ------------------------------------------------------------
### --- wrange/produce analytics datasets

### --- The fundamental dataset
dataSet <- fread(paste0(dataDir, filename))
colnames(dataSet) <- c("user_id",
                       "user_name",
                       "revision_count",
                       "bot_by",
                       "namespace",
                       "revision_tags",
                       "revision_timestamp")

### --- Analytics Dataset 0: user_id + user_name + bot_by + revisions_Aug2021
userSet <- dataSet %>% 
  dplyr::select(user_id, user_name, bot_by)
userSet$bot_flag <- ifelse(grepl("name|group", userSet$bot_by), 
                           TRUE, FALSE)
userRev <- userSet %>% 
  dplyr::select(user_id) %>% 
  dplyr::group_by(user_id) %>% 
  dplyr::summarise(revisions_Aug2021 = dplyr::n())
userSet <- userSet[!duplicated(userSet), ]
userSet <- userSet %>% 
  dplyr::left_join(userRev, 
                   by = "user_id")
# - check:
length(userSet$user_id)
length(unique(userSet$user_id))
length(userSet$user_name)
length(unique(userSet$user_name))
userSet$bot_by <- NULL
# - check:
table(userSet$bot_flag)
# - check:
which(duplicated(userSet))
# - store:
write.csv(userSet, 
          paste0(analyticsDir, "WDS2021_USER_userFrame.csv"))

### --- Analytics Dataset 1: revisions per namespace

revisionsNamespaceFrame <- dataSet %>% 
  dplyr::select(user_id, namespace)
revisionsNamespaceFrame <- table(revisionsNamespaceFrame$user_id,
                                 revisionsNamespaceFrame$namespace)
revisionsNamespaceFrame <- as.data.frame(revisionsNamespaceFrame)
colnames(revisionsNamespaceFrame) <- c("user_id", "namespace", "Freq")
revisionsNamespaceFrame <- revisionsNamespaceFrame %>% 
  tidyr::pivot_wider(id_cols = user_id,
                     names_from = namespace,
                     values_from = Freq,
                     values_fill = 0)
revisionsNamespaceFrame <- as.data.frame(revisionsNamespaceFrame)
colnames(revisionsNamespaceFrame)[2:length(colnames(revisionsNamespaceFrame))] <- 
  paste0("ns_", colnames(revisionsNamespaceFrame)[2:length(colnames(revisionsNamespaceFrame))])
write.csv(revisionsNamespaceFrame, 
          paste0(analyticsDir, 
                 "WDS2021_USER_userRevisionNamespaceFrame.csv"))

### --- Analytics dataset 2: 
### --- user_id, timestamp_last_revision, cumulative edit counts

lastRevisionFrame <- dataSet %>% 
  dplyr::select(user_id,
                revision_count,
                revision_timestamp) %>% 
  dplyr::group_by(user_id) %>% 
  dplyr::arrange(revision_timestamp) %>% 
  dplyr::filter(dplyr::row_number() == dplyr::n())
write.csv(lastRevisionFrame, 
          paste0(analyticsDir, 
                 "WDS2021_USER_lastRevisionFrame.csv"))


### --- Analytics dataset 3: 
### --- user_id x revision_tag

tags <- gsub("^\\[|\\]$", "", dataSet$revision_tags)
ncores <- 30
library(snowfall)
sfInit(parallel = T, cpus = ncores)
sfExport('tags')
tags <- sfLapply(tags, function(x) {
  gsub('"', '', 
       strsplit(x, ",", fixed = T)[[1]]
  )
})
sfStop()
names(tags) <- dataSet$user_id
tags <- stack(tags)
tags <- tags %>% 
  dplyr::filter(values != "NULL")
colnames(tags) <- c("revision_tag", "user_id")
tagsFrame <- tags %>% 
  dplyr::group_by(user_id, revision_tag) %>% 
  dplyr::summarise(n_tag = dplyr::n())
tagsFrame <- tagsFrame %>% 
  tidyr::pivot_wider(id_cols = user_id,
                     names_from = revision_tag,
                     values_from = n_tag,
                     values_fill = 0)
write.csv(tagsFrame, 
          paste0(analyticsDir, 
                 "WDS2021_USER_userTagsFrame.csv"))


