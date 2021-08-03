### ---------------------------------------------------------------------------
### --- 01_WD_userRetention_ETL.R
### --- Author: Goran S. Milovanovic, Data Scientist, WMDE
### --- Developed under the contract between:
### --- Goran Milovanovic PR Data Kolektiv and WMDE.
### --- Contact: goran.milovanovic_ext@wikimedia.de
### --- July 2021.
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

### --- setup
library(data.table)
library(tidyverse)

### --- dirTree
dataDir <- 
  "/home/goransm/Analytics/Wikidata/WD_misc/WD_UserRetention/_data/"
analyticsDir <- 
  "/home/goransm/Analytics/Wikidata/WD_misc/WD_UserRetention/_analytics/"

### --- ETL 1: namespaces 0, 120, 146

# - Kerberos init
system(command = paste0('sudo -u analytics-privatedata ', 
                        'kerberos-run-command analytics-privatedata ', 
                        'hdfs dfs -ls'),
       wait = T)

# - Query
queryFile <- "WD_retention.hql"
filename <- "WD_retention.tsv"
connect <- paste0('sudo -u analytics-privatedata ', 
                  'kerberos-run-command analytics-privatedata ', 
                  '/usr/local/bin/beeline ', 
                  '--silent --incremental=true --verbose=false -f ')
query <- 'USE wmf; 
          SELECT 
            event_user_id, event_user_registration_timestamp, 
            substring(event_timestamp, 1, 4) AS year, 
            substring(event_timestamp, 6, 2) AS month, 
            COUNT(*) AS revisions FROM mediawiki_history 
          WHERE (
            event_entity = \'revision\' AND 
            event_type = \'create\' AND 
            wiki_db = \'wikidatawiki\' AND 
            event_user_is_anonymous = FALSE AND 
            NOT ARRAY_CONTAINS(event_user_is_bot_by, \'name\') AND 
            NOT ARRAY_CONTAINS(event_user_is_bot_by, \'group\') AND 
            NOT ARRAY_CONTAINS(event_user_is_bot_by_historical, \'name\') AND 
            NOT ARRAY_CONTAINS(event_user_is_bot_by_historical, \'group\') AND 
            NOT ARRAY_CONTAINS(event_user_groups, \'bot\') AND 
            NOT ARRAY_CONTAINS(event_user_groups_historical, \'bot\') AND 
            event_user_id != 0 AND 
            page_is_redirect = FALSE AND 
            revision_is_deleted_by_page_deletion = FALSE AND 
            (page_namespace = 0 OR page_namespace = 120 OR page_namespace = 146) AND 
            snapshot = \'2021-06\'
          ) 
          GROUP BY 
            event_user_id, 
            event_user_registration_timestamp, 
            substring(event_timestamp, 1, 4), 
            substring(event_timestamp, 6, 2);'
write(query, paste0(dataDir, queryFile))
out <- paste0("> ", dataDir, filename)

# - Execute
qCommand <- paste0(connect, '"', paste0(dataDir, queryFile), '" ', out)
system(qCommand, wait = T)

### --- Wrangle source data 1: Data Namespaces (ETL 1)
dataSet <- fread(paste0(dataDir, filename))
# - fix dataSet$month for leading zeros
dataSet$month <- sapply(dataSet$month, function(x) {
  if (nchar(x) == 1) {
    return(paste0("0", x))
  } else {
    return(x)
  }
})
dataSet$revisionYM <- paste0(dataSet$year, "-", dataSet$month)
dataSet$registrationYM <- 
  substr(dataSet$event_user_registration_timestamp, 1, 7)
dataSet <- select(dataSet, 
                  event_user_id, 
                  registrationYM,
                  revisionYM,
                  revisions)
colnames(dataSet)[1] <- 'userId'
dataSet <- arrange(dataSet, 
                   userId,
                   registrationYM,
                   revisionYM)

### --- ETL 2: talk namespaces 1, 3, 5, 7, 9, 11, 13, 15, 
### --- 121, 123, 147, 641, 829, 1199, 2301, 2303

# - Kerberos init
system(command = paste0('sudo -u analytics-privatedata ', 
                        'kerberos-run-command analytics-privatedata ', 
                        'hdfs dfs -ls'),
       wait = T)

# - Query
queryFile <- "WD_retentionTalk.hql"
filename <- "WD_retentionTalk.tsv"
connect <- paste0('sudo -u analytics-privatedata ', 
                  'kerberos-run-command analytics-privatedata ', 
                  '/usr/local/bin/beeline ', 
                  '--silent --incremental=true --verbose=false -f ')
query <- 'USE wmf; 
          SELECT 
            event_user_id, COUNT(*) AS talkrevisions FROM mediawiki_history 
          WHERE (
            event_entity = \'revision\' AND 
            event_type = \'create\' AND 
            wiki_db = \'wikidatawiki\' AND 
            event_user_is_anonymous = FALSE AND 
            NOT ARRAY_CONTAINS(event_user_is_bot_by, \'name\') AND 
            NOT ARRAY_CONTAINS(event_user_is_bot_by, \'group\') AND 
            NOT ARRAY_CONTAINS(event_user_is_bot_by_historical, \'name\') AND 
            NOT ARRAY_CONTAINS(event_user_is_bot_by_historical, \'group\') AND 
            NOT ARRAY_CONTAINS(event_user_groups, \'bot\') AND 
            NOT ARRAY_CONTAINS(event_user_groups_historical, \'bot\') AND 
            event_user_id != 0 AND 
            page_is_redirect = FALSE AND 
            revision_is_deleted_by_page_deletion = FALSE AND 
            (page_namespace = 1 OR 
            page_namespace = 3 OR 
            page_namespace = 5 OR 
            page_namespace = 7 OR 
            page_namespace = 9 OR 
            page_namespace = 11 OR 
            page_namespace = 13 OR 
            page_namespace = 15 OR 
            page_namespace = 121 OR 
            page_namespace = 123 OR 
            page_namespace = 147 OR 
            page_namespace = 641 OR 
            page_namespace = 829 OR 
            page_namespace = 1199 OR 
            page_namespace = 2301 OR 
            page_namespace = 2303) AND 
            snapshot = \'2021-06\'
          ) 
          GROUP BY event_user_id;'
write(query, paste0(dataDir, queryFile))
out <- paste0("> ", dataDir, filename)

# - Execute
qCommand <- paste0(connect, '"', paste0(dataDir, queryFile), '" ', out)
system(qCommand, wait = T)

### --- Wrangle source data 2: Talk Namespaces (ETL 2)
talkDataSet <- fread(paste0(dataDir, filename))
colnames(talkDataSet)[1] <- 'userId'

# - Anonimize userId in BOTH datasets: 
# - (1) revisions in data namespaces, (2) revisions in talk namespaces
userIds <- unique(c(dataSet$userId, 
                    talkDataSet$userId)
                  )
anons <- seq(1, length(userIds), by = 1)
anons <- sample(anons, size = length(anons), replace = F)
anons <- data.frame(anons = anons, 
                    userId = userIds)
# - anonimize dataSet
dataSet <- left_join(dataSet,
                     anons,
                     by = "userId")
dataSet$userId <- dataSet$anons
dataSet$anons <- NULL
write.csv(dataSet,
          paste0(analyticsDir, "WD_UserRetention.csv"))
# - anonimize talkDataSet
talkDataSet <- left_join(talkDataSet,
                     anons,
                     by = "userId")
talkDataSet$userId <- talkDataSet$anons
talkDataSet$anons <- NULL
write.csv(talkDataSet,
          paste0(analyticsDir, "WD_UserRetention_TalkRevisions.csv"))
