
### --- Wikidata Analytics: Example Workflow

### --- ssh into your favorite Analytics Client
# - ssh stat1005.eqiad.wmnet

### --- navigate to a directory of your choice;
### --- that is where we are going to run R from
### --- (or Python)
### --- In this example, my working directory will be on stat1005:
### --- /home/goransm/wikidata_analytics_examples

# - cd /home/goransm/wikidata_analytics_examples

### --- make a data directory in your working directory;
### --- that is where we are going to store the outputs of our
### --- queries as .csv or .tsv files.

# - mkdir _data

### --- start R console

# - R

### --- define directory_tree
data_dir <- paste0(getwd(), "/_data/")

### --- Example 1.
### --- list my hdfs directory

# - remember to kinit for Kerberos Auth
command <- "kinit"
system(command, wait = TRUE) # - enter your password
# - list my hdfs directory
command <- "hdfs dfs -ls"
system(command, wait = TRUE)
# - save a listing of my hdfs dir to data_dir
filename <- "hdfs_listing.txt"
command <- paste0("hdfs dfs -ls >> ", 
                  data_dir, 
                  filename)
system(command, wait = TRUE)
l <- list.files(data_dir)
print(l)
hdfs_listing <- readLines(paste0(data_dir, filename))
length(hdfs_listing)
head(hdfs_listing)

### --- Example 2.
### --- HiveQL query to select all Wikidata bot users
### --- who made edits in November 2021.
### --- I will use the wmf.mediawiki_history table:
### --- https://wikitech.wikimedia.org/wiki/Analytics/Data_Lake/Edits/MediaWiki_history

query <- "\"SELECT DISTINCT event_user_text FROM wmf.mediawiki_history 
          WHERE (ARRAY_CONTAINS(event_user_is_bot_by, 'group') OR 
                ARRAY_CONTAINS(event_user_is_bot_by, 'name')) AND 
                substr(event_timestamp, 1, 7) = '2021-11' AND 
                event_entity = 'revision' AND 
                event_type = 'create' AND 
                wiki_db = 'wikidatawiki' AND 
                snapshot = '2021-12';\""
filename <- "bots_who_edited_in_202111.tsv"
command <- paste0("beeline -e", 
                  query, 
                  " >> ", 
                  paste0(data_dir, filename))
system(command, wait = TRUE)
l <- list.files(data_dir)
print(l)
bots_edited202111 <- readLines(paste0(data_dir, filename))
length(bots_edited202111)
print(bots_edited202111)

