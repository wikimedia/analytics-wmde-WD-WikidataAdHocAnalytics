
### --- project: Global South Contributions to Wikidata
### --- https://phabricator.wikimedia.org/T291170

### --- setup

# - env
renv::load()

# - lib
library(WMDEData)

# - dirTree
workDir <- paste0(getwd(), 
                  "/WD_GlobalSouth_202109/")
dataDir <- paste0(workDir, 
                  "_data/")
analyticsDir <- paste0(workDir,
                       "_analytics/")

## --- Analytics

# - Q2. For Wikidata from countries from the Global South:
# - the number of active editors per country.

timeSpan <- c("2020-09",
              "2020-10",
              "2020-11",
              "2020-12",
              "2021-01",
              "2021-02",
              "2021-03",
              "2021-04",
              "2021-05",
              "2021-06",
              "2021-07", 
              "2021-08")

# - Kerberos init
WMDEData::kerberos_init(kerberosUser = "analytics-privatedata")
# - HiveQL
lapply(timeSpan, function(x) {
  
  hiveQLquery <- 
    paste0(
      'SELECT * FROM wmf.geoeditors_monthly WHERE wiki_db="wikidatawiki" AND month = "',
      x,
      '";')
  # - Define query
  queryFile <- paste0(workDir, 
                      "WD_GS_activeEditors.hql")
  write(hiveQLquery, queryFile)
  # - Run HiveQL query
  filename <- paste0("WD_GS_activeEditors_", x, ".tsv")
  WMDEData::kerberos_runHiveQL(kerberosUser = "analytics-privatedata",
                               query = queryFile,
                               localPath = dataDir,
                               localFilename = filename)

})

# - compose active users dataset
gs_active_editors <- data.table::rbindlist(
  lapply(paste0(dataDir,
                list.files(dataDir)),
         data.table::fread,
         sep = "\t")
)

# - filter out unrecognized countries
gs_active_editors <- gs_active_editors %>% 
  dplyr::filter(country_code != "--")

# - filter out inactive users;
# - def: activity_level is > 5 edits monthly
gs_active_editors <- gs_active_editors %>% 
  dplyr::filter(activity_level != "1 to 4")

# - aggregated dataset
gs_active_editors <- gs_active_editors %>% 
  dplyr::select(country_code, 
                distinct_editors, 
                namespace_zero_distinct_editors, 
                month) %>% 
  dplyr::group_by(country_code, month) %>% 
  dplyr::summarise(active_editors = 
                     sum(distinct_editors), 
                   active_editors_ns0 = 
                     sum(namespace_zero_distinct_editors)) %>% 
  dplyr::arrange(country_code, month)

write.csv(gs_active_editors, 
          paste0(analyticsDir,
                 "gs_active_editors.csv"))

# - clear dataDir
lapply(paste0(dataDir, list.files(dataDir)), 
       file.remove)


# - Q1. For Wikidata from countries from the Global South:
# - the number of edits in the last 12 months per country

timeSpan <- c("2020-09",
              "2020-10",
              "2020-11",
              "2020-12",
              "2021-01",
              "2021-02",
              "2021-03",
              "2021-04",
              "2021-05",
              "2021-06",
              "2021-07", 
              "2021-08")

# - Kerberos init
WMDEData::kerberos_init(kerberosUser = "analytics-privatedata")
# - HiveQL
lapply(timeSpan, function(x) {
  
  hiveQLquery <- 
    paste0(
      'SELECT * FROM wmf.geoeditors_edits_monthly WHERE wiki_db="wikidatawiki" AND month = "',
      x,
      '";')
  # - Define query
  queryFile <- paste0(workDir, 
                      "WD_GS_activeEditorsEdits.hql")
  write(hiveQLquery, queryFile)
  # - Run HiveQL query
  filename <- paste0("WD_GS_activeEditorsEdits_", x, ".tsv")
  WMDEData::kerberos_runHiveQL(kerberosUser = "analytics-privatedata",
                               query = queryFile,
                               localPath = dataDir,
                               localFilename = filename)
  
})

# - compose active users dataset
gs_edits <- data.table::rbindlist(
  lapply(paste0(dataDir,
                list.files(dataDir)),
         data.table::fread,
         sep = "\t")
)

# - filter out unrecognized countries
gs_edits <- gs_edits %>% 
  dplyr::filter(country_code != "--")

# - aggregated dataset
gs_edits <- gs_edits %>% 
  dplyr::select(country_code, 
                edit_count, 
                namespace_zero_edit_count, 
                month) %>% 
  dplyr::group_by(country_code, month) %>% 
  dplyr::summarise(edits = 
                     sum(edit_count), 
                   edits_ns0 = 
                     sum(namespace_zero_edit_count)) %>% 
  dplyr::arrange(country_code, month)

write.csv(gs_edits, 
          paste0(analyticsDir,
                 "gs_edits.csv"))

# - clear dataDir
lapply(paste0(dataDir, list.files(dataDir)), 
       file.remove)
