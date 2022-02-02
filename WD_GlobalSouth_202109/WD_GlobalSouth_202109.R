
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

### ------------------------------------------------------
### --- Protected countries
### ------------------------------------------------------

# - see: https://wikitech.wikimedia.org/wiki/Analytics/Data_Lake/Edits/Geoeditors/Public#Country_Protection_List
protectedCountries <- unique(c("Azerbaijan",
                               "Bahrain",
                               "Belarus",
                               "China",
                               "Cuba",
                               "Egypt",
                               "Ethiopia", 
                               "Iran",
                               "Kazakhstan",
                               "Myanmar",
                               "Pakistan",
                               "Russia",
                               "Rwanda",
                               "Saudi Arabia",
                               "Sudan",
                               "Syria",
                               "Thailand",
                               "Turkey",
                               "United Arab Emirates",
                               "Uzbekistan",
                               "Venezuela",
                               "Vietnam",
                               "Azerbaijan",
                               "Bahrain",
                               "Burundi",
                               "China",
                               "Cuba",
                               "Djibouti",
                               "Egypt",
                               "Equatorial Guinea",
                               "Eritrea",
                               "Iran",
                               "Iraq",
                               "Laos",
                               "Libya",
                               "North Korea",
                               "Saudi Arabia",
                               "Singapore",
                               "Somalia",
                               "Sudan",
                               "Syria",
                               "Tajikistan",
                               "Turkmenistan",
                               "Vietnam",
                               "Yemen")
)
# fuzzy match: protectedCountries to ISOcodes::ISO_3166_1$Name
ixProtected <- lapply(protectedCountries, 
                      function(x) {
                        a <- which.min(
                          as.numeric(adist(x, ISOcodes::ISO_3166_1$Name))
                        )
                        return(
                          data.frame(
                            protected_country = x,
                            ISO_3166_1_Name = ISOcodes::ISO_3166_1$Name[a],
                            code = ISOcodes::ISO_3166_1$Alpha_2[a], 
                            stringsAsFactors = F
                          )
                        )
                      })
ixProtected <- data.table::rbindlist(ixProtected)
# - Correct mismatches manually:
# -  8: Iran, Iraq, IQ
ixProtected$ISO_3166_1_Name[8] <- "Iran"
ixProtected$code[8] <- "IR"
# - 12: Russia, Austria, AT
ixProtected$ISO_3166_1_Name[12] <- "Russia"
ixProtected$code[12] <- "RU"
# - 16: Syria, Serbia, RS
ixProtected$ISO_3166_1_Name[16] <- "Syria"
ixProtected$code[16] <- "SY"
# - 21: Venezuela, Senegal, SN
ixProtected$ISO_3166_1_Name[21] <- "Venezuela"
ixProtected$code[21] <- "VE"
# - 28: Laos, Gabon, GA
ixProtected$ISO_3166_1_Name[28] <- "Laos"
ixProtected$code[28] <- "LA"
# - 30: North Korea, South Africa, ZA
ixProtected$ISO_3166_1_Name[30] <- "North Korea"
ixProtected$code[30] <- "KP"

## --- Analytics

### ------------------------------------------------------
# - Q2. For Wikidata from countries from the Global South:
# - the number of active editors per country.
### ------------------------------------------------------

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

### --- produce gs_active_editors_public

gs_active_editors_public <- gs_active_editors
gs_active_editors_public <- gs_active_editors_public %>% 
  dplyr::filter(country_code != "--")
gs_active_editors_public <- gs_active_editors_public %>% 
  dplyr::filter(activity_level != "1 to 4")
gs_active_editors_public <-gs_active_editors_public %>% 
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
gs_active_editors_public$active_editors[gs_active_editors_public$active_editors > 100] <- 110
gs_active_editors_public <- gs_active_editors_public %>% 
  dplyr::mutate(active_editors_bins = 
                  cut(active_editors, breaks = c(seq(0,
                                                     max(gs_active_editors_public$active_editors),
                                                     by = 10))
                      )
                )

gs_active_editors_public$active_editors_bins <- 
  sapply(gs_active_editors_public$active_editors_bins, 
         function(x) {
           if (is.na(x)) {
             return("0")
           } else {
             if (x == "(100,110]") {
               return("> 100")
             } else {
               b <- stringr::str_extract_all(x, 
                                             "[[:digit:]]+")
               return(
                 paste0(
                   as.numeric(b[[1]][1])+1,
                   " - ",
                   b[[1]][2]
                 )
               )
             }
           }
         })

gs_active_editors_public$active_editors_ns0[gs_active_editors_public$active_editors_ns0 > 100] <- 110
gs_active_editors_public <- gs_active_editors_public %>% 
  dplyr::mutate(active_editors_ns0_bins = 
                  cut(active_editors_ns0, breaks = c(seq(0,
                                                     max(gs_active_editors_public$active_editors_ns0),
                                                     by = 10))
                  )
  )
gs_active_editors_public$active_editors_ns0_bins <- 
  sapply(gs_active_editors_public$active_editors_ns0_bins, 
         function(x) {
           if (is.na(x)) {
             return("0")
           } else {
             if (x == "(100,110]") {
               return("> 100")
             } else {
               b <- stringr::str_extract_all(x, 
                                             "[[:digit:]]+")
               return(
                 paste0(
                   as.numeric(b[[1]][1])+1,
                   " - ",
                   b[[1]][2]
                 )
               )
             }
           }
        })

# - final public datasets
gs_active_editors_public <- gs_active_editors_public %>% 
  dplyr::select(country_code, 
                month,
                active_editors_bins, 
                active_editors_ns0_bins)
# - filter out protected countries
wProtected <- which(
  gs_active_editors_public$country_code %in% ixProtected$code
  )
if (length(wProtected) > 0) {
  gs_active_editors_public <-
    gs_active_editors_public[-wProtected, ]
}
# - store
colnames(gs_active_editors_public)[3:4] <- 
  c("active_editors", "active_editors_ns0")
write.csv(gs_active_editors_public, 
          paste0(analyticsDir,
                 "gs_active_editors_PUBLIC.csv"))

### --- Continue processing for the NDA group

# - filter out unrecognized countries
gs_active_editors <- gs_active_editors %>% 
  dplyr::filter(country_code != "--")

# - filter out inactive users;
# - def: activity_level is < 5 edits monthly
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

### ------------------------------------------------------
# - Q1. For Wikidata from countries from the Global South:
# - the number of edits in the last 12 months per country
### ------------------------------------------------------

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

### --- produce gs_active_editors_public

gs_edits_public <- gs_edits
gs_edits_public <- gs_edits_public %>% 
  dplyr::filter(country_code != "--")
gs_edits_public <- gs_edits_public %>% 
  dplyr::select(country_code, 
                edit_count, 
                namespace_zero_edit_count, 
                month) %>% 
  dplyr::group_by(country_code, month) %>% 
  dplyr::summarise(edit_count = 
                     sum(edit_count), 
                   edit_count_ns0 = 
                     sum(namespace_zero_edit_count)) %>% 
  dplyr::arrange(country_code, month)
gs_edits_public$edit_count[gs_edits_public$edit_count > 1000] <- 1100
gs_edits_public <- gs_edits_public %>% 
  dplyr::mutate(edit_count_bins = 
                  cut(edit_count, breaks = c(seq(0,
                                                 max(gs_edits_public$edit_count),
                                                 by = 100))
                  )
  )
gs_edits_public$edit_count_bins <- 
  sapply(gs_edits_public$edit_count_bins, 
         function(x) {
           if (is.na(x)) {
             return("0")
           } else {
             if (x == "(1e+03,1.1e+03]") {
               return("> 1000")
             } else {
               if (x == "(900,1e+03]") {
                 return("901 - 1000")
               } else {
                 b <- stringr::str_extract_all(x, 
                                               "[[:digit:]]+")
                 return(
                   paste0(
                     as.numeric(b[[1]][1])+1,
                     " - ",
                     b[[1]][2]
                   )
                 )
               }
             }
           }
         })
gs_edits_public$edit_count_ns0[gs_edits_public$edit_count_ns0 > 1000] <- 1100
gs_edits_public <- gs_edits_public %>% 
  dplyr::mutate(edit_count_ns0_bins = 
                  cut(edit_count_ns0, breaks = c(seq(0,
                                                 max(gs_edits_public$edit_count_ns0),
                                                 by = 100))
                  )
  )
gs_edits_public$edit_count_ns0_bins <- 
  sapply(gs_edits_public$edit_count_ns0_bins, 
         function(x) {
           if (is.na(x)) {
             return("0")
           } else {
             if (x == "(1e+03,1.1e+03]") {
               return("> 1000")
             } else {
               if (x == "(900,1e+03]") {
                 return("901 - 1000")
               } else {
                 b <- stringr::str_extract_all(x, 
                                               "[[:digit:]]+")
                 return(
                   paste0(
                     as.numeric(b[[1]][1])+1,
                     " - ",
                     b[[1]][2]
                   )
                 )
               }
             }
           }
         })

# - final public datasets
gs_edits_public <- gs_edits_public %>% 
  dplyr::select(country_code, 
                month,
                edit_count_bins, 
                edit_count_ns0_bins)
# - filter out protected countries
wProtected <- which(
  gs_edits_public$country_code %in% ixProtected$code
)
if (length(wProtected) > 0) {
  gs_edits_public <-
    gs_edits_public[-wProtected, ]
}
# - store
colnames(gs_edits_public)[3:4] <- 
  c("edits", "edits_ns0")
write.csv(gs_edits_public, 
          paste0(analyticsDir,
                 "gs_edits_PUBLIC.csv"))

### --- Continue processing for the NDA group

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
