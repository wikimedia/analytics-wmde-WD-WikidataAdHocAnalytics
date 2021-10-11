#!/usr/bin/env Rscript

### ---------------------------------------------------------------------------
### --- Project: WMDE Banner Impressions
### --- Version 1.0.0
### --- Script: WMDE_BannerImpressions.R
### --- September 2021.
### --- Author: Goran S. Milovanovic, Data Scientist, WMDE
### --- Developed under the contract between Goran Milovanovic PR Data Kolektiv
### --- and WMDE.
### --- Description: obtain WMDE banner impressions
### --- 
### --- Reference Phab ticket: Provide Provide aggregated banner impression counts
### --- https://phabricator.wikimedia.org/T283015
### --- Contact: goran.milovanovic_ext@wikimedia.de
### ---------------------------------------------------------------------------

### ---------------------------------------------------------------------------
### --- LICENSE:
### ---------------------------------------------------------------------------
### --- GPL v2
### --- This file is part of the WMDE Banner Impressions project (WMDEBIP)
### ---
### --- WMDEBIP is free software: you can redistribute it and/or modify
### --- it under the terms of the GNU General Public License as published by
### --- the Free Software Foundation, either version 2 of the License, or
### --- (at your option) any later version.
### ---
### --- WMDEBIP is distributed in the hope that it will be useful,
### --- but WITHOUT ANY WARRANTY; without even the implied warranty of
### --- MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
### --- GNU General Public License for more details.
### ---
### --- You should have received a copy of the GNU General Public License
### --- along with QURATOR Curious Facts If not, see <http://www.gnu.org/licenses/>.
### ---------------------------------------------------------------------------

### --- fPath: where the scripts is run from?
fPath <- as.character(commandArgs(trailingOnly = FALSE)[4])
fPath <- gsub("--file=", "", fPath, fixed = TRUE)
fPath <- unlist(strsplit(fPath, split = "/", fixed = TRUE))
fPath <- paste(
  paste(fPath[1:length(fPath) - 1], collapse = "/"),
  "/",
  sep = "")

### --- dirTree
dataDir <- paste0(fPath, "_data/")
publicDir <- 
  "/srv/published/datasets/wmde-analytics-engineering/WMDE_Banners/"

### --- Environment
renv::load(project = fPath, 
           quiet = FALSE)

### --- Lib
library(WMDEData)

### --- Logic

# - determine time
time_now <- Sys.time()
time_target <- time_now - 2*60*60
time_target <- as.character(time_target)
year <- substr(time_target, 1, 4)
month <- substr(time_target, 6, 7)
day <- substr(time_target, 9, 10)
hour <- substr(time_target, 12, 13)

### --- Report
print(paste0("--- END time_now: ", 
             time_now, " done for ", 
             year, month, day, ", hour:  ", hour))

# - compose query
query <- paste0('SELECT uri_query, dt FROM wmf.webrequest 
                  WHERE 
                    (uri_host = "de.wikipedia.org" 
                    OR uri_host = "de.m.wikipedia.org" 
                    OR uri_host = "en.wikipedia.org" 
                    OR uri_host = "en.m.wikipedia.org") 
                    AND uri_path = "/beacon/impression" 
                    AND year = ', year, 
                    ' AND month = ', month, 
                    ' AND day = ', day,
                    ' AND hour = ', hour, 
                    ' AND uri_query LIKE "%WMDE%";')

# - run query
# - Kerberos init
WMDEData::kerberos_init(kerberosUser = "analytics-privatedata")
# - Define query
queryFile <- paste0(dataDir, 
                    "WMDEBanners_HiveQL_Query.hql")
write(query, queryFile)
# - Run HiveQL query
filename <- "wmdebanners_batch.tsv"
WMDEData::kerberos_runHiveQL(kerberosUser = "analytics-privatedata",
                             query = queryFile,
                             localPath = dataDir,
                             localFilename = filename)
file.remove(queryFile)

# - load and wrangle raw data
data_set <- data.table::fread(paste0(dataDir, filename))
data_set$dt <- as.character(data_set$dt)
data_set$hh <- substr(data_set$dt, 12, 13)
data_set$mm <- substr(data_set$dt, 15, 16)
data_set$dt <- NULL
banner_set <- vector(mode = "list", length = 4)
banner_set[[1]] <- data_set %>% 
  dplyr::filter(mm >= 0 & mm < 15) %>% 
  dplyr::select(-hh, -mm)
banner_set[[2]] <- data_set %>% 
  dplyr::filter(mm >= 15 & mm < 30) %>% 
  dplyr::select(-hh, -mm)
banner_set[[3]] <- data_set %>% 
  dplyr::filter(mm >= 30 & mm < 45) %>% 
  dplyr::select(-hh, -mm)
banner_set[[4]] <- data_set %>% 
  dplyr::filter(mm >= 45 & mm <= 59) %>% 
  dplyr::select(-hh, -mm)
rm(data_set)

# - process banners
banner_set <- lapply(banner_set, function(x) {
  
  # - get data
  banner_data <- x$uri_query
    
  # - split
  banner_data <- strsplit(banner_data, 
                          split = "&", fixed = T)
  
  # - extract relevant fields
  # - banner:
  banner <- sapply(banner_data, function(x) {
    x[which(grepl("^banner=", x))]
  })
  banner <- gsub("^banner=", "", banner)
  
  # - recordImpressionSampleRate:
  recordImpressionSampleRate <- sapply(banner_data, function(x) {
    x[which(grepl("^recordImpressionSampleRate=", x))]
  })
  recordImpressionSampleRate <- as.numeric(
    gsub("^recordImpressionSampleRate=", "", recordImpressionSampleRate)
  )
  
  # - result:
  result <- sapply(banner_data, function(x) {
    x[which(grepl("^result=", x))]
  })
  result <- gsub("^result=", "", result)
  
  # - compose table:
  banner_observations <- 
    data.frame(banner = banner,
               recordImpressionSampleRate = recordImpressionSampleRate,
               result = result,
               stringsAsFactors = F)
  
  # - filter result for B\d{2}_WMDE
  w <- which(grepl("^B[[:digit:]][[:digit:]]_WMDE", 
                   banner_observations$banner)
  )
  
  # - switch: target banners found:
  if (length(w) > 0) {
    
    # - keep target banners
    banner_observations <- banner_observations[w, ]
    
    # - filter for result == show
    banner_observations <- dplyr::filter(banner_observations,
                                         result == "show")
    
    # - correction for recordImpressionSampleRate
    banner_observations$recordImpressionSampleRate <- 
      1/banner_observations$recordImpressionSampleRate
    
    # - aggregate:
    banner_observations <- banner_observations %>% 
      dplyr::select(banner, recordImpressionSampleRate) %>% 
      dplyr::group_by(banner) %>% 
      dplyr::summarise(impressions = sum(recordImpressionSampleRate))
    
  } else {
    banner_observations <- 
      setNames(data.frame(matrix(ncol = 2, nrow = 0)), 
               c("banner", "impressions"))

  }
  
  # - output
  return(banner_observations)

})

# - determine which files to delete, if any
if (nchar(month) == 1) {
  month <- paste0("0", month)
}
if (nchar(day) == 1) {
  month <- paste0("0", day)
}
if (nchar(hour) == 1) {
  month <- paste0("0", hour)
}
new_filenames <- paste0("banner_impressions_",
                        year, month, day, 
                        "_", hour, 
                        c("00", "15", "30", "45")
                        )
filenames <- list.files(publicDir)
filenames <- c(filenames, new_filenames)
filedates <- unname(sapply(filenames, function(x) {
  d <- strsplit(x, "_")[[1]][3]
}))
if (length(unique(filedates)) > 30) {
  delete_date <- head(sort(unique(filedates)), 1)
  w_del <- which(grepl(delete_date, filedates))
  lapply(filenames[w_del], function(x) {
    cmd <- paste0("rm ", publicDir, x)
    system(command = cmd,
           wait = TRUE)
  })
}

# - copy to public dir
new_filenames <- paste0(new_filenames, ".csv")
for (i in 1:length(banner_set)) {
  write.csv(banner_set[[i]], 
            paste0(publicDir, new_filenames[i]))
}
# - clear dataDir
lapply(paste0(dataDir, list.files(dataDir)), 
       file.remove)

### --- Report
print(paste0("--- END time_now: ", 
             as.character(Sys.time()), " done for ", 
             year, month, day, ", hour:  ", hour))
