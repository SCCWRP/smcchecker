library(DBI)
library(dplyr)
library(RPostgreSQL)
library(CSCI)
library(readxl)

args = commandArgs(trailingOnly=TRUE)

dir <- args[1]
filename <- args[2]

print('Connecting to the database')
con <- dbConnect(
  PostgreSQL(),
  host = Sys.getenv('DB_HOST'),
  dbname = Sys.getenv('DB_NAME'),
  user = Sys.getenv('DB_USER'),
  password = Sys.getenv('DB_PASSWORD')
)
print('Created database connection')

bugs <-  readxl::read_excel(file.path(dir, filename))

# This view does not limit the stations to the ones that are in unified_taxonomy, for some reason
gis <- dbGetQuery(con, 'SELECT * FROM vw_csci_gispredictors') %>% filter(StationCode %in% bugs$stationcode)

bugs <- bugs %>%
    mutate(
        sampleid = paste(stationcode, gsub("[/\\s:]", "", format(sampledate, "%Y%m%d")), fieldreplicate, sep = "_")) %>%
    select(c('stationcode', 'sampleid' ,'baresult', 'lifestagecode', 'distinctcode', 'finalid')) %>%
    rename(
        `StationCode` = stationcode,
        `SampleID` = sampleid,
        `FinalID` = finalid,
        `BAResult` = baresult,
        `LifeStageCode` = lifestagecode,
        `Distinct` = distinctcode
    )

bugs <- cleanData(bugs)
bugs <- bugs %>% mutate(BAResult = as.numeric(BAResult)) # this can be altered in the view definition
metadata <- BMIMetrics::loadMetaData()
namecheck <- paste(bugs$FinalID, bugs$LifeStageCode) %in% paste(metadata$FinalID, metadata$LifeStageCode)
missing <- which(!namecheck)
if(length(missing)>0){
  casenamecheck <- paste(toupper(bugs$FinalID[missing]), bugs$LifeStageCode[missing]) %in% paste(toupper(metadata$FinalID), metadata$LifeStageCode)
  bugs$FinalID[missing][casenamecheck] <- 
    as.character(metadata$FinalID[match(toupper(bugs$FinalID[missing][casenamecheck]), toupper(metadata$FinalID))])
  if(length(namecheck) != (sum(namecheck) + sum(casenamecheck)))
    warning(c("The following FinalID/LifeStageCode combinations did not match with the internal database:",
              paste(unique(paste(bugs$FinalID[!namecheck], bugs$LifeStageCode[!namecheck])), collapse=", ")))
}
notfound <- bugs %>% filter(!namecheck)
bugs <- bugs %>% filter(namecheck)

result <- CSCI(bugs, gis)

core <- result$core
suppl1_grps <- result$Suppl1_grps
suppl1_mmi <- result$Suppl1_mmi
suppl2_mmi <- result$Suppl2_mmi
suppl1_oe <- result$Suppl1_OE
suppl2_oe <- result$Suppl2_OE

write.csv(core, file = file.path(dir, 'core.csv'), row.names = FALSE)
write.csv(suppl1_grps, file = file.path(dir, 'suppl1_grps.csv'), row.names = FALSE)
write.csv(suppl1_mmi, file = file.path(dir, 'suppl1_mmi.csv'), row.names = FALSE)
write.csv(suppl2_mmi, file = file.path(dir, 'suppl2_mmi.csv'), row.names = FALSE)
write.csv(suppl1_oe, file = file.path(dir, 'suppl1_oe.csv'), row.names = FALSE)
write.csv(suppl2_oe, file = file.path(dir, 'suppl2_oe.csv'), row.names = FALSE)
