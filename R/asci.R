library(DBI)
library(dplyr)
library(RPostgreSQL)
library(ASCI)
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

alg <- readxl::read_excel(file.path(dir, filename))

# Select from vw_asci_checker_gispredictors since the other view vw_asci_gispredictors only grabs records for stations in unified algae
# That view is for the sync script
gis <- dbGetQuery(con, 'SELECT * FROM vw_asci_checker_gispredictors') %>% filter(StationCode %in% alg$stationcode)

alg <- alg %>% 
  select(c('stationcode','sampledate','replicate','sampletypecode','baresult','result','finalid')) %>% 
  rename(
    `StationCode` = stationcode,
    `SampleDate` = sampledate,
    `Replicate` = replicate,
    `SampleTypeCode` = sampletypecode,
    `BAResult` = baresult,
    `Result` = result,
    `FinalID` = finalid                
  )


output <- ASCI(alg, gis) %>% smcify 

write.csv(output, file = file.path(dir, 'ASCI_scores.csv'), row.names = FALSE)

