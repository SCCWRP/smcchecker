library(PHABMetrics)
library(PHAB)


##########---R Script----##############################
randomdataframe <- data.frame(testcol1 = c(1,2,3,4,5), testcol2 = c(5,6,7,8,9), testcol3 = c('a','b','c','d','e'))
print("dataframe: ")
randomdataframe

args <- commandArgs(trailingOnly=TRUE)
print("args:")
args
dir <- args[1]
print("dir:")
dir
filename <- args[2]
print("filename:")
filename
wbpath <- file.path(dir, filename)
print("wbpath:")
wbpath
output <- (randomdataframe)
print("Output:")
output

write.csv(randomdataframe, file = file.path(dir, 'output.csv'), row.names=FALSE)


########################################