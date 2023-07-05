randomdataframe <- data.frame(testcol1 = c(1,2,3,4,5), testcol2 = c(5,6,7,8,9), testcol3 = c('a','b','c','d','e'))
randomdataframe

# # Define the output CSV file name
# output_csv_name <- "/var/www/smc/checker/proj/R/output.csv"

# write.csv(randomdataframe, "/var/www/smc/checker/proj/R/output.csv", row.names = FALSE)

# # Write the dataframe to a CSV file in the current directory
# # write.csv(randomdataframe, file = output_csv_name, row.names = FALSE)

#########################################################

args <- commandArgs(trailingOnly=TRUE)
dir <- args[1]
filename <- args[2]
wbpath <- file.path(dir, filename)

output <- (randomdataframe)

write.csv(output, file = file.path(dir, 'output.csv'), row.names = FALSE)
