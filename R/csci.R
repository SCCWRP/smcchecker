#GOAL: DONE!
# Look in the bight checker and try to figure out how we did this, and implement it here
# just read the code (on nexus or nexus-dev) in the repository /var/www/bight23/checker

# In proj/custom/ocean_acidification.py there is a routine at the end of the file that calls an R script with the subprocess library
# Python opens a subprocess and the R script gets executed
# There is a file in that same repo in a folder called "R" and the file name is oa.R
# That is a short analysis script that reads in the excel file from the submission directory and writes a CSV to the submission directory

# Back in the python script that opened the subprocess, it checks if the csv got written out, 
# and if it did, it reads it in and appends the data to the user's submission file

# If not, it gives a warning on all rows of the user's dataframe

# Here in the SMC checker we will do all that except for issuing the warning. If it doesnt calculate the analysis, it is what it is

# For now, call this script with subprocess if there are no taxonomy errors. Write out a random dataframe to a CSV

# Back in taxonomy_custom.py, if the dataframe got written out, append it to the user's submission file

# Then do a taxonomy submission and see if your download file has your dataframe that you appended

# You will not yet write this script to actually calculate CSCI, because Duy needs to finish some work on the SMC database, specifically the GIS Metrics table, which you will need

# so, lets say this is the random dataframe that will be written out to a CSV and appended to the user's submission file

# randomdataframe <- data.frame(testcol1 = c(1,2,3,4,5), testcol2 = c(5,6,7,8,9), testcol3 = c('a','b','c','d','e'))
# randomdataframe
# write.csv(randomdataframe,"C:\\Users\\GisUser\\SCCWRP\\Staff - P Drive\\Data\\PartTimers\\Aria\\R-Project\\R-downloads\\data.csv", row.names=FALSE)


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
#################################################################################
# #Play Ground testing this through R
# randomdataframe <- data.frame(testcol1 = c(1,2,3,4,5), testcol2 = c(5,6,7,8,9), testcol3 = c('a','b','c','d','e'))
# print("dataframe: ")
# randomdataframe

# args <- commandArgs(trailingOnly=TRUE)
# print("args:")
# args
# dir <- args[1]
# print("dir:")
# dir
# filename <- args[2]
# print("filename:")
# filename
# wbpath <- file.path(dir, filename)
# print("wbpath:")
# wbpath
# output <- (randomdataframe)
# print("Output:")
# output

# write.csv(randomdataframe, file = file.path(dir, 'output.csv'), row.names=FALSE)


# # Check if the workbook exists
# if(file.exists(wbpath)){
#   # Load the existing workbook
#   wb <- loadWorkbook(wbpath)
# } else {
#   # Create a new workbook if it doesn't exist
#   wb <- createWorkbook()
# }

# # Check if the sheet already exists
# sheetName <- "analysis_csci_placeholder"
# if (existsSheet(wb, sheetName)) {
#   # Append a suffix if the sheet already exists
#   sheetName <- paste0(sheetName, "_new")
# }

# # Add the new worksheet with the dataframe
# addWorksheet(wb, sheetName)
# writeData(wb, sheetName, output)

# # Save the workbook
# saveWorkbook(wb, wbpath, overwrite = TRUE)

########################################
# Good luck

