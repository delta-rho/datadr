context("read from text file checks")

csvFile <- file.path(tempdir(), "iris.csv")
write.csv(iris, file = csvFile, row.names = FALSE, quote = FALSE)

test_that("read local text", {
   irisTextConn <- localDiskConn(file.path(tempdir(), "irisText"), autoYes = TRUE)
   
   a <- readTextFileByChunk(input = csvFile, 
      output = irisTextConn, linesPerBlock = 10, 
      fn = function(x, header) {
         colNames <- strsplit(header, ",")[[1]]
         read.csv(textConnection(paste(x, collapse = "\n")), 
            col.names = colNames, header = FALSE)
      })
   expect_equivalent(head(a[[7]][[2]]), head(iris))
})

unlink(csvFile)
unlink(file.path(tempdir(), "irisText"), recursive = TRUE)
