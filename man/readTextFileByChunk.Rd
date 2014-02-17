\name{readTextFileByChunk}
\alias{readTextFileByChunk}
\title{Experimental sequential text reader helper function}
\usage{
readTextFileByChunk(input, output, linesPerBlock = 10000, fn = NULL,
  header = TRUE, skip = 0, recordEndRegex = NULL)
}
\arguments{
  \item{input}{the path to an input text file}

  \item{output}{an output connection such as those created
  with \code{\link{localDiskConn}}, and
  \code{\link{hdfsConn}}}

  \item{linesPerBlock}{how many lines at a time to read}

  \item{fn}{function to be applied to each chunk of lines
  (see details)}

  \item{header}{does the file have a header}

  \item{skip}{number of lines to skip before reading}

  \item{recordEndRegex}{an optional regular expression that
  finds lines in the text file that indicate the end of a
  record (for multi-line records)}
}
\description{
Experimental helper function for reading text data
sequentially from a file on disk and adding to connection
using \code{\link{addData}}
}
\details{
The function \code{fn} should have one argument, which
should expect to receive a vector of strings, each element
of which is a line in the file.  It is also possible for
\code{fn} to take two arguments, in which case the second
argument is the header line from the file (some parsing
methods might need to know the header).
}
\examples{
csvFile <- file.path(tempdir(), "iris.csv")
write.csv(iris, file = csvFile, row.names = FALSE, quote = FALSE)
myoutput <- localDiskConn(file.path(tempdir(), "irisText"), autoYes = TRUE)
a <- readTextFileByChunk(csvFile,
   output = myoutput, linesPerBlock = 10,
   fn = function(x, header) {
      colNames <- strsplit(header, ",")[[1]]
      read.csv(textConnection(paste(x, collapse = "\\n")), col.names = colNames, header = FALSE)
   })
a[[1]]
}

