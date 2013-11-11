\name{hdfsConn}
\alias{hdfsConn}
\title{Connect to Data Source on HDFS}
\usage{
  hdfsConn(loc, type = "sequence", autoYes = FALSE,
    reset = FALSE, verbose = TRUE)
}
\arguments{
  \item{loc}{location on HDFS for the data source}

  \item{type}{the type of data ("map", "sequence", "text")}

  \item{autoYes}{automatically answer "yes" to questions
  about creating a path on HDFS}

  \item{reset}{should existing metadata for this object be
  overwritten?}

  \item{verbose}{logical - print messages about what is
  being done}
}
\value{
  a "kvConnection" object of class "hdfsConn"
}
\description{
  Connect to a data source on HDFS
}
\details{
  This simply creates a "connection" to a directory on HDFS
  (which need not have data in it).  To actually do things
  with this data, see \code{\link{ddo}}, etc.
}
\examples{
\dontrun{
   # connect to empty HDFS directory
   conn <- hdfsConn("/test/irisSplit")
   # add some data
   addData(conn, list(list("1", iris[1:10,])))
   addData(conn, list(list("2", iris[11:110,])))
   addData(conn, list(list("3", iris[111:150,])))
   # represent it as a distributed data frame
   hdd <- ddf(conn)
}
}
\author{
  Ryan Hafen
}
\seealso{
  \code{addData}, \code{\link{ddo}}, \code{\link{ddf}},
  \code{\link{localDiskConn}}
}

