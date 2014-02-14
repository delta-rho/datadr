\name{drJoin}
\alias{drJoin}
\title{Join Two Data Sources by Key}
\usage{
drJoin(..., output = NULL, postTransFn = NULL, control = NULL)
}
\arguments{
  \item{\ldots}{named lists of input objects - assumed that
  all are of same type (all HDFS, all localDisk, all
  in-memory)}

  \item{output}{a "kvConnection" object indicating where
  the output data should reside (see
  \code{\link{localDiskConn}}, \code{\link{hdfsConn}}).  If
  \code{NULL} (default), output will be an in-memory "ddo"
  object.}

  \item{postTransFn}{an optional function to be applied to
  the each final key-value pair after joining}

  \item{control}{parameters specifying how the backend
  should handle things (most-likely parameters to
  \code{rhwatch} in RHIPE) - see \code{\link{rhipeControl}}
  and \code{\link{localDiskControl}}}
}
\value{
a 'ddo' object stored in the \code{output} connection,
where the values are named lists with names according to
the names given to the input data objects, and values are
the corresponding data
}
\description{
Join two data sources by key
}
\examples{
bySpecies <- divide(iris, by = "Species")
# get independent lists of just SW and SL
sw <- lapply(bySpecies, function(x) x$Sepal.Width)
sl <- lapply(bySpecies, function(x) x$Sepal.Length)
a1 <- drJoin(Sepal.Width=sw, Sepal.Length=sl, postTransFn = as.data.frame)
}
\author{
Ryan Hafen
}
\seealso{
\code{\link{drFilter}}
}

