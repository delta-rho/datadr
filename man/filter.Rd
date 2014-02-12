\name{filter}
\alias{filter}
\title{Filter a 'ddo' or 'ddf' Object}
\usage{
  filter(x, fraction, output)
}
\arguments{
  \item{x}{an object of class 'ddo' or 'ddf'}

  \item{filterFn}{function that takes the keys and/or
  values and returns either \code{TRUE} or \code{FALSE} -
  if \code{TRUE}, that key-value pair will be present in
  the result}

  \item{output}{a "kvConnection" object indicating where
  the output data should reside (see
  \code{\link{localDiskConn}}, \code{\link{hdfsConn}}).  If
  \code{NULL} (default), output will be an in-memory "ddo"
  object.}

  \item{control}{parameters specifying how the backend
  should handle things (most-likely parameters to
  \code{\link{rhwatch}} in RHIPE) - see
  \code{\link{rhipeControl}} and
  \code{\link{localDiskControl}}}
}
\value{
  a 'ddo' or 'ddf' object
}
\description{
  Filter a 'ddo' or 'ddf' object
}
\author{
  Ryan Hafen
}
\seealso{
  \code{\link{join}}
}

