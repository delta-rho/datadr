\name{recombine}
\alias{recombine}
\title{Recombine}
\usage{
recombine(data, apply = NULL, combine = NULL, output = NULL,
  params = NULL, control = NULL, verbose = TRUE)
}
\arguments{
  \item{data}{an object of class "ddo" of "ddf"}

  \item{apply}{a function specifying the analytic method to
  apply to each subset, or a pre-defined apply function
  (see \code{\link{drBLB}}, \code{\link{drGLM}}, for
  example)}

  \item{combine}{the method to combine the results}

  \item{output}{a "kvConnection" object indicating where
  the output data should reside (see
  \code{\link{localDiskConn}}, \code{\link{hdfsConn}}).  If
  \code{NULL} (default), output will be an in-memory "ddo"
  object.}

  \item{params}{a named list of parameters external to the
  input data that are needed in the distributed computing
  (most should be taken care of automatically such that
  this is rarely necessary to specify)}

  \item{control}{parameters specifying how the backend
  should handle things (most-likely parameters to
  \code{rhwatch} in RHIPE) - see \code{\link{rhipeControl}}
  and \code{\link{localDiskControl}}}

  \item{verbose}{logical - print messages about what is
  being done}
}
\value{
depends on \code{combine}
}
\description{
Apply an analytic recombination method to a ddo/ddf object
and combine the results
}
\author{
Ryan Hafen
}
\references{
\itemize{ \item \url{http://www.datadr.org} \item
\href{http://onlinelibrary.wiley.com/doi/10.1002/sta4.7/full}{Guha,
S., Hafen, R., Rounds, J., Xia, J., Li, J., Xi, B., &
Cleveland, W. S. (2012). Large complex data: divide and
recombine (D&R) with RHIPE. \emph{Stat}, 1(1), 53-67.} }
}
\seealso{
\code{\link{divide}}, \code{\link{ddo}}, \code{\link{ddf}},
\code{\link{drGLM}}, \code{\link{drBLB}},
\code{\link{combMeanCoef}}, \code{\link{combMean}},
\code{\link{combCollect}}, \code{\link{combRbind}},
\code{\link{drLapply}}
}

