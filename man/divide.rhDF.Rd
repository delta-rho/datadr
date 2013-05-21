\name{divide.rhDF}
\alias{divide.rhDF}
\title{Divide a RHIPE 'rhDF' Object}
\usage{
  divide.rhDF(data, by = NULL, orderBy = NULL,
    output = NULL, control = NULL, preTrans = NULL,
    postTrans = NULL, trans = NULL, update = FALSE)
}
\arguments{
  \item{data}{an object of class 'rhDF'}

  \item{by}{specification of how to divide the data -
  conditional (factor-level or shingles), random replicate,
  or near-exact replicate -- see details}

  \item{orderBy}{within each division, how should the data
  be sorted?  Either a vector of variable names (which will
  be sorted in ascending order) or a list such as the
  following: \code{list(c("var1", "desc"), c("var2",
  "asc"))}, which would sort the data first by \code{var1}
  in descending order, and then by \code{var2} in ascending
  order}

  \item{output}{where on HDFS the output data should
  reside}

  \item{control}{parameters specifying how the backend
  should handle things (most-likely parameters to
  \code{\link{rhwatch}} in RHIPE) - see
  \code{\link{rhipeControl}}}

  \item{preTrans}{a transformation function (if desired) to
  applied prior to division}

  \item{postTrans}{a transformation function (if desired)
  to apply to each final subset}

  \item{trans}{transformation function to coerce data
  transformed with postTrans back into a data.frame, so the
  result can behave like an object of class 'rhDF' (if
  desired)}
}
\value{
  an object of class 'rhData', 'rhDF', and 'rhDiv' which
  points to the divided data on HDFS
}
\description{
  Divide a RHIPE 'rhDF' object into subsets based on
  different criteria
}
\details{
  The division methods this function will support include
  conditioning variable division for factors (implemented
  -- see \code{\link{condDiv}}), conditioning variable
  division for numerical variables through shingles, random
  replicate, and near-exact replicate.  If \code{by} is a
  vector of variable names, the data will be divided by
  these variables.  Alternatively, this can be specified by
  e.g.  \code{condDiv(c("var1", "var2"))}.
}
\examples{
\dontrun{

}
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
  \code{\link{rhDF}}, \code{\link{condDiv}},
  \code{\link{rrDiv}}
}

