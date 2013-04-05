\name{str.rhData}
\alias{str.rhData}
\title{str Method for 'rhData'}
\usage{
  str.rhData(x, ...)
}
\arguments{
  \item{x}{'rhData' object}

  \item{\ldots}{additional parameters}
}
\description{
  str method for 'rhData'
}
\details{
  Avoids clutter with \code{\link{str}}, hiding
  \code{sourceJobData} and \code{example}, whose structure
  can be seen with, e.g. \code{str(obj$example)}
}
\author{
  Ryan Hafen
}

