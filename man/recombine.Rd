\name{recombine}
\alias{recombine}
\title{Recombine a RHIPE 'rhDiv' Object}
\usage{
  recombine(data = NULL, method = NULL, recomb = NULL,
    mapred = NULL)
}
\arguments{
  \item{data}{an object of class 'rhDF'}

  \item{method}{the analytic method to apply to each
  subset}

  \item{recomb}{the method to combine output from the
  analytic method on each subset}

  \item{mapred}{mapreduce-specific parameters to send to
  RHIPE job that performs the recombination (see
  \code{\link{rhwatch}})}
}
\value{
  depends on \code{recomb} - should be a final set of model
  coefficients, etc.
}
\description{
  Apply an analytic method to a RHIPE 'rhDiv' object and
  combine the results
}
\details{
  This is an experimental prototype - subject to change and
  be more flexible.
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
  \code{\link{divide}}, \code{\link{rrDiv}},
  \code{\link{drGLM}}, \code{\link{wMeanCoef}}
}

