\name{recombine}
\alias{recombine}
\title{Recombine a RHIPE 'rhDiv' Object}
\usage{
  recombine(data = NULL, apply = NULL,
    combine = combCollect(), control = NULL)
}
\arguments{
  \item{data}{an object of class 'rhDF'}

  \item{apply}{the analytic method to apply to each subset}

  \item{combine}{the method to combine the results}

  \item{control}{parameters specifying how the backend
  should handle things (most-likely parameters to
  \code{\link{rhwatch}} in RHIPE) - see
  \code{\link{rhipeControl}}}
}
\value{
  depends on \code{combine}
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

