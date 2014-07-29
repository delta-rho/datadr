\name{updateAttributes}
\alias{updateAttributes}
\title{Update Attributes of a 'ddo' or 'ddf' Object}
\usage{
updateAttributes(obj, control = NULL)
}
\arguments{
  \item{obj}{an object of class 'ddo' or 'ddf'}

  \item{control}{parameters specifying how the backend
  should handle things (most-likely parameters to
  \code{rhwatch} in RHIPE) - see
  \code{\link{rhipeControl}}}
}
\value{
an object of class 'ddo' or 'ddf'
}
\description{
Update attributes of a 'ddo' or 'ddf' object
}
\details{
This function looks for missing attributes related to a ddo
or ddf (distributed data object or data frame) object and
runs MapReduce to update them.  These attributes include
"splitSizeDistn", "keys", "nDiv", "nRow", and
"splitRowDistn".  These attributes are useful for
subsequent computations that might rely on them.  The
result is the input modified to reflect the updated
attributes, and thus it should be used as \code{obj <-
updateAttributes(obj)}.
}
\author{
Ryan Hafen
}
\references{
Bennett, Janine, et al. "Numerically stable, single-pass,
parallel statistics algorithms." Cluster Computing and
Workshops, 2009. \emph{CLUSTER'09. IEEE International
Conference on.} IEEE, 2009.
}
\seealso{
\code{\link{ddo}}, \code{\link{ddf}}, \code{\link{divide}}
}

