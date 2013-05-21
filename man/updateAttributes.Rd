\name{updateAttributes}
\alias{updateAttributes}
\title{Update Attributes of a 'rhData' or 'rhDF' Object}
\usage{
  updateAttributes(obj, control = NULL)
}
\arguments{
  \item{obj}{an object of class 'rhData' or 'rhDF'}

  \item{control}{parameters specifying how the backend
  should handle things (most-likely parameters to
  \code{\link{rhwatch}} in RHIPE) - see
  \code{\link{rhipeControl}}}
}
\value{
  an object of class 'rhData' or 'rhDF'
}
\description{
  Update attributes of a 'rhData' or 'rhDF' object
}
\details{
  This function looks for missing attributes related to a
  RHIPE data object and runs a MapReduce job to update
  them.  These attributes include "splitSizeDistn",
  "hasKeys", "ndiv", "nrow", and "splitRowDistn".  These
  attributes are useful for subsequent computations that
  might rely on them.  The result is the input modified to
  reflect the updated attributes, and thus it should be
  used as \code{obj <- updateAttributes(obj)}.  Metadata
  files on HDFS are also updated.
}
\examples{
\dontrun{

}
}
\author{
  Ryan Hafen
}
\seealso{
  \code{\link{rhData}}, \code{\link{rhDF}},
  \code{\link{divide.rhData}}
}

