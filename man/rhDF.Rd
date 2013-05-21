\name{rhDF}
\alias{rhDF}
\title{Load a RHIPE 'rhDF' Object}
\usage{
  rhDF(loc, type = "sequence", trans = NULL, reset = FALSE,
    update = FALSE, control = NULL)
}
\arguments{
  \item{loc}{location on HDFS where the data resides (can
  be a relative path if using \code{\link{hdfs.setwd}})}

  \item{type}{the type of data (map, sequence, text)}

  \item{trans}{a transformation to be applied to the data
  to coerce it into a data.frame}

  \item{reset}{should existing metadata for this object be
  overwritten?}

  \item{update}{should a RHIPE job be run to obtain
  additional attributes for the data prior to returning?}

  \item{control}{parameters specifying how the backend
  should handle things (most-likely parameters to
  \code{\link{rhwatch}} in RHIPE) - see
  \code{\link{rhipeControl}}}
}
\value{
  an object of class 'rhDF'
}
\description{
  Load / initialize a RHIPE 'rhDF' (RHIPE data.frame)
  object
}
\details{
  This function creates an R object representation of
  collections of key/value pairs data on HDFS where the
  values are data.frames or easily coerced into
  data.frames.  The more general case of structureless
  key/value pair representation in R can be done with
  \code{\link{rhDdata}}, upon which this class builds.
  Several pieces of metadata are stored in an HDFS
  subdirectory of "loc" called "_rh_meta", including the
  variables in the data.frame, the total number of rows,
  the distribution of number of rows, etc. (call
  \code{print} on one of these objects to see the
  attributes).  These attributes are useful for subsequent
  computations that might rely on them.  Some attributes
  can't be computed in just one pass, in which case
  \code{\link{updateAttributes}} can be called.
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
  \code{\link{rhData}}, \code{\link{updateAttributes}},
  \code{\link{getKeys}}, \code{\link{divide.rhData}}
}

