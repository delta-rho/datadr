\name{rhData}
\alias{rhData}
\title{Load a RHIPE 'rhData' Object}
\usage{
  rhData(loc, type = "sequence", reset = FALSE,
    update = FALSE, mapred = NULL)
}
\arguments{
  \item{loc}{location on HDFS where the data resides (can
  be a relative path if using \code{\link{hdfs.setwd}})}

  \item{type}{the type of data (map, sequence, text)}

  \item{reset}{should existing metadata for this object be
  overwritten?}

  \item{update}{should a RHIPE job be run to obtain
  additional attributes for the data prior to returning?}

  \item{mapred}{mapreduce-specific parameters to send to
  RHIPE job that performs the update (see
  \code{\link{rhwatch}})}
}
\value{
  an object of class 'rhData'
}
\description{
  Load / initialize a RHIPE 'rhData' object
}
\details{
  This function creates an R object representation of any
  arbitrary set of key/value pair data on HDFS.  See
  \code{\link{rhDF}} for HDFS data objects that behave like
  distributed data.frames.  Several pieces of metadata are
  stored in a subdirectory of "loc" called "_rh_meta",
  including attributes such as the number of key/value
  pairs, the distribution of their size, data related to
  the job that created this data (if any), etc. (call
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
  \code{\link{rhDF}}, \code{\link{updateAttributes}},
  \code{\link{getKeys}}, \code{\link{divide.rhData}}
}

