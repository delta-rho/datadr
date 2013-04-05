\name{divide.data.frame}
\alias{divide.data.frame}
\title{Divide a data.frame}
\usage{
  divide.data.frame(data, by = NULL, orderBy = NULL,
    postTrans = NULL, trans = NULL)
}
\arguments{
  \item{data}{data frame to be split}

  \item{by}{a vector of column names of \code{dat} to split
  by}

  \item{orderBy}{within each division, how should the data
  be sorted?  Either a vector of variable names (which will
  be sorted in ascending order) or a list such as the
  following: \code{list(c("var1", "desc"), c("var2",
  "asc"))}, which would sort the data first by \code{var1}
  in descending order, and then by \code{var2} in ascending
  order}

  \item{postTrans}{a transformation function (if desired)
  to apply to each subset after it has been formed}

  \item{trans}{transformation function to coerce data
  transformed with postTrans back into a data.frame, so the
  result can behave like a data.frame (if desired)}
}
\value{
  list of data frames split according to "by", of class
  "localDiv", with names denoting the current split Objects
  of class "localDiv" are lists where each list element has
  the following: \describe{ \item{split:}{a list of the
  splitVars corresponding to the current split}
  \item{splitKey:}{the current splitVars concatenate into a
  single string} \item{data:}{The rows of the dat that
  correspond to the current split (with the splitVar
  columns omitted)} }
}
\description{
  Divide a data.frame into subsets based on different
  criteria
}
\examples{
irisSplit1 <- divide(iris, "Species")
}
\author{
  Ryan Hafen
}
\seealso{
  \code{\link{divJoin}}, \code{\link{divExample}},
  \code{\link{SplitExampleKey}}
}

