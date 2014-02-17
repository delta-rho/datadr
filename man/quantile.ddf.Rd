\name{quantile.ddf}
\alias{quantile.ddf}
\title{Sample Quantiles for 'ddf' Objects}
\usage{
\method{quantile}{ddf}(x, var, by = NULL, probs = seq(0, 1, 0.005),
  transFn = identity, nBins = 10000, tails = 100, control = NULL, ...)
}
\arguments{
  \item{x}{a 'ddf' object}

  \item{var}{the name of the variable to compute quantiles
  for}

  \item{by}{the (optional) variable by which to group
  quantile computations}

  \item{probs}{numeric vector of probabilities with values
  in [0-1]}

  \item{transFn}{transformation to apply to variable prior
  to computing quantiles}

  \item{nBins}{how many bins should the range of the
  variable be split into?}

  \item{tails}{how many exact values at each tail should be
  retained?}

  \item{control}{parameters specifying how the backend
  should handle things (most-likely parameters to
  \code{rhwatch} in RHIPE) - see \code{\link{rhipeControl}}
  and \code{\link{localDiskControl}}}

  \item{\ldots}{additional arguments}
}
\value{
data frame of quantiles \code{q} and their associated
f-value \code{fval}.  If \code{by} is specified, then also
a variable \code{group}.
}
\description{
Compute sample quantiles for 'ddf' objects
}
\details{
This division-agnostic quantile calculation algorithm takes
the range of the variable of interest and splits it into
\code{nBins} bins, tabulates counts for those bins, and
reconstructs a quantile approximation from them.
\code{nBins} should not get too large, but larger
\code{nBins} gives more accuracy.  If \code{tails} is
positive, the first and last \code{tails} ordered values
are attached to the quantile estimate - this is useful for
long-tailed distributions or distributions with outliers
for which you would like more detail in the tails.
}
\examples{
# break the iris data into k/v pairs
irisSplit <- list(
   list("1", iris[1:10,]), list("2", iris[11:110,]), list("3", iris[111:150,])
)
# represent it as ddf
irisSplit <- ddf(irisSplit, update = TRUE)

# approximate quantiles over the divided data set
probs <- seq(0, 1, 0.005)
iq <- quantile(irisSplit, var = "Sepal.Length", tails = 0, probs = probs)
plot(iq$fval, iq$q)

# compare to the all-data quantile "type 1" result
plot(probs, quantile(iris$Sepal.Length, probs = probs, type = 1))
}
\author{
Ryan Hafen
}
\seealso{
\code{\link{updateAttributes}}
}

