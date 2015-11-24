# TODO:  the combMeanCoef needs tests
#' Mean Coefficient Recombination
#'
#' Mean coefficient recombination -- Calculate the weighted average of parameter estimates for a model fit to each subset
#'
# @param \ldots Additional list elements that will be added to the returned object
#'
#' @usage recombine(distributed_data_object, combine = combMeanCoef, ...)
#'
#' @details \code{combMeanCoef} is passed to the argument \code{combine} in \code{\link{recombine}}
#'
#' This method is designed to calculate the mean of each model coefficient, where the same model has been fit to
#' subsets via a transformation. The mean is a weighted average of each coefficient, where the weights are the
#' number of observations in each subset.  In particular, \code{\link{drLM}} and \code{\link{drGLM}} functions should be
#' used to add the transformation to the ddo that will be recombined using \code{combMeanCoef}.
#'
# @return An object of class \code{combCollect}
#'
#' @author Ryan Hafen
#'
#' @seealso \code{\link{divide}}, \code{\link{recombine}}, \code{\link{rrDiv}}, \code{\link{combCollect}}, \code{\link{combDdo}}, \code{\link{combRbind}}, \code{\link{combMean}}
#'
#' @examples
#' # Create an irregular number of observations for each species
#' indexes <- sort(c(sample(1:50, 40), sample(51:100, 37), sample(101:150, 46)))
#' irisIrr <- iris[indexes,]
#' 
#' # Create a distributed data frame using the irregular iris data set
#' bySpecies <- divide(irisIrr, by = "Species")
#'
#' # Fit a linear model of Sepal.Length vs. Sepal.Width for each species
#' # using 'drLM()' (or we could have used 'drGLM()' for a generlized linear model)
#' lmTrans <- function(x) drLM(Sepal.Length ~ Sepal.Width, data = x)
#' bySpeciesFit <- addTransform(bySpecies, lmTrans)
#' 
#' # Average the coefficients from the linear model fits of each species, weighted
#' # by the number of observations in each species
#' out1 <- recombine(bySpeciesFit, combine = combMeanCoef)
#' out1
#' 
#' # The following illustrates an equivalent, but more tedious approach
#' lmTrans2 <- function(x) t(c(coef(lm(Sepal.Length ~ Sepal.Width, data = x)), n = nrow(x)))
#' res <- recombine(addTransform(bySpecies, lmTrans2), combine = combRbind)
#' colnames(res) <- c("Species", "Intercept", "Sepal.Width", "n")
#' res
#' out2 <- c("(Intercept)" = with(res, sum(Intercept * n) / sum(n)),
#'           "Sepal.Width" = with(res, sum(Sepal.Width * n) / sum(n)))
#' 
#' # These are the same
#' identical(out1, out2)
#' 
#' @export
combMeanCoef <- function(...) {
  structure(
  list(
    reduce = expression(
      pre = {
        res <- list()
        n <- as.numeric(0)
        coefNames <- NULL
      },
      reduce = {
        if(is.null(coefNames))
          coefNames <- reduce.values[[1]]$names

        n <- sum(c(n, unlist(lapply(reduce.values, function(x) x$n))), na.rm = TRUE)
        res <- do.call(rbind, c(res, lapply(reduce.values, function(x) {
          x$coef * x$n
        })))
        res <- apply(res, 2, sum)
      },
      post = {
        res <- res / n
        names(res) <- coefNames
        collect("final", res)
      }
    ),
    final = function(x, ...) x[[1]][[2]],
    validateOutput = c("nullConn"),
    group = TRUE,
    ...
  ),
  class = "combMeanCoef")
}

# TODO:  The combMean method needs tests
#' Mean Recombination
#'
#' Mean recombination -- Calculate the elementwise mean of a vector in each value
#' 
# @param \ldots Additional list elements that will be added to the returned object
#'
#' @usage recombine(distributed_data_object, combine = combMean, ...)
#'
#' @details \code{combMean} is passed to the argument \code{combine} in \code{\link{recombine}}
#'
#' This method assumes that the values of the key-value pairs each consist of a numeric vector (with the same length).
#' The mean is calculated elementwise across all the keys.
#'
# @return An object of class \code{combCollect}
#'
#' @author Ryan Hafen
#'
#' @seealso \code{\link{divide}}, \code{\link{recombine}}, \code{\link{combCollect}}, \code{\link{combDdo}}, \code{\link{combRbind}}, \code{\link{combMeanCoef}}
#'
#' @examples
#' # Create a distributed data frame using the iris data set
#' bySpecies <- divide(iris, by = "Species")
#'
#' # Add a transformation that returns a vector of sums for each subset, one
#' # mean for each variable
#' bySpeciesTrans <- addTransform(bySpecies, function(x) apply(x, 2, sum))
#' bySpeciesTrans[[1]]
#'
#' # Calculate the elementwise mean of the vector of sums produced by
#' # the transform, across the keys
#' out1 <- recombine(bySpeciesTrans, combine = combMean)
#' out1
#'
#' # This manual, non-datadr approach illustrates the above computation
#' 
#' # This step mimics the transformation above
#' sums <- aggregate(. ~ Species, data = iris, sum)
#' sums
#' 
#' # And this step mimics the mean recombination
#' out2 <- apply(sums[,-1], 2, mean)
#' out2
#'
#' # These are the same
#' identical(out1, out2)
#' 
#' @export
combMean <- function(...) {
  structure(
  list(
    reduce = expression(
      pre = {
        res <- list()
        n <- as.numeric(0)
      },
      reduce = {
        n <- sum(c(n, length(reduce.values)))
        res <- do.call(rbind, c(res, lapply(reduce.values, function(x) {
          x
        })))
        res <- apply(res, 2, sum)
      },
      post = {
        res <- res / n
        collect("final", res)
      }
    ),
    final = function(x, ...) {
      if(length(x) == 1) {
        return(x[[1]][[2]])
      } else {
        return(getAttribute(x, "conn")$data)
      }
    } ,
    validateOutput = c("nullConn"),
    group = TRUE,
    ...
  ),
  class = "combMean")
}

#' "DDO" Recombination
#'
#' "DDO" recombination - simply collect the results into a "ddo" object
#'
# @param \ldots Additional list elements that will be added to the returned object
#'
#' @usage recombine(distributed_data_object, combine = combDdo, ...)
#'
#' @details \code{combDdo} is passed to the argument \code{combine} in \code{\link{recombine}}
#'
# @return An object of class \code{combCollect}
#'
#' @author Ryan Hafen
#'
#' @seealso \code{\link{divide}}, \code{\link{recombine}}, \code{\link{combCollect}}, \code{\link{combMeanCoef}}, \code{\link{combRbind}}, \code{\link{combMean}}
#'
#' @examples
#' # Divide the iris data
#' bySpecies <- divide(iris, by = "Species")
#'
#' # Add a transform that returns a list for each subset
#' listTrans <- function(x) {
#'   list(meanPetalWidth = mean(x$Petal.Width),
#'        maxPetalLength = max(x$Petal.Length))
#' }
#'
#' # Apply the transform and combine using combDdo
#' combined <- recombine(addTransform(bySpecies, listTrans), combine = combDdo)
#' combined
#' combined[[1]]
#' 
#' @export
combDdo <- function(...) {
  structure(
  list(
    reduce = expression(reduce = {
      lapply(reduce.values, function(r) collect(reduce.key, r))
    }),
    final = identity,
    validateOutput = c("localDiskConn", "hdfsConn", "nullConn"),
    group = FALSE,
    ...
  ),
  class = "combCollect")
}

#' "DDF" Recombination
#'
#' "DDF" recombination - results into a "ddf" object, rbinding if necessary
#'
# @param \ldots Additional list elements that will be added to the returned object
#'
#' @usage recombine(distributed_data_object, combine = combDdf, ...)
#'
#' @details \code{combDdf} is passed to the argument \code{combine} in \code{\link{recombine}}.
#'
#' If the \code{value} of the "ddo" object that will be recombined is a list, then the elements in the list will be
#' collapsed together via \code{\link{rbind}}.
#'
# @return An object of class \code{combCollect}
#'
#' @author Ryan Hafen
#'
#' @seealso \code{\link{divide}}, \code{\link{recombine}}, \code{\link{combCollect}}, \code{\link{combMeanCoef}}, \code{\link{combRbind}}, \code{\link{combDdo}}
#'
#' @examples
#' # Divide the iris data
#' bySpecies <- divide(iris, by = "Species")
#'
#' ############################################################
#' # Simple combination to form a ddf
#' ############################################################
#' 
#' # Add a transform that selects the petal width and length variables
#' selVars <- function(x) x[,c("Petal.Width", "Petal.Length")]
#'
#' # Apply the transform and combine using combDdo
#' combined <- recombine(addTransform(bySpecies, selVars), combine = combDdf)
#' combined
#' combined[[1]]
#'
#' ############################################################
#' # Combination that involves rbinding to give the ddf
#' ############################################################
#' 
#' # A transformation that returns a list
#' listTrans <- function(x) {
#'   list(meanPetalWidth = mean(x$Petal.Width),
#'        maxPetalLength = max(x$Petal.Length))
#' }
#' 
#' # Apply the transformation and look at the result
#' bySpeciesTran <- addTransform(bySpecies, listTrans)
#' bySpeciesTran[[1]]
#'
#' # And if we rbind the "value" of the first subset:
#' out1 <- rbind(bySpeciesTran[[1]]$value)
#' out1
#'
#' # Note how the combDdf method row binds the two data frames
#' combined <- recombine(bySpeciesTran, combine = combDdf)
#' out2 <- combined[[1]]
#' out2
#'
#' # These are equivalent
#' identical(out1, out2$value)
#' 
#' @export
combDdf <- function(...) {
  structure(
  list(
    reduce = expression(
      pre = {
        adata <- list()
      },
      reduce = {
        adata[[length(adata) + 1]] <- reduce.values
      },
      post = {
        adata <- do.call(rbind, unlist(adata, recursive = FALSE))
        collect(reduce.key, adata)
      }
    ),
    final = identity,
    validateOutput = c("localDiskConn", "hdfsConn", "nullConn"),
    group = FALSE,
    ...
  ),
  class = "combCollect")
}


#' "Collect" Recombination
#'
#' "Collect" recombination - collect the results into a local list of key-value pairs
#'
# @param \ldots Additional list elements that will be added to the returned object
#'
#' @usage recombine(distributed_data_object, combine = combCollect, ...)
#'
#' @details \code{combCollect} is passed to the argument \code{combine} in \code{\link{recombine}}
#'
# @return An object of class \code{combCollect}
#'
#' @author Ryan Hafen
#'
#' @seealso \code{\link{divide}}, \code{\link{recombine}}, \code{\link{combDdo}}, \code{\link{combMeanCoef}}, \code{\link{combRbind}}, \code{\link{combMean}}
#'
#' @examples
#' # Create a distributed data frame using the iris data set
#' bySpecies <- divide(iris, by = "Species")
#' 
#' # Function to calculate the mean of the petal widths
#' meanPetal <- function(x) mean(x$Petal.Width)
#'
#' # Combine the results using rbind
#' combined <- recombine(addTransform(bySpecies, meanPetal), combine = combCollect)
#' class(combined)
#' combined
#' 
#' @export
combCollect <- function(...) {
  structure(
  list(
    reduce = expression(reduce = {
      lapply(reduce.values, function(r) collect(reduce.key, r))
    }),
    final = function(x, ...)
      lapply(getAttribute(x, "conn")$data, function(y) {
        class(y) <- "kvPair"
        names(y) <- c("key", "value")
        y
      }),
    validateOutput = c("nullConn"),
    group = FALSE,
    ...
  ),
  class = "combCollect")
}

#' "rbind" Recombination
#'
#' "rbind" recombination - Combine divisions by row binding
#'
# @param \ldots Additional list elements that will be added to the returned object
#'
#' @usage recombine(distributed_data_object, combine = combRbind, ...)
#'
#' @details \code{combRbind} is passed to the argument \code{combine} in \code{\link{recombine}}
#'
# @return An object of class \code{combRbind}
#'
#' @author Ryan Hafen
#'
#' @seealso \code{\link{divide}}, \code{\link{recombine}}, \code{\link{combDdo}}, \code{\link{combCollect}}, \code{\link{combMeanCoef}}, \code{\link{combMean}}
#'
#' @examples
#' # Create a distributed data frame using the iris data set
#' bySpecies <- divide(iris, by = "Species")
#' 
#' # Create a function that will calculate the standard deviation of each
#' # variable in in a subset. The calls to 'as.data.frame()' and 't()'
#' # convert the vector output of 'apply()' into a data.frame with a single row
#' sdCol <- function(x) as.data.frame(t(apply(x, 2, sd)))
#'
#' # Combine the results using rbind
#' combined <- recombine(addTransform(bySpecies, sdCol), combine = combRbind)
#' class(combined)
#' combined
#'
#' @export
combRbind <- function(...) {
  red <- expression(
    pre = {
      adata <- list()
    },
    reduce = {
      adata[[length(adata) + 1]] <- reduce.values
    },
    post = {
      adata <- do.call(rbind, unlist(adata, recursive = FALSE))
      collect(reduce.key, adata)
    }
  )
  # attr(red, "combine") <- TRUE

  structure(
  list(
    reduce = red,
    final = function(x, ...) {
      if(length(x) == 1) {
        return(x[[1]][[2]])
      } else {
        return(getAttribute(x, "conn")$data)
      }
    },
    mapHook = function(key, value) {
      if(length(value) == 0)
        return(NULL)
      attrs <- attributes(value)
      if(!is.null(attrs$split)) {
        if(!is.data.frame(value)) {
          value <- list(val = value)
        }
        value <- data.frame(c(attrs$split, as.list(value)), stringsAsFactors = FALSE)
      }
      value
    },
    validateOutput = c("nullConn"),
    group = TRUE,
    ...
    # TODO: should make sure the result won't be too big (approximate by size of output from test run times number of divisions)
  ),
  class = "combRbind")
}
