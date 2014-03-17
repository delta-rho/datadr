

# load adult data for quantile example
data(adult)
adultDdf <- ddf(adult)
# divide it by education
# must have update = TRUE to get range of variables
byEd <- divide(adultDdf, by = "education", update = TRUE)



# compute quantiles of hoursperweek
hpwQuant <- quantile(byEd, var = "hoursperweek")
head(hpwQuant)



plot(hpwQuant)



# compute quantiles of hoursperweek by sex
hpwBySexQuant <- quantile(byEd, var = "hoursperweek", by = "sex")
xyplot(q ~ fval, groups = group, data = hpwBySexQuant, auto.key = TRUE)


