op <- par(mar = c(10, 4, 4, 2) + 0.1) #margin formatting
barplot(mtcars$mpg, 
        names.arg = row.names(mtcars), 
        las = 2, 
        ylab = "Fuel Efficiency in Miles per Gallon")

head(mtcars)

pairs(mtcars[1:7], lower.panel = NULL)

plot(y = mtcars$mpg, x = mtcars$wt, xlab = "Vehicle Weight",
     ylab = "Vehicle Fuel Efficiency in Miles per Gallon")

mt.model <- lm(formula = mpg ~ wt, data = mtcars)

coef(mt.model)

?lm
