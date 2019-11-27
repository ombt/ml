# supervised and unsupervised machine learning

# In the universe of machine learning algorithms, there are two major types: supervised
# and unsupervised. Supervised learning models are those in which a machine learning
# model is scored and tuned against some sort of known quantity. The majority of
# machine learning algorithms are supervised learners. Unsupervised learning models
# are those in which the machine learning model derives patterns and information
# from data while determining the known quantity tuning parameter itself. These are
# more rare in practice, but are useful in their own right and can help guide our thinking
# on where to explore the data for further analysis.

head(mtcars)
plot(y = mtcars$mpg, x = mtcars$disp, xlab = "Engine Size (cubic inches)",
     ylab = "Fuel Efficiency (Miles per Gallon)")

model <- lm(mtcars$mpg ~ mtcars$disp)
coef(model)

model
print(model)

summary(model)

# training and test sets
split_size = 0.8
sample_size = floor(split_size * nrow(mtcars))
set.seed(123)
train_indices <- sample(seq_len(nrow(mtcars)), size = sample_size)

train <- mtcars[train_indices, ]
test <- mtcars[-train_indices, ]

train
test

# build model using train set and test using test set
names(train)
model2 <- lm(mpg ~ disp, data = train)
new.data <- data.frame(disp = test$disp)
test$output <- predict(model2, new.data)
sqrt(sum(test$mpg - test$output)^2/nrow(test))

# classification

plot(x = mtcars$mpg, y = mtcars$am, xlab = "Fuel Efficiency (Miles per Gallon)",
     ylab = "Vehicle Transmission Type (0 = Automatic, 1 = Manual)")

library(caTools)

# function for logistic regression is called LogitBoost

Label.train = train[, 9]
Data.train = train[, -9]

model = LogitBoost(Data.train, Label.train)
Data.test = test
Lab = predict(model, Data.test, type = "raw")
data.frame(row.names(test), test$mpg, test$am, Lab)

# supervized clustering methods

plot(x = iris$Petal.Length, y = iris$Petal.Width, xlab = "Petal Length",
     ylab = "Petal Width")

# K-means - This algorithm works by first placing a number of random test points in our data—in
# this case, two. Each of our real data points is measured as a distance from these test
# points, and then the test points are moved in a way to minimize that distance

data = data.frame(iris$Petal.Length, iris$Petal.Width)
iris.kmeans <- kmeans(data, 2) # two centers for clusters
plot(x = iris$Petal.Length, y = iris$Petal.Width, pch = iris.kmeans$cluster,
     xlab = "Petal Length", ylab = "Petal Width")
points(iris.kmeans$centers, pch = 8, cex = 2)

# three cluster points
iris.kmeans3 <- kmeans(data, 3)
plot(x = iris$Petal.Length, y = iris$Petal.Width, pch = iris.kmeans3$cluster,
     xlab = "Petal Length", ylab = "Petal Width")
points(iris.kmeans3$centers, pch = 8, cex = 2)

# comparison of predictions vs actual

par(mfrow = c(1, 2))
plot(x = iris$Petal.Length, y = iris$Petal.Width, pch = iris.kmeans3$cluster,
     xlab = "Petal Length", ylab = "Petal Width", main = "Model Output")
plot(x = iris$Petal.Length, y = iris$Petal.Width,
     pch = as.integer(iris$Species),
     xlab = "Petal Length", ylab = "Petal Width", main = "Actual Data")

# look at the confusion matrix
table(iris.kmeans3$cluster, iris$Species)

# tree-based models


