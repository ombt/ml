#!/usr/bin/python3
#
#############################################################################
#
Chapter summary
 Define the problem at hand and the data on which you’ll train. Collect
this data, or annotate it with labels if need be.
 Choose how you’ll measure success on your problem. Which metrics will
you monitor on your validation data?
 Determine your evaluation protocol: hold-out validation? K-fold valida-
tion? Which portion of the data should you use for validation?
 Develop a first model that does better than a basic baseline: a model with
statistical power.
 Develop a model that overfits.
 Regularize your model and tune its hyperparameters, based on perfor-
mance on the validation data. A lot of machine-learning research tends to
focus only on this step—but keep the big picture in mind.
