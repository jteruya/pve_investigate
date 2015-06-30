library(reshape)
library(ggplot2)
library(stringr)
library(scales)

setwd('/Users/jonathanteruya/scp_recieve')

data <- read.csv('pve_non_pride_view_count.csv')

colnames(data)[1] <- "vieweduserid"
colnames(data)[2] <- "eventtype"
colnames(data)[3] <- "profile_view_cnt"

data[,'profile_view_cnt'] <- as.numeric(data[,'profile_view_cnt'])

hist(data$profile_view_cnt[data$profile_view_cnt <= quantile(data$profile_view_cnt, 0.90)], main = "Profile View Counts Freq (90th Percentile)", xlab = "Profile View Counts")

quantile(data$profile_view_cnt, 0.99)