install.packages(c(
  "shiny",
  "DBI",
  "RSQLite",
  "dplyr",
  "ggplot2",
  "DT"
))

library(DBI)
library(RSQLite)

con <- dbConnect(
  SQLite(),
  "tunes.db"
)

tunes <- dbReadTable(con, "tunes")

dbDisconnect(con)

print(nrow(tunes))
print(names(tunes))

head(tunes)
summary(tunes)
table(tunes)
