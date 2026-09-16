
# TRADITIONAL MUSIC DATA ANALYSIS

# This script explores a dataset of traditional tunes stored
# in an SQLite database.
#
# The analysis includes:
#   1. Initial data exploration
#   2. Data cleaning
#   3. Exploratory data analysis
#   4. Musical note extraction from ABC notation
#   5. Musical feature engineering
#   6. Focused comparison of reels, jigs and hornpipes



# 1. SETUP

# install.packages(c(
#   "DBI",
#   "RSQLite",
#   "dplyr",
#   "ggplot2",
#   "stringr",
#   "tidyr"
# ))

library(DBI)
library(RSQLite)
library(dplyr)
library(ggplot2)
library(stringr)
library(tidyr)



# 2. LOAD DATA FROM SQLITE DATABASE


# Connect to database
con <- dbConnect(
  SQLite(),
  "tunes.db"
)

# Read tunes table into R
tunes <- dbReadTable(
  con,
  "tunes"
)

# Close database connection
dbDisconnect(con)



# 3. INITIAL DATA EXPLORATION


# Number of tunes
nrow(tunes)

# Column names
names(tunes)

# Preview dataset
head(tunes)

# Summary of variables
summary(tunes)



# 4. DATA CLEANING


# Created cleaned versions of rhythm and key.
# Rhythm names are converted to lowercase so values such as
# "Reel" and "reel" are treated as the same category.


tunes <- tunes %>%
  mutate(
    rhythm_clean = trimws(tolower(rhythm)),
    key_clean = trimws(key)
  )



#5. EXPLORATORY DATA ANALYSIS



#Top 10 most common tune types


top_rhythms <- tunes %>%
  count(
    rhythm_clean,
    sort = TRUE
  ) %>%
  slice_head(n = 10)

top_rhythms



#Top 10 most common musical keys


top_keys <- tunes %>%
  count(
    key_clean,
    sort = TRUE
  ) %>%
  slice_head(n = 10)

top_keys


#Tune types within each book

rhythms_by_book <- tunes %>%
  count(
    book,
    rhythm_clean,
    sort = TRUE
  )

rhythms_by_book


#Distribution of tunes between books

book_distribution <- tunes %>%
  count(book) %>%
  mutate(
    percentage = round(
      n / sum(n) * 100,
      1
    )
  )

book_distribution


#Rhythm distribution within each book

# Percentages allow the books to be compared even though
# they contain different numbers of tunes.

rhythm_distribution <- tunes %>%
  filter(
    !is.na(rhythm_clean)
  ) %>%
  count(
    book,
    rhythm_clean
  ) %>%
  group_by(book) %>%
  mutate(
    percentage = round(
      n / sum(n) * 100,
      1
    )
  ) %>%
  ungroup() %>%
  arrange(
    book,
    desc(percentage)
  )

rhythm_distribution



#6. EXTRACT MUSICAL CONTENT FROM ABC NOTATION


# The raw_text column contains ABC notation togethr with
# metadata such as title, rhythm, meter and key.
# Musical notation begins after the (eg. K: field)
# Extract the content appearing after this field

tunes <- tunes %>%
  mutate(
    music_body = sub(
      ".*?K:[^\\n]*\\n",
      "",
      raw_text
    )
  )


#7. REMOVE NON-MUSICAL LINES


# Some metadata may still appear after the K: field.#
# Remove lines beginning with an ABC field identifier
# Inline notation sch as [M:3/4] is preserved because
# it does not appear at the beginning of a line.

tunes <- tunes %>%
  mutate(
    music_clean = gsub(
      "(?m)^[A-Za-z]:.*$",
      "",
      music_body,
      perl = TRUE
    )
  )



#8. EXTRACT MUSICAL NOTES

# Extract note letters A-G from the cleaned musical notation.
# Uppercase and lowercase ABC notes represent different octaves.
# For this exploratory analysis, octave differences are ignored and all notes are converted to uppercase.

tunes <- tunes %>%
  mutate(
    notes = str_extract_all(
      music_clean,
      "[A-Ga-g]"
    ),
    notes = lapply(
      notes,
      toupper
    )
  )


# Preview extracted notes
tunes %>%
  select(
    title,
    key_clean,
    notes
  ) %>%
  slice_head(n = 3)



# 9. MUSICAL FEATURE ENGINEERING


# Create numerical features describing each individual tune.

tunes <- tunes %>%
  rowwise() %>%
  mutate(
    
    # Total number of extracted notes
    total_notes = length(notes),
    
    # Number of different note letters used
    unique_notes = length(unique(notes)),
    
    # Most frequently occurring note
    dominant_note = if (length(notes) > 0) {
      names(
        sort(
          table(notes),
          decreasing = TRUE
        )
      )[1]
    } else {
      NA_character_
    },
    
    # Number of occurrences of the dominant note
    dominant_note_count = if (length(notes) > 0) {
      max(table(notes))
    } else {
      NA_integer_
    },
    
    # Percentage of  tune represented by its dominant note
    dominant_note_percentage = if (total_notes > 0) {
      round(
        dominant_note_count / total_notes * 100,
        1
      )
    } else {
      NA_real_
    }
  ) %>%
  ungroup()


# Preview engineered features
tunes %>%
  select(
    title,
    rhythm_clean,
    key_clean,
    total_notes,
    unique_notes,
    dominant_note,
    dominant_note_count,
    dominant_note_percentage
  ) %>%
  slice_head(n = 10)



# 10. FOCUSED ANALYSIS: REELS, JIGS AND HORNPIPES


# Reels, jigs and hornpipes were selected for deeper analysis
# because they are among the major tune types in the dataset.
# The complete 'tunes' dataset remains unchanged.

focus_tunes <- tunes %>%
  filter(
    rhythm_clean %in% c(
      "reel",
      "jig",
      "hornpipe"
    )
  )


# Check the number of tunes in each group

focus_tunes %>%
  count(
    rhythm_clean,
    sort = TRUE
  )



# 11. NOTE DISTRIBUTION BY TUNE TYPE


# Convert the list of extracted notes into individual rows.
# This allows the frequency of each note to be compared
# across reels, jigs and hornpipes.

focus_note_data <- focus_tunes %>%
  select(
    id,
    title,
    rhythm_clean,
    key_clean,
    notes
  ) %>%
  unnest(notes)


# Calculate the percentage distribution of notes A-G
# within each tune type.

focus_note_profile <- focus_note_data %>%
  count(
    rhythm_clean,
    notes
  ) %>%
  group_by(rhythm_clean) %>%
  mutate(
    percentage = round(
      n / sum(n) * 100,
      2
    )
  ) %>%
  ungroup()


# Display all 21 combinations:
# 3 tune types x 7 musical notes

focus_note_profile %>%
  arrange(
    notes,
    rhythm_clean
  ) %>%
  print(n = 21)




# 12. DOMINANT NOTE ANALYSIS

# Calculate how often each note is the dominant
# (most frequently occurring) note within each tune type

dominant_note_profile <- focus_tunes %>%
  filter(
    !is.na(dominant_note)
  ) %>%
  count(
    rhythm_clean,
    dominant_note
  ) %>%
  group_by(rhythm_clean) %>%
  mutate(
    percentage = round(
      n / sum(n) * 100,
      2
    )
  ) %>%
  ungroup()


# Find the most common dominant note for each tune type.

top_dominant_notes <- dominant_note_profile %>%
  group_by(rhythm_clean) %>%
  slice_max(
    percentage,
    n = 1,
    with_ties = FALSE
  ) %>%
  ungroup()

top_dominant_notes




# 13. TUNE LENGTH ANALYSIS


# Summarise tune length to compare the typical length
# and variation between reels, jigs and hornpipes.

tune_length_summary <- focus_tunes %>%
  group_by(rhythm_clean) %>%
  summarise(
    tunes = n(),
    median_notes = median(total_notes),
    mean_notes = round(mean(total_notes), 1),
    Q1 = quantile(total_notes, 0.25),
    Q3 = quantile(total_notes, 0.75),
    IQR = IQR(total_notes),
    max_notes = max(total_notes),
    .groups = "drop"
  )

tune_length_summary




# 14. DIFFERENCES IN NOTE USAGE


# Calculate the difference between the highest and lowest
# usage percentage for each note across the three tune types.

note_differences <- focus_note_profile %>%
  group_by(notes) %>%
  summarise(
    lowest_percentage = min(percentage),
    highest_percentage = max(percentage),
    difference = round(
      highest_percentage - lowest_percentage,
      2
    ),
    .groups = "drop"
  ) %>%
  arrange(desc(difference))

note_differences




# EXPLORATORY VISUALISATIONS


# 1. Top 10 Most Common Tune Types

ggplot(
  top_rhythms,
  aes(
    x = reorder(rhythm_clean, n),
    y = n
  )
) +
  geom_col(fill = "steelblue") +
  geom_text(
    aes(label = n),
    hjust = -0.2
  ) +
  coord_flip() +
  labs(
    title = "Top 10 Most Common Tune Types",
    subtitle = "Frequency of the most common rhythm categories in the dataset",
    x = "Tune Type",
    y = "Number of Tunes"
  ) +
  theme_minimal()

ggsave("plots/EX_top_tune_types.png", width = 8, height = 5)

# 2. Top 10 Most Common Musical Keys

ggplot(
  top_keys,
  aes(
    x = reorder(key_clean, n),
    y = n
  )
) +
  geom_col(fill = "darkseagreen") +
  geom_text(
    aes(label = n),
    hjust = -0.2
  ) +
  coord_flip() +
  labs(
    title = "Top 10 Most Common Musical Keys",
    subtitle = "Frequency of the most common musical keys in the dataset",
    x = "Musical Key",
    y = "Number of Tunes"
  ) +
  theme_minimal()

ggsave("plots/EX_top_common_keys.png", width = 8, height = 5)

# 3. Distribution of Tunes Across Books

ggplot(
  book_distribution,
  aes(
    x = factor(book),
    y = n
  )
) +
  geom_col(fill = "purple",
    width = 0.6
  ) +
  geom_text(
    aes(
      label = paste0(
        n,
        " (",
        percentage,
        "%)"
      )
    ),
    vjust = -0.5,
    size = 4
  ) +
  labs(
    title = "Distribution of Tunes Across Books",
    subtitle = "Number and percentage of tunes contained in each book",
    x = "Book",
    y = "Number of Tunes"
  ) +
  theme_minimal()

ggsave("plots/EX_distribution_tunes_books.png", width = 8, height = 5)


# EXPLANATORY VISUALISATIONS

#1. Melodic Note Profile by Tune Type

ggplot(
  focus_note_profile,
  aes(
    x = notes,
    y = rhythm_clean,
    fill = percentage
  )
) +
  geom_tile() +
  geom_text(
    aes(
      label = paste0(percentage, "%")
    ),
    size = 4
  ) +
  labs(
    title = "Melodic Note Profile by Tune Type",
    subtitle = "Distribution of notes A-G across reels, jigs and hornpipes",
    x = "Musical Note",
    y = "Tune Type",
    fill = "Percentage"
  ) +
  theme_minimal()

ggsave("plots/EXP_melody_note.png", width = 8, height = 5)

#2. Tune Length Distribution by Tune Type

ggplot(
  focus_tunes,
  aes(
    x = rhythm_clean,
    y = total_notes,
    fill = rhythm_clean
  )
) +
  geom_violin(
    trim = FALSE,
    alpha = 0.6
  ) +
  geom_boxplot(
    width = 0.15,
    outlier.shape = NA
  ) +
  labs(
    title = "Tune Length Distribution by Tune Type",
    subtitle = "Distribution of total extracted notes across reels, jigs and hornpipes",
    x = "Tune Type",
    y = "Total Number of Notes"
  ) +
  theme_minimal() +
  theme(
    legend.position = "none"
  )

ggsave("plots/EXP_tune_length.png", width = 8, height = 5)

#3. Most Common Dominant Note by Tune Type

ggplot(
  top_dominant_notes,
  aes(
    x = rhythm_clean,
    y = percentage,
    fill = rhythm_clean
  )
) +
  geom_col(
    width = 0.6
  ) +
  geom_text(
    aes(
      label = paste0(
        dominant_note,
        " (",
        percentage,
        "%)"
      )
    ),
    vjust = -0.5,
    size = 5
  ) +
  labs(
    title = "Most Common Dominant Note by Tune Type",
    subtitle = "Most frequently occurring dominant note within reels, jigs and hornpipes",
    x = "Tune Type",
    y = "Percentage of Tunes"
  ) +
  theme_minimal() +
  theme(
    legend.position = "none"
  )

ggsave("plots/EXP_dominant_note.png", width = 8, height = 5)

dir.create("plots", showWarnings = FALSE)