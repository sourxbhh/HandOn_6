# main.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, hour, rank, desc
from pyspark.sql.window import Window

# Initialize SparkSession 🚀
spark = SparkSession.builder.appName("MusicAnalysis").getOrCreate()

# ----------------------------------------------------------------
## Load Datasets
# ----------------------------------------------------------------
# [cite_start]Load the two CSV files into Spark DataFrames. [cite: 6]
listening_logs_df = spark.read.csv("listening_logs.csv", header=True, inferSchema=True)
songs_metadata_df = spark.read.csv("songs_metadata.csv", header=True, inferSchema=True)

# ----------------------------------------------------------------
## Task 1: Find Each User's Favourite Genre
# ----------------------------------------------------------------
# [cite_start]Objective: Identify the most listened-to genre for each user. [cite: 45]
print("Processing Task 1: User's Favorite Genres...")

# Join the dataframes to link users to genres via song_id.
user_genre_df = listening_logs_df.join(songs_metadata_df, "song_id")

# [cite_start]Count how many times each user played songs in each genre. [cite: 45]
user_genre_counts = user_genre_df.groupBy("user_id", "genre").agg(count("*").alias("play_count"))

# Use a window function to rank genres by play_count for each user.
window_spec = Window.partitionBy("user_id").orderBy(col("play_count").desc())
user_favorite_genre = user_genre_counts.withColumn("rank", rank().over(window_spec)) \
    .filter(col("rank") == 1) \
    .select("user_id", "genre", "play_count")

user_favorite_genre.show()
user_favorite_genre.coalesce(1).write.format("csv").option("header", "true").mode("overwrite").save("output/user_favorite_genres")

# ----------------------------------------------------------------
## Task 2: Calculate the Average Listen Time per Song
# ----------------------------------------------------------------
# [cite_start]Objective: Compute the average duration (in seconds) for each song. [cite: 46]
print("Processing Task 2: Average Listen Time per Song...")

# Group by song_id and calculate the average of duration_sec.
avg_listen_time = listening_logs_df.groupBy("song_id") \
    .agg(avg("duration_sec").alias("avg_duration_sec"))

# Join with metadata to include song title and artist for better context.
avg_listen_time_with_titles = avg_listen_time.join(songs_metadata_df, "song_id") \
    .select("song_id", "title", "artist", "avg_duration_sec")

avg_listen_time_with_titles.show()
avg_listen_time_with_titles.coalesce(1).write.format("csv").option("header", "true").mode("overwrite").save("output/avg_listen_time_per_song")

# ----------------------------------------------------------------
## Task 3: Compute the Genre Loyalty Score for Each User
# ----------------------------------------------------------------
# [cite_start]Objective: Calculate the proportion of a user's plays that are their favorite genre and filter for scores > 0.8. [cite: 47, 48]
print("Processing Task 3: Genre Loyalty Scores...")

# Get the total number of songs played by each user.
total_plays_per_user = listening_logs_df.groupBy("user_id").agg(count("*").alias("total_plays"))

# Join total plays with the favorite genre counts from Task 1.
loyalty_df = total_plays_per_user.join(user_favorite_genre, "user_id")

# [cite_start]Calculate the loyalty score and filter for users with a score above 0.8. [cite: 48]
genre_loyalty_scores = loyalty_df.withColumn("loyalty_score", col("play_count") / col("total_plays")) \
    .filter(col("loyalty_score") > 0.8) \
    .select("user_id", "genre", "loyalty_score")

genre_loyalty_scores.show()
genre_loyalty_scores.coalesce(1).write.format("csv").option("header", "true").mode("overwrite").save("output/genre_loyalty_scores")

# ----------------------------------------------------------------
## Task 4: Identify Users Who Listen Between 12 AM and 5 AM
# ----------------------------------------------------------------
# [cite_start]Objective: Find users who listen to music during late-night hours. [cite: 49]
print("Processing Task 4: Night Owl Users...")

# Extract the hour from the timestamp column.
# Filter for timestamps where the hour is between 0 (12 AM) and 4 (up to 5 AM).
night_owl_users = listening_logs_df.withColumn("hour", hour(col("timestamp"))) \
    .filter((col("hour") >= 0) & (col("hour") < 5)) \
    .select("user_id").distinct()

night_owl_users.show()
night_owl_users.coalesce(1).write.format("csv").option("header", "true").mode("overwrite").save("output/night_owl_users")

print("✅ All tasks completed and outputs saved.")
# Stop the SparkSession
spark.stop()