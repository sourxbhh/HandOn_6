from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
import os

def create_spark_session():
    """Create and return Spark session"""
    return SparkSession.builder \
        .appName("MusicListenerAnalysis") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()

def load_data(spark):
    """Load the CSV files into Spark DataFrames"""
    listening_logs = spark.read.option("header", "true").option("inferSchema", "true").csv("listening_logs.csv")
    songs_metadata = spark.read.option("header", "true").option("inferSchema", "true").csv("songs_metadata.csv")
    
    print("Data loaded successfully:")
    print(f"Listening logs count: {listening_logs.count()}")
    print(f"Songs metadata count: {songs_metadata.count()}")
    
    return listening_logs, songs_metadata

def task1_user_favorite_genre(listening_logs, songs_metadata):
    """Task 1: Find each user's favourite genre"""
    print("\n=== Task 1: Finding each user's favorite genre ===")
    
    # Join listening logs with songs metadata to get genre information
    user_genre_plays = listening_logs.join(songs_metadata, "song_id") \
        .groupBy("user_id", "genre") \
        .agg(count("*").alias("play_count"))
    
    # Window function to rank genres by play count for each user
    window_spec = Window.partitionBy("user_id").orderBy(desc("play_count"))
    
    user_favorite_genres = user_genre_plays.withColumn("rank", row_number().over(window_spec)) \
        .filter(col("rank") == 1) \
        .select("user_id", "genre", "play_count") \
        .orderBy("user_id")
    
    print("User favorite genres sample:")
    user_favorite_genres.show(10)
    
    # Save results
    user_favorite_genres.write.mode("overwrite").csv("output/user_favorite_genres", header=True)
    print("Task 1 results saved to output/user_favorite_genres/")
    
    return user_favorite_genres

def task2_avg_listen_time_per_song(listening_logs):
    """Task 2: Calculate the average listen time per song"""
    print("\n=== Task 2: Calculating average listen time per song ===")
    
    avg_listen_time = listening_logs.groupBy("song_id") \
        .agg(
            avg("duration_sec").alias("avg_duration_sec"),
            count("*").alias("total_plays")
        ) \
        .orderBy(desc("total_plays"))
    
    print("Average listen time per song sample:")
    avg_listen_time.show(10)
    
    # Save results
    avg_listen_time.write.mode("overwrite").csv("output/avg_listen_time_per_song", header=True)
    print("Task 2 results saved to output/avg_listen_time_per_song/")
    
    return avg_listen_time

def task3_genre_loyalty_score(listening_logs, songs_metadata):
    """Task 3: Compute genre loyalty score for each user"""
    print("\n=== Task 3: Computing genre loyalty scores ===")
    
    # Get user genre plays
    user_genre_plays = listening_logs.join(songs_metadata, "song_id") \
        .groupBy("user_id", "genre") \
        .agg(count("*").alias("genre_plays"))
    
    # Calculate total plays per user
    user_total_plays = listening_logs.groupBy("user_id") \
        .agg(count("*").alias("total_plays"))
    
    # Find max genre plays per user
    user_max_genre = user_genre_plays.groupBy("user_id") \
        .agg(max("genre_plays").alias("max_genre_plays"))
    
    # Calculate loyalty score
    loyalty_scores = user_genre_plays.join(user_max_genre, "user_id") \
        .filter(col("genre_plays") == col("max_genre_plays")) \
        .join(user_total_plays, "user_id") \
        .withColumn("loyalty_score", round(col("max_genre_plays") / col("total_plays"), 2)) \
        .select("user_id", "genre", "max_genre_plays", "total_plays", "loyalty_score") \
        .filter(col("loyalty_score") > 0.69) \
        .orderBy(desc("loyalty_score"))
    
    print("Users with loyalty score > 0.69:")
    loyalty_scores.show(10)
    
    # Save results
    loyalty_scores.write.mode("overwrite").csv("output/genre_loyalty_scores", header=True)
    print("Task 3 results saved to output/genre_loyalty_scores/")
    
    return loyalty_scores

def task4_night_owl_users(listening_logs):
    """Task 4: Identify users who listen to music between 12 AM and 5 AM"""
    print("\n=== Task 4: Identifying night owl users ===")
    
    # Convert timestamp and extract hour
    night_owls = listening_logs \
        .withColumn("timestamp_parsed", to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss")) \
        .withColumn("hour", hour(col("timestamp_parsed"))) \
        .filter((col("hour") >= 0) & (col("hour") <= 5)) \
        .groupBy("user_id") \
        .agg(count("*").alias("night_plays")) \
        .filter(col("night_plays") >= 3) \
        .orderBy(desc("night_plays"))
    
    print("Night owl users (listening between 12 AM - 5 AM):")
    night_owls.show(10)
    
    # Save results
    night_owls.write.mode("overwrite").csv("output/night_owl_users", header=True)
    print("Task 4 results saved to output/night_owl_users/")
    
    return night_owls

def main():
    """Main function to run all tasks"""
    # Create Spark session
    spark = create_spark_session()
    
    try:
        # Load data
        listening_logs, songs_metadata = load_data(spark)
        
        # Execute all tasks
        task1_user_favorite_genre(listening_logs, songs_metadata)
        task2_avg_listen_time_per_song(listening_logs)
        task3_genre_loyalty_score(listening_logs, songs_metadata)
        task4_night_owl_users(listening_logs)
        
        print("\n=== All tasks completed successfully! ===")
        print("Output files are saved in the output/ directory")
        
    except Exception as e:
        print(f"Error occurred: {str(e)}")
        raise e
    
    finally:
        spark.stop()

if __name__ == "__main__":
    main()