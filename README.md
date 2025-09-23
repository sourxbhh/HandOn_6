# Name- Sourabh Kumar Dubey
# id- 801429834

## H6: Spark Structured APIs Music Listener Behaviour Analysis 

This project analyzes user listening behavior and music trends using 

Apache Spark's Structured APIs. The analysis is performed on two datasets from a fictional music streaming platform: 

listening_logs.csv and songs_metadata.csv. The objective is to gain insights into genre preferences, song popularity, and listener engagement.



## Dataset Description 💿

listening_logs.csv: Contains records of user listening activity.


user_id: Unique ID for each user.


song_id: Unique ID for each song.


timestamp: The date and time a song was played.


duration_sec: The duration in seconds the song was played.


songs_metadata.csv: Contains metadata for each song.


song_id: Unique ID for each song.


title: The title of the song.


artist: The name of the artist.


genre: The genre of the song (e.g., Pop, Rock, Jazz).


mood: The mood category of the song (e.g., Happy, Sad, Energetic).

## Approach & Tasks 
The analysis was performed using 

Spark Structured APIs. The project includes the following tasks:


Finding Each User's Favorite Genre: Identify the most-listened-to genre for each user by counting the number of plays for each genre.


Calculating Average Listen Time per Song: Compute the average duration in seconds for each song based on user play history.

Computing Genre Loyalty Score: For each user, calculate the proportion of their plays that belong to their most-listened-to genre. Users with a loyalty score greater than 0.8 are outputted.


Identifying Night Owl Users: Extract users who frequently listen to music between 12 AM and 5 AM based on their listening timestamps.

## Results 
The results for each task are saved in a specific folder structure within the 

outputs/ directory. The outputs are organized as follows:


outputs/

user_favorite_genres/: Contains the most-listened-to genre for each user.

avg_listen_time_per_song/: Contains the average listen time for each song.

genre_loyalty_scores/: Contains the loyalty scores for each user, with a focus on those above the 0.8 threshold.

night_owl_users/: Contains a list of users who listen to music during late-night hours.

All output files are saved in 

CSV or JSON format.
