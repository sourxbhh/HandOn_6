# Name- Sourabh Kumar Dubey
# id- 801429834

**Assignment:** Hands-on L6: Spark Structured API  

---

## 📌 Project Overview
This assignment explores **user listening behaviour** and **music trends** using **Apache Spark Structured APIs**.  
The goal is to process structured data from a fictional music streaming platform and gain insights into:
- Listener genre preferences
- Song popularity
- Engagement patterns

Two datasets were provided/generated:
- **listening_logs.csv** → user activity logs  
- **songs_metadata.csv** → song metadata (title, artist, genre, mood)

---

## 🎯 Learning Goals
- **Data Loading & Preparation:** Import and preprocess CSV data using Spark Structured APIs.  
- **Data Analysis:** Perform filtering, aggregations, joins, and transformations to answer business questions.  

---

## 📂 Dataset Description
### 1. `listening_logs.csv`
| Column        | Description                                   |
|---------------|-----------------------------------------------|
| `user_id`     | Unique ID of the user                         |
| `song_id`     | Unique ID of the song                         |
| `timestamp`   | Date & time of playback (e.g., `2025-03-23 14:05:00`) |
| `duration_sec`| Duration (seconds) the song was played        |

### 2. `songs_metadata.csv`
| Column    | Description                  |
|-----------|------------------------------|
| `song_id` | Unique ID of the song        |
| `title`   | Song title                   |
| `artist`  | Artist name                  |
| `genre`   | Song genre (Pop, Rock, Jazz…)|
| `mood`    | Song mood (Happy, Sad, Chill, Energetic…) |

---

## ✅ Tasks Implemented
1. **User’s Favourite Genre**  
   - Count plays per genre per user.  
   - Select the top genre for each user.  

2. **Average Listen Time per Song**  
   - Compute mean duration per song across all plays.  

3. **Genre Loyalty Score**  
   - For each user:  
     \[
     \text{loyalty} = \frac{\text{plays in top genre}}{\text{total plays}}
     \]  
   - Output users with score > 0.8.  

4. **Night Owl Users (12 AM – 5 AM)**  
   - Extract users who listen frequently in late-night hours.  

---

## Repository Structure

Hands-on L6: Spark Structured API/
│── datagen.py # Generates CSV datasets
│── main.py # Spark Structured API analysis
│── listening_logs.csv # Generated input dataset
│── songs_metadata.csv # Generated input dataset
│── output/ 
      |── user_favorite_genres/
      |── avg_listen_time_per_song/
      |── genre_loyalty_scores/
      |── night_owl_users/
|── requirements
│── README.md

## Output Directory Structure
│── output/ 
      |── user_favorite_genres/
      |── avg_listen_time_per_song/
      |── genre_loyalty_scores/
      |── night_owl_users/



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

Each folder contains the respective result files.

---

## 🚀 Approach
1. Loaded CSV datasets using `spark.read.csv(..., header=True, inferSchema=True)`.  
2. Cleaned and preprocessed data (ensured correct schema, handled timestamps).  
3. Performed **joins** between logs and metadata on `song_id`.  
4. Applied **aggregations, groupBy, window functions, and filters** for insights.  
5. Saved results into structured **output folders**.  

---


## 📝 Results Summary
- **Favourite Genre:** Most users showed strong preference for one or two genres.  
- **Avg Listen Time:** Popular songs had longer average play durations.  
- **Genre Loyalty:** A subset of users had loyalty scores > 0.8, showing strong attachment to a single genre.  
- **Night Owl Users:** Identified active late-night listeners (potential target group for features like playlists).  

---

## Execution Instructions
## *Prerequisites*

Before starting the assignment, ensure you have the following software installed and properly configured on your machine:

1. *Python 3.x*:
   - [Download and Install Python](https://www.python.org/downloads/)
   - Verify installation:
     ```bash
     python3 --version
     ```

2. *PySpark*:
   - Install using pip:
     ```bash
     pip install pyspark
     ```

3. *Apache Spark*:
   - Ensure Spark is installed. You can download it from the [Apache Spark Downloads](https://spark.apache.org/downloads.html) page.
   - Verify installation by running:
     ```bash
     spark-submit --version
     ```

### *2. Running the Analysis Tasks*

####  *Running Locally*

1. *Generate the Input*:
  ```bash
   python3 input_generator.py
   ```

2. **Execute Each Task Using spark-submit**:
   ```bash
     spark-submit main.py
   ```

3. *Verify the Outputs*:
   Check the outputs/ directory for the resulting files:
   ```bash
   ls outputs/
   ```

## Errors and Resolutions
File Not Found Error:
Ensure listening_logs.csv and songs_metadata.csv exist in the same folder as main.py.

Spark Session Errors:
Check that PySpark is installed correctly and the SPARK_HOME environment variable is set if necessary.

Permission Issues Writing Output:
Ensure the output/ directory exists or allow the script to create it automatically.
---

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
