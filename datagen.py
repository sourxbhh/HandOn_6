import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os

def generate_listening_logs(num_records=1000):
    """Generate listening logs dataset"""
    np.random.seed(42)
    
    # Generate sample data
    user_ids = [f"user_{i:03d}" for i in range(1, 101)]
    song_ids = [f"song_{i:03d}" for i in range(1, 51)]
    genres = ["Pop", "Rock", "Jazz", "Hip-Hop", "Electronic", "Classical", "Country", "R&B"]
    moods = ["Happy", "Sad", "Energetic", "Chill", "Romantic"]
    
    data = []
    
    for _ in range(num_records):
        user_id = np.random.choice(user_ids)
        song_id = np.random.choice(song_ids)
        
        # Generate random timestamp within the last 30 days
        days_ago = np.random.randint(0, 30)
        hours_ago = np.random.randint(0, 24)
        minutes_ago = np.random.randint(0, 60)
        
        timestamp = datetime.now() - timedelta(days=days_ago, hours=hours_ago, minutes=minutes_ago)
        
        # Generate duration between 30 seconds and 300 seconds (5 minutes)
        duration_sec = np.random.randint(30, 300)
        
        data.append({
            'user_id': user_id,
            'song_id': song_id,
            'timestamp': timestamp.strftime('%Y-%m-%d %H:%M:%S'),
            'duration_sec': duration_sec
        })
    
    df = pd.DataFrame(data)
    df.to_csv('listening_logs.csv', index=False)
    print(f"Generated listening_logs.csv with {len(data)} records")

def generate_songs_metadata():
    """Generate songs metadata dataset"""
    np.random.seed(42)
    
    song_ids = [f"song_{i:03d}" for i in range(1, 51)]
    genres = ["Pop", "Rock", "Jazz", "Hip-Hop", "Electronic", "Classical", "Country", "R&B"]
    moods = ["Happy", "Sad", "Energetic", "Chill", "Romantic"]
    
    artists = [
        "Taylor Swift", "Drake", "Bad Bunny", "The Weeknd", "Ed Sheeran",
        "Billie Eilish", "Kendrick Lamar", "Ariana Grande", "Post Malone", "Dua Lipa"
    ]
    
    data = []
    
    for song_id in song_ids:
        title = f"Song {song_id.split('_')[1]}"
        artist = np.random.choice(artists)
        genre = np.random.choice(genres)
        mood = np.random.choice(moods)
        
        data.append({
            'song_id': song_id,
            'title': title,
            'artist': artist,
            'genre': genre,
            'mood': mood
        })
    
    df = pd.DataFrame(data)
    df.to_csv('songs_metadata.csv', index=False)
    print(f"Generated songs_metadata.csv with {len(data)} records")

if __name__ == "__main__":
    generate_listening_logs(1000)  # Generate 1000 records as required
    generate_songs_metadata()