import pandas as pd
import random
import time
import uuid

# Load dataset
movies = pd.read_csv("data/wb_movies.csv")

def generate_session():
    session_id = str(uuid.uuid4())
    user_id = random.randint(1, 50000)
    movie = movies.sample(1).iloc[0]

    movie_id = int(movie["id"])
    movie_name = movie["title"]

    session_events = []

    # Session flow
    session_events.append(("play", random.randint(1, 5)))

    for _ in range(random.randint(1, 5)):
        session_events.append((
            random.choices(["pause", "play"],
            weights=[0.3, 0.7]
            )[0],
            random.randint(5, 30)
        ))

    session_events.append(("stop", random.randint(1, 5)))

    return session_id, user_id, movie_id, movie_name, session_events


def emit_event(session_id, user_id, movie_id, movie_name, event, watch_time):
    return {
        "session_id": session_id,
        "user_id": user_id,
        "movie_id": movie_id,
        "movie_name": movie_name,
        "event": event,
        "watch_time": watch_time,
        "timestamp": time.time()
    }


print("Starting realistic streaming simulation...\n")

while True:
    session_id, user_id, movie_id, movie_name, session_events = generate_session()

    for event_type, duration in session_events:
        event = emit_event(
            session_id, user_id, movie_id, movie_name,
            event_type, duration
        )

        print(event)

        time.sleep(0.05)  