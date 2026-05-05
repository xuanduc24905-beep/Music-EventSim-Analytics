"""
simulator.py — Music streaming event simulator (EventSim replacement).
Generates realistic JSON events continuously and pushes to Kafka.
Schema mirrors the original EventSim / Sparkify dataset.
"""
import json
import os
import random
import time
import logging
from datetime import datetime, timezone

from faker import Faker
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC             = os.getenv("KAFKA_TOPIC", "music-events")
EVENTS_PER_SECOND       = float(os.getenv("EVENTS_PER_SECOND", "10"))
NUM_USERS               = int(os.getenv("NUM_USERS", "1000"))

fake = Faker()

# ── Static catalogue ──────────────────────────────────────────
ARTISTS = [
    "The Beatles", "Taylor Swift", "Ed Sheeran", "Adele", "Coldplay",
    "Radiohead", "Kendrick Lamar", "Beyoncé", "Drake", "Billie Eilish",
    "The Weeknd", "Post Malone", "Ariana Grande", "BTS", "Olivia Rodrigo",
    "Harry Styles", "Dua Lipa", "Bad Bunny", "Justin Bieber", "Rihanna",
    "Eminem", "Jay-Z", "Kanye West", "Bruno Mars", "Lady Gaga",
    "Katy Perry", "Maroon 5", "One Direction", "Imagine Dragons", "Twenty One Pilots",
    "Foo Fighters", "Red Hot Chili Peppers", "Linkin Park", "Metallica", "Nirvana",
    "Pink Floyd", "Led Zeppelin", "Queen", "David Bowie", "Fleetwood Mac",
]

SONGS_BY_ARTIST = {
    "The Beatles":        ["Hey Jude", "Let It Be", "Come Together", "Yesterday", "Blackbird"],
    "Taylor Swift":       ["Shake It Off", "Blank Space", "Love Story", "Anti-Hero", "Cruel Summer"],
    "Ed Sheeran":         ["Shape of You", "Thinking Out Loud", "Perfect", "Photograph", "Bad Habits"],
    "Adele":              ["Hello", "Rolling in the Deep", "Someone Like You", "Easy On Me", "Skyfall"],
    "Coldplay":           ["Yellow", "The Scientist", "Fix You", "A Sky Full of Stars", "Clocks"],
    "Radiohead":          ["Creep", "Karma Police", "Fake Plastic Trees", "No Surprises", "Paranoid Android"],
    "Kendrick Lamar":     ["HUMBLE.", "DNA.", "Alright", "Swimming Pools", "Money Trees"],
    "Beyoncé":            ["Crazy in Love", "Halo", "Single Ladies", "Lemonade", "Formation"],
    "Drake":              ["God's Plan", "Hotline Bling", "One Dance", "In My Feelings", "Started From the Bottom"],
    "Billie Eilish":      ["Bad Guy", "Happier Than Ever", "Ocean Eyes", "Lovely", "Therefore I Am"],
    "The Weeknd":         ["Blinding Lights", "Save Your Tears", "Starboy", "Can't Feel My Face", "The Hills"],
    "Post Malone":        ["Sunflower", "Rockstar", "Circles", "Congratulations", "Better Now"],
    "Ariana Grande":      ["Thank U, Next", "7 Rings", "Problem", "Break Free", "God Is a Woman"],
    "BTS":                ["Dynamite", "Butter", "Boy With Luv", "DNA", "Fake Love"],
    "Olivia Rodrigo":     ["drivers license", "good 4 u", "deja vu", "brutal", "traitor"],
    "Harry Styles":       ["Watermelon Sugar", "As It Was", "Adore You", "Lights Up", "Sign of the Times"],
    "Dua Lipa":           ["Levitating", "Don't Start Now", "New Rules", "Physical", "Blow Your Mind"],
    "Bad Bunny":          ["Dakiti", "MIA", "Tití Me Preguntó", "Me Porto Bonito", "Un Verano Sin Ti"],
    "Justin Bieber":      ["Baby", "Sorry", "Love Yourself", "Stay", "Peaches"],
    "Rihanna":            ["Umbrella", "We Found Love", "Diamonds", "Stay", "Work"],
    "Eminem":             ["Lose Yourself", "Slim Shady", "Love The Way You Lie", "Not Afraid", "Without Me"],
    "Jay-Z":              ["Empire State of Mind", "99 Problems", "HOVA Song", "Big Pimpin", "Run This Town"],
    "Kanye West":         ["Gold Digger", "Stronger", "All Falls Down", "Flashing Lights", "Power"],
    "Bruno Mars":         ["Uptown Funk", "Just the Way You Are", "Grenade", "Locked Out of Heaven", "24K Magic"],
    "Lady Gaga":          ["Bad Romance", "Poker Face", "Just Dance", "Shallow", "Born This Way"],
    "Katy Perry":         ["Roar", "Firework", "Dark Horse", "Teenage Dream", "California Gurls"],
    "Maroon 5":           ["Sugar", "Animals", "Maps", "Moves Like Jagger", "She Will Be Loved"],
    "One Direction":      ["What Makes You Beautiful", "Story of My Life", "Best Song Ever", "Night Changes", "Perfect"],
    "Imagine Dragons":    ["Radioactive", "Demons", "Believer", "Thunder", "Enemy"],
    "Twenty One Pilots":  ["Stressed Out", "Heathens", "Ride", "Jumpsuit", "Chlorine"],
    "Foo Fighters":       ["Everlong", "Best of You", "Learn to Fly", "The Pretender", "Times Like These"],
    "Red Hot Chili Peppers": ["Under the Bridge", "Californication", "By the Way", "Scar Tissue", "Snow"],
    "Linkin Park":        ["In the End", "Numb", "Crawling", "Breaking the Habit", "Somewhere I Belong"],
    "Metallica":          ["Enter Sandman", "Master of Puppets", "Nothing Else Matters", "One", "Fade to Black"],
    "Nirvana":            ["Smells Like Teen Spirit", "Come as You Are", "Heart-Shaped Box", "Lithium", "In Bloom"],
    "Pink Floyd":         ["Comfortably Numb", "Wish You Were Here", "Money", "Another Brick in the Wall", "Time"],
    "Led Zeppelin":       ["Stairway to Heaven", "Kashmir", "Whole Lotta Love", "Black Dog", "Rock and Roll"],
    "Queen":              ["Bohemian Rhapsody", "We Will Rock You", "Don't Stop Me Now", "Another One Bites the Dust", "Somebody to Love"],
    "David Bowie":        ["Heroes", "Space Oddity", "Let's Dance", "Changes", "Life on Mars"],
    "Fleetwood Mac":      ["Dreams", "Go Your Own Way", "The Chain", "Landslide", "Gold Dust Woman"],
}

PAGE_TYPES = ["NextSong", "Home", "Login", "Logout", "Settings", "About",
              "Upgrade", "Error", "Submit Upgrade", "Roll Advert", "Add to Playlist"]
PAGE_WEIGHTS = [0.75, 0.08, 0.04, 0.02, 0.02, 0.01, 0.02, 0.01, 0.01, 0.02, 0.02]

LOCATIONS = [
    "New York, NY", "Los Angeles, CA", "Chicago, IL", "Houston, TX",
    "Phoenix, AZ", "Philadelphia, PA", "San Antonio, TX", "San Diego, CA",
    "Dallas, TX", "San Jose, CA", "Austin, TX", "Jacksonville, FL",
    "Fort Worth, TX", "Columbus, OH", "Charlotte, NC", "Indianapolis, IN",
    "San Francisco, CA", "Seattle, WA", "Denver, CO", "Nashville, TN",
]

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 15_0 like Mac OS X) AppleWebKit/605.1.15",
    "Mozilla/5.0 (Android 12; Mobile; rv:95.0) Gecko/95.0 Firefox/95.0",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/96.0.4664.45",
]


def make_user(user_id: int) -> dict:
    gender = random.choice(["M", "F"])
    return {
        "userId":       user_id,
        "gender":       gender,
        "firstName":    fake.first_name_male() if gender == "M" else fake.first_name_female(),
        "lastName":     fake.last_name(),
        "level":        random.choices(["free", "paid"], weights=[0.65, 0.35])[0],
        "location":     random.choice(LOCATIONS),
        "userAgent":    random.choice(USER_AGENTS),
        "registration": int(fake.date_time_between(start_date="-2y", end_date="-30d").timestamp() * 1000),
        "sessionId":    random.randint(1, 99999),
        "itemInSession": 0,
    }


def make_event(user: dict) -> dict:
    page = random.choices(PAGE_TYPES, weights=PAGE_WEIGHTS)[0]
    artist = random.choice(ARTISTS)
    song   = random.choice(SONGS_BY_ARTIST.get(artist, ["Unknown Song"]))

    event = {
        "artist":        artist if page == "NextSong" else None,
        "song":          song   if page == "NextSong" else None,
        "duration":      round(random.uniform(120.0, 420.0), 3) if page == "NextSong" else None,
        "ts":            int(datetime.now(timezone.utc).timestamp() * 1000),
        "userId":        user["userId"],
        "sessionId":     user["sessionId"],
        "page":          page,
        "level":         user["level"],
        "location":      user["location"],
        "userAgent":     user["userAgent"],
        "gender":        user["gender"],
        "firstName":     user["firstName"],
        "lastName":      user["lastName"],
        "registration":  user["registration"],
        "itemInSession": user["itemInSession"],
        "status":        200 if page != "Error" else 404,
        "method":        "PUT" if page in ["NextSong", "Thumbs Up", "Thumbs Down", "Add to Playlist"] else "GET",
    }

    user["itemInSession"] += 1
    if random.random() < 0.05:
        user["sessionId"] = random.randint(1, 99999)
        user["itemInSession"] = 0

    return event


def connect_kafka(retries: int = 10, delay: int = 5) -> KafkaProducer:
    for attempt in range(retries):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS.split(","),
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                acks="all",
                retries=3,
            )
            log.info(f"Connected to Kafka at {KAFKA_BOOTSTRAP_SERVERS}")
            return producer
        except NoBrokersAvailable:
            log.warning(f"Kafka not ready, retry {attempt + 1}/{retries} in {delay}s...")
            time.sleep(delay)
    raise RuntimeError("Could not connect to Kafka after retries")


def main():
    log.info(f"EventSim starting — {NUM_USERS} users, {EVENTS_PER_SECOND} events/sec → {KAFKA_TOPIC}")

    producer = connect_kafka()

    users = [make_user(i) for i in range(1, NUM_USERS + 1)]

    interval = 1.0 / EVENTS_PER_SECOND
    total    = 0

    while True:
        user  = random.choice(users)
        event = make_event(user)

        producer.send(KAFKA_TOPIC, value=event)
        total += 1

        if total % 500 == 0:
            producer.flush()
            log.info(f"Sent {total:,} events — latest: {event['page']} | {event.get('artist', '-')} - {event.get('song', '-')}")

        time.sleep(interval)


if __name__ == "__main__":
    main()
