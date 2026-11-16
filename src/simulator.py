
import random
import time
import uuid
from datetime import datetime
import json
from kafka import KafkaProducer
producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

# ============================================================
# CONFIGURATION
# ============================================================

TRANSACTION_INTERVAL = 1.0

LARGE_TRANSACTION_PROBABILITY = 0.05
IMPOSSIBLE_TRAVEL_PROBABILITY = 0.03


# ============================================================
# LOCATION DATA
# ============================================================

locations = {
    "Delhi": {
        "region": "DL",
        "country": "IN",
        "coordinates": (28.6139, 77.2090)
    },

    "Mumbai": {
        "region": "MH",
        "country": "IN",
        "coordinates": (19.0760, 72.8777)
    },

    "Bangalore": {
        "region": "KA",
        "country": "IN",
        "coordinates": (12.9716, 77.5946)
    },

    "Jaipur": {
        "region": "RJ",
        "country": "IN",
        "coordinates": (26.9124, 75.7873)
    },

    "Hyderabad": {
        "region": "TS",
        "country": "IN",
        "coordinates": (17.3850, 78.4867)
    },

    "New York": {
        "region": "NY",
        "country": "US",
        "coordinates": (40.7128, -74.0060)
    },

    "London": {
        "region": "LND",
        "country": "UK",
        "coordinates": (51.5074, -0.1278)
    },

    "Dubai": {
        "region": "DU",
        "country": "AE",
        "coordinates": (25.2048, 55.2708)
    }
}


# ============================================================
# USER PROFILES
# ============================================================

users = {
    "U001": {
        "home_location": "Delhi",
        "avg_amount": 800,
        "card_id": "CARD1001",
        "card_type": "debit",
        "card_category": "visa",
        "p_email_domain": "gmail.com",
        "r_email_domain": "gmail.com",
        "device_type": "mobile",
        "device_info": "Android"
    },

    "U002": {
        "home_location": "Mumbai",
        "avg_amount": 3000,
        "card_id": "CARD1002",
        "card_type": "credit",
        "card_category": "mastercard",
        "p_email_domain": "outlook.com",
        "r_email_domain": "gmail.com",
        "device_type": "desktop",
        "device_info": "Windows"
    },

    "U003": {
        "home_location": "Bangalore",
        "avg_amount": 1500,
        "card_id": "CARD1003",
        "card_type": "debit",
        "card_category": "visa",
        "p_email_domain": "gmail.com",
        "r_email_domain": "gmail.com",
        "device_type": "mobile",
        "device_info": "Android"
    },

    "U004": {
        "home_location": "Jaipur",
        "avg_amount": 600,
        "card_id": "CARD1004",
        "card_type": "debit",
        "card_category": "rupay",
        "p_email_domain": "yahoo.com",
        "r_email_domain": "gmail.com",
        "device_type": "mobile",
        "device_info": "Android"
    },

    "U005": {
        "home_location": "Hyderabad",
        "avg_amount": 5000,
        "card_id": "CARD1005",
        "card_type": "credit",
        "card_category": "visa",
        "p_email_domain": "gmail.com",
        "r_email_domain": "gmail.com",
        "device_type": "desktop",
        "device_info": "Windows"
    }
}


# ============================================================
# PRODUCT CODES
# ============================================================

PRODUCT_CODES = [
    "W",
    "C",
    "H",
    "S",
    "R"
]


# ============================================================
# TRANSACTION HISTORY
# ============================================================

# Stores information about the user's previous transaction.
#
# This allows us to later calculate:
#
# - time_since_previous_transaction
# - distance_from_previous_transaction
# - transactions_last_5min
# - transactions_last_1hour
# - amount_vs_user_average
# - is_new_device
# - is_new_location

user_history = {}


# ============================================================
# TRANSACTION ID
# ============================================================

transaction_number = 1


# ============================================================
# HELPER FUNCTIONS
# ============================================================

def generate_amount(user, is_large_transaction):
    """
    Generate a transaction amount.

    Normal transaction:
        0.5x - 1.5x user's average

    Large transaction:
        8x - 20x user's average
    """

    if is_large_transaction:

        return round(
            random.uniform(
                user["avg_amount"] * 8,
                user["avg_amount"] * 20
            ),
            2
        )

    return round(
        random.uniform(
            user["avg_amount"] * 0.5,
            user["avg_amount"] * 1.5
        ),
        2
    )


def choose_location(user):
    """
    Normally keep the user near their home location.
    """

    if random.random() < 0.90:
        return user["home_location"]

    return random.choice(list(locations.keys()))


def choose_impossible_location(previous_location):
    """
    Choose a geographically distant location.
    """

    possible_locations = [
        location
        for location in locations
        if location != previous_location
    ]

    return random.choice(possible_locations)


def calculate_distance(location1, location2):
    """
    Calculate approximate distance between two locations
    using the Haversine formula.

    Returns distance in kilometers.
    """

    lat1, lon1 = locations[location1]["coordinates"]
    lat2, lon2 = locations[location2]["coordinates"]

    lat1 = lat1 * 3.141592653589793 / 180
    lon1 = lon1 * 3.141592653589793 / 180
    lat2 = lat2 * 3.141592653589793 / 180
    lon2 = lon2 * 3.141592653589793 / 180

    dlat = lat2 - lat1
    dlon = lon2 - lon1

    a = (
        (pow((dlat / 2), 2))
        + (
            pow((dlon / 2), 2)
            * 1
        )
    )

    # For this simulator, we only need an approximate
    # distance between known locations.

    from math import sin, cos, sqrt, atan2

    a = (
        sin(dlat / 2) ** 2
        + cos(lat1)
        * cos(lat2)
        * sin(dlon / 2) ** 2
    )

    c = 2 * atan2(sqrt(a), sqrt(1 - a))

    earth_radius = 6371

    return round(earth_radius * c, 2)


def get_transaction_hour():
    """
    Return current hour.
    """

    return datetime.now().hour


def get_transaction_day():
    """
    Return day of week.

    Monday = 0
    Sunday = 6
    """

    return datetime.now().weekday()


# ============================================================
# TRANSACTION GENERATOR
# ============================================================

def generate_transaction(user_id):
    """
    Generate one transaction event.
    """

    user = users[user_id]

    current_time = datetime.now()

    previous_transaction = user_history.get(user_id)


    # --------------------------------------------------------
    # Decide whether this transaction is an anomaly
    # --------------------------------------------------------

    random_value = random.random()

    impossible_travel = (
        random_value < IMPOSSIBLE_TRAVEL_PROBABILITY
        and previous_transaction is not None
    )

    large_transaction = (
        not impossible_travel
        and random_value < (
            IMPOSSIBLE_TRAVEL_PROBABILITY
            + LARGE_TRANSACTION_PROBABILITY
        )
    )


    # --------------------------------------------------------
    # Determine location
    # --------------------------------------------------------

    if impossible_travel:

        previous_location = previous_transaction["location"]

        current_location = choose_impossible_location(
            previous_location
        )

    else:

        current_location = choose_location(user)


    # --------------------------------------------------------
    # Determine transaction amount
    # --------------------------------------------------------

    amount = generate_amount(
        user,
        large_transaction
    )


    # --------------------------------------------------------
    # Transaction properties
    # --------------------------------------------------------

    product_code = random.choice(PRODUCT_CODES)

    distance = None

    if previous_transaction is not None:

        distance = calculate_distance(
            previous_transaction["location"],
            current_location
        )


    # --------------------------------------------------------
    # Create transaction
    # --------------------------------------------------------

    transaction = {

        # IEEE-CIS inspired
        "transaction_id":
            f"TX{transaction_number:06d}",

        "timestamp":
            current_time.isoformat(),

        "amount":
            amount,

        "product_code":
            product_code,

        "card_id":
            user["card_id"],

        "card_type":
            user["card_type"],

        "card_category":
            user["card_category"],

        "billing_region":
            locations[current_location]["region"],

        "billing_country":
            locations[current_location]["country"],

        "distance":
            distance,

        "p_email_domain":
            user["p_email_domain"],

        "r_email_domain":
            user["r_email_domain"],

        "device_type":
            user["device_type"],

        "device_info":
            user["device_info"],

        # Streaming-specific
        "user_id":
            user_id,

        "transaction_hour":
            get_transaction_hour(),

        "transaction_day":
            get_transaction_day()
    }


    # --------------------------------------------------------
    # Update history
    # --------------------------------------------------------

    user_history[user_id] = {

        "timestamp":
            current_time,

        "location":
            current_location,

        "amount":
            amount,

        "device_info":
            user["device_info"]
    }


    return transaction


# ============================================================
# MAIN STREAM
# ============================================================

while True:

    transaction = generate_transaction(
        random.choice(list(users.keys()))
    )
    producer.send("transactions", transaction)
    producer.flush()

    print(transaction)

    transaction_number += 1

    time.sleep(TRANSACTION_INTERVAL)