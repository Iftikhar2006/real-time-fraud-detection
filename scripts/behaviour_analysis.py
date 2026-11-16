import random
import json
from collections import deque

import numpy as np
import pandas as pd

DATA_PATH = (
    "/mnt/c/Users/Iftikhar alam/Downloads/sem5/"
    "DEPROJECT/ieee-fraud-detection/train_transaction.csv"
)

CARD_COLS = ["card1", "card2", "card3", "card4", "card5", "card6"]
CHUNK_SIZE = 5000
SAMPLE_SIZE = 5000

rng = random.Random(42)

# One state per complete anonymized card signature.
history = {}

# Bounded reservoir samples: never retain all 590k feature values.
samples = {
    label: {
        feature: []
        for feature in [
            "previous_count",
            "previous_average",
            "time_since_previous",
            "amount_ratio",
            "recent_count_10m",
            "recent_spend_10m",
            "recent_count_1h",
            "recent_spend_1h",
        ]
    }
    for label in (0, 1)
}

sample_seen = {
    label: {feature: 0 for feature in samples[label]}
    for label in (0, 1)
}

totals = {0: 0, 1: 0}
eligible = {0: 0, 1: 0}
incomplete = 0

rules = {
    label: {
        "amount_ratio_3": 0,
        "rapid_60s": 0,
        "previous_count_5": 0,
    }
    for label in (0, 1)
}


def add_sample(label, feature, value):
    if not np.isfinite(value):
        return

    sample_seen[label][feature] += 1
    seen = sample_seen[label][feature]
    reservoir = samples[label][feature]

    if len(reservoir) < SAMPLE_SIZE:
        reservoir.append(float(value))
    else:
        index = rng.randrange(seen)
        if index < SAMPLE_SIZE:
            reservoir[index] = float(value)


print("Starting bounded-memory chronological analysis...", flush=True)

reader = pd.read_csv(
    DATA_PATH,
    usecols=[
        "TransactionDT",
        "TransactionAmt",
        "isFraud",
        *CARD_COLS,
    ],
    dtype={col: "string" for col in CARD_COLS},
    chunksize=CHUNK_SIZE,
    low_memory=False,
)

processed = 0

for chunk in reader:
    chunk = chunk[
        ["TransactionDT", "TransactionAmt", "isFraud", *CARD_COLS]
    ]
    for row in chunk.itertuples(index=False, name=None):
        dt, amount, label, *card_values = row

        label = int(label)
        dt = float(dt)
        amount = float(amount)

        totals[label] += 1
        processed += 1

        if any(pd.isna(value) for value in card_values):
            incomplete += 1
            continue

        signature = tuple(str(value) for value in card_values)

        state = history.get(signature)

        if state is None:
            state = {
                "count": 0,
                "total": 0.0,
                "last_dt": None,
                "recent_10m": deque(),
                "recent_1h": deque(),
                "spend_10m": 0.0,
                "spend_1h": 0.0,
            }
            history[signature] = state

        previous_count = state["count"]
        previous_average = (
            state["total"] / previous_count
            if previous_count else np.nan
        )

        time_since = (
            dt - state["last_dt"]
            if state["last_dt"] is not None else np.nan
        )

        amount_ratio = (
            amount / previous_average
            if previous_count and previous_average > 0
            else np.nan
        )

        q10 = state["recent_10m"]
        q60 = state["recent_1h"]

        while q10 and dt - q10[0][0] > 600:
            _, old_amount = q10.popleft()
            state["spend_10m"] -= old_amount

        while q60 and dt - q60[0][0] > 3600:
            _, old_amount = q60.popleft()
            state["spend_1h"] -= old_amount

        features = {
            "previous_count": previous_count,
            "previous_average": previous_average,
            "time_since_previous": time_since,
            "amount_ratio": amount_ratio,
            "recent_count_10m": len(q10),
            "recent_spend_10m": state["spend_10m"],
            "recent_count_1h": len(q60),
            "recent_spend_1h": state["spend_1h"],
        }

        eligible[label] += 1

        for feature, value in features.items():
            add_sample(label, feature, value)

        rules[label]["amount_ratio_3"] += int(
            np.isfinite(amount_ratio) and amount_ratio >= 3
        )
        rules[label]["rapid_60s"] += int(
            np.isfinite(time_since) and 0 <= time_since <= 60
        )
        rules[label]["previous_count_5"] += int(previous_count >= 5)

        # Update state only AFTER calculating current transaction features.
        state["count"] += 1
        state["total"] += amount
        state["last_dt"] = dt

        q10.append((dt, amount))
        q60.append((dt, amount))
        state["spend_10m"] += amount
        state["spend_1h"] += amount

    if processed % 50000 == 0:
        print(
            f"Processed {processed:,} rows; "
            f"tracked signatures: {len(history):,}",
            flush=True,
        )

summary = {
    "transactions": totals,
    "eligible_complete_signatures": eligible,
    "incomplete_signatures": incomplete,
    "unique_signatures": len(history),
    "rules": rules,
    "quantiles": {},
}

quantile_levels = [0.50, 0.75, 0.90, 0.95, 0.99]

for feature in samples[0]:
    summary["quantiles"][feature] = {}

    for label, name in [(0, "legitimate"), (1, "fraud")]:
        values = samples[label][feature]

        summary["quantiles"][feature][name] = {
            "sample_size": len(values),
            "observations": sample_seen[label][feature],
            "values": (
                dict(zip(
                    map(str, quantile_levels),
                    np.quantile(values, quantile_levels).round(3).tolist()
                ))
                if values else {}
            ),
        }

print("\n===== SUMMARY =====")
print(json.dumps(summary, indent=2))

output_path = "results/behaviour_summary.json"

with open(output_path, "w") as f:
    json.dump(summary, f, indent=2)

print(f"\nSaved results to {output_path}", flush=True)
print("Analysis completed.", flush=True)
