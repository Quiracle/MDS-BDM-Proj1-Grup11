#!/usr/bin/env python3

import json
import pandas as pd
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

def classify_review_heuristic(review_text):
    sentiment = analyzer.polarity_scores(review_text)['compound']
    review_lower = review_text.lower()

    # Simple rule-based override
    if "crash" in review_lower or "doesn't launch" in review_lower:
        return "Borked"
    elif sentiment > 0.8:
        return "Platinum"
    elif sentiment > 0.5:
        return "Gold"
    elif sentiment > 0.2:
        return "Silver"
    elif sentiment > -0.1:
        return "Bronze"
    else:
        return "Borked"

# Initialize sentiment analyzer
analyzer = SentimentIntensityAnalyzer()

# Load generated reviews
with open("generated_reviews.json", "r", encoding="utf-8") as f:
    data = json.load(f)

print("\n=== Heuristic Sentiment Analysis Results ===")

# Classify and add predicted score
for game in data:
    review = game.get("review", "")
    predicted_score = classify_review_heuristic(review)
    game["heuristic_category"] = predicted_score

    print(f"{game['name']}: {predicted_score}")

# Convert to DataFrame
df = pd.DataFrame(data)

# Reorder columns if desired
preferred_order = [
    "name", "current", "historical_max", "trending", "confidence",
    "global_score", "heuristic_category", "review"
]
df = df[[col for col in preferred_order if col in df.columns]]

# Save to CSV
df.to_csv("compatibility_reviews.csv", index=False)
print("\nCategorized reviews saved to compatibility_reviews.csv")
