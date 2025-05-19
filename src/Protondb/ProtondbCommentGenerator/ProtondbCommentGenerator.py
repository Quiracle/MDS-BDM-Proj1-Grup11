#!/usr/bin/env python3

from transformers import pipeline, set_seed
import os

generator = pipeline("text-generation", model="distilgpt2", device=0)

set_seed(42)

# Sample game compatibility data
games = [
    {
        "name": "Battlefield Echo",
        "current": "Bronze",
        "historical_max": "Gold",
        "trending": "Silver",
        "confidence": 0.85
    },
    {
        "name": "Stardust Racer",
        "current": "Platinum",
        "historical_max": "Platinum",
        "trending": "Platinum",
        "confidence": 0.98
    },
    {
        "name": "Zombie Architects",
        "current": "Silver",
        "historical_max": "Gold",
        "trending": "Bronze",
        "confidence": 0.65
    }
]

comments = []

for game in games:
    prompt = (
        f"I have played the videogame {game['name']} on my Linux PC, and the compatibility score for the game was \"{game['current']}\". In my experience, the compatibility was "
    )

    result = generator(
        prompt,
        max_new_tokens=80,
        temperature=0.9,
        top_k=50,
        top_p=0.95,
        repetition_penalty=1.2,
        do_sample=True,
        num_return_sequences=1,
    )

    print(f"Result: {result}")
    generated_text = result[0]['generated_text']
    comments.append(f"{game['name']}:\n{generated_text.strip()}\n\n")

# Save to file
output_path = "compatibility_comments.txt"
with open(output_path, "w", encoding="utf-8") as f:
    f.writelines(comments)

print(f"Comments saved to {output_path}")
