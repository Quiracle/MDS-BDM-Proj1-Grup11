#!/usr/bin/env python3

import os
import torch
import json
from transformers import set_seed, pipeline, AutoTokenizer, AutoModelForCausalLM
from huggingface_hub import login

# Authenticate using Hugging Face token
hf_token = os.getenv("HF_TOKEN")
if not hf_token:
    raise RuntimeError("HF_TOKEN not set in environment")
login(token=hf_token)

# Load Falcon model
model_id = "tiiuae/falcon-rw-1b"
tokenizer = AutoTokenizer.from_pretrained(model_id)
model = AutoModelForCausalLM.from_pretrained(model_id, torch_dtype=torch.float16).to("cuda")
generator = pipeline("text-generation", model=model, tokenizer=tokenizer, device=0)

# Set reproducibility
set_seed(42)

# Sample game compatibility data
games = [
    {"name": "Battlefield Echo", "current": "Bronze", "historical_max": "Gold", "trending": "Silver", "confidence": 0.85},
    {"name": "Stardust Racer", "current": "Platinum", "historical_max": "Platinum", "trending": "Platinum", "confidence": 0.98},
    {"name": "Zombie Architects", "current": "Silver", "historical_max": "Gold", "trending": "Bronze", "confidence": 0.65},
    {"name": "Magefall Legacy", "current": "Gold", "historical_max": "Gold", "trending": "Silver", "confidence": 0.92},
    {"name": "Cyberhowl 2079", "current": "Borked", "historical_max": "Bronze", "trending": "Borked", "confidence": 0.30},
    {"name": "Tactical Shellshock", "current": "Silver", "historical_max": "Silver", "trending": "Bronze", "confidence": 0.68},
    {"name": "Mystic Rails 2", "current": "Gold", "historical_max": "Platinum", "trending": "Gold", "confidence": 0.88},
    {"name": "Ghostline Protocol", "current": "Bronze", "historical_max": "Silver", "trending": "Bronze", "confidence": 0.55},
    {"name": "Dungeon Dreamers", "current": "Platinum", "historical_max": "Platinum", "trending": "Gold", "confidence": 0.97},
    {"name": "Neon Rebellion", "current": "Gold", "historical_max": "Gold", "trending": "Gold", "confidence": 0.91},
    {"name": "Echo Shift", "current": "Silver", "historical_max": "Silver", "trending": "Silver", "confidence": 0.72},
    {"name": "Frozen Circuit", "current": "Borked", "historical_max": "Bronze", "trending": "Borked", "confidence": 0.42},
    {"name": "Pixel Raider", "current": "Bronze", "historical_max": "Silver", "trending": "Bronze", "confidence": 0.60},
    {"name": "Gladiator Grove", "current": "Gold", "historical_max": "Gold", "trending": "Silver", "confidence": 0.89},
    {"name": "Crimson Havoc", "current": "Silver", "historical_max": "Gold", "trending": "Silver", "confidence": 0.78},
    {"name": "Terraform Protocol", "current": "Platinum", "historical_max": "Platinum", "trending": "Platinum", "confidence": 0.99},
    {"name": "Quantum Passage", "current": "Gold", "historical_max": "Platinum", "trending": "Gold", "confidence": 0.94},
    {"name": "Skybound Siege", "current": "Silver", "historical_max": "Silver", "trending": "Bronze", "confidence": 0.70},
    {"name": "Nightfall Outlaws", "current": "Bronze", "historical_max": "Silver", "trending": "Bronze", "confidence": 0.56},
    {"name": "Galactic Fringe", "current": "Gold", "historical_max": "Platinum", "trending": "Gold", "confidence": 0.90}
]

# Compatibility tier → adjective
compat_score_dict = {
    "Borked": "horrible",
    "Bronze": "bad",
    "Silver": "acceptable",
    "Gold": "good",
    "Platinum": "excellent"
}

# Tier ↔ numeric mapping
tier_to_num = {"Borked": 0, "Bronze": 1, "Silver": 2, "Gold": 3, "Platinum": 4}
num_to_tier = {v: k for k, v in tier_to_num.items()}

# Generate and store reviews
for game in games:
    # Compute weighted global score
    weights = {"current": 0.5, "trending": 0.3, "historical_max": 0.2}
    weighted_score = (
        weights["current"] * tier_to_num[game["current"]] +
        weights["trending"] * tier_to_num[game["trending"]] +
        weights["historical_max"] * tier_to_num[game["historical_max"]]
    )
    global_score_numeric = round(weighted_score)
    global_score_label = num_to_tier[global_score_numeric]
    game["global_score"] = global_score_label

    prompt = (
        f"User review: My experience with the Linux compatibility of {game['name']} is {compat_score_dict[global_score_label]} because"
    )

    result = generator(
        prompt,
        max_new_tokens=80,
        temperature=0.7,
        top_k=40,
        top_p=0.9,
        repetition_penalty=1.2,
        do_sample=True,
        num_return_sequences=1,
    )

    generated_text = result[0]['generated_text'].strip()
    game["review"] = generated_text

    print(f"{game['name']} ({global_score_label}) → {generated_text}\n")

# Save to JSON file
output_path = "generated_reviews.json"
with open(output_path, "w", encoding="utf-8") as f:
    json.dump(games, f, indent=2)

print(f"Generated reviews saved to {output_path}")
