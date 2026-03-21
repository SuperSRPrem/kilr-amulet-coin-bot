# 🪙 KILR Amulet Coin Challenge Bot

A real-time Discord bot that tracks clan performance in Territorial.io and runs a daily randomized competition.

## 🚀 Features

- 📊 Tracks live match data from Territorial.io
- 🧠 Aggregates player performance in real-time
- ⏱️ Randomized daily event trigger
- 🏆 Automatically announces winners in Discord
- 📈 Displays leaderboard (Top 10)
- 🔁 Runs continuously using GitHub Actions (no server required)

## ⚙️ How it Works

- GitHub Actions runs the bot every 20 minutes
- The bot collects new match data from public logs
- Scores are accumulated locally in a JSON state
- At a random time each day, the winner is selected
- Results are posted to Discord

## 🧩 Tech Stack

- Python
- Discord API
- GitHub Actions (Cron Jobs)
- JSON-based state persistence

## 📌 Key Design Decisions

- Uses rolling accumulation instead of relying on external history
- Avoids database usage for simplicity and cost
- Handles unreliable log retention (~30 min window)

## 🏁 Result

A fully automated, serverless, real-time competitive system for community engageme
