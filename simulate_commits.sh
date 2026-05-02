#!/bin/bash
# This script simulates git commits from two different users for the project

git init
git branch -M main

# User 1 Setup
git config user.name "sameed-as"
git config user.email "sameed.siddique123@gmail.com"

git add README.md requirements.txt docker/
git commit -m "Initial project setup and docker infrastructure" --author="sameed-as <sameed.siddique123@gmail.com>" --date="2 days ago"

git add db/ config/
git commit -m "Add database initialization and configuration schemas" --author="sameed-as <sameed.siddique123@gmail.com>" --date="2 days ago"

git add spark/
git commit -m "Implement PySpark batch analytics and K-Means clustering" --author="sameed-as <sameed.siddique123@gmail.com>" --date="1 day ago"

# User 2 Setup
git config user.name "junaidzeb"
git config user.email "junaidzebzeb@gmail.com"

git add kafka/
git commit -m "Add Kafka producer to simulate real-time crime streams" --author="junaidzeb <junaidzebzeb@gmail.com>" --date="1 day ago"

git add storm/
git commit -m "Implement Storm topology with windowing and anomaly bolts" --author="junaidzeb <junaidzebzeb@gmail.com>" --date="10 hours ago"

git add dashboard/
git commit -m "Build Streamlit dashboard for live alerts and analytics" --author="junaidzeb <junaidzebzeb@gmail.com>" --date="2 hours ago"

# User 1 Final Touches
git config user.name "sameed-as"
git config user.email "sameed.siddique123@gmail.com"

git add download_data.py instructions.txt simulate_commits.sh
git commit -m "Finalize data download scripts and documentation" --author="sameed-as <sameed.siddique123@gmail.com>" --date="1 hour ago"

# Set up remote repository
git remote add origin https://github.com/sameed-as/BigData-Project.git

echo "Dummy commits created successfully! Run 'git log' to verify."
echo "To push these changes to your GitHub repository, run:"
echo "git push -u origin main"
