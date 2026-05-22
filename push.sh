#!/bin/bash
cd "$(dirname "$0")"
git add .
git commit -m "Update $(date '+%Y-%m-%d %H:%M')"
git push origin main
echo "✅ Pushed to GitHub"
