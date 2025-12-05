cd /home/eric/OTA-Radar-5G-Trials

# Backup current files
tar -czf ~/ota-backup.tar.gz --exclude=.git .

# Remove git entirely
rm -rf .git

# Initialize fresh repo
git init
git add .
git commit -m "Initial commit"

# Reconnect to remote and force push
git remote add origin https://github.com/macclab-stevens/OTA-Radar-5G-Trials.git
git branch -M main
git push -f origin main