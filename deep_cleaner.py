
#Limit Snap Retention: By default, Snap keeps 3 versions of every app. You can reduce this to 2 (the minimum) to free up space permanently
cmd1 = 'sudo snap set system refresh.retain=2'

#Remove Old Snap Versions: You can use a small script to remove all "disabled" (old) versions of snaps. Copy and paste this into your terminal
cmd2 = """
snap list --all | awk '/disabled/{print $1, $3}' | while read snapname revision; do sudo snap remove "$snapname" --revision="$revision"; done

"""
#Clear Snap Cache: Sometimes Snap leaves large temporary files in its own cache:
cmd3='sudo rm -rf /var/lib/snapd/cache/*'

#2. Clear the .cache Folder (113.9 GB)

#The .cache directory contains temporary files for apps (browser data, thumbnails, etc.). It is generally safe to delete, as apps will just recreate what they need
cmd21 = "find ~/.cache/ -type f -atime +30 -delete"

#Aggressive approach: Delete everything (close your browser first):
cmd23 = "rm -rf ~/.cache/*"
#3. Handle the "Hidden" Culprit: miniconda3.bak (71.2 GB)
#In your image, miniconda3.bak is a backup folder.
#Action: If your current miniconda3 installation is working fine, you likely don't need this 71 GB backup.
#How to delete:

cmd3="rm -rf ~/miniconda3.bak"