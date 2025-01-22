mkdir unreadable
chmod -r unreadable # removes the read permission recursively (all files and subfolders)
#chmod 000 unreadable # the folder unreadable (but not the contents inside it)
touch unreadable.csv
chmod a-r unreadable.csv # Remove Read Permissions for All Users
#chmod 000 unreadable.csv # To make the file completely inaccessible (no read, write, or execute permissions)
