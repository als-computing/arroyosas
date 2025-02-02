#!/bin/bash
source ../.env
# Ensure a directory is provided
if [ -z "$1" ]; then
    echo "Usage: $0 <directory>"
    exit 1
fi

BASE_DIR="$(realpath "$1")"  # Get the absolute path of the base directory
BASE_URL="http://localhost:8000/api/v1"

# Recursively find files and register them
find "$BASE_DIR" -type f | while read -r file; do
    # Get the relative path of the file
    rel_path="${file#$BASE_DIR/}"
    
    # Extract the folder structure and filename
    folder_path=$(dirname "$rel_path")
    
    # Register the file with tiled using full path
    echo "Registering: $BASE_URL/$folder_path $file"
    tiled register "$BASE_URL" "$file" --api-key "$TILED_SINGLE_USER_API_KEY"
done