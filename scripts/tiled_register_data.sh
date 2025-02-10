#!/bin/bash
source ../.env
echo "Executing Folder: $PWD"
export PYTHONPATH="$PYTHONPATH:$PWD/tiled/config/"
echo "Python Path: $PYTHONPATH"
echo "Data catalog for raw data: $PATH_TO_RAW_DATA_CATALOG"
echo "Data catalog for processed data: $PATH_TO_PROCESSED_DATA_CATALOG"
echo "Single User API Key: $TILED_SINGLE_USER_API_KEY"
echo "Tiled URI: $TILED_URI"

## Will overwrite
if [ -d "$PATH_TO_RAW_DATA" ]; then
     tiled register $TILED_URI --verbose \
            --prefix 'sample_data' \
            --api-key $TILED_SINGLE_USER_API_KEY \
            --walker 'tiled.client.register:one_node_per_item' \
            "$PATH_TO_RAW_DATA"
else
    echo "The directory for raw data ($PATH_TO_RAW_DATA) does not exist."
fi