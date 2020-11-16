#!/bin/bash
source config.sh

docker run --rm\
    -v $(pwd):/src\
    -w /src\
    -e EDM_DATA=$EDM_DATA\
    -e V_PFF=$V_PFF\
    nycplanning/cook:latest bash -c "
        python3 pff_download.py" |  
    psql $EDM_DATA -f sql/create.sql
