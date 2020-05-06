#!/bin/bash
############################################################################
#
# Usage: get_tl_shp_file.sh 
#
# Download shp file from https://www2.census.gov/geo/tiger
#
# Options:
#  -y|--year [ year of data ]
#  -f|--filepath [ filepath from year folder ]
#
############################################################################

## Flags

## Functions
show_help () {
        echo "Usage:"
        echo "  ./get_tl_shp_file.sh" 
        echo "      -y|--year [ year of data ]"
        echo "      -f|--filepath [ filepath from year folder ]"
}

## Argument handling
while [ -n "$1" ]; do

	case "$1" in

    -y | --year)
        YEAR=$2
        shift
        ;;

    -f | --filepath)
        FILEPATH=$2
        shift
        ;;

    -h | --help)
        show_help
        exit 0
        ;;

	*)  
        echo "Did not recognize $1"
        echo ""
        show_help
        exit -1
        ;;

	esac

	shift

done

## Variables
BASE_URL="https://www2.census.gov/geo/tiger"
YEAR_URL="${BASE_URL}/TIGER${YEAR:-2018}"
FULL_URL="${YEAR_URL}/${FILEPATH}"

## Download to /tmp/
curl "${FULL_URL}" -o "/tmp/${FILENAME:-tiger_data}.zip" #FIX OUTPUT FILENAME

