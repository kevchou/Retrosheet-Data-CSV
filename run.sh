#!/usr/bin/env bash

# Input time frame here
startYear=2000
endYear=2001

# Make needed folders
mkdir -p ./downloaded
mkdir -p ./unparsed
mkdir -p ./parsed

# Download and unzip retrosheet data
for (( year=$startYear; year<=$endYear; year++ ))
do
    wget -O ./downloaded/"$year"eve.zip http://www.retrosheet.org/events/"$year"eve.zip 
    unzip ./downloaded/"$year"eve.zip -d ./unparsed/
done

# function that parses the downloaded retrosheet data into .csv files.
# I wrote this as a function so I use "cd" in a bash script
function parse_data() {
   
    cd unparsed/

     # Parse retrosheet data
    for (( year=$startYear; year<=$endYear; year++ ))
    do
        echo "******************************"
        echo Parsing year $year
        echo $PWD
        
        cwevent -f 0-96 -x 0-62 -y "$year" "$year"*.EV* > ../parsed/all"$year".csv
        cwgame -f 0-83 -y "$year" "$year"*.EV* > ../parsed/games"$year".csv
        cwsub -f 0-9 -y "$year" "$year"*.EV* > ../parsed/sub"$year".csv
    done

    cd ..
}

parse_data


# clean up
rm -rf ./downloaded/
rm -rf ./unparsed/

echo "done!"