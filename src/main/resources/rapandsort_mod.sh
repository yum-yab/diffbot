#!/bin/bash

# NOTE: this script does NOT compress the file at the end

VERSION=$1
location=$2


if [ ! -d "${location}/tmpfolder" ]; then
    mkdir -p $location/tmpfolder
fi

# skip parent
if [ "$VERSION" = "common-metadata" ]; then
    exit
fi

LOG=$location/tmpfolder/logfile
echo -n "" > $LOG

TMPFILE=$location/tmpfolder/tmpfile

# now customized to the repo data structure
for file in $VERSION/*.ttl.bz2; do
        echo "processing $file ..." >> $LOG  ;
        lbzip2 -dc $file |\
	      rapper -i ntriples -O - - file 2>>$LOG |\
      	LC_ALL=C sort --parallel=4 -u -T $location/tmpfolder > $TMPFILE;
        echo "finished processing $file" >> $LOG ;
        mv $TMPFILE ${file%.*};
	rm $file
done
