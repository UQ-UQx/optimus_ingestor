#!/bin/bash
#
# Script to download, unpack, decrypt, and copy to RDSI, the latest edX research data package for the institution
#  - optionally leave a copy of the decrypted files on the local machine
#
# find the dump file for the institution in the edx amazon s3 bucket
tmpfolder=`echo -n /tmp/;date "+%Y%m%d%H%M%S"`
institution=uqx
institution_upper=UQx
mkdir $tmpfolder
cd $tmpfolder
ddate=`aws s3 ls s3://course-data | grep $institution | sort | tail -1 | sed 's/^.*$institution-//' | sed 's/.zip$//'`
echo Proposing to retrieve $institution-$ddate.zip
read -p "Proceed? " -n 1 -r
echo    # (optional) move to a new line
if [[ $REPLY =~ ^[Yy]$ ]]
then
    # copy the most recent dump file from the bucket to your local computer
    echo Fetching from Amazon S3
    aws s3 cp s3://course-data/$institution-$ddate.zip .
    # save a copy copy of the raw files to the RDSI gpg-files directory
    echo Save a raw copy to RDSI
    scp $institution-$ddate.zip username@collectionhostname:/data/Q0025/gpg_datadumps/
    # unzip the dump file
    echo Extracting and decrypting
    unzip $institution-$ddate.zip -x '$institution*/$institution_upper-THINK101x*' '$institution*/*-edge*'
    cd $institution-$ddate
    for file in *.gpg; do gpg --output ${file%.*} --decrypt $file; rm $file; done
    for file in *.gz; do tar xzf $file; rm $file; done
    cd ..
    # copy to RDSI
    echo Copy extracted files to RDSI
    scp -r $institution-$ddate username@collectionhostname:/data/Q0025/database_state/
    # remove the zip file
    rm $institution-$ddate.zip
    read -p "Remove local files? " -n 1 -r
    echo    # (optional) move to a new line
    if [[ $REPLY =~ ^[Yy]$ ]]
    then
        rm -rf $institution-$ddate
    fi
    # link to latest files
    read -p "Move link to latest? " -n 1 -r
    echo    # (optional) move to a new line
    if [[ $REPLY =~ ^[Yy]$ ]]
    then
        ssh username@collectionhostname bash -c "'cd database_state; rm latest; ln -s $institution-$ddate latest'"
    fi

fi
# download event logs from the edx amazon s3 bucket
echo Proposing to retrieve new event logs
read -p "Proceed? " -n 1 -r
echo    # (optional) move to a new line
if [[ $REPLY =~ ^[Yy]$ ]]
then
        # copy the most recent tracking file from the bucket to your local computer
        year=2015
        for gpgfile in $( aws s3 ls s3://edx-course-data/$institution/edx/events/$year/  | sed 's/^.*$institution/$institution/' ); do
                gzfile=${gpgfile%.*}
                logfile=${gzfile%.*}
                if ssh -q username@collectionhostname ! [ -a clickstream_logs/events/$logfile ] ; then
                        echo Fetching $gzfile
                        aws s3 cp s3://edx-course-data/$institution/edx/events/$year/$gpgfile .
                        scp $gpgfile username@collectionhostname:/data/Q0025/gpg_datadumps/
                        gpg --output $gzfile --decrypt $gpgfile
                        gunzip $gzfile
                        rm $gpgfile
                        scp $logfile username@collectionhostname:/data/Q0025/clickstream_logs/events
                        rm $logfile
                fi
        done
fi
cd ..