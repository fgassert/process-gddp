REMOTE=s3://md.cc/tmp/nex-gddp/
LOCAL=cache
mkdir -p $LOCAL

while read line; do
    if [ ! -z "$line" ]
    then
        s3cmd get $REMOTE$line $LOCAL/
    fi
done < outputs.txt
