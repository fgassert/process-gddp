TMPUSERDATA=.user-data.sh
echo "#!/bin/bash" > $TMPUSERDATA
cat ../.env >> $TMPUSERDATA
echo "set -x" >> $TMPUSERDATA
echo "cd ~" >> $TMPUSERDATA
echo "exec > >(tee ./user-data.log|logger -t user-data -s 2>/dev/console) 2>&1" >> $TMPUSERDATA
echo "sudo yum update -y" >> $TMPUSERDATA
echo "sudo yum install -y docker git" >> $TMPUSERDATA
echo "sudo service docker start" >> $TMPUSERDATA
echo "git clone https://github.com/fgassert/process-gddp.git" >> $TMPUSERDATA
echo "cd process-gddp" >> $TMPUSERDATA
echo "echo \"$(cat ../.env)\" > .env" >> $TMPUSERDATA
echo "export LOG; ./start.sh \$(cat scripts/outputs.txt)" >> $TMPUSERDATA
echo "sleep 60; sudo shutdown -h now" >> $TMPUSERDATA
