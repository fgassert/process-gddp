NAME=$(basename $(pwd))
LOG=${LOG:-udp://localhost}

echo $LOG

docker build -t $NAME .
docker run --log-driver=syslog --log-opt syslog-address=$LOG --log-opt tag=$NAME --name=$NAME --env-file .env --rm $NAME python main.py $@
