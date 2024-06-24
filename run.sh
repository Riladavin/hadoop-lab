DATANODES_COUNT=1
OPTIMIZED=False

while [[ $# -gt 0 ]]
do
    case "$1" in
        --optimized)
            OPTIMIZED="$2"
            shift 2
            ;;
        --datanodes)
            DATANODES_COUNT="$2"
            shift 2
            ;;
        *)
            echo "Неизвестный параметр: $1"
            exit 1
            ;;
    esac
done
echo "Количество DataNode: $DATANODES_COUNT, оптимизированный запуск Spark: $OPTIMIZED."


# определяем со сколькими DataNode запускать контейнер
if [[ $DATANODES_COUNT -eq 1 ]] # проверяем ключ на равенство (-eq)
then
    docker-compose -f docker-compose.yml up -d # запускаем контейнеры (-d — в виде демона)
elif [[ $DATANODES_COUNT -eq 3 ]] # проверяем ключ на равенство (-eq)
then
    docker-compose -f docker-compose-3.yml up -d # запускаем контейнеры (-d — в виде демона)
else
    echo "Переданное количество DataNode=$DATANODES_COUNT не поддерживается!"
    exit 1 # выходим с exit кодом 1
fi


# отправляем данные в Hadoop
docker cp ./data/train.csv namenode:/ # копирование данных в контейнер
docker cp ./src/HDFS.sh namenode:/ # копирование данных в контейнер
docker exec -it namenode bash HDFS.sh # подключаемся к запущенному докер-контейнеру (Hadoop) и выполняем переданный скрипт
# docker exec -it namenode bash # подключаемся к запущенному докер-контейнеру (Hadoop)
# hdfs dfs -mkdir /data/ # создаём папку в hdfs под названием data
# hdfs dfs -D dfs.block.size=32M -put /train.csv /data/ # отправляем данные с NameNode в hdfs (на DataNode)
# hdfs dfsadmin -setSpaceQuota 5g / # выставление ограничение на используемую память 
# exit # выходим из вложенного терминала Hadoop


# отправляем приложение в контейнер со Spark и запускаем его
docker cp ./src/spark_app.py spark-master:/ # копируем Spark приложение в контейнер
docker cp ./src/Spark.sh spark-master:/ # копируем Spark приложение в контейнер
docker exec -it spark-master bash Spark.sh ${OPTIMIZED} # подключаемся к запущенному докер-контейнеру (Spark) и выполняем переданный скрипт
# docker exec -it spark-master bash # подключаемся к запущенному докер-контейнеру (Spark)
# apk add --update make automake gcc g++ # обновляем gcc (так как возникает ошибка при скачивании numpy)
# apk add --update python-dev # обновляем python-dev (из-за ошибки SystemError: Cannot compile 'Python.h'. Perhaps you need to install python-dev|python-devel.)
#pip install numpy
# for ((i=1; i<=N; i++)) # делаем 20 запусков
# do # начало цикла
#     /spark/bin/spark-submit /spark_app.py $OPTIMIZED # запускаем Spark приложение
# done # завершение цикла
# exit # выходим из вложенного терминала Spark

docker cp spark-master:/log.txt ./logs/log_DataNodes_${DATANODES_COUNT}_opt_${OPTIMIZED}.txt # сохраняем полученные логи на host устройство (с заменой имени)


# выключаем запущенные контейнеры
if [[ $DATANODES_COUNT -eq 1 ]] # проверяем ключ на равенство (-eq)
then
    docker-compose -f docker-compose.yml down # отключаем докер-контейнеры
else
    docker-compose -f docker-compose-3.yml down # отключаем докер-контейнеры
fi