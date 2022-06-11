# Спринт 5 #

* Скопируйте проект в директорию:
```shell script
git clone https://github.com/practicum-de/s5-lessons.git
```
* Перейдите в директорию c проектом:
```shell script
cd s5-lessons
```
* Создайте [виртуальное окружение](https://docs.python.org/3/library/venv.html) и активируйте его:
```shell script
python3 -m venv venv
```

* Активируйте его:
```shell script
source venv/bin/activate
```
или в Windowns
```shell script
source venv/Scripts/activate
```

* Обновите pip до последней версии:
```shell script
pip install --upgrade pip
```
* Установите зависимости:
```shell script
pip install -r requirements.txt
```

Для выполнения заданий скачайте и запустите docker образ с базой данных и airflow (для заданий без debezium, иначе выполните запуск dockre-compose по инструкции с платформы):

`docker run -d --rm -p 3000:3000 -p 3002:3002 -p 15432:5432 --name=de-sprint-5-server-local sindb/de-pg-cr-af:latest
`

Перед запуском тестов убедитесь что запущен контейнер.

Если у Вас не установлен python 3.8 то самое время сделать это. 
