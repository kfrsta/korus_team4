# Проект: Дашборд для отслеживания показателей прогресса сотрудников
## Описание
Этот проект направлен на создание дашборда для отслеживания показателей прогресса сотрудников. Проект использует стек технологий Docker, Apache Airflow и Luxmsbi для оркестрации, обработки данных и визуализации соответственно.

## Описание файлов и директорий
### dags/: Директория, содержащая DAG-файлы для Apache Airflow.

test_dag.py: Тестовый DAG, который выгружает одну строчку из базы данных.

ods_init.py: DAG, отвечающий за создание схемы ODS, подобной схеме в источнике данных.

ods_load.py: DAG, который выполняет функцию основного пайплайна данных. Содержит 7 Задач: 1) Загрузка данных в слой ODS 2) Создание схемы andronov_dds DDS 3) Создание схемы andronov_broken для данных с ошибками
4) Обработка и загрузка в слой DDS справочников 5) Обработка и загрузка в слой DDS таблиц сертификаты_пользователей и сотрудники дар 6) Обработка и загрузка в слой DDS основных таблиц.

docker-compose.yaml: Файл конфигурации Docker Compose для запуска необходимых сервисов (Airflow, базы данных и т.д.).

### scr/: Директория, в которой находятся скрипты для выполнения пайплайна.

sql/create_scheme_dds.sql Скрипт, который создаёт DDS схему.
sql/create_broken_schema.sql Скрипт, который создаёт схему для невалидных данных

### Установка и запуск
Клонирование репозитория:
git clone https://github.com/kfrsta/korus_team4
cd korus_team4
### Запуск сервисов с помощью Docker Compose:
docker-compose up -d
### Доступ к Airflow:
Откройте браузер и перейдите по адресу http://localhost:8080
