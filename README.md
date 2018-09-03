### Модули test_download и test_preprocessing для обработки файлов

Для запуска необходимо:
* Создать пользователя:

    `sudo -u postgres createuser <user>`


* Создать базу данных №1 и базу данных №2 (пример - на localhost):

    `sudo -u postgres createdb -O <user> <dbname1>`

    `sudo -u postgres createdb -O <user> <dbname2>`


* Создать необходимые таблицы:

    `psql -h <host> -U <user> -d <dbname1> -f init1.sql`

    `psql -h <host> -U <user> -d <dbname2> -f init2.sql`


* В файле `usage_example.py`:
    * указать необходимые параметры в конфигах `dbparams` и `db2params`
    * указать имя сервера и файлов для проверки


* Для проверки на поедоставленных файлах в директории проекта запустить сервер:

    `python -m http.server`


* Выполнить:

    `python3 usage_example`


Для тестирования модулей были созданы файлы из предоставленных примеров.
Файлы `input-2017-02-01-bad.json.gz` и `reward-2017-02-01-bad.csv.gz` повторяют эти примеры.
Файлы `input-2017-02-01-ok.json.gz`и `reward-2017-02-01-ok.csv.gz` - корректированные версии тех же файлов с удалёнными невалидными строками.
