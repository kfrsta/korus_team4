CREATE SCHEMA IF NOT EXISTS andronov_dds;

CREATE TABLE
    IF NOT EXISTS andronov_dds."инструменты_и_уровень_знаний_сотр" (
        id INTEGER PRIMARY KEY,
        дата VARCHAR,
        инструменты VARCHAR,
        уровень_знаний VARCHAR
    );

CREATE TABLE
    IF NOT EXISTS andronov_dds."базы_данных_и_уровень_знаний_сотру" (
        id INTEGER PRIMARY KEY,
        базы_данных VARCHAR,
        дата VARCHAR,
        уровень_знаний VARCHAR
    );

CREATE TABLE
    IF NOT EXISTS andronov_dds."опыт_сотрудника_в_отраслях" (
        id INTEGER PRIMARY KEY,
        дата VARCHAR,
        отрасли VARCHAR,
        уровень_знаний_в_отрасли VARCHAR
    );

CREATE TABLE
    IF NOT EXISTS andronov_dds."образование_пользователей" (
        id INTEGER PRIMARY KEY,
        уровень_образования VARCHAR,
        название_учебного_заведения VARCHAR,
        фиктивное_название VARCHAR,
        факультет_кафедра VARCHAR,
        специальность VARCHAR,
        квалификация VARCHAR,
        год_окончания VARCHAR
    );

CREATE TABLE
    IF NOT EXISTS andronov_dds."языки_пользователей" (
        id INTEGER PRIMARY KEY,
        язык VARCHAR,
        уровень_знаний_ин_языка VARCHAR
    );

CREATE TABLE
    IF NOT EXISTS andronov_dds."опыт_сотрудника_в_предметных_обла" (
        id INTEGER PRIMARY KEY,
        дата VARCHAR,
        предметные_области VARCHAR,
        уровни_знаний_в_предметной_обла VARCHAR
    );

CREATE TABLE
    IF NOT EXISTS andronov_dds."типы_систем_и_уровень_знаний_сотру" (
        id INTEGER PRIMARY KEY,
        дата VARCHAR,
        типы_систем VARCHAR,
        уровень_знаний VARCHAR
    );

CREATE TABLE
    IF NOT EXISTS andronov_dds."среды_разработки_и_уровень_знаний" (
        id INTEGER PRIMARY KEY,
        дата VARCHAR,
        среды_разработки VARCHAR,
        уровень_знаний VARCHAR
    );

CREATE TABLE
    IF NOT EXISTS andronov_dds."технологии_и_уровень_знаний_сотру" (
        id INTEGER PRIMARY KEY,
        дата VARCHAR,
        технологии VARCHAR,
        уровень_знаний VARCHAR
    );

CREATE TABLE
    IF NOT EXISTS andronov_dds."языки_программирования_и_уровень" (
        id INTEGER PRIMARY KEY,
        дата VARCHAR,
        уровень_знаний VARCHAR,
        языки_программирования VARCHAR
    );

CREATE TABLE
    IF NOT EXISTS andronov_dds."фреймворки_и_уровень_знаний_сотру" (
        id INTEGER PRIMARY KEY,
        дата VARCHAR,
        уровень_знаний VARCHAR,
        фреймворки VARCHAR
    );

CREATE TABLE
    IF NOT EXISTS andronov_dds."платформы_и_уровень_знаний_сотруд" (
        id INTEGER PRIMARY KEY,
        дата VARCHAR,
        платформы VARCHAR,
        уровень_знаний VARCHAR
    );

CREATE TABLE
    IF NOT EXISTS andronov_dds."сотрудники_дар" (
        id INTEGER PRIMARY KEY,
        дата_рождения VARCHAR,
        активность VARCHAR,
        пол VARCHAR,
        фамилия VARCHAR,
        имя VARCHAR,
        последняя_авторизация VARCHAR,
        должность VARCHAR,
        цфо VARCHAR,
        дата_регистрации VARCHAR,
        дата_изменения VARCHAR,
        подразделения VARCHAR,
        email VARCHAR,
        логин VARCHAR,
        компания VARCHAR,
        город_проживания VARCHAR
    );

CREATE TABLE
    IF NOT EXISTS andronov_dds."сертификаты_пользователей" (
        user_id VARCHAR,
        id INTEGER PRIMARY KEY,
        год_сертификата VARCHAR,
        наименование_сертификата VARCHAR,
        организация_выдавшая_сертификат VARCHAR
    );

CREATE TABLE
    IF NOT EXISTS andronov_dds."резюмедар" (
        user_id INTEGER REFERENCES andronov_dds."сотрудники_дар" (id),
        resumeid INTEGER PRIMARY KEY
    );

CREATE TABLE
    IF NOT EXISTS andronov_dds."резюме_образование" (
        resumeid INTEGER,
        infoid INTEGER,
        PRIMARY KEY (resumeid, infoid),
        FOREIGN KEY (resumeid) REFERENCES andronov_dds."резюмедар" (resumeid),
        FOREIGN KEY (infoid) REFERENCES andronov_dds."образование_пользователей" (id)
    );

CREATE TABLE
    IF NOT EXISTS andronov_dds."резюме_сертификаты" (
        resumeid INTEGER,
        infoid INTEGER,
        PRIMARY KEY (resumeid, infoid),
        FOREIGN KEY (resumeid) REFERENCES andronov_dds."резюмедар" (resumeid),
        FOREIGN KEY (infoid) REFERENCES andronov_dds."сертификаты_пользователей" (id)
    );

CREATE TABLE
    IF NOT EXISTS andronov_dds."резюме_языки" (
        resumeid INTEGER,
        infoid INTEGER,
        PRIMARY KEY (resumeid, infoid),
        FOREIGN KEY (resumeid) REFERENCES andronov_dds."резюмедар" (resumeid),
        FOREIGN KEY (infoid) REFERENCES andronov_dds."языки_пользователей" (id)
    );

CREATE TABLE
    IF NOT EXISTS andronov_dds."резюме_базыданных" (
        resumeid INTEGER,
        infoid INTEGER,
        PRIMARY KEY (resumeid, infoid),
        FOREIGN KEY (resumeid) REFERENCES andronov_dds."резюмедар" (resumeid),
        FOREIGN KEY (infoid) REFERENCES andronov_dds."базы_данных_и_уровень_знаний_сотру" (id)
    );

CREATE TABLE
    IF NOT EXISTS andronov_dds."резюме_инструменты" (
        resumeid INTEGER,
        infoid INTEGER,
        PRIMARY KEY (resumeid, infoid),
        FOREIGN KEY (resumeid) REFERENCES andronov_dds."резюмедар" (resumeid),
        FOREIGN KEY (infoid) REFERENCES andronov_dds."инструменты_и_уровень_знаний_сотр" (id)
    );

CREATE TABLE
    IF NOT EXISTS andronov_dds."резюме_опыт_в_отраслях" (
        resumeid INTEGER,
        infoid INTEGER,
        PRIMARY KEY (resumeid, infoid),
        FOREIGN KEY (resumeid) REFERENCES andronov_dds."резюмедар" (resumeid),
        FOREIGN KEY (infoid) REFERENCES andronov_dds."опыт_сотрудника_в_отраслях" (id)
    );

CREATE TABLE
    IF NOT EXISTS andronov_dds."резюме_платформы" (
        resumeid INTEGER,
        infoid INTEGER,
        PRIMARY KEY (resumeid, infoid),
        FOREIGN KEY (resumeid) REFERENCES andronov_dds."резюмедар" (resumeid),
        FOREIGN KEY (infoid) REFERENCES andronov_dds."платформы_и_уровень_знаний_сотруд" (id)
    );

CREATE TABLE
    IF NOT EXISTS andronov_dds."резюме_опыт_в_предметных_обла" (
        resumeid INTEGER,
        infoid INTEGER,
        PRIMARY KEY (resumeid, infoid),
        FOREIGN KEY (resumeid) REFERENCES andronov_dds."резюмедар" (resumeid),
        FOREIGN KEY (infoid) REFERENCES andronov_dds."опыт_сотрудника_в_предметных_обла" (id)
    );

CREATE TABLE
    IF NOT EXISTS andronov_dds."резюме_среды_разработки" (
        resumeid INTEGER,
        infoid INTEGER,
        PRIMARY KEY (resumeid, infoid),
        FOREIGN KEY (resumeid) REFERENCES andronov_dds."резюмедар" (resumeid),
        FOREIGN KEY (infoid) REFERENCES andronov_dds."среды_разработки_и_уровень_знаний" (id)
    );

CREATE TABLE
    IF NOT EXISTS andronov_dds."резюме_типы_систем" (
        resumeid INTEGER,
        infoid INTEGER,
        PRIMARY KEY (resumeid, infoid),
        FOREIGN KEY (resumeid) REFERENCES andronov_dds."резюмедар" (resumeid),
        FOREIGN KEY (infoid) REFERENCES andronov_dds."типы_систем_и_уровень_знаний_сотру" (id)
    );

CREATE TABLE
    IF NOT EXISTS andronov_dds."резюме_фреймворки" (
        resumeid INTEGER,
        infoid INTEGER,
        PRIMARY KEY (resumeid, infoid),
        FOREIGN KEY (resumeid) REFERENCES andronov_dds."резюмедар" (resumeid),
        FOREIGN KEY (infoid) REFERENCES andronov_dds."фреймворки_и_уровень_знаний_сотру" (id)
    );

CREATE TABLE
    IF NOT EXISTS andronov_dds."резюме_языки_программирования" (
        resumeid INTEGER,
        infoid INTEGER,
        PRIMARY KEY (resumeid, infoid),
        FOREIGN KEY (resumeid) REFERENCES andronov_dds."резюмедар" (resumeid),
        FOREIGN KEY (infoid) REFERENCES andronov_dds."языки_программирования_и_уровень" (id)
    );

CREATE TABLE
    IF NOT EXISTS andronov_dds."резюме_технологии" (
        resumeid INTEGER,
        infoid INTEGER,
        PRIMARY KEY (resumeid, infoid),
        FOREIGN KEY (resumeid) REFERENCES andronov_dds."резюмедар" (resumeid),
        FOREIGN KEY (infoid) REFERENCES andronov_dds."технологии_и_уровень_знаний_сотру" (id)
    );