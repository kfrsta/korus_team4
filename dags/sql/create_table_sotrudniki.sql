CREATE TABLE IF NOT EXISTS {{ params.schema_name }}.сотрудники_дар (
    user_id int4 PRIMARY KEY,
    активность VARCHAR(50),
    фамилия VARCHAR(25),
    имя VARCHAR(25),
    последняя_авторизация TIMESTAMP,
    должность VARCHAR(50),
    цфо VARCHAR(100),
    подразделения VARCHAR(200)
);