CREATE SCHEMA IF NOT EXISTS dvyacheslav_dds;

CREATE TABLE IF NOT EXISTS dvyacheslav_dds.сотрудники_дар (
    user_id int4 PRIMARY KEY,
    дата_рождения text,
    активность text,
    пол text,
    фамилия text,
    имя text,
    последняя_авторизация text,
    должность text,
    цфо text,
    дата_регистрации text,
    дата_изменения text,
    подразделения text,
    "e-mail" text,
    логин text,
    компания text,
    город_проживания text
);
