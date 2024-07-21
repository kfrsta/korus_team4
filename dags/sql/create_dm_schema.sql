CREATE SCHEMA IF NOT EXISTS dvyacheslav_dm;

CREATE TABLE IF NOT EXISTS dvyacheslav_dm.сотрудники_дар (
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

CREATE TABLE IF NOT EXISTS dvyacheslav_dm.инструменты (
	"id" int4 PRIMARY KEY,
	"название" character varying
);

CREATE TABLE IF NOT EXISTS dvyacheslav_dm.уровни_знаний (
	"id" int4 PRIMARY KEY,
	"название" character varying
);

CREATE TABLE IF NOT EXISTS dvyacheslav_dm.среды_разработки (
	"id" int4 PRIMARY KEY,
	"название" character varying
);

CREATE TABLE IF NOT EXISTS dvyacheslav_dm.фреймворки (
	"id" int4 PRIMARY KEY,
	"название" character varying
);

CREATE TABLE IF NOT EXISTS dvyacheslav_dm.инструменты_и_уровень_знаний_сотр (
	user_id int4,
	id int8,
	дата text,
	инструменты int8,
	уровень_знаний int8,
	FOREIGN KEY (уровень_знаний) REFERENCES dvyacheslav_dm.уровни_знаний (id),
	FOREIGN KEY (инструменты) REFERENCES dvyacheslav_dm.инструменты (id),
	FOREIGN KEY (user_id) REFERENCES dvyacheslav_dm.сотрудники_дар (user_id)
);

CREATE TABLE IF NOT EXISTS dvyacheslav_dm.среды_разработки_и_уровень_знаний_ (
	user_id int4 NULL,
	id int8 NULL,
	дата text NULL,
	среды_разработки int8 NULL,
	уровень_знаний int8 NULL,
	FOREIGN KEY (среды_разработки) REFERENCES dvyacheslav_dm.среды_разработки (id),
	FOREIGN KEY (уровень_знаний) REFERENCES dvyacheslav_dm.уровни_знаний (id),
	FOREIGN KEY (user_id) REFERENCES dvyacheslav_dm.сотрудники_дар (user_id)
);

CREATE TABLE IF NOT EXISTS dvyacheslav_dm.фреймворки_и_уровень_знаний_сотру (
	user_id int4 NULL,
	id int8 NULL,
	дата text NULL,
	уровень_знаний int8 NULL,
	фреймворки int8 NULL,
	FOREIGN KEY (уровень_знаний) REFERENCES dvyacheslav_dm.уровни_знаний (id),
	FOREIGN KEY (фреймворки) REFERENCES dvyacheslav_dm.фреймворки (id),
	FOREIGN KEY (user_id) REFERENCES dvyacheslav_dm.сотрудники_дар (user_id)
);

CREATE TABLE IF NOT EXISTS dvyacheslav_dm.сертификаты_пользователей (
	user_id int4 NULL,
	id int8 NULL,
	год_сертификата float8 NULL,
	наименование_сертификата text NULL,
	"организация,_выдавшая_сертификат" text NULL,
	FOREIGN KEY (user_id) REFERENCES dvyacheslav_dm.сотрудники_дар(user_id)
);