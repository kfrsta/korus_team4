CREATE SCHEMA IF NOT EXISTS andronov_dds;


CREATE TABLE IF NOT EXISTS andronov_dds.базы_данных (
	id INTEGER PRIMARY KEY,
	название VARCHAR
);


CREATE TABLE IF NOT EXISTS andronov_dds.инструменты (
	id INTEGER PRIMARY KEY,
	название VARCHAR
);


CREATE TABLE IF NOT EXISTS andronov_dds.отрасли (
	id INTEGER PRIMARY KEY,
	название VARCHAR
);


CREATE TABLE IF NOT EXISTS andronov_dds.платформы (
	id INTEGER PRIMARY KEY,
	название VARCHAR
);


CREATE TABLE IF NOT EXISTS andronov_dds.предметная_область (
	id INTEGER PRIMARY KEY,
	название VARCHAR
);


CREATE TABLE IF NOT EXISTS andronov_dds.сотрудники_дар (
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


CREATE TABLE IF NOT EXISTS andronov_dds.среды_разработки (
	id INTEGER PRIMARY KEY,
	название VARCHAR
);

CREATE TABLE IF NOT EXISTS andronov_dds.технологии (
	id INTEGER PRIMARY KEY,
	название VARCHAR
);


CREATE TABLE IF NOT EXISTS andronov_dds.типы_систем (
	id INTEGER PRIMARY KEY,
	название VARCHAR
);


CREATE TABLE IF NOT EXISTS andronov_dds.уровень_образования (
	id INTEGER PRIMARY KEY,
	название VARCHAR
);



CREATE TABLE IF NOT EXISTS andronov_dds.уровни_владения_ин (
	id INTEGER PRIMARY KEY,
	название VARCHAR
);


CREATE TABLE IF NOT EXISTS andronov_dds.уровни_знаний (
	id INTEGER PRIMARY KEY,
	название VARCHAR
);


CREATE TABLE IF NOT EXISTS andronov_dds.уровни_знаний_в_отрасли (
	id INTEGER PRIMARY KEY,
	название VARCHAR
);


CREATE TABLE IF NOT EXISTS andronov_dds.уровни_знаний_в_предметной_област (
	id INTEGER PRIMARY KEY,
	название VARCHAR
);


CREATE TABLE IF NOT EXISTS andronov_dds.фреймворки (
	id INTEGER PRIMARY KEY,
	название VARCHAR
);


CREATE TABLE IF NOT EXISTS andronov_dds.языки (
	id INTEGER PRIMARY KEY,
	название VARCHAR
);


CREATE TABLE IF NOT EXISTS andronov_dds.языки_программирования (
	id INTEGER PRIMARY KEY,
	название VARCHAR
);


CREATE TABLE IF NOT EXISTS andronov_dds.базы_данных_и_уровень_знаний_сотру (
	user_id INTEGER REFERENCES andronov_dds.сотрудники_дар(id),
	id INTEGER PRIMARY KEY,
	базы_данных INTEGER REFERENCES andronov_dds.базы_данных(id),
	дата VARCHAR,
	уровень_знаний INTEGER REFERENCES andronov_dds.уровни_знаний(id)
);


CREATE TABLE IF NOT EXISTS andronov_dds.инструменты_и_уровень_знаний_сотр (
	user_id INTEGER REFERENCES andronov_dds.сотрудники_дар(id),
	id INTEGER PRIMARY KEY,
	дата VARCHAR,
	инструменты INTEGER REFERENCES andronov_dds.инструменты(id),
	уровень_знаний INTEGER REFERENCES andronov_dds.уровни_знаний(id) 
);


CREATE TABLE IF NOT EXISTS andronov_dds.образование_пользователей (
	user_id INTEGER REFERENCES andronov_dds.сотрудники_дар(id),
	id INTEGER PRIMARY KEY,
	уровень_образование INTEGER REFERENCES andronov_dds.уровень_образования(id),
	название_учебного_заведения VARCHAR,
	фиктивное_название VARCHAR,
	факультет_кафедра VARCHAR,
	специальность VARCHAR,
	квалификация VARCHAR,
	год_окончания INTEGER
);


CREATE TABLE IF NOT EXISTS andronov_dds.опыт_сотрудника_в_отраслях (
	user_id INTEGER REFERENCES andronov_dds.сотрудники_дар(id),
	id INTEGER PRIMARY KEY,
	дата VARCHAR,
	отрасли INTEGER REFERENCES andronov_dds.отрасли(id),
	уровень_знаний_в_отрасли INTEGER REFERENCES andronov_dds.уровни_знаний_в_отрасли(id)
);

CREATE TABLE IF NOT EXISTS andronov_dds.опыт_сотрудника_в_предметных_обла (
	user_id INTEGER REFERENCES andronov_dds.сотрудники_дар(id),
	id INTEGER PRIMARY KEY,
	дата VARCHAR,
	предментые_области INTEGER REFERENCES andronov_dds.предметная_область(id),
	уровень_знаний_в_предметной_облас INTEGER REFERENCES andronov_dds.уровни_знаний_в_предметной_област(id)
);


CREATE TABLE IF NOT EXISTS andronov_dds.платформы_и_уровень_знаний_сотруд (
	user_id INTEGER REFERENCES andronov_dds.сотрудники_дар(id),
	id INTEGER PRIMARY KEY,
	дата VARCHAR,
	платформы INTEGER REFERENCES andronov_dds.платформы(id),
	уровень_знаний INTEGER REFERENCES andronov_dds.уровни_знаний(id)
);


CREATE TABLE IF NOT EXISTS andronov_dds.сертификаты_пользователей (
	user_id INTEGER REFERENCES andronov_dds.сотрудники_дар(id),
	id INTEGER PRIMARY KEY,
	год_сертификата INTEGER,
	наименование_сертификата VARCHAR,
	организация_выдавшая_сертификат VARCHAR
);


CREATE TABLE IF NOT EXISTS andronov_dds.среды_разработки_и_уровень_знаний_ (
	user_id INTEGER REFERENCES andronov_dds.сотрудники_дар(id),
	id INTEGER PRIMARY KEY,
	дата VARCHAR,
	среды_разработки INTEGER REFERENCES andronov_dds.среды_разработки(id),
	уровень_знаний INTEGER REFERENCES andronov_dds.уровни_знаний(id)
);


CREATE TABLE IF NOT EXISTS andronov_dds.технологии_и_уровень_знаний_сотру (
	user_id INTEGER REFERENCES andronov_dds.сотрудники_дар(id),
	id INTEGER PRIMARY KEY,
	дата VARCHAR,
	технологии INTEGER REFERENCES andronov_dds.технологии(id),
	уровень_знаний INTEGER REFERENCES andronov_dds.уровни_знаний(id)
);


CREATE TABLE IF NOT EXISTS andronov_dds.типы_систем_и_уровень_знаний_сотру (
	user_id INTEGER REFERENCES andronov_dds.сотрудники_дар(id),
	id INTEGER PRIMARY KEY,
	дата VARCHAR,
	типы_систем INTEGER REFERENCES andronov_dds.типы_систем(id),
	уровень_знаний INTEGER REFERENCES andronov_dds.уровни_знаний(id)
);


CREATE TABLE IF NOT EXISTS andronov_dds.фреймворки_и_уровень_знаний_сотру (
	user_id INTEGER REFERENCES andronov_dds.сотрудники_дар(id),
	id INTEGER PRIMARY KEY,
	дата VARCHAR,
	уровень_знаний INTEGER REFERENCES andronov_dds.уровни_знаний(id),
	фреймворки INTEGER REFERENCES andronov_dds.фреймворки(id)
);


CREATE TABLE IF NOT EXISTS andronov_dds.языки_пользователей (
	user_id INTEGER REFERENCES andronov_dds.сотрудники_дар(id),
	id INTEGER PRIMARY KEY,
	язык INTEGER REFERENCES andronov_dds.языки(id),
	уровень_знаний_ин_языка INTEGER REFERENCES andronov_dds.уровни_владения_ин(id)
);


CREATE TABLE IF NOT EXISTS andronov_dds.языки_программирования_и_уровень (
	user_id INTEGER REFERENCES andronov_dds.сотрудники_дар(id),
	id INTEGER PRIMARY KEY,
	дата VARCHAR,
	уровень_знаний INTEGER REFERENCES andronov_dds.уровни_знаний(id),
	языки_программирования INTEGER REFERENCES andronov_dds.языки_программирования(id)
);