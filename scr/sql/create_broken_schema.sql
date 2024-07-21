CREATE SCHEMA IF NOT EXISTS andronov_broken;


CREATE TABLE IF NOT EXISTS andronov_broken.базы_данных (
	id INTEGER,
	название VARCHAR
);


CREATE TABLE IF NOT EXISTS andronov_broken.инструменты (
	id INTEGER,
	название VARCHAR
);


CREATE TABLE IF NOT EXISTS andronov_broken.отрасли (
	id INTEGER,
	название VARCHAR
);


CREATE TABLE IF NOT EXISTS andronov_broken.платформы (
	id INTEGER,
	название VARCHAR
);


CREATE TABLE IF NOT EXISTS andronov_broken.предметная_область (
	id INTEGER,
	название VARCHAR
);


CREATE TABLE IF NOT EXISTS andronov_broken.сотрудники_дар (
	id INTEGER,
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


CREATE TABLE IF NOT EXISTS andronov_broken.среды_разработки (
	id INTEGER,
	название VARCHAR
);

CREATE TABLE IF NOT EXISTS andronov_broken.технологии (
	id INTEGER,
	название VARCHAR
);


CREATE TABLE IF NOT EXISTS andronov_broken.типы_систем (
	id INTEGER,
	название VARCHAR
);


CREATE TABLE IF NOT EXISTS andronov_broken.уровень_образования (
	id INTEGER,
	название VARCHAR
);



CREATE TABLE IF NOT EXISTS andronov_broken.уровни_владения_ин (
	id INTEGER,
	название VARCHAR
);


CREATE TABLE IF NOT EXISTS andronov_broken.уровни_знаний (
	id INTEGER,
	название VARCHAR
);


CREATE TABLE IF NOT EXISTS andronov_broken.уровни_знаний_в_отрасли (
	id INTEGER,
	название VARCHAR
);


CREATE TABLE IF NOT EXISTS andronov_broken.уровни_знаний_в_предметной_област (
	id INTEGER,
	название VARCHAR
);


CREATE TABLE IF NOT EXISTS andronov_broken.фреймворки (
	id INTEGER,
	название VARCHAR
);


CREATE TABLE IF NOT EXISTS andronov_broken.языки (
	id INTEGER,
	название VARCHAR
);


CREATE TABLE IF NOT EXISTS andronov_broken.языки_программирования (
	id INTEGER,
	название VARCHAR
);


CREATE TABLE IF NOT EXISTS andronov_broken.базы_данных_и_уровень_знаний_сотру (
	user_id INTEGER,
	id INTEGER,
	базы_данных VARCHAR,
	дата VARCHAR,
	уровень_знаний VARCHAR
);


CREATE TABLE IF NOT EXISTS andronov_broken.инструменты_и_уровень_знаний_сотр (
	user_id INTEGER,
	id INTEGER,
	дата VARCHAR,
	инструменты VARCHAR,
	уровень_знаний VARCHAR 
);


CREATE TABLE IF NOT EXISTS andronov_broken.образование_пользователей (
	user_id INTEGER,
	id INTEGER,
	уровень_образование VARCHAR,
	название_учебного_заведения VARCHAR,
	фиктивное_название VARCHAR,
	факультет_кафедра VARCHAR,
	специальность VARCHAR,
	квалификация VARCHAR,
	год_окончания INTEGER
);


CREATE TABLE IF NOT EXISTS andronov_broken.опыт_сотрудника_в_отраслях (
	user_id INTEGER,
	id INTEGER,
	дата VARCHAR,
	отрасли VARCHAR,
	уровень_знаний_в_отрасли VARCHAR
);

CREATE TABLE IF NOT EXISTS andronov_broken.опыт_сотрудника_в_предметных_обла (
	user_id INTEGER,
	id INTEGER,
	дата VARCHAR,
	предментые_области VARCHAR,
	уровень_знаний_в_предметной_облас VARCHAR
);


CREATE TABLE IF NOT EXISTS andronov_broken.платформы_и_уровень_знаний_сотруд (
	user_id INTEGER,
	id INTEGER,
	дата VARCHAR,
	платформы VARCHAR,
	уровень_знаний VARCHAR
);


CREATE TABLE IF NOT EXISTS andronov_broken.сертификаты_пользователей (
	user_id INTEGER,
	id INTEGER,
	год_сертификата INTEGER,
	наименование_сертификата VARCHAR,
	организация_выдавшая_сертификат VARCHAR
);


CREATE TABLE IF NOT EXISTS andronov_broken.среды_разработки_и_уровень_знаний_ (
	user_id INTEGER,
	id INTEGER,
	дата VARCHAR,
	среды_разработки VARCHAR,
	уровень_знаний VARCHAR
);


CREATE TABLE IF NOT EXISTS andronov_broken.технологии_и_уровень_знаний_сотру (
	user_id INTEGER,
	id INTEGER,
	дата VARCHAR,
	технологии VARCHAR,
	уровень_знаний VARCHAR
);


CREATE TABLE IF NOT EXISTS andronov_broken.типы_систем_и_уровень_знаний_сотру (
	user_id INTEGER,
	id INTEGER,
	дата VARCHAR,
	типы_систем VARCHAR,
	уровень_знаний VARCHAR
);


CREATE TABLE IF NOT EXISTS andronov_broken.фреймворки_и_уровень_знаний_сотру (
	user_id INTEGER,
	id INTEGER,
	дата VARCHAR,
	уровень_знаний VARCHAR,
	фреймворки VARCHAR
);


CREATE TABLE IF NOT EXISTS andronov_broken.языки_пользователей (
	user_id INTEGER,
	id INTEGER,
	язык VARCHAR,
	уровень_знаний_ин_языка VARCHAR
);


CREATE TABLE IF NOT EXISTS andronov_broken.языки_программирования_и_уровень (
	user_id INTEGER,
	id INTEGER,
	дата VARCHAR,
	уровень_знаний VARCHAR,
	языки_программирования VARCHAR
);