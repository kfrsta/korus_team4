CREATE SCHEMA IF NOT EXISTS dvyacheslav_dm;

CREATE TABLE IF NOT EXISTS dvyacheslav_dm.skills (
    skill_id int4 PRIMARY KEY,
    название VARCHAR NOT NULL
);

TRUNCATE TABLE dvyacheslav_dm.skills CASCADE;
-- Перенос данных из таблицы инструменты
INSERT INTO dvyacheslav_dm.skills (skill_id, название)
SELECT id, название FROM dvyacheslav_dds.инструменты;

-- Перенос данных из таблицы среды_разработки
INSERT INTO dvyacheslav_dm.skills (skill_id, название)
SELECT id, название FROM dvyacheslav_dds.среды_разработки;

-- Перенос данных из таблицы фреймворки
INSERT INTO dvyacheslav_dm.skills (skill_id, название)
SELECT id, название FROM dvyacheslav_dds.фреймворки;

CREATE TABLE IF NOT EXISTS dvyacheslav_dm.knowledge_levels (
    knowledge_level_id int4 PRIMARY KEY,
    название VARCHAR NOT NULL
);

CREATE TABLE IF NOT EXISTS dvyacheslav_dm.users (
	user_id int4 PRIMARY KEY,
	дата_рождения text NULL,
	активность text NULL,
	пол text NULL,
	фамилия text NULL,
	имя text NULL,
	последняя_авторизация text NULL,
	должность text NULL,
	цфо text NULL,
	дата_регистрации text NULL,
	дата_изменения text NULL,
	подразделения text NULL,
	"e-mail" text NULL,
	логин text NULL,
	компания text NULL,
	город_проживания text NULL
);

CREATE TABLE IF NOT EXISTS dvyacheslav_dm.fact_employee_skills (
	fact_id SERIAL PRIMARY KEY,
    user_id int4,
    skill_id int4,
    knowledge_level_id int4,
    дата DATE,
    FOREIGN KEY (skill_id) REFERENCES dvyacheslav_dm.skills (skill_id),
    FOREIGN KEY (knowledge_level_id) REFERENCES dvyacheslav_dm.knowledge_levels (knowledge_level_id),
    FOREIGN KEY (user_id) REFERENCES dvyacheslav_dm.users (user_id)
);

CREATE TABLE IF NOT EXISTS dvyacheslav_dm.sertificates (
    sert_id int4 PRIMARY KEY,
    наименование_сертификата VARCHAR NOT NULL,
    организация_выдавшая_сертификат VARCHAR NULL
);

CREATE TABLE IF NOT EXISTS dvyacheslav_dm.fact_employee_sertificates (
	fact_id SERIAL PRIMARY KEY,
    user_id int4,
    sert_id int4,
    год_сертификата INT,
    FOREIGN KEY (sert_id) REFERENCES dvyacheslav_dm.sertificates (sert_id),
    FOREIGN KEY (user_id) REFERENCES dvyacheslav_dm.users (user_id)
);

--FOREIGN KEY (уровень_знаний) REFERENCES dvyacheslav_dm.уровни_знаний (id),
--FOREIGN KEY (инструменты) REFERENCES dvyacheslav_dm.инструменты (id),
--FOREIGN KEY (user_id) REFERENCES dvyacheslav_dm.сотрудники_дар (user_id)
