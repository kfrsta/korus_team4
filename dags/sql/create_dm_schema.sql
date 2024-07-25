CREATE SCHEMA IF NOT EXISTS dvyacheslav_dm;

CREATE TABLE IF NOT EXISTS dvyacheslav_dm.skills (
    skill_id int4 PRIMARY KEY,
    название VARCHAR(100) NOT NULL,
    область VARCHAR(100) NOT NULL
);

CREATE TABLE IF NOT EXISTS dvyacheslav_dm.knowledge_levels (
    knowledge_level_id int4 PRIMARY KEY,
    название VARCHAR(50) NOT NULL
);

CREATE TABLE IF NOT EXISTS dvyacheslav_dm.users (
	user_id int4 PRIMARY KEY,
	дата_рождения text NULL,
	активность text NULL,
	пол text NULL,
	фамилия text NULL,
	имя text NULL,
	последняя_авторизация TIMESTAMP,
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

CREATE TABLE IF NOT EXISTS dvyacheslav_dm.fact_employee_sertificates (
	fact_id SERIAL PRIMARY KEY,
    user_id int4,
    sert_id int4,
    год_сертификата INT,
    FOREIGN KEY (user_id) REFERENCES dvyacheslav_dm.users (user_id),
    CONSTRAINT unique_user_sert UNIQUE (user_id, sert_id)
);
