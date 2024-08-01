TRUNCATE TABLE dvyacheslav_dm.skills CASCADE;
-- Перенос данных из таблицы инструменты
INSERT INTO dvyacheslav_dm.skills (skill_id, название, область)
SELECT id, название, 'инструменты' AS область FROM dvyacheslav_dds.инструменты;

-- Перенос данных из таблицы среды_разработки
INSERT INTO dvyacheslav_dm.skills (skill_id, название, область)
SELECT id, название, 'среды_разработки' AS область FROM dvyacheslav_dds.среды_разработки;

-- Перенос данных из таблицы фреймворки
INSERT INTO dvyacheslav_dm.skills (skill_id, название, область)
SELECT id, название, 'фреймворки' AS область FROM dvyacheslav_dds.фреймворки;

TRUNCATE TABLE dvyacheslav_dm.fact_employee_skills;

INSERT INTO dvyacheslav_dm.fact_employee_skills (user_id, skill_id, knowledge_level_id, дата)
SELECT user_id, инструменты, уровень_знаний, дата FROM dvyacheslav_dds.инструменты_и_уровень_знаний_сотр;

INSERT INTO dvyacheslav_dm.fact_employee_skills (user_id, skill_id, knowledge_level_id, дата)
SELECT user_id, среды_разработки, уровень_знаний, дата FROM dvyacheslav_dds.среды_разработки_и_уровень_знаний_;

INSERT INTO dvyacheslav_dm.fact_employee_skills (user_id, skill_id, knowledge_level_id, дата)
SELECT user_id, фреймворки, уровень_знаний, дата FROM dvyacheslav_dds.фреймворки_и_уровень_знаний_сотру;

INSERT INTO dvyacheslav_dm.fact_employee_sertificates (user_id, sert_id, год_сертификата)
SELECT user_id, id, год_сертификата FROM dvyacheslav_dds.сертификаты_пользователей;