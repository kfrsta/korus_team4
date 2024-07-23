TRUNCATE TABLE dvyacheslav_dm.fact_employee_skills;

INSERT INTO dvyacheslav_dm.fact_employee_skills (user_id, skill_id, knowledge_level_id, дата)
SELECT user_id, инструменты, уровень_знаний, CAST(дата AS DATE) FROM dvyacheslav_dds.инструменты_и_уровень_знаний_сотр;

INSERT INTO dvyacheslav_dm.fact_employee_skills (user_id, skill_id, knowledge_level_id, дата)
SELECT user_id, среды_разработки, уровень_знаний, CAST(дата AS DATE) FROM dvyacheslav_dds.среды_разработки_и_уровень_знаний_;

INSERT INTO dvyacheslav_dm.fact_employee_skills (user_id, skill_id, knowledge_level_id, дата)
SELECT user_id, фреймворки, уровень_знаний, CAST(дата AS DATE) FROM dvyacheslav_dds.фреймворки_и_уровень_знаний_сотру;

INSERT INTO dvyacheslav_dm.fact_employee_sertificates (user_id, sert_id, год_сертификата)
SELECT user_id, id, CAST(год_сертификата AS INT) FROM dvyacheslav_dds.сертификаты_пользователей;