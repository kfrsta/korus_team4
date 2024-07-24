TRUNCATE TABLE andronov_dm.dim_users;
TRUNCATE TABLE andronov_dm.dim_knowledge_level;
TRUNCATE TABLE andronov_dm.skills;
TRUNCATE TABLE andronov_dm.fact_employee_skills;
TRUNCATE TABLE andronov_dm.fact_employee_sertificate;

INSERT INTO andronov_dm.dim_users (user_id, имя, фамилия, логин, email, дата_рождения, активность, город_проживания, компания, подразделения, цфо, должность, пол)
SELECT 
    sd.id, sd.имя, sd.фамилия, sd.логин, sd.email, sd.дата_рождения, sd.активность, sd.город_проживания, sd.компания, sd.подразделения, sd.цфо, sd.должность, sd.пол
FROM 
    andronov_dds.сотрудники_дар sd;

INSERT INTO andronov_dm.dim_knowledge_level (knowledge_level_id, название)
SELECT id, название
FROM andronov_dds.уровни_знаний;

INSERT INTO andronov_dm.skills (skill_id, название, область)
SELECT DISTINCT id, название, 'фреймворки'
FROM andronov_dds.фреймворки;

INSERT INTO andronov_dm.skills (skill_id, название, область)
SELECT DISTINCT id, название, 'инструменты'
FROM andronov_dds.инструменты;

INSERT INTO andronov_dm.skills (skill_id, название, область)
SELECT DISTINCT id, название, 'среды_разработки'
FROM andronov_dds.среды_разработки;

INSERT INTO andronov_dm.fact_employee_skills (user_id, skill_id, knowledge_level_id, дата)
SELECT 
    s.user_id,
    sk.skill_id,
    kl.id,
    to_date(s.дата, 'DD.MM.YYYY')
FROM 
    andronov_dds.среды_разработки_и_уровень_знаний_ s
JOIN andronov_dm.skills sk ON s.среды_разработки = sk.skill_id AND sk.область = 'среды_разработки'
JOIN andronov_dds.уровни_знаний kl ON s.уровень_знаний = kl.id;

INSERT INTO andronov_dm.fact_employee_skills (user_id, skill_id, knowledge_level_id, дата)
SELECT 
    f.user_id,
    sk.skill_id,
    kl.id,
    to_date(f.дата, 'DD.MM.YYYY')
FROM 
    andronov_dds.фреймворки_и_уровень_знаний_сотру f
JOIN andronov_dm.skills sk ON f.фреймворки = sk.skill_id AND sk.область = 'фреймворки'
JOIN andronov_dds.уровни_знаний kl ON f.уровень_знаний = kl.id;

INSERT INTO andronov_dm.fact_employee_skills (user_id, skill_id, knowledge_level_id, дата)
SELECT 
    i.user_id,
    sk.skill_id,
    kl.id,
    to_date(i.дата, 'DD.MM.YYYY')
FROM 
    andronov_dds.инструменты_и_уровень_знаний_сотр i
JOIN andronov_dm.skills sk ON i.инструменты = sk.skill_id AND sk.область = 'инструменты'
JOIN andronov_dds.уровни_знаний kl ON i.уровень_знаний = kl.id;

INSERT INTO andronov_dm.fact_employee_sertificate (user_id, sert_id, год_сертификата)
SELECT 
    c.user_id,
    c.id,
    c.год_сертификата
FROM 
    andronov_dds.сертификаты_пользователей c;