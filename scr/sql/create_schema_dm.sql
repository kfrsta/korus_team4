CREATE SCHEMA IF NOT EXISTS andronov_dm;

CREATE TABLE IF NOT EXISTS andronov_dm.dim_users (
    user_id INT PRIMARY KEY,
    имя VARCHAR,
    фамилия VARCHAR,
    логин VARCHAR,
    email VARCHAR,
    дата_рождения VARCHAR,
    активность VARCHAR,
    город_проживания VARCHAR,
    компания VARCHAR,
    подразделения VARCHAR,
    цфо VARCHAR,
    должность VARCHAR,
    пол VARCHAR
);

CREATE TABLE IF NOT EXISTS andronov_dm.skills(
    skill_id INT PRIMARY KEY,
    название VARCHAR,
    область VARCHAR
);

CREATE TABLE IF NOT EXISTS andronov_dm.dim_knowledge_level (
    knowledge_level_id INT PRIMARY KEY,
    название VARCHAR
);

CREATE TABLE IF NOT EXISTS andronov_dm.fact_employee_sertificate (
    fact_id SERIAL PRIMARY KEY,
    user_id INT,
    sert_id INT,
    год_сертификата INT,
    FOREIGN KEY (user_id) REFERENCES andronov_dm.dim_users(user_id)
);

CREATE TABLE IF NOT EXISTS andronov_dm.fact_employee_skills (
    fact_id SERIAL PRIMARY KEY,
    user_id INT,
    skill_id INT,
    knowledge_level_id INT,
    дата DATE,
    FOREIGN KEY (user_id) REFERENCES andronov_dm.dim_users(user_id),
    FOREIGN KEY (skill_id) REFERENCES andronov_dm.skills(skill_id),
    FOREIGN KEY (knowledge_level_id) REFERENCES andronov_dm.dim_knowledge_level(knowledge_level_id)
);