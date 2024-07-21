CREATE TABLE IF NOT EXISTS dvyacheslav_dm.справочник (
    id INT PRIMARY KEY,
    NOVICE_SCORE_FR NUMERIC (5, 2),
    JUNIOR_SCORE_FR NUMERIC (5, 2),
    MIDDLE_SCORE_FR NUMERIC (5, 2),
    SENIOR_SCORE_FR NUMERIC (5, 2),
    EXPERT_SCORE_FR NUMERIC (5, 2),
    PROJECT_SCORE_FR NUMERIC (5, 2),
    NOVICE_SCORE_IN NUMERIC (5, 2),
    JUNIOR_SCORE_IN NUMERIC (5, 2),
    MIDDLE_SCORE_IN NUMERIC (5, 2),
    SENIOR_SCORE_IN NUMERIC (5, 2),
    EXPERT_SCORE_IN NUMERIC (5, 2),
    PROJECT_SCORE_IN NUMERIC (5, 2),
    NOVICE_SCORE_DEV NUMERIC (5, 2),
    JUNIOR_SCORE_DEV NUMERIC (5, 2),
    MIDDLE_SCORE_DEV NUMERIC (5, 2),
    SENIOR_SCORE_DEV NUMERIC (5, 2),
    EXPERT_SCORE_DEV NUMERIC (5, 2),
    PROJECT_SCORE_DEV NUMERIC (5, 2),
    QUANTITY_CERTIFICATE NUMERIC (5, 2),
    JUNIOR_SCORE NUMERIC (5, 2),
    MIDDLE_SCORE NUMERIC (5, 2),
    SENIOR_SCORE NUMERIC (5, 2),
    NOVICE_COUNT NUMERIC (5, 2),
    JUNIOR_COUNT NUMERIC (5, 2),
    MIDDLE_COUNT NUMERIC (5, 2),
    SENIOR_COUNT NUMERIC (5, 2),
    EXPERT_COUNT NUMERIC (5, 2)
);

TRUNCATE TABLE dvyacheslav_dm.справочник;

INSERT INTO dvyacheslav_dm.справочник (id, novice_score_fr, junior_score_fr, middle_score_fr, senior_score_fr, expert_score_fr, project_score_fr, novice_score_in, junior_score_in, middle_score_in, senior_score_in, expert_score_in, project_score_in, novice_score_dev, junior_score_dev, middle_score_dev, senior_score_dev, expert_score_dev, project_score_dev, quantity_certificate)
VALUES (1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0);

--Расчет фреймворков

WITH junior_employees AS (
    SELECT DISTINCT s.user_id
    FROM dvyacheslav_dm.сотрудники_дар s
    JOIN dvyacheslav_dm.фреймворки_и_уровень_знаний_сотру fu ON s.user_id = fu.user_id
    JOIN dvyacheslav_dm.уровни_знаний uz ON fu.уровень_знаний = uz.id
    WHERE uz.название = 'Novice'
),
all_framework_employees AS (
    SELECT DISTINCT s.user_id
    FROM dvyacheslav_dm.сотрудники_дар s
    JOIN dvyacheslav_dm.фреймворки_и_уровень_знаний_сотру fu ON s.user_id = fu.user_id
)
UPDATE dvyacheslav_dm.справочник
SET NOVICE_SCORE_FR = (
    SELECT (COUNT(*) * 100.0 / (SELECT COUNT(*) FROM all_framework_employees))
    FROM junior_employees
)
WHERE id = 1;

WITH junior_employees AS (
    SELECT DISTINCT s.user_id
    FROM dvyacheslav_dm.сотрудники_дар s
    JOIN dvyacheslav_dm.фреймворки_и_уровень_знаний_сотру fu ON s.user_id = fu.user_id
    JOIN dvyacheslav_dm.уровни_знаний uz ON fu.уровень_знаний = uz.id
    WHERE uz.название = 'Junior'
),
all_framework_employees AS (
    SELECT DISTINCT s.user_id
    FROM dvyacheslav_dm.сотрудники_дар s
    JOIN dvyacheslav_dm.фреймворки_и_уровень_знаний_сотру fu ON s.user_id = fu.user_id
)
UPDATE dvyacheslav_dm.справочник
SET JUNIOR_SCORE_FR = (
    SELECT (COUNT(*) * 100.0 / (SELECT COUNT(*) FROM all_framework_employees))
    FROM junior_employees
)
WHERE id = 1;

WITH junior_employees AS (
    SELECT DISTINCT s.user_id
    FROM dvyacheslav_dm.сотрудники_дар s
    JOIN dvyacheslav_dm.фреймворки_и_уровень_знаний_сотру fu ON s.user_id = fu.user_id
    JOIN dvyacheslav_dm.уровни_знаний uz ON fu.уровень_знаний = uz.id
    WHERE uz.название = 'Middle'
),
all_framework_employees AS (
    SELECT DISTINCT s.user_id
    FROM dvyacheslav_dm.сотрудники_дар s
    JOIN dvyacheslav_dm.фреймворки_и_уровень_знаний_сотру fu ON s.user_id = fu.user_id
)
UPDATE dvyacheslav_dm.справочник
SET MIDDLE_SCORE_FR = (
    SELECT (COUNT(*) * 100.0 / (SELECT COUNT(*) FROM all_framework_employees))
    FROM junior_employees
)
WHERE id = 1;

WITH junior_employees AS (
    SELECT DISTINCT s.user_id
    FROM dvyacheslav_dm.сотрудники_дар s
    JOIN dvyacheslav_dm.фреймворки_и_уровень_знаний_сотру fu ON s.user_id = fu.user_id
    JOIN dvyacheslav_dm.уровни_знаний uz ON fu.уровень_знаний = uz.id
    WHERE uz.название = 'Senior'
),
all_framework_employees AS (
    SELECT DISTINCT s.user_id
    FROM dvyacheslav_dm.сотрудники_дар s
    JOIN dvyacheslav_dm.фреймворки_и_уровень_знаний_сотру fu ON s.user_id = fu.user_id
)
UPDATE dvyacheslav_dm.справочник
SET SENIOR_SCORE_FR = (
    SELECT (COUNT(*) * 100.0 / (SELECT COUNT(*) FROM all_framework_employees))
    FROM junior_employees
)
WHERE id = 1;

WITH junior_employees AS (
    SELECT DISTINCT s.user_id
    FROM dvyacheslav_dm.сотрудники_дар s
    JOIN dvyacheslav_dm.фреймворки_и_уровень_знаний_сотру fu ON s.user_id = fu.user_id
    JOIN dvyacheslav_dm.уровни_знаний uz ON fu.уровень_знаний = uz.id
    WHERE uz.название = 'Expert'
),
all_framework_employees AS (
    SELECT DISTINCT s.user_id
    FROM dvyacheslav_dm.сотрудники_дар s
    JOIN dvyacheslav_dm.фреймворки_и_уровень_знаний_сотру fu ON s.user_id = fu.user_id
)
UPDATE dvyacheslav_dm.справочник
SET EXPERT_SCORE_FR = (
    SELECT (COUNT(*) * 100.0 / (SELECT COUNT(*) FROM all_framework_employees))
    FROM junior_employees
)
WHERE id = 1;

WITH junior_employees AS (
    SELECT DISTINCT s.user_id
    FROM dvyacheslav_dm.сотрудники_дар s
    JOIN dvyacheslav_dm.фреймворки_и_уровень_знаний_сотру fu ON s.user_id = fu.user_id
    JOIN dvyacheslav_dm.уровни_знаний uz ON fu.уровень_знаний = uz.id
    WHERE uz.название = 'Использовал на проекте'
),
all_framework_employees AS (
    SELECT DISTINCT s.user_id
    FROM dvyacheslav_dm.сотрудники_дар s
    JOIN dvyacheslav_dm.фреймворки_и_уровень_знаний_сотру fu ON s.user_id = fu.user_id
)
UPDATE dvyacheslav_dm.справочник
SET PROJECT_SCORE_FR = (
    SELECT (COUNT(*) * 100.0 / (SELECT COUNT(*) FROM all_framework_employees))
    FROM junior_employees
)
WHERE id = 1;

--Расчет инструментов

WITH junior_employees AS (
    SELECT DISTINCT s.user_id
    FROM dvyacheslav_dm.сотрудники_дар s
    JOIN dvyacheslav_dm.инструменты_и_уровень_знаний_сотр fu ON s.user_id = fu.user_id
    JOIN dvyacheslav_dm.уровни_знаний uz ON fu.уровень_знаний = uz.id
    WHERE uz.название = 'Novice'
),
all_framework_employees AS (
    SELECT DISTINCT s.user_id
    FROM dvyacheslav_dm.сотрудники_дар s
    JOIN dvyacheslav_dm.инструменты_и_уровень_знаний_сотр fu ON s.user_id = fu.user_id
)
UPDATE dvyacheslav_dm.справочник
SET NOVICE_SCORE_IN = (
    SELECT (COUNT(*) * 100.0 / (SELECT COUNT(*) FROM all_framework_employees))
    FROM junior_employees
)
WHERE id = 1;

WITH junior_employees AS (
    SELECT DISTINCT s.user_id
    FROM dvyacheslav_dm.сотрудники_дар s
    JOIN dvyacheslav_dm.инструменты_и_уровень_знаний_сотр fu ON s.user_id = fu.user_id
    JOIN dvyacheslav_dm.уровни_знаний uz ON fu.уровень_знаний = uz.id
    WHERE uz.название = 'Junior'
),
all_framework_employees AS (
    SELECT DISTINCT s.user_id
    FROM dvyacheslav_dm.сотрудники_дар s
    JOIN dvyacheslav_dm.инструменты_и_уровень_знаний_сотр fu ON s.user_id = fu.user_id
)
UPDATE dvyacheslav_dm.справочник
SET JUNIOR_SCORE_IN = (
    SELECT (COUNT(*) * 100.0 / (SELECT COUNT(*) FROM all_framework_employees))
    FROM junior_employees
)
WHERE id = 1;

WITH junior_employees AS (
    SELECT DISTINCT s.user_id
    FROM dvyacheslav_dm.сотрудники_дар s
    JOIN dvyacheslav_dm.инструменты_и_уровень_знаний_сотр fu ON s.user_id = fu.user_id
    JOIN dvyacheslav_dm.уровни_знаний uz ON fu.уровень_знаний = uz.id
    WHERE uz.название = 'Middle'
),
all_framework_employees AS (
    SELECT DISTINCT s.user_id
    FROM dvyacheslav_dm.сотрудники_дар s
    JOIN dvyacheslav_dm.инструменты_и_уровень_знаний_сотр fu ON s.user_id = fu.user_id
)
UPDATE dvyacheslav_dm.справочник
SET MIDDLE_SCORE_IN = (
    SELECT (COUNT(*) * 100.0 / (SELECT COUNT(*) FROM all_framework_employees))
    FROM junior_employees
)
WHERE id = 1;

WITH junior_employees AS (
    SELECT DISTINCT s.user_id
    FROM dvyacheslav_dm.сотрудники_дар s
    JOIN dvyacheslav_dm.инструменты_и_уровень_знаний_сотр fu ON s.user_id = fu.user_id
    JOIN dvyacheslav_dm.уровни_знаний uz ON fu.уровень_знаний = uz.id
    WHERE uz.название = 'Senior'
),
all_framework_employees AS (
    SELECT DISTINCT s.user_id
    FROM dvyacheslav_dm.сотрудники_дар s
    JOIN dvyacheslav_dm.инструменты_и_уровень_знаний_сотр fu ON s.user_id = fu.user_id
)
UPDATE dvyacheslav_dm.справочник
SET SENIOR_SCORE_IN = (
    SELECT (COUNT(*) * 100.0 / (SELECT COUNT(*) FROM all_framework_employees))
    FROM junior_employees
)
WHERE id = 1;

WITH junior_employees AS (
    SELECT DISTINCT s.user_id
    FROM dvyacheslav_dm.сотрудники_дар s
    JOIN dvyacheslav_dm.инструменты_и_уровень_знаний_сотр fu ON s.user_id = fu.user_id
    JOIN dvyacheslav_dm.уровни_знаний uz ON fu.уровень_знаний = uz.id
    WHERE uz.название = 'Expert'
),
all_framework_employees AS (
    SELECT DISTINCT s.user_id
    FROM dvyacheslav_dm.сотрудники_дар s
    JOIN dvyacheslav_dm.инструменты_и_уровень_знаний_сотр fu ON s.user_id = fu.user_id
)
UPDATE dvyacheslav_dm.справочник
SET EXPERT_SCORE_IN = (
    SELECT (COUNT(*) * 100.0 / (SELECT COUNT(*) FROM all_framework_employees))
    FROM junior_employees
)
WHERE id = 1;

WITH junior_employees AS (
    SELECT DISTINCT s.user_id
    FROM dvyacheslav_dm.сотрудники_дар s
    JOIN dvyacheslav_dm.инструменты_и_уровень_знаний_сотр fu ON s.user_id = fu.user_id
    JOIN dvyacheslav_dm.уровни_знаний uz ON fu.уровень_знаний = uz.id
    WHERE uz.название = 'Использовал на проекте'
),
all_framework_employees AS (
    SELECT DISTINCT s.user_id
    FROM dvyacheslav_dm.сотрудники_дар s
    JOIN dvyacheslav_dm.инструменты_и_уровень_знаний_сотр fu ON s.user_id = fu.user_id
)
UPDATE dvyacheslav_dm.справочник
SET PROJECT_SCORE_IN = (
    SELECT (COUNT(*) * 100.0 / (SELECT COUNT(*) FROM all_framework_employees))
    FROM junior_employees
)
WHERE id = 1;

--среды разработки

WITH junior_employees AS (
    SELECT DISTINCT s.user_id
    FROM dvyacheslav_dm.сотрудники_дар s
    JOIN dvyacheslav_dm.среды_разработки_и_уровень_знаний_ fu ON s.user_id = fu.user_id
    JOIN dvyacheslav_dm.уровни_знаний uz ON fu.уровень_знаний = uz.id
    WHERE uz.название = 'Novice'
),
all_framework_employees AS (
    SELECT DISTINCT s.user_id
    FROM dvyacheslav_dm.сотрудники_дар s
    JOIN dvyacheslav_dm.среды_разработки_и_уровень_знаний_ fu ON s.user_id = fu.user_id
)
UPDATE dvyacheslav_dm.справочник
SET NOVICE_SCORE_DEV = (
    SELECT (COUNT(*) * 100.0 / (SELECT COUNT(*) FROM all_framework_employees))
    FROM junior_employees
)
WHERE id = 1;

WITH junior_employees AS (
    SELECT DISTINCT s.user_id
    FROM dvyacheslav_dm.сотрудники_дар s
    JOIN dvyacheslav_dm.среды_разработки_и_уровень_знаний_ fu ON s.user_id = fu.user_id
    JOIN dvyacheslav_dm.уровни_знаний uz ON fu.уровень_знаний = uz.id
    WHERE uz.название = 'Junior'
),
all_framework_employees AS (
    SELECT DISTINCT s.user_id
    FROM dvyacheslav_dm.сотрудники_дар s
    JOIN dvyacheslav_dm.среды_разработки_и_уровень_знаний_ fu ON s.user_id = fu.user_id
)
UPDATE dvyacheslav_dm.справочник
SET JUNIOR_SCORE_DEV = (
    SELECT (COUNT(*) * 100.0 / (SELECT COUNT(*) FROM all_framework_employees))
    FROM junior_employees
)
WHERE id = 1;

WITH junior_employees AS (
    SELECT DISTINCT s.user_id
    FROM dvyacheslav_dm.сотрудники_дар s
    JOIN dvyacheslav_dm.среды_разработки_и_уровень_знаний_ fu ON s.user_id = fu.user_id
    JOIN dvyacheslav_dm.уровни_знаний uz ON fu.уровень_знаний = uz.id
    WHERE uz.название = 'Middle'
),
all_framework_employees AS (
    SELECT DISTINCT s.user_id
    FROM dvyacheslav_dm.сотрудники_дар s
    JOIN dvyacheslav_dm.среды_разработки_и_уровень_знаний_ fu ON s.user_id = fu.user_id
)
UPDATE dvyacheslav_dm.справочник
SET MIDDLE_SCORE_DEV = (
    SELECT (COUNT(*) * 100.0 / (SELECT COUNT(*) FROM all_framework_employees))
    FROM junior_employees
)
WHERE id = 1;

WITH junior_employees AS (
    SELECT DISTINCT s.user_id
    FROM dvyacheslav_dm.сотрудники_дар s
    JOIN dvyacheslav_dm.среды_разработки_и_уровень_знаний_ fu ON s.user_id = fu.user_id
    JOIN dvyacheslav_dm.уровни_знаний uz ON fu.уровень_знаний = uz.id
    WHERE uz.название = 'Senior'
),
all_framework_employees AS (
    SELECT DISTINCT s.user_id
    FROM dvyacheslav_dm.сотрудники_дар s
    JOIN dvyacheslav_dm.среды_разработки_и_уровень_знаний_ fu ON s.user_id = fu.user_id
)
UPDATE dvyacheslav_dm.справочник
SET SENIOR_SCORE_DEV = (
    SELECT (COUNT(*) * 100.0 / (SELECT COUNT(*) FROM all_framework_employees))
    FROM junior_employees
)
WHERE id = 1;

WITH junior_employees AS (
    SELECT DISTINCT s.user_id
    FROM dvyacheslav_dm.сотрудники_дар s
    JOIN dvyacheslav_dm.среды_разработки_и_уровень_знаний_ fu ON s.user_id = fu.user_id
    JOIN dvyacheslav_dm.уровни_знаний uz ON fu.уровень_знаний = uz.id
    WHERE uz.название = 'Expert'
),
all_framework_employees AS (
    SELECT DISTINCT s.user_id
    FROM dvyacheslav_dm.сотрудники_дар s
    JOIN dvyacheslav_dm.среды_разработки_и_уровень_знаний_ fu ON s.user_id = fu.user_id
)
UPDATE dvyacheslav_dm.справочник
SET EXPERT_SCORE_DEV = (
    SELECT (COUNT(*) * 100.0 / (SELECT COUNT(*) FROM all_framework_employees))
    FROM junior_employees
)
WHERE id = 1;

WITH junior_employees AS (
    SELECT DISTINCT s.user_id
    FROM dvyacheslav_dm.сотрудники_дар s
    JOIN dvyacheslav_dm.среды_разработки_и_уровень_знаний_ fu ON s.user_id = fu.user_id
    JOIN dvyacheslav_dm.уровни_знаний uz ON fu.уровень_знаний = uz.id
    WHERE uz.название = 'Использовал на проекте'
),
all_framework_employees AS (
    SELECT DISTINCT s.user_id
    FROM dvyacheslav_dm.сотрудники_дар s
    JOIN dvyacheslav_dm.среды_разработки_и_уровень_знаний_ fu ON s.user_id = fu.user_id
)
UPDATE dvyacheslav_dm.справочник
SET PROJECT_SCORE_DEV = (
    SELECT (COUNT(*) * 100.0 / (SELECT COUNT(*) FROM all_framework_employees))
    FROM junior_employees
)
WHERE id = 1;

--сертификаты

UPDATE dvyacheslav_dm.справочник
SET QUANTITY_CERTIFICATE = (
select count(*)
from dvyacheslav_dm.сертификаты_пользователей
where год_сертификата = 2023
)
WHERE id = 1;

--Процент от общего числа сотрудников, достигших грейда

WITH all_count AS (
    SELECT DISTINCT
        сд.user_id,
        уз.название AS фреймворки_уровень,
        уз2.название AS среды_разработки_уровень,
        уз3.название AS инструменты_уровень
    FROM dvyacheslav_dm.сотрудники_дар сд
    JOIN dvyacheslav_dm.фреймворки_и_уровень_знаний_сотру фиузс ON сд.user_id = фиузс.user_id
    JOIN dvyacheslav_dm.фреймворки фр ON фиузс.фреймворки = фр.id
    JOIN dvyacheslav_dm.уровни_знаний уз ON фиузс.уровень_знаний = уз.id
    JOIN dvyacheslav_dm.среды_разработки_и_уровень_знаний_сриуз сриуз ON сд.user_id = сриуз.user_id
    JOIN dvyacheslav_dm.среды_разработки ср ON сриуз.среды_разработки = ср.id
    JOIN dvyacheslav_dm.уровни_знаний уз2 ON сриуз.уровень_знаний = уз2.id
    JOIN dvyacheslav_dm.инструменты_и_уровень_знаний_сотр ииузс ON сд.user_id = ииузс.user_id
    JOIN dvyacheslav_dm.инструменты ин ON ииузс.инструменты = ин.id
    JOIN dvyacheslav_dm.уровни_знаний уз3 ON ииузс.уровень_знаний = уз3.id
),
junior_count AS (
    SELECT
        COUNT(*) AS cnt
    FROM all_count
    WHERE фреймворки_уровень = 'Junior'
      AND среды_разработки_уровень = 'Junior'
      AND инструменты_уровень = 'Junior'
),
total_count AS (
    SELECT
        COUNT(DISTINCT user_id) AS cnt
    FROM dvyacheslav_dm.сотрудники_дар
)

UPDATE dvyacheslav_dm.справочник
SET JUNIOR_SCORE = (
SELECT
    (j.cnt::float / t.cnt * 100)
FROM junior_count j, total_count t
)
WHERE id = 1;

WITH all_count AS (
    SELECT DISTINCT
        сд.user_id,
        уз.название AS фреймворки_уровень,
        уз2.название AS среды_разработки_уровень,
        уз3.название AS инструменты_уровень
    FROM dvyacheslav_dm.сотрудники_дар сд
    JOIN dvyacheslav_dm.фреймворки_и_уровень_знаний_сотру фиузс ON сд.user_id = фиузс.user_id
    JOIN dvyacheslav_dm.фреймворки фр ON фиузс.фреймворки = фр.id
    JOIN dvyacheslav_dm.уровни_знаний уз ON фиузс.уровень_знаний = уз.id
    JOIN dvyacheslav_dm.среды_разработки_и_уровень_знаний_сриуз сриуз ON сд.user_id = сриуз.user_id
    JOIN dvyacheslav_dm.среды_разработки ср ON сриуз.среды_разработки = ср.id
    JOIN dvyacheslav_dm.уровни_знаний уз2 ON сриуз.уровень_знаний = уз2.id
    JOIN dvyacheslav_dm.инструменты_и_уровень_знаний_сотр ииузс ON сд.user_id = ииузс.user_id
    JOIN dvyacheslav_dm.инструменты ин ON ииузс.инструменты = ин.id
    JOIN dvyacheslav_dm.уровни_знаний уз3 ON ииузс.уровень_знаний = уз3.id
),
junior_count AS (
    SELECT
        COUNT(*) AS cnt
    FROM all_count
    WHERE фреймворки_уровень = 'Middle'
      AND среды_разработки_уровень = 'Middle'
      AND инструменты_уровень = 'Middle'
),
total_count AS (
    SELECT
        COUNT(DISTINCT user_id) AS cnt
    FROM dvyacheslav_dm.сотрудники_дар
)

UPDATE dvyacheslav_dm.справочник
SET MIDDLE_SCORE = (
SELECT
    (j.cnt::float / t.cnt * 100)
FROM junior_count j, total_count t
)
WHERE id = 1;

WITH all_count AS (
    SELECT DISTINCT
        сд.user_id,
        уз.название AS фреймворки_уровень,
        уз2.название AS среды_разработки_уровень,
        уз3.название AS инструменты_уровень
    FROM dvyacheslav_dm.сотрудники_дар сд
    JOIN dvyacheslav_dm.фреймворки_и_уровень_знаний_сотру фиузс ON сд.user_id = фиузс.user_id
    JOIN dvyacheslav_dm.фреймворки фр ON фиузс.фреймворки = фр.id
    JOIN dvyacheslav_dm.уровни_знаний уз ON фиузс.уровень_знаний = уз.id
    JOIN dvyacheslav_dm.среды_разработки_и_уровень_знаний_сриуз сриуз ON сд.user_id = сриуз.user_id
    JOIN dvyacheslav_dm.среды_разработки ср ON сриуз.среды_разработки = ср.id
    JOIN dvyacheslav_dm.уровни_знаний уз2 ON сриуз.уровень_знаний = уз2.id
    JOIN dvyacheslav_dm.инструменты_и_уровень_знаний_сотр ииузс ON сд.user_id = ииузс.user_id
    JOIN dvyacheslav_dm.инструменты ин ON ииузс.инструменты = ин.id
    JOIN dvyacheslav_dm.уровни_знаний уз3 ON ииузс.уровень_знаний = уз3.id
),
junior_count AS (
    SELECT
        COUNT(*) AS cnt
    FROM all_count
    WHERE фреймворки_уровень = 'Senior'
      AND среды_разработки_уровень = 'Senior'
      AND инструменты_уровень = 'Senior'
),
total_count AS (
    SELECT
        COUNT(DISTINCT user_id) AS cnt
    FROM dvyacheslav_dm.сотрудники_дар
)

UPDATE dvyacheslav_dm.справочник
SET SENIOR_SCORE = (
SELECT
    (j.cnt::float / t.cnt * 100)
FROM junior_count j, total_count t
)
WHERE id = 1;

--Процент сотрудников по инструментам, средам разработки и фреймворков, который имеют хотя бы один навык на уровне

WITH all_count AS (
    SELECT DISTINCT
        сд.user_id,
        уз.название AS фреймворки_уровень,
        уз2.название AS среды_разработки_уровень,
        уз3.название AS инструменты_уровень
    FROM dvyacheslav_dm.сотрудники_дар сд
    JOIN dvyacheslav_dm.фреймворки_и_уровень_знаний_сотру фиузс ON сд.user_id = фиузс.user_id
    JOIN dvyacheslav_dm.фреймворки фр ON фиузс.фреймворки = фр.id
    JOIN dvyacheslav_dm.уровни_знаний уз ON фиузс.уровень_знаний = уз.id
    JOIN dvyacheslav_dm.среды_разработки_и_уровень_знаний_сриуз сриуз ON сд.user_id = сриуз.user_id
    JOIN dvyacheslav_dm.среды_разработки ср ON сриуз.среды_разработки = ср.id
    JOIN dvyacheslav_dm.уровни_знаний уз2 ON сриуз.уровень_знаний = уз2.id
    JOIN dvyacheslav_dm.инструменты_и_уровень_знаний_сотр ииузс ON сд.user_id = ииузс.user_id
    JOIN dvyacheslav_dm.инструменты ин ON ииузс.инструменты = ин.id
    JOIN dvyacheslav_dm.уровни_знаний уз3 ON ииузс.уровень_знаний = уз3.id
),
junior_count AS (
    SELECT
        COUNT(*) AS cnt
    FROM all_count
    WHERE фреймворки_уровень = 'Novice'
      OR среды_разработки_уровень = 'Novice'
      OR инструменты_уровень = 'Novice'
),
total_count AS (
    SELECT
        COUNT(DISTINCT user_id) AS cnt
    FROM dvyacheslav_dm.сотрудники_дар
)

UPDATE dvyacheslav_dm.справочник
SET NOVICE_COUNT = (
SELECT
    (j.cnt::float / t.cnt * 100)
FROM junior_count j, total_count t
)
WHERE id = 1;

WITH all_count AS (
    SELECT DISTINCT
        сд.user_id,
        уз.название AS фреймворки_уровень,
        уз2.название AS среды_разработки_уровень,
        уз3.название AS инструменты_уровень
    FROM dvyacheslav_dm.сотрудники_дар сд
    JOIN dvyacheslav_dm.фреймворки_и_уровень_знаний_сотру фиузс ON сд.user_id = фиузс.user_id
    JOIN dvyacheslav_dm.фреймворки фр ON фиузс.фреймворки = фр.id
    JOIN dvyacheslav_dm.уровни_знаний уз ON фиузс.уровень_знаний = уз.id
    JOIN dvyacheslav_dm.среды_разработки_и_уровень_знаний_сриуз сриуз ON сд.user_id = сриуз.user_id
    JOIN dvyacheslav_dm.среды_разработки ср ON сриуз.среды_разработки = ср.id
    JOIN dvyacheslav_dm.уровни_знаний уз2 ON сриуз.уровень_знаний = уз2.id
    JOIN dvyacheslav_dm.инструменты_и_уровень_знаний_сотр ииузс ON сд.user_id = ииузс.user_id
    JOIN dvyacheslav_dm.инструменты ин ON ииузс.инструменты = ин.id
    JOIN dvyacheslav_dm.уровни_знаний уз3 ON ииузс.уровень_знаний = уз3.id
),
junior_count AS (
    SELECT
        COUNT(*) AS cnt
    FROM all_count
    WHERE фреймворки_уровень = 'Junior'
      OR среды_разработки_уровень = 'Junior'
      OR инструменты_уровень = 'Junior'
),
total_count AS (
    SELECT
        COUNT(DISTINCT user_id) AS cnt
    FROM dvyacheslav_dm.сотрудники_дар
)

UPDATE dvyacheslav_dm.справочник
SET JUNIOR_COUNT = (
SELECT
    (j.cnt::float / t.cnt * 100)
FROM junior_count j, total_count t
)
WHERE id = 1;

WITH all_count AS (
    SELECT DISTINCT
        сд.user_id,
        уз.название AS фреймворки_уровень,
        уз2.название AS среды_разработки_уровень,
        уз3.название AS инструменты_уровень
    FROM dvyacheslav_dm.сотрудники_дар сд
    JOIN dvyacheslav_dm.фреймворки_и_уровень_знаний_сотру фиузс ON сд.user_id = фиузс.user_id
    JOIN dvyacheslav_dm.фреймворки фр ON фиузс.фреймворки = фр.id
    JOIN dvyacheslav_dm.уровни_знаний уз ON фиузс.уровень_знаний = уз.id
    JOIN dvyacheslav_dm.среды_разработки_и_уровень_знаний_сриуз сриуз ON сд.user_id = сриуз.user_id
    JOIN dvyacheslav_dm.среды_разработки ср ON сриуз.среды_разработки = ср.id
    JOIN dvyacheslav_dm.уровни_знаний уз2 ON сриуз.уровень_знаний = уз2.id
    JOIN dvyacheslav_dm.инструменты_и_уровень_знаний_сотр ииузс ON сд.user_id = ииузс.user_id
    JOIN dvyacheslav_dm.инструменты ин ON ииузс.инструменты = ин.id
    JOIN dvyacheslav_dm.уровни_знаний уз3 ON ииузс.уровень_знаний = уз3.id
),
junior_count AS (
    SELECT
        COUNT(*) AS cnt
    FROM all_count
    WHERE фреймворки_уровень = 'Middle'
      OR среды_разработки_уровень = 'Middle'
      OR инструменты_уровень = 'Middle'
),
total_count AS (
    SELECT
        COUNT(DISTINCT user_id) AS cnt
    FROM dvyacheslav_dm.сотрудники_дар
)

UPDATE dvyacheslav_dm.справочник
SET MIDDLE_COUNT = (
SELECT
    (j.cnt::float / t.cnt * 100)
FROM junior_count j, total_count t
)
WHERE id = 1;

WITH all_count AS (
    SELECT DISTINCT
        сд.user_id,
        уз.название AS фреймворки_уровень,
        уз2.название AS среды_разработки_уровень,
        уз3.название AS инструменты_уровень
    FROM dvyacheslav_dm.сотрудники_дар сд
    JOIN dvyacheslav_dm.фреймворки_и_уровень_знаний_сотру фиузс ON сд.user_id = фиузс.user_id
    JOIN dvyacheslav_dm.фреймворки фр ON фиузс.фреймворки = фр.id
    JOIN dvyacheslav_dm.уровни_знаний уз ON фиузс.уровень_знаний = уз.id
    JOIN dvyacheslav_dm.среды_разработки_и_уровень_знаний_сриуз сриуз ON сд.user_id = сриуз.user_id
    JOIN dvyacheslav_dm.среды_разработки ср ON сриуз.среды_разработки = ср.id
    JOIN dvyacheslav_dm.уровни_знаний уз2 ON сриуз.уровень_знаний = уз2.id
    JOIN dvyacheslav_dm.инструменты_и_уровень_знаний_сотр ииузс ON сд.user_id = ииузс.user_id
    JOIN dvyacheslav_dm.инструменты ин ON ииузс.инструменты = ин.id
    JOIN dvyacheslav_dm.уровни_знаний уз3 ON ииузс.уровень_знаний = уз3.id
),
junior_count AS (
    SELECT
        COUNT(*) AS cnt
    FROM all_count
    WHERE фреймворки_уровень = 'Senior'
      OR среды_разработки_уровень = 'Senior'
      OR инструменты_уровень = 'Senior'
),
total_count AS (
    SELECT
        COUNT(DISTINCT user_id) AS cnt
    FROM dvyacheslav_dm.сотрудники_дар
)

UPDATE dvyacheslav_dm.справочник
SET SENIOR_COUNT = (
SELECT
    (j.cnt::float / t.cnt * 100)
FROM junior_count j, total_count t
)
WHERE id = 1;

WITH all_count AS (
    SELECT DISTINCT
        сд.user_id,
        уз.название AS фреймворки_уровень,
        уз2.название AS среды_разработки_уровень,
        уз3.название AS инструменты_уровень
    FROM dvyacheslav_dm.сотрудники_дар сд
    JOIN dvyacheslav_dm.фреймворки_и_уровень_знаний_сотру фиузс ON сд.user_id = фиузс.user_id
    JOIN dvyacheslav_dm.фреймворки фр ON фиузс.фреймворки = фр.id
    JOIN dvyacheslav_dm.уровни_знаний уз ON фиузс.уровень_знаний = уз.id
    JOIN dvyacheslav_dm.среды_разработки_и_уровень_знаний_сриуз сриуз ON сд.user_id = сриуз.user_id
    JOIN dvyacheslav_dm.среды_разработки ср ON сриуз.среды_разработки = ср.id
    JOIN dvyacheslav_dm.уровни_знаний уз2 ON сриуз.уровень_знаний = уз2.id
    JOIN dvyacheslav_dm.инструменты_и_уровень_знаний_сотр ииузс ON сд.user_id = ииузс.user_id
    JOIN dvyacheslav_dm.инструменты ин ON ииузс.инструменты = ин.id
    JOIN dvyacheslav_dm.уровни_знаний уз3 ON ииузс.уровень_знаний = уз3.id
),
junior_count AS (
    SELECT
        COUNT(*) AS cnt
    FROM all_count
    WHERE фреймворки_уровень = 'Expert'
      OR среды_разработки_уровень = 'Expert'
      OR инструменты_уровень = 'Expert'
),
total_count AS (
    SELECT
        COUNT(DISTINCT user_id) AS cnt
    FROM dvyacheslav_dm.сотрудники_дар
)

UPDATE dvyacheslav_dm.справочник
SET EXPERT_COUNT = (
SELECT
    (j.cnt::float / t.cnt * 100)
FROM junior_count j, total_count t
)
WHERE id = 1;