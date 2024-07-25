CREATE SCHEMA IF NOT EXISTS andronov_ods;

CREATE TABLE IF NOT EXISTS andronov_ods.базы_данных (
	название varchar NULL,
	активность varchar NULL,
	"Сорт." int4 NULL,
	"Дата изм." varchar NULL,
	id int4 NULL
);

CREATE TABLE IF NOT EXISTS andronov_ods.базы_данных_и_уровень_знаний_сотру (
	название varchar NULL,
	активность varchar NULL,
	"Сорт." int4 NULL,
	"Дата изм." varchar NULL,
	id int4 NULL,
	"Базы данных" varchar NULL,
	дата varchar NULL,
	"Уровень знаний" varchar NULL
);


CREATE TABLE IF NOT EXISTS andronov_ods.инструменты (
	название varchar NULL,
	активность varchar NULL,
	"Сорт." int4 NULL,
	"Дата изм." varchar NULL,
	id int4 NULL
);

CREATE TABLE IF NOT EXISTS andronov_ods.инструменты_и_уровень_знаний_сотр (
	название varchar NULL,
	активность varchar NULL,
	"Сорт." int4 NULL,
	"Дата изм." varchar NULL,
	id int4 NULL,
	дата varchar NULL,
	инструменты varchar NULL,
	"Уровень знаний" varchar NULL
);


CREATE TABLE IF NOT EXISTS andronov_ods.образование_пользователей (
	"User ID" int4 NULL,
	активность text NULL,
	"Сорт." int4 NULL,
	"Дата изм." text NULL,
	id int4 NULL,
	"Уровень образование" text NULL,
	"Название учебного заведения" text NULL,
	"Фиктивное название" text NULL,
	"Факультет, кафедра" text NULL,
	специальность text NULL,
	квалификация text NULL,
	"Год окончания" int4 NULL
);


CREATE TABLE IF NOT EXISTS andronov_ods.опыт_сотрудника_в_отраслях (
	"User ID" int4 NULL,
	активность varchar NULL,
	"Сорт." int4 NULL,
	"Дата изм." varchar NULL,
	id int4 NULL,
	дата varchar NULL,
	отрасли varchar NULL,
	"Уровень знаний в отрасли" varchar NULL
);


CREATE TABLE IF NOT EXISTS andronov_ods.опыт_сотрудника_в_предметных_обла (
	"User ID" int4 NULL,
	активность varchar NULL,
	"Сорт." int4 NULL,
	"Дата изм." varchar NULL,
	id int4 NULL,
	дата varchar NULL,
	"Предментые области" varchar NULL,
	"Уровень знаний в предметной облас" varchar NULL
);


CREATE TABLE IF NOT EXISTS andronov_ods.отрасли (
	название varchar NULL,
	активность varchar NULL,
	"Сорт." int4 NULL,
	"Дата изм." varchar NULL,
	id int4 NULL
);


CREATE TABLE IF NOT EXISTS andronov_ods.платформы (
	название varchar NULL,
	активность varchar NULL,
	"Сорт." int4 NULL,
	"Дата изм." varchar NULL,
	id int4 NULL
);


CREATE TABLE IF NOT EXISTS andronov_ods.платформы_и_уровень_знаний_сотруд (
	"User ID" int4 NULL,
	активность varchar NULL,
	"Сорт." int4 NULL,
	"Дата изм." varchar NULL,
	id int4 NULL,
	дата varchar NULL,
	платформы varchar NULL,
	"Уровень знаний" varchar NULL
);


CREATE TABLE IF NOT EXISTS andronov_ods.предметная_область (
	название varchar NULL,
	активность varchar NULL,
	"Сорт." int4 NULL,
	"Дата изм." varchar NULL,
	id int4 NULL
);


CREATE TABLE IF NOT EXISTS andronov_ods.резюмедар (
	"UserID" int4 NULL,
	"ResumeID" int4 NULL,
	"Активность" text NULL,
	"Образование" text NULL,
	"Сертификаты/Курсы" text NULL,
	"Языки" text NULL,
	"Базыданных" text NULL,
	"Инструменты" text NULL,
	"Отрасли" text NULL,
	"Платформы" text NULL,
	"Предметныеобласти" text NULL,
	"Средыразработки" text NULL,
	"Типысистем" text NULL,
	"Фреймворки" text NULL,
	"Языкипрограммирования" text NULL,
	"Технологии" text NULL
);

CREATE TABLE IF NOT EXISTS andronov_ods.сертификаты_пользователей (
	"User ID" int4 NULL,
	активность text NULL,
	"Сорт." int4 NULL,
	"Дата изм." text NULL,
	id int4 NULL,
	"Год сертификата" int4 NULL,
	"Наименование сертификата" text NULL,
	"Организация, выдавшая сертификат" text NULL
);


CREATE TABLE IF NOT EXISTS andronov_ods.сотрудники_дар (
	id int4 NULL,
	"Дата рождения" text NULL,
	активность text NULL,
	пол text NULL,
	фамилия text NULL,
	имя text NULL,
	"Последняя авторизация" text NULL,
	должность text NULL,
	цфо text NULL,
	"Дата регистрации" text NULL,
	"Дата изменения" text NULL,
	подразделения text NULL,
	"E-Mail" text NULL,
	логин text NULL,
	компания text NULL,
	"Город проживания" text NULL
);

CREATE TABLE IF NOT EXISTS andronov_ods.среды_разработки (
	название text NULL,
	активность text NULL,
	"Сорт." int4 NULL,
	"Дата изм." text NULL,
	id int4 NULL
);


CREATE TABLE IF NOT EXISTS andronov_ods.среды_разработки_и_уровень_знаний_ (
	название varchar NULL,
	активность varchar NULL,
	"Сорт." int4 NULL,
	"Дата изм." varchar NULL,
	id int4 NULL,
	дата varchar NULL,
	"Среды разработки" varchar NULL,
	"Уровень знаний" varchar NULL
);

CREATE TABLE IF NOT EXISTS andronov_ods.технологии (
	название text NULL,
	активность text NULL,
	"Сорт." int4 NULL,
	"Дата изм." text NULL,
	id int4 NULL
);

CREATE TABLE IF NOT EXISTS andronov_ods.технологии_и_уровень_знаний_сотру (
	название text NULL,
	активность text NULL,
	"Сорт." int4 NULL,
	"Дата изм." text NULL,
	id int4 NULL,
	дата text NULL,
	технологии text NULL,
	"Уровень знаний" text NULL
);


CREATE TABLE IF NOT EXISTS andronov_ods.типы_систем (
	название text NULL,
	активность text NULL,
	"Сорт." int4 NULL,
	"Дата изм." text NULL,
	id int4 NULL
);


CREATE TABLE IF NOT EXISTS andronov_ods.типы_систем_и_уровень_знаний_сотру (
	название varchar NULL,
	активность varchar NULL,
	"Сорт." int4 NULL,
	"Дата изм." varchar NULL,
	id int4 NULL,
	дата varchar NULL,
	"Типы систем" varchar NULL,
	"Уровень знаний" varchar NULL
);


CREATE TABLE IF NOT EXISTS andronov_ods.уровень_образования (
	название text NULL,
	активность text NULL,
	"Сорт." int4 NULL,
	"Дата изм." text NULL,
	id int4 NULL
);


CREATE TABLE IF NOT EXISTS andronov_ods.уровни_владения_ин (
	название text NULL,
	активность text NULL,
	"Сорт." int4 NULL,
	"Дата изм." text NULL,
	id int4 NULL
);


CREATE TABLE IF NOT EXISTS andronov_ods.уровни_знаний (
	название text NULL,
	активность text NULL,
	"Сорт." int4 NULL,
	"Дата изм." text NULL,
	id int4 NULL
);


CREATE TABLE IF NOT EXISTS andronov_ods.уровни_знаний_в_отрасли (
	название text NULL,
	активность text NULL,
	"Сорт." int4 NULL,
	"Дата изм." text NULL,
	id int4 NULL
);


CREATE TABLE IF NOT EXISTS andronov_ods.уровни_знаний_в_предметной_област (
	название text NULL,
	активность text NULL,
	"Сорт." int4 NULL,
	"Дата изм." text NULL,
	id int4 NULL
);

CREATE TABLE IF NOT EXISTS andronov_ods.фреймворки (
	название text NULL,
	активность text NULL,
	"Сорт." int4 NULL,
	"Дата изм." text NULL,
	id int4 NULL
);


CREATE TABLE IF NOT EXISTS andronov_ods.фреймворки_и_уровень_знаний_сотру (
	название text NULL,
	активность text NULL,
	"Сорт." int4 NULL,
	"Дата изм." text NULL,
	id int4 NULL,
	дата text NULL,
	"Уровень знаний" text NULL,
	фреймворки text NULL
);


CREATE TABLE IF NOT EXISTS andronov_ods.языки (
	название text NULL,
	активность text NULL,
	"Сорт." int4 NULL,
	"Дата изм." text NULL,
	id int4 NULL
);


CREATE TABLE IF NOT EXISTS andronov_ods.языки_пользователей (
	название text NULL,
	активность text NULL,
	"Сорт." int4 NULL,
	"Дата изм." text NULL,
	id int4 NULL,
	язык text NULL,
	"Уровень знаний ин. языка" text NULL
);


CREATE TABLE IF NOT EXISTS andronov_ods.языки_программирования (
	название text NULL,
	активность text NULL,
	"Сорт." int4 NULL,
	"Дата изм." text NULL,
	id int4 NULL
);


CREATE TABLE IF NOT EXISTS andronov_ods.языки_программирования_и_уровень (
	название text NULL,
	активность text NULL,
	"Сорт." int4 NULL,
	"Дата изм." text NULL,
	id int4 NULL,
	дата text NULL,
	"Уровень знаний" text NULL,
	"Языки программирования" text NULL
);