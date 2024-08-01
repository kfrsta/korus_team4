CREATE TABLE IF NOT EXISTS dvyacheslav_dds.базы_данных_и_уровень_знаний_сотру (
	user_id int4 NULL,
	id int8 NULL,
	базы_данных int4 NULL,
	дата date NULL,
	уровень_знаний int4 NULL,
	CONSTRAINT базы_данных_и_ур_уровень_знаний_fkey FOREIGN KEY (уровень_знаний) REFERENCES dvyacheslav_dds.уровни_знаний(id),
	CONSTRAINT базы_данных_и_урове_базы_данных_fkey FOREIGN KEY (базы_данных) REFERENCES dvyacheslav_dds.базы_данных(id),
	CONSTRAINT базы_данных_и_уровень_знани_user_id_fkey FOREIGN KEY (user_id) REFERENCES dvyacheslav_dds.сотрудники_дар(user_id)
);

CREATE TABLE IF NOT EXISTS dvyacheslav_dds.инструменты_и_уровень_знаний_сотр (
	user_id int4 NULL,
	id int8 NULL,
	дата date NULL,
	инструменты int4 NULL,
	уровень_знаний int4 NULL,
	CONSTRAINT инструменты_и_ур_уровень_знаний_fkey FOREIGN KEY (уровень_знаний) REFERENCES dvyacheslav_dds.уровни_знаний(id),
	CONSTRAINT инструменты_и_уров_инструменты_fkey FOREIGN KEY (инструменты) REFERENCES dvyacheslav_dds.инструменты(id),
	CONSTRAINT инструменты_и_уровень_знан_user_id_fkey FOREIGN KEY (user_id) REFERENCES dvyacheslav_dds.сотрудники_дар(user_id)
);

CREATE TABLE IF NOT EXISTS dvyacheslav_dds.образование_пользователей (
	user_id int4 NULL,
	id int8 NULL,
	уровень_образование int4 NULL,
	название_учебного_заведения text NULL,
	фиктивное_название text NULL,
	факультет_кафедра text NULL,
	специальность text NULL,
	квалификация text NULL,
	год_окончания int4 NULL,
	CONSTRAINT образование_пол_уровень_образо_fkey FOREIGN KEY (уровень_образование) REFERENCES dvyacheslav_dds.уровень_образования(id),
	CONSTRAINT образование_пользователей_user_id_fkey FOREIGN KEY (user_id) REFERENCES dvyacheslav_dds.сотрудники_дар(user_id)
);

CREATE TABLE IF NOT EXISTS dvyacheslav_dds.опыт_сотрудника_в_отраслях (
	user_id int4 NULL,
	id int8 NULL,
	дата date NULL,
	отрасли int4 NULL,
	уровень_знаний_в_отрасли int4 NULL,
	CONSTRAINT опыт_сотрудник_уровень_знаний__fkey1 FOREIGN KEY (уровень_знаний_в_отрасли) REFERENCES dvyacheslav_dds.уровни_знаний_в_отрасли(id),
	CONSTRAINT опыт_сотрудника_в_отрас_отрасли_fkey FOREIGN KEY (отрасли) REFERENCES dvyacheslav_dds.отрасли(id),
	CONSTRAINT опыт_сотрудника_в_отраслях_user_id_fkey FOREIGN KEY (user_id) REFERENCES dvyacheslav_dds.сотрудники_дар(user_id)
);

CREATE TABLE IF NOT EXISTS dvyacheslav_dds.опыт_сотрудника_в_предметных_обла (
	user_id int4 NULL,
	id int8 NULL,
	дата date NULL,
	предментые_области int4 NULL,
	уровень_знаний_в_предметной_облас int4 NULL,
	CONSTRAINT опыт_сотрудника_в_предметн_user_id_fkey FOREIGN KEY (user_id) REFERENCES dvyacheslav_dds.сотрудники_дар(user_id),
	CONSTRAINT опыт_сотрудника_предментые_обл_fkey FOREIGN KEY (предментые_области) REFERENCES dvyacheslav_dds.предметная_область(id),
	CONSTRAINT опыт_сотрудника_уровень_знаний__fkey FOREIGN KEY (уровень_знаний_в_предметной_облас) REFERENCES dvyacheslav_dds.уровни_знаний_в_предметной_област(id)
);

CREATE TABLE IF NOT EXISTS dvyacheslav_dds.платформы_и_уровень_знаний_сотруд (
	user_id int4 NULL,
	id int8 NULL,
	дата date NULL,
	платформы int4 NULL,
	уровень_знаний int4 NULL,
	CONSTRAINT платформы_и_уров_уровень_знаний_fkey FOREIGN KEY (уровень_знаний) REFERENCES dvyacheslav_dds.уровни_знаний(id),
	CONSTRAINT платформы_и_уровень_з_платформы_fkey FOREIGN KEY (платформы) REFERENCES dvyacheslav_dds.платформы(id),
	CONSTRAINT платформы_и_уровень_знаний__user_id_fkey FOREIGN KEY (user_id) REFERENCES dvyacheslav_dds.сотрудники_дар(user_id)
);

CREATE TABLE IF NOT EXISTS dvyacheslav_dds.сертификаты_пользователей (
	user_id int4 NULL,
	id int8 NULL,
	год_сертификата int4 NULL,
	наименование_сертификата text NULL,
	организация_выдавшая_сертификат text NULL,
	CONSTRAINT сертификаты_пользователей_user_id_fkey FOREIGN KEY (user_id) REFERENCES dvyacheslav_dds.сотрудники_дар(user_id)
);

CREATE TABLE IF NOT EXISTS dvyacheslav_dds.среды_разработки_и_уровень_знаний_ (
	user_id int4 NULL,
	id int8 NULL,
	дата date NULL,
	среды_разработки int4 NULL,
	уровень_знаний int4 NULL,
	CONSTRAINT среды_разработк_среды_разработ_fkey FOREIGN KEY (среды_разработки) REFERENCES dvyacheslav_dds.среды_разработки(id),
	CONSTRAINT среды_разработк_уровень_знаний_fkey FOREIGN KEY (уровень_знаний) REFERENCES dvyacheslav_dds.уровни_знаний(id),
	CONSTRAINT среды_разработки_и_уровень__user_id_fkey FOREIGN KEY (user_id) REFERENCES dvyacheslav_dds.сотрудники_дар(user_id)
);

CREATE TABLE IF NOT EXISTS dvyacheslav_dds.технологии_и_уровень_знаний_сотру (
	user_id int4 NULL,
	id int8 NULL,
	дата date NULL,
	технологии int4 NULL,
	уровень_знаний int4 NULL,
	CONSTRAINT технологии_и_уро_уровень_знаний_fkey FOREIGN KEY (уровень_знаний) REFERENCES dvyacheslav_dds.уровни_знаний(id),
	CONSTRAINT технологии_и_уровен_технологии_fkey FOREIGN KEY (технологии) REFERENCES dvyacheslav_dds.технологии(id),
	CONSTRAINT технологии_и_уровень_знани_user_id_fkey FOREIGN KEY (user_id) REFERENCES dvyacheslav_dds.сотрудники_дар(user_id)
);

CREATE TABLE IF NOT EXISTS dvyacheslav_dds.типы_систем_и_уровень_знаний_сотру (
	user_id int4 NULL,
	id int8 NULL,
	дата date NULL,
	типы_систем int4 NULL,
	уровень_знаний int4 NULL,
	CONSTRAINT типы_систем_и_ур_уровень_знаний_fkey FOREIGN KEY (уровень_знаний) REFERENCES dvyacheslav_dds.уровни_знаний(id),
	CONSTRAINT типы_систем_и_урове_типы_систем_fkey FOREIGN KEY (типы_систем) REFERENCES dvyacheslav_dds.типы_систем(id),
	CONSTRAINT типы_систем_и_уровень_знани_user_id_fkey FOREIGN KEY (user_id) REFERENCES dvyacheslav_dds.сотрудники_дар(user_id)
);

CREATE TABLE IF NOT EXISTS dvyacheslav_dds.уровни_знаний_в_предметной_област (
	id int4 NOT NULL,
	название varchar(100) NULL,
	CONSTRAINT уровни_знаний_в_предметной_обла_pkey PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS dvyacheslav_dds.фреймворки_и_уровень_знаний_сотру (
	user_id int4 NULL,
	id int8 NULL,
	дата date NULL,
	уровень_знаний int4 NULL,
	фреймворки int4 NULL,
	CONSTRAINT фреймворки_и_уро_уровень_знаний_fkey FOREIGN KEY (уровень_знаний) REFERENCES dvyacheslav_dds.уровни_знаний(id),
	CONSTRAINT фреймворки_и_уровен_фреймворки_fkey FOREIGN KEY (фреймворки) REFERENCES dvyacheslav_dds.фреймворки(id),
	CONSTRAINT фреймворки_и_уровень_знани_user_id_fkey FOREIGN KEY (user_id) REFERENCES dvyacheslav_dds.сотрудники_дар(user_id)
);

CREATE TABLE IF NOT EXISTS dvyacheslav_dds.языки_программирования_и_уровень (
	user_id int4 NULL,
	id int8 NULL,
	дата date NULL,
	уровень_знаний int4 NULL,
	языки_программирования int4 NULL,
	CONSTRAINT языки_программи_уровень_знаний_fkey FOREIGN KEY (уровень_знаний) REFERENCES dvyacheslav_dds.уровни_знаний(id),
	CONSTRAINT языки_программи_языки_программ_fkey FOREIGN KEY (языки_программирования) REFERENCES dvyacheslav_dds.языки_программирования(id),
	CONSTRAINT языки_программирования_и_у_user_id_fkey FOREIGN KEY (user_id) REFERENCES dvyacheslav_dds.сотрудники_дар(user_id)
);

CREATE TABLE IF NOT EXISTS dvyacheslav_dds.языки_пользователей (
	user_id int4 NULL,
	id int4 NULL,
	язык int4 NULL,
	уровень_знаний_ин_языка int4 NULL,
	CONSTRAINT языки_пользователей_pkey PRIMARY KEY (id),
	CONSTRAINT языки_пользоват_уровень_знаний__fkey FOREIGN KEY (уровень_знаний_ин_языка) REFERENCES dvyacheslav_dds.уровни_владения_ин(id),
	CONSTRAINT языки_пользователей_язык_fkey FOREIGN KEY (язык) REFERENCES dvyacheslav_dds.языки(id),
	CONSTRAINT языки_пользователей_user_id_fkey FOREIGN KEY (user_id) REFERENCES dvyacheslav_dds.сотрудники_дар(user_id)
);