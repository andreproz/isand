createDBSQL = '''CREATE DATABASE "isandDB"
    WITH
    OWNER = postgres
    ENCODING = 'UTF8'
    CONNECTION LIMIT = -1
    IS_TEMPLATE = False;
'''

createTablesSQL = '''
BEGIN;

CREATE TABLE IF NOT EXISTS public."PUBLICATION"
(
    id_publ bigserial NOT NULL,
    id_prime bigint,
    path text,
    path_to_text text,
    path_to_deltas text,
    "encrypted" character varying(9),
    extractable character varying(9),
    file_name character varying(100),
    unknown_chars boolean,
    foreign_language boolean,
    doi character(100),
    title text,
    abstract text,
    id_user bigint,
    PRIMARY KEY (id_publ)
);

CREATE TABLE IF NOT EXISTS public."PUBL_TEXT"
(
    id_publ_text bigserial NOT NULL,
    id_publ bigint NOT NULL,
    publ_text text NOT NULL,
    PRIMARY KEY (id_publ_text)
);

CREATE TABLE IF NOT EXISTS public."DELTAS"
(
    id_deltas bigserial NOT NULL,
    id_publ bigint NOT NULL,
    deltas bytea NOT NULL,
    PRIMARY KEY (id_deltas)
);

CREATE TABLE IF NOT EXISTS public."KEYWORD"
(
    id_keyword bigserial NOT NULL,
    keyword text NOT NULL,
    PRIMARY KEY (id_keyword)
);

CREATE TABLE IF NOT EXISTS public."AUTHOR"
(
    id_author bigserial NOT NULL,
    firstname text,
    middlename text,
    surname text,
    email character varying(256),
    id_publ_author bigint NOT NULL,
	id_author_affil bigint NOT NULL,
    "position" text,
    PRIMARY KEY (id_author)
);

CREATE TABLE IF NOT EXISTS public."PUBL_AUTHOR"
(
    id_publ_author bigserial NOT NULL,
    id_publ bigint NOT NULL,
    id_author bigint NOT NULL,
    PRIMARY KEY (id_publ_author)
);

CREATE TABLE IF NOT EXISTS public."PUBL_KEYWORD"
(
    id_publ_keyword bigserial NOT NULL,
    id_publ bigint NOT NULL,
    id_keyword bigint NOT NULL,
    PRIMARY KEY (id_publ_keyword)
);

CREATE TABLE IF NOT EXISTS public."AFFILIATION"
(
    id_affil bigserial NOT NULL,
	affiliation_raw text,
	title text,
	post_code character varying(100),
	adr_line text,
    city text,
    country text,
    PRIMARY KEY (id_affil)
);

CREATE TABLE IF NOT EXISTS public."PUBL_AFFIL"
(
    id_publ_affil bigserial NOT NULL,
    id_publ bigint NOT NULL,
    id_affil bigint NOT NULL,
    PRIMARY KEY (id_publ_affil)
);

CREATE TABLE IF NOT EXISTS public."BIBLIOGRAPHY"
(
    id_bibl bigserial NOT NULL,
    title text NOT NULL,
    doi character varying(100),
    source_title text,
    city text,
    volume text,
    issue text,
    "section" text,
    pages integer,
    publisher text,
    "year" date,
    PRIMARY KEY (id_bibl)
);

CREATE TABLE IF NOT EXISTS public."PUBL_BIBL"
(
    id_publ_bibl bigserial NOT NULL,
    id_publ bigint NOT NULL,
    id_bibl bigint NOT NULL,
    PRIMARY KEY (id_publ_bibl)
);

CREATE TABLE IF NOT EXISTS public."BIBL_AUTHOR"
(
    id_bibl_author bigserial NOT NULL,
    id_bibl bigint NOT NULL,
    id_author bigint NOT NULL,
    PRIMARY KEY (id_bibl_author)
);

CREATE TABLE IF NOT EXISTS public."AUTHOR_AFFIL"
(
    id_author_affil bigserial NOT NULL,
    id_author bigint NOT NULL,
    id_affil bigint NOT NULL,
    PRIMARY KEY (id_author_affil)
);

CREATE TABLE IF NOT EXISTS public."USER"
(
    id_user bigserial NOT NULL,
    org_name text,
    email text,
    "token" text,
    "password" text,
    phone character varying(12),
    PRIMARY KEY (id_user)
);

ALTER TABLE IF EXISTS public."PUBL_TEXT"
    ADD CONSTRAINT id_publ FOREIGN KEY (id_publ)
    REFERENCES public."PUBLICATION" (id_publ) MATCH SIMPLE
    ON UPDATE CASCADE
    ON DELETE CASCADE
    NOT VALID;


ALTER TABLE IF EXISTS public."DELTAS"
    ADD CONSTRAINT id_publ FOREIGN KEY (id_publ)
    REFERENCES public."PUBLICATION" (id_publ) MATCH SIMPLE
    ON UPDATE CASCADE
    ON DELETE CASCADE
    NOT VALID;


ALTER TABLE IF EXISTS public."PUBL_AUTHOR"
    ADD CONSTRAINT id_publ FOREIGN KEY (id_publ)
    REFERENCES public."PUBLICATION" (id_publ) MATCH SIMPLE
    ON UPDATE CASCADE
    ON DELETE CASCADE
    NOT VALID;


ALTER TABLE IF EXISTS public."PUBL_AUTHOR"
    ADD CONSTRAINT id_author FOREIGN KEY (id_author)
    REFERENCES public."AUTHOR" (id_author) MATCH SIMPLE
    ON UPDATE CASCADE
    ON DELETE CASCADE
    NOT VALID;


ALTER TABLE IF EXISTS public."PUBL_KEYWORD"
    ADD CONSTRAINT id_publ FOREIGN KEY (id_publ)
    REFERENCES public."PUBLICATION" (id_publ) MATCH SIMPLE
    ON UPDATE CASCADE
    ON DELETE CASCADE
    NOT VALID;


ALTER TABLE IF EXISTS public."PUBL_KEYWORD"
    ADD CONSTRAINT id_keyword FOREIGN KEY (id_keyword)
    REFERENCES public."KEYWORD" (id_keyword) MATCH SIMPLE
    ON UPDATE CASCADE
    ON DELETE CASCADE
    NOT VALID;


ALTER TABLE IF EXISTS public."PUBL_AFFIL"
    ADD CONSTRAINT id_publ FOREIGN KEY (id_publ)
    REFERENCES public."PUBLICATION" (id_publ) MATCH SIMPLE
    ON UPDATE CASCADE
    ON DELETE CASCADE
    NOT VALID;


ALTER TABLE IF EXISTS public."PUBL_AFFIL"
    ADD CONSTRAINT id_affil FOREIGN KEY (id_affil)
    REFERENCES public."AFFILIATION" (id_affil) MATCH SIMPLE
    ON UPDATE CASCADE
    ON DELETE CASCADE
    NOT VALID;


ALTER TABLE IF EXISTS public."PUBL_BIBL"
    ADD CONSTRAINT id_publ FOREIGN KEY (id_publ)
    REFERENCES public."PUBLICATION" (id_publ) MATCH SIMPLE
    ON UPDATE CASCADE
    ON DELETE CASCADE
    NOT VALID;


ALTER TABLE IF EXISTS public."PUBL_BIBL"
    ADD CONSTRAINT id_bibl FOREIGN KEY (id_bibl)
    REFERENCES public."BIBLIOGRAPHY" (id_bibl) MATCH SIMPLE
    ON UPDATE CASCADE
    ON DELETE CASCADE
    NOT VALID;


ALTER TABLE IF EXISTS public."BIBL_AUTHOR"
    ADD CONSTRAINT id_bibl FOREIGN KEY (id_bibl)
    REFERENCES public."BIBLIOGRAPHY" (id_bibl) MATCH SIMPLE
    ON UPDATE CASCADE
    ON DELETE CASCADE
    NOT VALID;


ALTER TABLE IF EXISTS public."BIBL_AUTHOR"
    ADD CONSTRAINT id_author FOREIGN KEY (id_author)
    REFERENCES public."AUTHOR" (id_author) MATCH SIMPLE
    ON UPDATE CASCADE
    ON DELETE CASCADE
    NOT VALID;
	
ALTER TABLE IF EXISTS public."PUBLICATION"
    ADD CONSTRAINT id_user FOREIGN KEY (id_user)
    REFERENCES public."USER" (id_user) MATCH SIMPLE
    ON UPDATE CASCADE
    ON DELETE CASCADE
    NOT VALID;
	
ALTER TABLE IF EXISTS public."AUTHOR_AFFIL"
    ADD CONSTRAINT id_author FOREIGN KEY (id_author)
    REFERENCES public."AUTHOR" (id_author) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION
    NOT VALID;


ALTER TABLE IF EXISTS public."AUTHOR_AFFIL"
    ADD CONSTRAINT id_affil FOREIGN KEY (id_affil)
    REFERENCES public."AFFILIATION" (id_affil) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION
    NOT VALID;

END;
'''