CREATE TABLE T_USER
(
    username   VARCHAR2(32),
    email      VARCHAR2(64) NOT NULL,
    role       VARCHAR2(6)  NOT NULL,
    status     VARCHAR2(6)  NOT NULL,
    created_on TIMESTAMP(6) NOT NULL,
    updated_on TIMESTAMP(6),
    updated_by VARCHAR2(32),
    CONSTRAINT user_u_pk PRIMARY KEY (username),
    CONSTRAINT user_email_check CHECK (email LIKE '%_@__%.__%')
);