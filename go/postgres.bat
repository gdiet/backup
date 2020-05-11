docker run --rm --name postgres -e POSTGRES_PASSWORD=password -p 5432:5432 -d postgres:12.2-alpine
exit /B

user postgres
password password
database postgres
port 5432

docker exec -it postgres psql -U postgres

set PGUSER=postgres
set PGPASSWORD=password
go build todo.go

create table tasks (
  id serial primary key,
  description text not null
);


CREATE SEQUENCE idSeq START WITH 1;

CREATE TABLE DataEntries (
  id     BIGINT NOT NULL,
  start  BIGINT NOT NULL,
  stop   BIGINT NOT NULL,
  hash   BYTEA NOT NULL,
  CONSTRAINT pk_DataEntries PRIMARY KEY (id)
);

CREATE TABLE TreeEntries (
  id           BIGINT NOT NULL,
  parentId     BIGINT NOT NULL,
  name         VARCHAR(255) NOT NULL,
  time         BIGINT NOT NULL,
  deleted      BIGINT NOT NULL DEFAULT 0,
  dataId       BIGINT DEFAULT NULL,
  CONSTRAINT pk_TreeEntries PRIMARY KEY (id),
  CONSTRAINT un_TreeEntries UNIQUE (parentId, name, deleted),
  CONSTRAINT fk_TreeEntries_parentId FOREIGN KEY (parentId) REFERENCES TreeEntries(id)
);

INSERT INTO TreeEntries (id, parentId, name, time) VALUES (0, 0, '', 0);

select * from treeentries;
