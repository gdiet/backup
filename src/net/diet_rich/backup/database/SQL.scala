// Copyright (c) 2011 Georg Dietrich
// Licensed under the MIT license: http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.backup.database
import net.diet_rich.util.io.SectionDataFile

object SQL {

  lazy val sectionsWithComments = SectionDataFile.getSections(sql.split("[\r\n]+").toIterator)
  lazy val sections = sectionsWithComments.mapValues(SectionDataFile.removeComments(_))
  lazy val sectionsWithConstraints = {
    val constraintKeys = sections.keys.filter(_.endsWith(" constraints"))
    val constraintMap = constraintKeys.map( key => (key, sections(key).mkString("\n")) ).toMap
    val noConstraintsMap = sections.filterKeys(!_.endsWith(" constraints"))
    noConstraintsMap.mapValues(SectionDataFile.insertVariables(_, constraintMap))
  }

  private val sql =
    """
[create tables]
CREATE TABLE RepositoryInfo (
    key    VARCHAR(32) PRIMARY KEY,
    value  VARCHAR(256) NOT NULL
);
[initialize tables]
// EVENTUALLY use status on loading the database
INSERT INTO RepositoryInfo (key, value) VALUES ( 'status', 'shut down' );
// EVENTUALLY use
INSERT INTO RepositoryInfo (key, value) VALUES ( 'version', '1' );
// EVENTUALLY use
INSERT INTO RepositoryInfo (key, value) VALUES ( 'hash', 'MD5' );

[create tables]
CREATE TABLE Entries (
    id          BIGINT PRIMARY KEY,
    parent      BIGINT NULL,                        // NULL for root only
    name        VARCHAR(256) NOT NULL,
    type        VARCHAR(4) DEFAULT 'DIR' NOT NULL,  // DIR or FILE
    deleted     BOOLEAN DEFAULT FALSE NOT NULL,
    deleteTime  BIGINT DEFAULT 0 NOT NULL           // timestamp if marked deleted, else 0
${Entries constraints}
);
[initialize tables]
INSERT INTO Entries (id, parent, name) VALUES ( 0, NULL, '' );

[Entries constraints]
  , FOREIGN KEY (parent) REFERENCES Entries(id) // reference integrity of parent
  , UNIQUE (parent, name, deleted, deleteTime)  // unique entries only
  , CHECK (parent != id)                        // no self references
  , CHECK (deleted OR deleteTime = 0)           // defined deleted status
  , CHECK ((id = 0) = (parent IS NULL))         // only one root
  , CHECK (type = 'DIR' OR type = 'FILE')       // no other types yet

[create tables]
CREATE TABLE StoredData (
    id     BIGINT PRIMARY KEY,
    size   BIGINT NOT NULL,             // entry size (uncompressed)
    header BIGINT NOT NULL,
    hash   VARBINARY(16) NOT NULL,      // EVENTUALLY make configurable: MD5: 16, SHA-256: 64
    usage  INTEGER DEFAULT 0 NOT NULL,  // usage count
    method VARCHAR(8) NOT NULL          // store method (e.g. PLAIN, DEFLATE)
${StoredData constraints}
);

[StoredData constraints]
  , CHECK (size >= 0)
  , CHECK (usage >= 0)
  , UNIQUE (size, header, hash)

[create tables]
CREATE TABLE Files (
    id      BIGINT UNIQUE NOT NULL,
    time    BIGINT NOT NULL,
    data    BIGINT NOT NULL
${Files constraints}
);

[Files constraints]
  , FOREIGN KEY (id) REFERENCES Entries(id)
  , FOREIGN KEY (data) REFERENCES StoredData(id)

[create tables]
CREATE TABLE ByteStore (
    id    BIGINT NULL,      // reference to StoredData#id or NULL if free
    index INTEGER NOT NULL, // data part index
    start BIGINT NOT NULL,  // data part start position
    fin   BIGINT NOT NULL   // data part end position + 1
${ByteStore constraints}
);

[ByteStore constraints]
  , UNIQUE (start)
  , UNIQUE (fin)
  , FOREIGN KEY (id) REFERENCES StoredData(id)
  , CHECK (fin > start AND start >= 0)
// EVENTUALLY check that start has matching fin or is 0

[END OF FILE]
    """
  
}


/*
[create tables]
CREATE TABLE dataentries (
    pk     BIGINT IDENTITY,       // entry key
    size   BIGINT,                // entry size
    header BIGINT,                // header checksum
    hash   BINARY(${hashlength})  // entry hash
    ${dataentries constraints}    // , UNIQUE (size, header, hash)
);

[dataentries constraints]
, UNIQUE (size, header, hash)

[create tables]
CREATE INDEX dataentries_size   ON dataentries (size);
CREATE INDEX dataentries_header ON dataentries (header);
CREATE INDEX dataentries_hash   ON dataentries (hash);

CREATE TABLE datachunks (
    key BIGINT,               // entry key
    part INTEGER,             // serial number of entry part
    size BIGINT,              // entry part size
    fileid BIGINT,            // data file ID
    location INTEGER,         // location in data file
    deleted BOOLEAN           // deleted flag
    ${datachunks constraints} // , FOREIGN KEY (key) REFERENCES dataentries(pk)
);

[datachunks constraints]
, FOREIGN KEY (key) REFERENCES dataentries(pk)

[create tables]
CREATE INDEX datachunks_key     ON datachunks (key);
CREATE INDEX datachunks_fileid  ON datachunks (fileid);
CREATE INDEX datachunks_deleted ON datachunks (deleted);
*/