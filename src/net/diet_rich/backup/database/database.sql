// Copyright (c) 2011 Georg Dietrich
// Licensed under the MIT license: http://www.opensource.org/licenses/mit-license.php

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
