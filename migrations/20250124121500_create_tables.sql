-- config is supposed to be immutable
-- Also, there can only be a single config at any time
CREATE TABLE config
(
    vbd_id       BLOB    NOT NULL CHECK (TYPEOF(vbd_id) == 'blob' AND
                                         LENGTH(vbd_id) == 16),
    cluster_size INTEGER NOT NULL CHECK (cluster_size == 256),
    block_size   INTEGER NOT NULL CHECK (block_size IN (16384, 65536, 262144)),
    content_hash TEXT    NOT NULL CHECK (content_hash IN ('TENT', 'BLAKE3', 'XXH3_128')),
    meta_hash    TEXT    NOT NULL CHECK (content_hash IN ('TENT', 'BLAKE3', 'XXH3_128'))
);

CREATE TRIGGER prevent_multiple_configs
    BEFORE INSERT
    ON config
    WHEN (SELECT COUNT(*)
          FROM config) >= 1
BEGIN
    SELECT RAISE(ABORT, 'Only one config entry allowed');
END;

CREATE TRIGGER prevent_config_updates
    BEFORE UPDATE
    ON config
BEGIN
    SELECT RAISE(ABORT, 'Config is immutable');
END;

CREATE TRIGGER prevent_config_deletes
    BEFORE DELETE
    ON config
BEGIN
    SELECT RAISE(ABORT, 'Config cannot be deleted');
END;

-- Keeps track of all known blocks, chunks & commits
-- Entries will be auto-deleted once they aren't referenced anymore
CREATE TABLE known_blocks
(
    block_id  BLOB    NOT NULL PRIMARY KEY CHECK (TYPEOF(block_id) == 'blob' AND
                                                  LENGTH(block_id) >= 16 AND
                                                  LENGTH(block_id <= 32)),
    used      INTEGER NOT NULL CHECK (used >= 0),
    available INTEGER NOT NULL CHECK (available >= 0)
);

-- Prevent Id changes
CREATE TRIGGER prevent_known_blocks_id_update
    BEFORE UPDATE
    ON known_blocks
    FOR EACH ROW
    WHEN NEW.block_id != OLD.block_id
BEGIN
    SELECT RAISE(ABORT, 'Updates to block_id columns are not allowed.');
END;

-- GC
CREATE TRIGGER delete_obsolete_blocks
    AFTER UPDATE
    ON known_blocks
    WHEN NEW.used = 0 AND NEW.available = 0
BEGIN
    DELETE FROM known_blocks WHERE block_id = NEW.block_id;
END;

CREATE TABLE known_clusters
(
    cluster_id BLOB    NOT NULL PRIMARY KEY CHECK (TYPEOF(cluster_id) == 'blob' AND
                                                   LENGTH(cluster_id) >= 16 AND
                                                   LENGTH(cluster_id <= 32)),
    used       INTEGER NOT NULL CHECK (used >= 0),
    available  INTEGER NOT NULL CHECK (available >= 0)
);

-- Prevent Id changes
CREATE TRIGGER prevent_known_cluster_id_update
    BEFORE UPDATE
    ON known_clusters
    FOR EACH ROW
    WHEN NEW.cluster_id != OLD.cluster_id
BEGIN
    SELECT RAISE(ABORT, 'Updates to cluster_id columns are not allowed.');
END;

-- GC
CREATE TRIGGER delete_obsolete_clusters
    AFTER UPDATE
    ON known_clusters
    WHEN NEW.used = 0 AND NEW.available = 0
BEGIN
    DELETE FROM known_clusters WHERE cluster_id = NEW.cluster_id;
END;

CREATE TABLE known_commits
(
    commit_id BLOB    NOT NULL PRIMARY KEY CHECK (TYPEOF(commit_id) == 'blob' AND
                                                  LENGTH(commit_id) >= 16 AND
                                                  LENGTH(commit_id <= 32)),
    used      INTEGER NOT NULL CHECK (used >= 0),
    available INTEGER NOT NULL CHECK (available >= 0)
);

-- Prevent Id changes
CREATE TRIGGER prevent_known_commits_id_update
    BEFORE UPDATE
    ON known_commits
    FOR EACH ROW
    WHEN NEW.commit_id != OLD.commit_id
BEGIN
    SELECT RAISE(ABORT, 'Updates to commit_id columns are not allowed.');
END;

-- GC
CREATE TRIGGER delete_obsolete_commits
    AFTER UPDATE
    ON known_commits
    WHEN NEW.used = 0 AND NEW.available = 0
BEGIN
    DELETE FROM known_commits WHERE commit_id = NEW.commit_id;
END;

CREATE TABLE cluster_content
(
    cluster_id  BLOB    NOT NULL CHECK (TYPEOF(cluster_id) == 'blob' AND
                                        LENGTH(cluster_id) >= 16 AND
                                        LENGTH(cluster_id <= 32)),
    block_index INTEGER NOT NULL CHECK (block_index >= 0),
    block_id    BLOB    NOT NULL CHECK (TYPEOF(block_id) == 'blob' AND
                                        LENGTH(block_id) >= 16 AND
                                        LENGTH(block_id <= 32)),

    FOREIGN KEY (cluster_id) REFERENCES known_clusters (cluster_id),
    FOREIGN KEY (block_id) REFERENCES known_blocks (block_id),

    UNIQUE (cluster_id, block_index)
);

-- Prevent Updates
CREATE TRIGGER prevent_cluster_content_update
    BEFORE UPDATE
    ON cluster_content
    FOR EACH ROW
BEGIN
    SELECT RAISE(ABORT, 'Updates to cluster_content are not allowed.');
END;

-- Reference Counting
CREATE TRIGGER increment_used_counters_before_cluster_content_block_insert
    BEFORE INSERT
    ON cluster_content
BEGIN
    -- Upsert known_blocks
    INSERT INTO known_blocks (block_id, used, available)
    VALUES (NEW.block_id, 1, 0)
    ON CONFLICT(block_id) DO UPDATE SET used = used + 1;
END;

CREATE TRIGGER decrement_used_counters_after_cluster_content_delete
    AFTER DELETE
    ON cluster_content
BEGIN
    UPDATE known_blocks
    SET used = used - 1
    WHERE block_id = OLD.block_id;
END;


CREATE TABLE commit_content
(
    commit_id     BLOB    NOT NULL CHECK (TYPEOF(commit_id) == 'blob' AND
                                          LENGTH(commit_id) >= 16 AND
                                          LENGTH(commit_id <= 32)),
    cluster_index INTEGER NOT NULL CHECK (cluster_index >= 0),
    cluster_id    BLOB    NOT NULL CHECK (TYPEOF(cluster_id) == 'blob' AND
                                          LENGTH(cluster_id) >= 16 AND
                                          LENGTH(cluster_id <= 32)),

    FOREIGN KEY (commit_id) REFERENCES known_commits (commit_id),
    FOREIGN KEY (cluster_id) REFERENCES known_clusters (cluster_id),

    UNIQUE (commit_id, cluster_index)
);

-- Prevent Updates
CREATE TRIGGER prevent_commit_content_update
    BEFORE UPDATE
    ON commit_content
    FOR EACH ROW
BEGIN
    SELECT RAISE(ABORT, 'Updates to commit_content are not allowed.');
END;

-- Reference Counting
CREATE TRIGGER increment_used_counters_before_commit_content_cluster_insert
    BEFORE INSERT
    ON commit_content
BEGIN
    -- Upsert known_clusters
    INSERT INTO known_clusters (cluster_id, used, available)
    VALUES (NEW.cluster_id, 1, 0)
    ON CONFLICT(cluster_id) DO UPDATE SET used = used + 1;
END;

CREATE TRIGGER decrement_used_counters_after_commit_content_delete
    AFTER DELETE
    ON commit_content
BEGIN
    UPDATE known_clusters
    SET used = used - 1
    WHERE cluster_id = OLD.cluster_id;
END;

CREATE TABLE branches
(
    name         TEXT    NOT NULL PRIMARY KEY COLLATE NOCASE CHECK (LENGTH(name) >= 1 AND
                                                                    LENGTH(name) <= 255 AND
                                                                    name GLOB '[A-Za-z0-9_-]*'),
    commit_id    BLOB    NOT NULL CHECK (TYPEOF(commit_id) == 'blob' AND
                                         LENGTH(commit_id) >= 16 AND
                                         LENGTH(commit_id <= 32)),
    num_clusters INTEGER NOT NULL CHECK (num_clusters > 0),

    FOREIGN KEY (commit_id) REFERENCES known_commits (commit_id)
);

-- Reference Counting
CREATE TRIGGER increment_used_counters_before_branch_insert
    BEFORE INSERT
    ON branches
BEGIN
    -- Upsert known_commits
    INSERT INTO known_commits (commit_id, used, available)
    VALUES (NEW.commit_id, 1, 0)
    ON CONFLICT(commit_id) DO UPDATE SET used = used + 1;
END;

CREATE TRIGGER increment_used_counters_before_branch_update
    BEFORE UPDATE
    ON branches
    WHEN NEW.commit_id != OLD.commit_id
BEGIN
    -- Upsert new commit_id
    INSERT INTO known_commits (commit_id, used, available)
    VALUES (NEW.commit_id, 1, 0)
    ON CONFLICT(commit_id) DO UPDATE SET used = used + 1;
END;

CREATE TRIGGER decrement_used_counters_after_branch_update
    AFTER UPDATE
    ON branches
    WHEN NEW.commit_id != OLD.commit_id
BEGIN
    -- Decrement old commit_id
    UPDATE known_commits
    SET used = used - 1
    WHERE commit_id = OLD.commit_id;
END;

CREATE TRIGGER decrement_used_counters_after_branch_delete
    AFTER DELETE
    ON branches
BEGIN
    UPDATE known_commits
    SET used = used - 1
    WHERE commit_id = OLD.commit_id;
END;

CREATE TABLE wal_files
(
    id   BLOB NOT NULL PRIMARY KEY CHECK (TYPEOF(id) == 'blob' AND
                                          LENGTH(id) == 16),
    etag BLOB NOT NULL CHECK (TYPEOF(id) == 'blob' AND
                              LENGTH(id) >= 8)
);

-- Prevent Id changes
CREATE TRIGGER prevent_wal_files_id_update
    BEFORE UPDATE
    ON wal_files
    FOR EACH ROW
    WHEN NEW.id != OLD.id
BEGIN
    SELECT RAISE(ABORT, 'Updates to id columns are not allowed.');
END;

CREATE TABLE wal_content
(
    wal_id         BLOB    NOT NULL CHECK (TYPEOF(wal_id) == 'blob' AND
                                           LENGTH(wal_id) == 16),
    file_offset    INTEGER NOT NULL CHECK (file_offset > 0),
    content_type   TEXT    NOT NULL CHECK (content_type IN ('B', 'C', 'S')),

    block_id       BLOB,
    cluster_id     BLOB,
    commit_id      BLOB,

    FOREIGN KEY (wal_id) REFERENCES wal_files (id) ON DELETE CASCADE,
    FOREIGN KEY (block_id) REFERENCES known_blocks (block_id),
    FOREIGN KEY (cluster_id) REFERENCES known_clusters (cluster_id),
    FOREIGN KEY (commit_id) REFERENCES known_commits (commit_id),

    UNIQUE (wal_id, file_offset),

    CHECK (
        (content_type = 'B' AND block_id IS NOT NULL AND cluster_id IS NULL AND commit_id IS NULL) OR
        (content_type = 'C' AND cluster_id IS NOT NULL AND block_id IS NULL AND commit_id IS NULL) OR
        (content_type = 'S' AND commit_id IS NOT NULL AND block_id IS NULL AND cluster_id IS NULL)
        )
);

-- Prevent Updates
CREATE TRIGGER prevent_wal_content_update
    BEFORE UPDATE
    ON wal_content
    FOR EACH ROW
BEGIN
    SELECT RAISE(ABORT, 'Updates to wal_content are not allowed.');
END;

-- Reference Counting
CREATE TRIGGER increment_used_counters_before_wal_content_block_insert
    BEFORE INSERT
    ON wal_content
    WHEN NEW.block_id IS NOT NULL
BEGIN
    -- Upsert known_blocks
    INSERT INTO known_blocks (block_id, used, available)
    VALUES (NEW.block_id, 0, 1)
    ON CONFLICT(block_id) DO UPDATE SET available = known_blocks.available + 1;
END;

CREATE TRIGGER increment_used_counters_before_wal_content_cluster_insert
    BEFORE INSERT
    ON wal_content
    WHEN NEW.cluster_id IS NOT NULL
BEGIN
    -- Upsert known_clusters
    INSERT INTO known_clusters (cluster_id, used, available)
    VALUES (NEW.cluster_id, 0, 1)
    ON CONFLICT(cluster_id) DO UPDATE SET available = available + 1;
END;

CREATE TRIGGER increment_used_counters_after_wal_content_commit_insert
    BEFORE INSERT
    ON wal_content
    WHEN NEW.commit_id IS NOT NULL
BEGIN
    -- Upsert known_commits
    INSERT INTO known_commits (commit_id, used, available)
    VALUES (NEW.commit_id, 0, 1)
    ON CONFLICT(commit_id) DO UPDATE SET available = available + 1;
END;

CREATE TRIGGER decrement_used_counters_after_wal_content_delete
    AFTER DELETE
    ON wal_content
BEGIN
    UPDATE known_blocks
    SET available = known_blocks.available - 1
    WHERE OLD.block_id IS NOT NULL
      AND block_id = OLD.block_id;

    UPDATE known_clusters
    SET available = known_clusters.available - 1
    WHERE OLD.cluster_id IS NOT NULL
      AND cluster_id = OLD.cluster_id;

    UPDATE known_commits
    SET available = known_commits.available - 1
    WHERE OLD.commit_id IS NOT NULL
      AND commit_id = OLD.commit_id;
END;