--DROP TABLE IF EXISTS likes;
--DROP TABLE IF EXISTS following;
--DROP TABLE IF EXISTS shorts;
--DROP TABLE IF EXISTS users;


CREATE TABLE IF NOT EXISTS users (
    id VARCHAR PRIMARY KEY,
    pwd VARCHAR NOT NULL,
    email VARCHAR UNIQUE NOT NULL,
    displayName VARCHAR NOT NULL
);

CREATE TABLE IF NOT EXISTS shorts (
    id VARCHAR PRIMARY KEY,
    ownerId VARCHAR NOT NULL,
    blobUrl VARCHAR NOT NULL,
    timestamp BIGINT NOT NULL,
    totalLikes INT DEFAULT 0,
    CONSTRAINT fk_ownerId FOREIGN KEY (ownerId) REFERENCES users(id)
);

CREATE TABLE IF NOT EXISTS following (
    id VARCHAR PRIMARY KEY,
    follower VARCHAR NOT NULL,
    followee VARCHAR NOT NULL,
    CONSTRAINT fk_follower FOREIGN KEY (follower) REFERENCES users(id),
    CONSTRAINT fk_followee FOREIGN KEY (followee) REFERENCES users(id)
);

CREATE TABLE IF NOT EXISTS likes (
    id VARCHAR PRIMARY KEY,
    userId VARCHAR NOT NULL,
    shortId VARCHAR NOT NULL,
    ownerId VARCHAR NOT NULL,
    CONSTRAINT fk_user_id FOREIGN KEY (userId) REFERENCES users(id),
    CONSTRAINT fk_short_id FOREIGN KEY (shortId) REFERENCES shorts(id),
    CONSTRAINT fk_ownerId FOREIGN KEY (ownerId) REFERENCES users(id)
);