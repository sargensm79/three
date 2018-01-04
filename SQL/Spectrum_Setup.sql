set autocommit = 1;

select date_format(date_add(curdate(), interval -1 day), '%Y%m%d') into @prev_cd;

DROP TABLE IF EXISTS spec_updates;
create temporary table spec_updates (key (profileID)) as
SELECT 
    spec_updates.*, profiles.creation
FROM
    (SELECT
        profileID, max(updateID) as updateID
    FROM
        profileSpectrums
    WHERE
        cohort = @prev_cd
#	cohort BETWEEN '20171109' AND '20180101'
        and parentName like 'demographics%'
        group by profileID) AS spec_updates
        INNER JOIN
    profiles ON profiles.profileID = spec_updates.profileID
WHERE
    application = 'threeuk';
    

DROP TABLE IF EXISTS spec_keys;
create table spec_keys (
        foreignKey varchar(128),
        profileID varchar(64),
        updateID bigint,
        creation bigint,
        key (foreignKey, updateID));

INSERT INTO spec_keys
SELECT 
    foreignKey, su.profileID, updateID, su.creation
FROM
    spec_updates su
        INNER JOIN
    foreignKeys ON foreignKeys.profileID = su.profileID
        WHERE CHAR_LENGTH(foreignKey) > 15
        AND cohortDay > '20171108'
#UNION
#select foreignKey, foreignKeys.profileID, max(updateID) as updateID, min(profiles.creation) as creation
#from reporting.foreignKeys
#inner join reporting.profileSpectrums
#on profileSpectrums.profileID = foreignKeys.profileID
#inner join reporting.profiles
#on profiles.profileID = foreignKeys.profileID
#where foreignKey in (
#'ca48851ae038c470d79ef70a74253a44a54e002e84b251b05d2ae61a3835514c'
#)
#group by foreignKey, foreignKeys.profileID
;

DROP TABLE IF EXISTS key_creation;
create table key_creation (
        foreignKey varchar(128),
        profileID varchar(64),
        creation bigint,
        updateID bigint,
key (profileID, updateID));

DROP TABLE IF EXISTS kc;
CREATE TABLE kc LIKE key_creation;

INSERT INTO key_creation
SELECT 
    sk.foreignKey, profileID, sk.creation, sk.updateID
FROM
    (SELECT 
        foreignKey,
            MIN(creation) AS creation,
            MAX(updateID) AS updateID
    FROM
        spec_keys
    GROUP BY foreignKey) AS sk
        INNER JOIN
    spec_keys ON spec_keys.foreignKey = sk.foreignKey
        AND spec_keys.updateID = sk.updateID;

DROP TABLE spec_keys;

SELECT * FROM key_creation
