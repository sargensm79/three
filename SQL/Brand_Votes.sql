set autocommit = 1;

DROP TABLE IF EXISTS bvotes;

create temporary table bvotes (key (profileID), key(brandID)) as
    SELECT 
        bv.profileID, bv.brandID, vote
    FROM
        (SELECT 
            kc.profileID, brandID, MAX(brandVotes.updateID) AS updateID
        FROM
            kc
        INNER JOIN brandVotes ON brandVotes.profileID = kc.profileID
            AND brandVotes.updateID <= kc.updateID
        GROUP BY kc.profileID , brandID) AS bv
            INNER JOIN
        brandVotes ON brandVotes.profileID = bv.profileID
            AND brandVotes.brandID = bv.brandID
            AND brandVotes.updateID = bv.updateID;

DROP TABLE IF EXISTS bn;
create temporary table bn (primary key (brandID)) as
SELECT DISTINCT
    brandID, name
FROM
    brands
WHERE
    application = 'threeuk';

SELECT 
    foreignKey AS 'Foreign Key',
    CONCAT('votes:', LOWER(name)) AS name,
    vote
FROM
    bvotes
        INNER JOIN
    kc ON kc.profileID = bvotes.profileID
        INNER JOIN
    bn ON bn.brandID = bvotes.brandID
