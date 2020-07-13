-- Copyright 2004-2020 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (https://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

CREATE TABLE TEST(A INT, B INT);
> ok

INSERT INTO TEST VALUES (1, 2);
> update count: 1

UPDATE TEST SET (A, B) = (3, 4);
> update count: 1

SELECT * FROM TEST;
> A B
> - -
> 3 4
> rows: 1

UPDATE TEST SET (B) = 5;
> update count: 1

SELECT B FROM TEST;
>> 5

UPDATE TEST SET (B) = ROW (6);
> update count: 1

SELECT B FROM TEST;
>> 6

UPDATE TEST SET (B) = (7);
> update count: 1

SELECT B FROM TEST;
>> 7

UPDATE TEST SET (B) = (2, 3);
> exception COLUMN_COUNT_DOES_NOT_MATCH

-- TODO
-- UPDATE TEST SET (A, B) = ARRAY[3, 4];
-- > exception COLUMN_COUNT_DOES_NOT_MATCH

EXPLAIN UPDATE TEST SET (A) = ROW(3), B = 4;
>> UPDATE "PUBLIC"."TEST" /* PUBLIC.TEST.tableScan */ SET "A" = 3, "B" = 4

EXPLAIN UPDATE TEST SET A = 3, (B) = 4;
>> UPDATE "PUBLIC"."TEST" /* PUBLIC.TEST.tableScan */ SET "A" = 3, "B" = 4

UPDATE TEST SET (A, B) = (1, 2), (B, A) = (2, 1);
> exception DUPLICATE_COLUMN_NAME_1

UPDATE TEST SET (A) = A * 3;
> update count: 1

DROP TABLE TEST;
> ok

CREATE TABLE TEST(ID INT) AS VALUES 100;
> ok

-- _ROWID_ modifications are not allowed
UPDATE TEST SET _ROWID_ = 2 WHERE ID = 100;
> exception SYNTAX_ERROR_2

DROP TABLE TEST;
> ok

CREATE TABLE TEST(A INT, B INT GENERATED ALWAYS AS (A + 1));
> ok

INSERT INTO TEST(A) VALUES 1;
> update count: 1

UPDATE TEST SET A = 2, B = DEFAULT;
> update count: 1

TABLE TEST;
> A B
> - -
> 2 3
> rows: 1

DROP TABLE TEST;
> ok

CREATE TABLE TEST(A INT, B INT GENERATED ALWAYS AS (A + 1));
> ok

INSERT INTO TEST(A) VALUES 1;
> update count: 1

UPDATE TEST SET B = 1;
> exception GENERATED_COLUMN_CANNOT_BE_ASSIGNED_1

UPDATE TEST SET B = DEFAULT;
> update count: 1

DROP TABLE TEST;
> ok

CREATE TABLE TEST(ID INT PRIMARY KEY, A INT, B INT, C INT, D INT, E INT, F INT) AS VALUES (1, 1, 1, 1, 1, 1, 1);
> ok

EXPLAIN UPDATE TEST SET
    (F, C, A) = (SELECT 2, 3, 4 FROM TEST FETCH FIRST ROW ONLY),
    (B, E) = (SELECT 5, 6 FROM TEST FETCH FIRST ROW ONLY)
    WHERE ID = 1;
#+mvStore#>> UPDATE "PUBLIC"."TEST" /* PUBLIC.PRIMARY_KEY_2: ID = 1 */ SET ("F", "C", "A") = (SELECT 2, 3, 4 FROM "PUBLIC"."TEST" /* PUBLIC.TEST.tableScan */ FETCH FIRST ROW ONLY), ("B", "E") = (SELECT 5, 6 FROM "PUBLIC"."TEST" /* PUBLIC.TEST.tableScan */ FETCH FIRST ROW ONLY) WHERE "ID" = 1
#-mvStore#>> UPDATE "PUBLIC"."TEST" /* PUBLIC.PRIMARY_KEY_2: ID = 1 */ SET ("F", "C", "A") = (SELECT 2, 3, 4 FROM "PUBLIC"."TEST" /* PUBLIC.PRIMARY_KEY_2 */ FETCH FIRST ROW ONLY), ("B", "E") = (SELECT 5, 6 FROM "PUBLIC"."TEST" /* PUBLIC.PRIMARY_KEY_2 */ FETCH FIRST ROW ONLY) WHERE "ID" = 1

UPDATE TEST SET
    (F, C, A) = (SELECT 2, 3, 4 FROM TEST FETCH FIRST ROW ONLY),
    (B, E) = (SELECT 5, 6 FROM TEST FETCH FIRST ROW ONLY)
    WHERE ID = 1;
> update count: 1

TABLE TEST;
> ID A B C D E F
> -- - - - - - -
> 1  4 5 3 1 6 2
> rows: 1

UPDATE TEST SET (C, C) = (SELECT 1, 2 FROM TEST);
> exception DUPLICATE_COLUMN_NAME_1

UPDATE TEST SET (A, B) = (SELECT 1, 2, 3 FROM TEST);
> exception COLUMN_COUNT_DOES_NOT_MATCH

UPDATE TEST SET (D, E) = NULL;
> exception DATA_CONVERSION_ERROR_1

DROP TABLE TEST;
> ok

CREATE TABLE TEST(ID BIGINT GENERATED ALWAYS AS IDENTITY, ID2 BIGINT GENERATED ALWAYS AS (ID + 1),
    V INT, U INT ON UPDATE (5));
> ok

INSERT INTO TEST(V) VALUES 1;
> update count: 1

TABLE TEST;
> ID ID2 V U
> -- --- - ----
> 1  2   1 null
> rows: 1

UPDATE TEST SET V = V + 1;
> update count: 1

UPDATE TEST SET V = V + 1, ID = DEFAULT, ID2 = DEFAULT;
> update count: 1

TABLE TEST;
> ID ID2 V U
> -- --- - -
> 1  2   3 5
> rows: 1

MERGE INTO TEST USING (VALUES 1) T(X) ON TRUE WHEN MATCHED THEN UPDATE SET V = V + 1;
> update count: 1

MERGE INTO TEST USING (VALUES 1) T(X) ON TRUE WHEN MATCHED THEN UPDATE SET V = V + 1, ID = DEFAULT, ID2 = DEFAULT;
> update count: 1

TABLE TEST;
> ID ID2 V U
> -- --- - -
> 1  2   5 5
> rows: 1

MERGE INTO TEST KEY(V) VALUES (DEFAULT, DEFAULT, 5, 1);
> update count: 1

TABLE TEST;
> ID ID2 V U
> -- --- - -
> 1  2   5 1
> rows: 1

DROP TABLE TEST;
> ok
