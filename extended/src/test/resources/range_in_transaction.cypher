unwind range(1, 1000) as row
with row
CALL   {
WITH row
MERGE (g:AutoTransaction{symbol: row})
}   IN TRANSACTIONS OF 500 ROWS;