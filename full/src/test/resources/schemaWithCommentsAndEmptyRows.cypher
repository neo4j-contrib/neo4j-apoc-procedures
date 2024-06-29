CREATE FULLTEXT INDEX CustomerIndex1 FOR (n:Customer1) ON EACH [n.name1];
;
    // RETURN "Step 2" as row;
 CREATE FULLTEXT INDEX CustomerIndex21 FOR (n:Customer21) ON EACH [n.name12];
  // RETURN "Step 4" as row;

// RETURN "Step 5" as row; 

  // RETURN "Step 5" as row; // RETURN "Step 5" as row; 
  /*comment*/   CREATE FULLTEXT INDEX CustomerIndex231 FOR (n:Customer213) /* comment*/ ON EACH [n.name123];  
;
/*comment*/RETURN '8'/*comment*/;
/*comment*/CREATE INDEX node_index_name FOR (n:Person) ON (n.surname);/*comment*/