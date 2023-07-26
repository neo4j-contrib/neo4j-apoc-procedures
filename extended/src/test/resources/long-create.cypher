UNWIND RANGE(0,99999) as id
CREATE (n:NodeOne {id:id});

UNWIND RANGE(0,99999) as id
CREATE (n:NodeTwo {id:id});
