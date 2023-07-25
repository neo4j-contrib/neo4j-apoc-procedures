UNWIND RANGE(0,99999) as id
CREATE (n:NodeOne {id:id});

UNWIND RANGE(0,99999) as id
CREATE (n:NodeTwo {id:id});

UNWIND RANGE(0,99999) as id
CREATE (n:NodeThree {id:id});

UNWIND RANGE(0,99999) as id
CREATE (n:NodeFour {id:id});

UNWIND RANGE(0,99999) as id
CREATE (n:NodeFive {id:id});

UNWIND RANGE(0,99999) as id
CREATE (n:NodeSix {id:id});

UNWIND RANGE(0,99999) as id
CREATE (n:NodeSeven {id:id});
