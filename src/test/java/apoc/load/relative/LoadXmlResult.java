package apoc.load.relative;

import apoc.util.Util;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class LoadXmlResult {

    public static Map StringXmlNestedSimpleMap (){

        List<Object> objectList = new ArrayList<>();
        Map<String, Object> map1 = Util.map("_type", "child", "name", "Neo4j", "_text", "Neo4j is a graph database");
        objectList.add(map1);
        List<Object> objectList1 = new ArrayList<>();
        Map<String, Object> map3 = Util.map("_type", "grandchild","name", "MySQL","_text", "MySQL is a database & relational");
        Map<String, Object> map4 = Util.map("_type", "grandchild", "name", "Postgres", "_text", "Postgres is a relational database");
        objectList1.add(map3);
        objectList1.add(map4);
        Map<String, Object> map2 = Util.map("_type", "child", "name", "relational", "_grandchild", objectList1);
        objectList.add(map2);
        Map<String, Object> map = Util.map("_type", "parent", "name", "databases", "_child", objectList);

        return map;
    }

    public static Map StringXmlNestedMap (){

        List<Object> objectList = new ArrayList<>();
        Map<String, Object> map1 = Util.map("_type", "child", "name", "Neo4j", "_text", "Neo4j is a graph database");
        objectList.add(map1);
        List<Object> objectList1 = new ArrayList<>();
        Map<String, Object> map3 = Util.map("_type", "grandchild","name", "MySQL", "_text", "MySQL is a database & relational");
        Map<String, Object> map4 = Util.map("_type", "grandchild","name", "Postgres", "_text", "Postgres is a relational database");
        objectList1.add(map3);
        objectList1.add(map4);
        Map<String, Object> map2 = Util.map("_type", "child", "name", "relational", "_children", objectList1);
        objectList.add(map2);
        Map<String, Object> map = Util.map("_type", "parent", "name", "databases", "_children", objectList);

        return map;
    }

}
