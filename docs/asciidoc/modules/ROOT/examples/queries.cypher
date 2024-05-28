// tag::constraint[]
CREATE CONSTRAINT n10s_unique_uri ON (r:Resource) ASSERT r.uri IS UNIQUE;
// end::constraint[]

// tag::init[]
CALL n10s.graphconfig.init({handleVocabUris: "MAP"});
// end::init[]

// tag::mappings[]
call n10s.nsprefixes.add('neo','neo4j://voc#');
CALL n10s.mapping.add("neo4j://voc#subCatOf","SUB_CAT_OF");
CALL n10s.mapping.add("neo4j://voc#about","ABOUT");
// end::mappings[]

// tag::software-systems[]
WITH "https://query.wikidata.org/sparql?query=prefix%20neo%3A%20%3Cneo4j%3A%2F%2Fvoc%23%3E%20%0A%23Cats%0A%23SELECT%20%3Fitem%20%3Flabel%20%0ACONSTRUCT%20%7B%0A%3Fitem%20a%20neo%3ACategory%20%3B%20neo%3AsubCatOf%20%3FparentItem%20.%20%20%0A%20%20%3Fitem%20neo%3Aname%20%3Flabel%20.%0A%20%20%3FparentItem%20a%20neo%3ACategory%3B%20neo%3Aname%20%3FparentLabel%20.%0A%20%20%3Farticle%20a%20neo%3AWikipediaPage%3B%20neo%3Aabout%20%3Fitem%20%3B%0A%20%20%20%20%20%20%20%20%20%20%20%0A%7D%0AWHERE%20%0A%7B%0A%20%20%3Fitem%20(wdt%3AP31%7Cwdt%3AP279)*%20wd%3AQ2429814%20.%0A%20%20%3Fitem%20wdt%3AP31%7Cwdt%3AP279%20%3FparentItem%20.%0A%20%20%3Fitem%20rdfs%3Alabel%20%3Flabel%20.%0A%20%20filter(lang(%3Flabel)%20%3D%20%22en%22)%0A%20%20%3FparentItem%20rdfs%3Alabel%20%3FparentLabel%20.%0A%20%20filter(lang(%3FparentLabel)%20%3D%20%22en%22)%0A%20%20%0A%20%20OPTIONAL%20%7B%0A%20%20%20%20%20%20%3Farticle%20schema%3Aabout%20%3Fitem%20%3B%0A%20%20%20%20%20%20%20%20%20%20%20%20schema%3AinLanguage%20%22en%22%20%3B%0A%20%20%20%20%20%20%20%20%20%20%20%20schema%3AisPartOf%20%3Chttps%3A%2F%2Fen.wikipedia.org%2F%3E%20.%0A%20%20%20%20%7D%0A%20%20%0A%7D" AS softwareSystemsUri
CALL n10s.rdf.import.fetch(softwareSystemsUri, 'Turtle' , { headerParams: { Accept: "application/x-turtle" } })
YIELD terminationStatus, triplesLoaded, triplesParsed, namespaces, callParams
RETURN terminationStatus, triplesLoaded, triplesParsed, namespaces, callParams;
// end::software-systems[]

// tag::programming-languages[]
WITH "https://query.wikidata.org/sparql?query=prefix%20neo%3A%20%3Cneo4j%3A%2F%2Fvoc%23%3E%20%0A%23Cats%0A%23SELECT%20%3Fitem%20%3Flabel%20%0ACONSTRUCT%20%7B%0A%3Fitem%20a%20neo%3ACategory%20%3B%20neo%3AsubCatOf%20%3FparentItem%20.%20%20%0A%20%20%3Fitem%20neo%3Aname%20%3Flabel%20.%0A%20%20%3FparentItem%20a%20neo%3ACategory%3B%20neo%3Aname%20%3FparentLabel%20.%0A%20%20%3Farticle%20a%20neo%3AWikipediaPage%3B%20neo%3Aabout%20%3Fitem%20%3B%0A%20%20%20%20%20%20%20%20%20%20%20%0A%7D%0AWHERE%20%0A%7B%0A%20%20%3Fitem%20(wdt%3AP31%7Cwdt%3AP279)*%20wd%3AQ9143%20.%0A%20%20%3Fitem%20wdt%3AP31%7Cwdt%3AP279%20%3FparentItem%20.%0A%20%20%3Fitem%20rdfs%3Alabel%20%3Flabel%20.%0A%20%20filter(lang(%3Flabel)%20%3D%20%22en%22)%0A%20%20%3FparentItem%20rdfs%3Alabel%20%3FparentLabel%20.%0A%20%20filter(lang(%3FparentLabel)%20%3D%20%22en%22)%0A%20%20%0A%20%20OPTIONAL%20%7B%0A%20%20%20%20%20%20%3Farticle%20schema%3Aabout%20%3Fitem%20%3B%0A%20%20%20%20%20%20%20%20%20%20%20%20schema%3AinLanguage%20%22en%22%20%3B%0A%20%20%20%20%20%20%20%20%20%20%20%20schema%3AisPartOf%20%3Chttps%3A%2F%2Fen.wikipedia.org%2F%3E%20.%0A%20%20%20%20%7D%0A%20%20%0A%7D" AS programmingLanguagesUri
CALL n10s.rdf.import.fetch(programmingLanguagesUri, 'Turtle' , { headerParams: { Accept: "application/x-turtle" } })
YIELD terminationStatus, triplesLoaded, triplesParsed, namespaces, callParams
RETURN terminationStatus, triplesLoaded, triplesParsed, namespaces, callParams;
// end::programming-languages[]

// tag::data-formats[]
WITH "https://query.wikidata.org/sparql?query=prefix%20neo%3A%20%3Cneo4j%3A%2F%2Fvoc%23%3E%20%0A%23Cats%0A%23SELECT%20%3Fitem%20%3Flabel%20%0ACONSTRUCT%20%7B%0A%3Fitem%20a%20neo%3ACategory%20%3B%20neo%3AsubCatOf%20%3FparentItem%20.%20%20%0A%20%20%3Fitem%20neo%3Aname%20%3Flabel%20.%0A%20%20%3FparentItem%20a%20neo%3ACategory%3B%20neo%3Aname%20%3FparentLabel%20.%0A%20%20%3Farticle%20a%20neo%3AWikipediaPage%3B%20neo%3Aabout%20%3Fitem%20%3B%0A%20%20%20%20%20%20%20%20%20%20%20%0A%7D%0AWHERE%20%0A%7B%0A%20%20%3Fitem%20(wdt%3AP31%7Cwdt%3AP279)*%20wd%3AQ24451526%20.%0A%20%20%3Fitem%20wdt%3AP31%7Cwdt%3AP279%20%3FparentItem%20.%0A%20%20%3Fitem%20rdfs%3Alabel%20%3Flabel%20.%0A%20%20filter(lang(%3Flabel)%20%3D%20%22en%22)%0A%20%20%3FparentItem%20rdfs%3Alabel%20%3FparentLabel%20.%0A%20%20filter(lang(%3FparentLabel)%20%3D%20%22en%22)%0A%20%20%0A%20%20OPTIONAL%20%7B%0A%20%20%20%20%20%20%3Farticle%20schema%3Aabout%20%3Fitem%20%3B%0A%20%20%20%20%20%20%20%20%20%20%20%20schema%3AinLanguage%20%22en%22%20%3B%0A%20%20%20%20%20%20%20%20%20%20%20%20schema%3AisPartOf%20%3Chttps%3A%2F%2Fen.wikipedia.org%2F%3E%20.%0A%20%20%20%20%7D%0A%20%20%0A%7D" AS dataFormatsUri
CALL n10s.rdf.import.fetch(dataFormatsUri, 'Turtle' , { headerParams: { Accept: "application/x-turtle" } })
YIELD terminationStatus, triplesLoaded, triplesParsed, namespaces, callParams
RETURN terminationStatus, triplesLoaded, triplesParsed, namespaces, callParams;
// end::data-formats[]

// tag::version-control[]
MATCH path = (c:Category {name: "version control system"})<-[:SUB_CAT_OF*]-(child)
RETURN path
LIMIT 25;
// end::version-control[]


// tag::dev-to-articles[]
LOAD CSV WITH HEADERS FROM 'https://github.com/neo4j-examples/nlp-knowledge-graph/raw/master/import/articles.csv' AS row
RETURN row
LIMIT 10;
// end::dev-to-articles[]

// tag::dev-to-import[]
CALL apoc.periodic.iterate(
  "LOAD CSV WITH HEADERS FROM 'https://github.com/neo4j-examples/nlp-knowledge-graph/raw/master/import/articles.csv' AS row
   RETURN row",
  "MERGE (a:Article {uri: row.uri})
   WITH a
   CALL apoc.load.html(a.uri, {
     body: 'body div.spec__body p',
     title: 'h1',
     time: 'time'
   })
   YIELD value
   UNWIND value.body AS item
   WITH a,
        apoc.text.join(collect(item.text), '') AS body,
        value.title[0].text AS title,
        value.time[0].attributes.datetime AS date
   SET a.body = body , a.title = title, a.datetime = datetime(date)",
  {batchSize: 5, parallel: true}
)
YIELD batches, total, timeTaken, committedOperations
RETURN batches, total, timeTaken, committedOperations;
// end::dev-to-import[]

// tag::set-key[]
:params key => ("<insert-key-here>")
// end::set-key[]

// tag::nlp-import[]
CALL apoc.periodic.iterate(
  "MATCH (a:Article)
   WHERE not(exists(a.processed))
   RETURN a",
  "CALL apoc.nlp.gcp.entities.stream([item in $_batch | item.a], {
     nodeProperty: 'body',
     key: $key
   })
   YIELD node, value
   SET node.processed = true
   WITH node, value
   UNWIND value.entities AS entity
   WITH entity, node
   WHERE not(entity.metadata.wikipedia_url is null)
   MERGE (page:Resource {uri: entity.metadata.wikipedia_url})
   SET page:WikipediaPage
   MERGE (node)-[:HAS_ENTITY]->(page)",
  {batchMode: "BATCH_SINGLE", batchSize: 10, params: {key: $key}})
YIELD batches, total, timeTaken, committedOperations
RETURN batches, total, timeTaken, committedOperations;
// end::nlp-import[]

// tag::semantic-search[]
MATCH (c:Category {name: "NoSQL database management system"})
CALL n10s.inference.nodesInCategory(c, {
  inCatRel: "ABOUT",
  subCatRel: "SUB_CAT_OF"
})
YIELD node
MATCH (node)<-[:HAS_ENTITY]-(article)
RETURN article.uri AS uri, article.title AS title, article.datetime AS date,
       collect(n10s.rdf.getIRILocalName(node.uri))  as explicitTopics
ORDER BY date DESC
LIMIT 5;
// end::semantic-search[]

// tag::similar-articles-1[]
MATCH (a:Article {uri: "https://dev.to/qainsights/performance-testing-neo4j-database-using-bolt-protocol-in-apache-jmeter-1oa9"}),
      path = (a)-[:HAS_ENTITY]->(wiki)-[:ABOUT]->(cat),
      otherPath = (wiki)<-[:HAS_ENTITY]-(other)
return path, otherPath;
// end::similar-articles-1[]

// tag::similar-articles-2[]
MATCH (a:Article {uri: "https://dev.to/qainsights/performance-testing-neo4j-database-using-bolt-protocol-in-apache-jmeter-1oa9"}),
      entityPath = (a)-[:HAS_ENTITY]->(wiki)-[:ABOUT]->(cat),
      path = (cat)-[:SUB_CAT_OF]->(parent)<-[:SUB_CAT_OF]-(otherCat),
      otherEntityPath = (otherCat)<-[:ABOUT]-(otherWiki)<-[:HAS_ENTITY]-(other)
RETURN other.title, other.uri,
       [(other)-[:HAS_ENTITY]->()-[:ABOUT]->(entity) | entity.name] AS otherCategories,
       collect([node in nodes(path) | node.name]) AS pathToOther;
// end::similar-articles-2[]

// tag::custom-ontology-init[]
CALL n10s.nsprefixes.add('owl','http://www.w3.org/2002/07/owl#');
CALL n10s.nsprefixes.add('rdfs','http://www.w3.org/2000/01/rdf-schema#');
CALL n10s.mapping.add("http://www.w3.org/2000/01/rdf-schema#subClassOf","SUB_CAT_OF");
CALL n10s.mapping.add("http://www.w3.org/2000/01/rdf-schema#label","name");
CALL n10s.mapping.add("http://www.w3.org/2002/07/owl#Class","Category");
// end::custom-ontology-init[]

// tag::custom-ontology-preview[]
CALL n10s.rdf.preview.fetch("http://www.nsmntx.org/2020/08/swStacks","Turtle");
// end::custom-ontology-preview[]

// tag::custom-ontology-import[]
CALL n10s.rdf.import.fetch("http://www.nsmntx.org/2020/08/swStacks","Turtle")
YIELD terminationStatus, triplesLoaded, triplesParsed, namespaces, callParams
RETURN terminationStatus, triplesLoaded, triplesParsed, namespaces, callParams;
// end::custom-ontology-import[]


// tag::zookeeper[]
match path = (c:WikipediaPage)-[:ABOUT]->(category)-[:SUB_CAT_OF*]->(:Category {name: "NoSQL database management system"})
where c.uri contains "Apache_ZooKeeper"
RETURN path;
// end::zookeeper[]