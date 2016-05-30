package apoc.algo;

import java.util.Map;
import java.util.stream.Stream;

import org.apache.commons.codec.language.Metaphone;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Procedure;
import org.neo4j.procedure.PerformsWrites;

import apoc.Description;
import apoc.result.Empty;
import apoc.util.PerformanceLoggerSingleton;

public class EntityResolution {

	@Context
	public GraphDatabaseService db;

	@Context
	public Log log;

	private static final String query = "MATCH (n:Record)-->(l)<--(m:Record) where n <> m RETURN "
			+ "n.idKey as idKeya, n.name as namea, n.surname as surnamea,n.source as sourcea, "
			+ "m.idKey as idKeyb, m.name as nameb, m.surname as surnameb,m.source as sourceb";

	@Procedure("apoc.algo.ER")
	@Description("CALL apoc.algo.ER()")
	@PerformsWrites
	public Stream<Empty> ER() {
		Transaction tx = db.beginTx();
		try {
			log.info("starting ER");
			Metaphone meta =new Metaphone();
			PerformanceLoggerSingleton metrics = PerformanceLoggerSingleton.getInstance("/Users/tommichiels/Desktop/");
			Result result = db.execute(query);
			while (result.hasNext()) {
				metrics.mark("ER");
				Map<String, Object> row = result.next();
				String nameA = row.get("namea") != null ? row.get("namea").toString():"" ;
				String nameB = row.get("nameb") != null ? row.get("nameb").toString():"" ;
				String idKeyA = row.get("idKeya") != null ? row.get("idKeya").toString():"" ;		
				String surnameA = row.get("surnamea") != null ?row.get("surnamea").toString():"" ;
				String surnameB = row.get("surnameb") != null ?row.get("surnameb").toString():"" ;
				String idKeyB = row.get("idKeyb") != null ?row.get("idKeyb").toString():"" ;
				if (compare(meta.encode(nameA),meta.encode(nameB)) 
						&& compare(meta.encode(surnameA),meta.encode(surnameB))){
					 db.execute("MATCH (a:Record {idKey:'"+idKeyA+"'}) "+
					         "MATCH (b:Record {idKey:'"+idKeyB+"'}) "+
							 "where NOT (a)-[:LINK]-(b) " +
					         "MERGE (a)-[:LINK {rulename:'NAME_SURNAME',strong:'TRUE'}]->(b);");
				}
			}
			tx.success();
			tx.close();
			log.info("ER done");
			// it.close();
			return Stream.empty();
		} catch (Exception e) {
			String errMsg = "Error encountered while doing ER job";
			tx.failure();
			log.error(errMsg, e);
			throw new RuntimeException(errMsg, e);
		} finally {
			tx.close();
		}
	}
	
	 private boolean compare(
		      String recField1,
		      String recField2){
		      if (!recField1.isEmpty() && !recField2.isEmpty()) {
		        return recField1.equals(recField2);
		      } else {
		        return false;
		      }
		    }

}
