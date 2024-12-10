package apoc.load;

import apoc.Extended;
import apoc.result.RowResult;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static apoc.load.Jdbc.executeUpdate;



// TODO - scrivere sulla pr che abbiamo testato anche le apoc.load.jdbc* con DuckDB e fixato eventuali errori

@Extended
public class Analytics {

    @Context
    public Log log;

    @Context
    public GraphDatabaseService db;

    @Context
    public Transaction tx;


    // TODO - PRENDERE COME ESEMPI https://chatgpt.com/share/67530793-c0d0-800c-a4fc-9ae01e098de3
    // TODO - testare principalmente per DuckDB
    
    //         TODO poi provare a testare con altri db, scopiazzando i container da MySQLJdbcTest e  PostgresJdbcTest
    
    //          se necessario, mettere qualcosa tipo config.getOrDefault("database", "duckDB") 
    //              e fare degli if-else/switch/etc.. per differenziare le query sql
    
    @Procedure("apoc.load.jdbc.analytics")
    // TODO 
    @Description("TODO - DESCRIZIONE")
    public Stream<RowResult> aggregate(
            @Name("neo4jQuery") String neo4jQuery,
            @Name("jdbc") String urlOrKey,
            @Name("sqlQuery") String sqlQuery,
            @Name(value = "params", defaultValue = "[]") List<Object> params,
            @Name(value = "config",defaultValue = "{}") Map<String, Object> config) {

        // TODO - scrivere sulla PR: add handling: some SQL database like Microsoft SQL Server create temp table in a different way
        //      e.g. CREATE TABLE #table_name (column_name datatype);
        //      document it

        // TODO step 1: temp table creation partendo dalla neo4jQuery
        //  mettere al posto di query la creazione di una tabella temporanea
        //  facendo leva su tx.execute(..) o db.executeTransactionally(...) e recuperandosi i risultati
         
        /* ad esempio, se passo la query neo4j
            SELECT 
                actor,
                genre,
                SUM(movies_count) AS movies_count
            FROM movies_data
            GROUP BY actor, genre
            ORDER BY movies_count DESC
        
        la tabella sarà qualcosa tipo:
            
            CREATE TEMPORARY TABLE movies_data AS 
            SELECT * FROM 
            (VALUES
                ('Keanu Reeves', 'Sci-Fi', 3),
                ('Carrie-Anne Moss', 'Sci-Fi', 2),
                ('Laurence Fishburne', 'Sci-Fi', 3),
                ('Keanu Reeves', 'Action', 4),
                ('Will Smith', 'Action', 5)
            ) AS t(actor, genre, movies_count);
            
         */
        executeUpdate(urlOrKey, "<TODO>", config, log, params);
        
        // TODO step 2: fare dei test in cui passo una query che interroga la tabella temporanea
        /* ad esempio
        WITH ranked_data AS (
            SELECT 
                category_column, 
                pivot_column, 
                value_column,
                ROW_NUMBER() OVER (PARTITION BY category_column ORDER BY value_column DESC) AS rank
            FROM neo4j_data
            )
         */
        
        // todo - altri test con query sql, tipo questo
        /*
        SELECT 
            actor,
            genre,
            movies_count,
            RANK() OVER (PARTITION BY genre ORDER BY movies_count DESC) AS rank
        FROM movies_data;
         */
        
        /*
        
         */
        executeUpdate(urlOrKey, sqlQuery, config, log, params);

        // TODO  step 3: return result
        return null;
    }


    /* TODO scrivere questa cosa sulla PR:
        meglio non aggregation, così è più personalizzabile, posso scegliere quali risultati ottenere e come ottenerli 
        altrimenti per fare qualcosa come sotto, con movies_count dovrei mettere un parametri aggKeys e fare cose strane
        
            MATCH (p:Person)-[r:ACTED_IN]->(m:Movie)
            RETURN 
                p.name AS actor, 
                m.genre AS genre, 
                r.roles AS roles, 
                COUNT(m) AS movies_count
     */


    
}
