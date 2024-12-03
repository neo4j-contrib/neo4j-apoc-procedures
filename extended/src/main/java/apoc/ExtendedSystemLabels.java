package apoc;

import org.neo4j.graphdb.Label;

public enum ExtendedSystemLabels implements Label
{
    ApocCypherProcedures,
    ApocCypherProceduresMeta,
    Procedure,
    Function,
    ApocUuid,
    ApocTrigger,
    ApocUuidMeta,
    DataVirtualizationCatalog,
    VectorDb
}
