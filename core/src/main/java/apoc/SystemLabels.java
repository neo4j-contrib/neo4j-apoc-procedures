package apoc;

import org.neo4j.graphdb.Label;

public enum SystemLabels implements Label {
    ApocCypherProcedures,
    ApocCypherProceduresMeta,
    Procedure,
    Function,
    ApocUuid,
    ApocUuidMeta,
    ApocTriggerMeta,
    ApocTrigger,
    DataVirtualizationCatalog
}
