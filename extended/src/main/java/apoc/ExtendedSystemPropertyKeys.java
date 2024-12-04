package apoc;

public enum ExtendedSystemPropertyKeys
{
    database,

    // cypher stored procedures/functions
    lastUpdated,
    statement,
    
    // cypher stored procedures/functions
    inputs,
    description,
    mode,
    outputs,
    output,
    forceSingle,
    prefix,
    mapResult,

    // triggers
    selector,
    params,
    paused,

    // dv
    data,

    // uuid handler
    label,
    addToSetLabel,
    addToExistingNodes,
    propertyName,
    
    // vector db
    host,
    credentials,
    name,
}
