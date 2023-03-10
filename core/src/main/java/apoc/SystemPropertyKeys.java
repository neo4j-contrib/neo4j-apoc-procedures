package apoc;

public enum SystemPropertyKeys  {
    database,
    name,

    // cypher stored procedures/functions
    lastUpdated,
    statement,
    inputs,
    description,
    mode,
    outputs,
    output,
    forceSingle,
    prefix,

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
    propertyName;
}
