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

    // uuid handler
    label,
    propertyName;
}
