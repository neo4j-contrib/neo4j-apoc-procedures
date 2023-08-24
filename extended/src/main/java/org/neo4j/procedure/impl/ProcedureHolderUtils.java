package org.neo4j.procedure.impl;

import org.neo4j.internal.kernel.api.procs.QualifiedName;
import org.neo4j.kernel.api.procedure.GlobalProcedures;

import java.lang.reflect.Field;

public class ProcedureHolderUtils {

    public static void unregisterProcedure(QualifiedName name, GlobalProcedures registry) {
        String kind = "procedures";
        unregisterCommon(name, registry, kind);
    }

    public static void unregisterFunction(QualifiedName name, GlobalProcedures registry) {
        String kind = "functions";
        unregisterCommon(name, registry, kind);
    }

    private static void unregisterCommon(QualifiedName name, GlobalProcedures registry, String kind) {
        try {
            GlobalProceduresRegistry globalProcRegistry = getGlobalProcRegistry(registry);

            // get the field `ProcedureRegistry registry` from the GlobalProceduresRegistry instance
            Field registryField = GlobalProceduresRegistry.class.getDeclaredField("registry");
            registryField.setAccessible(true);
            ProcedureRegistry procedureRegistry = (ProcedureRegistry) registryField.get(globalProcRegistry);

            // get `ProcedureHolder <kind>` (i.e `ProcedureHolder procedures` or `ProcedureHolder functions`) field from the ProcedureRegistry instance
            Field procHolderField = ProcedureRegistry.class.getDeclaredField(kind);
            procHolderField.setAccessible(true);
            ProcedureHolder procedureHolder = (ProcedureHolder) procHolderField.get(procedureRegistry);

            // unregister `name` from ProcedureHolder found
            procedureHolder.unregister(name);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static GlobalProceduresRegistry getGlobalProcRegistry(GlobalProcedures registry) {
        try {
            // with embedded test database, the instance is of type LazyProcedures,
            // so we get the field `globalProcedures` from the `LazyProcedures registry` instance
            Field globalProceduresField = Class.forName("org.neo4j.procedure.LazyProcedures").getDeclaredField("globalProcedures");
            globalProceduresField.setAccessible(true);

            return (GlobalProceduresRegistry) globalProceduresField.get(registry);

        } catch (Exception e) {
            // with a real instance, the above code produces, due to LazyProcedures, produce a `NoClassDefFoundError` or an `IllegalArgumentException`
            // because `registry` is directly of type GlobalProceduresRegistry, so we cast it
            return (GlobalProceduresRegistry) registry;
        }
    }
}
