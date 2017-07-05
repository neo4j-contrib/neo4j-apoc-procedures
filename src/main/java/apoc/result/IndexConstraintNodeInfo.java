package apoc.result;

import org.neo4j.graphdb.schema.ConstraintType;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.StatementTokenNameLookup;
import org.neo4j.kernel.api.constraints.NodePropertyConstraint;
import org.neo4j.kernel.api.constraints.PropertyConstraint;
import org.neo4j.kernel.api.constraints.UniquenessConstraint;
import org.neo4j.kernel.api.index.IndexDescriptor;

import java.util.Collections;
import java.util.List;

/**
 * Created by alberto.delazzari on 04/07/17.
 */
public class IndexConstraintNodeInfo {

    public final String name;

    public final String label;

    public final List<String> properties;

    public final String status;

    public final String type;

    /**
     * Constructor using ReadOperations and IndexDescriptor
     * It is used for wrapping index information
     *
     * @param readOperations
     * @param indexDescriptor
     * @throws Exception
     */
    public IndexConstraintNodeInfo(ReadOperations readOperations, IndexDescriptor indexDescriptor) throws Exception {
        // We use StatementTokenNameLookUp to get the name
        name = indexDescriptor.userDescription(new StatementTokenNameLookup(readOperations));
        label = readOperations.labelGetName(indexDescriptor.getLabelId());
        properties = Collections.singletonList(readOperations.propertyKeyGetName(indexDescriptor.getPropertyKeyId()));
        status = readOperations.indexGetState(indexDescriptor).name();
        type = "INDEX";
    }

    /**
     * Alternative constructor using ReadOperations and PropertyConstraint
     * It is used for wrapping constraint information
     *
     * @param readOperations
     * @param propertyConstraint
     * @throws Exception
     */
    public IndexConstraintNodeInfo(ReadOperations readOperations, PropertyConstraint propertyConstraint) throws Exception {
        // We use StatementTokenNameLookUp to get the names
        StatementTokenNameLookup lookup = new StatementTokenNameLookup(readOperations);

        name = propertyConstraint.userDescription(lookup);
        label = lookup.labelGetName(((NodePropertyConstraint) propertyConstraint).label());
        properties = Collections.singletonList(lookup.propertyKeyGetName(propertyConstraint.propertyKey()));
        status = "";
        type = propertyConstraint instanceof UniquenessConstraint ? ConstraintType.UNIQUENESS.name() : ConstraintType.NODE_PROPERTY_EXISTENCE.name();
    }
}
