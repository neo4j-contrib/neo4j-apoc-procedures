package apoc.generate.relationship;

/**
 * A {@link RelationshipCreator} that only creates relationship of type FRIEND_OF and assigns no properties to those
 * relationships.
 */
public class SocialNetworkRelationshipCreator extends DefaultRelationshipCreator {

    public SocialNetworkRelationshipCreator() {
        super("FRIEND_OF");
    }
}
