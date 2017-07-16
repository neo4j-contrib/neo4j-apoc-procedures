package apoc.export.cypher.formatter;

/**
 * @author AgileLARUS
 *
 * @since 16-06-2017
 */
public enum CypherFormat {

	CREATE("create") {
		@Override
		public CypherFormatter getFormatter() {
			return new CreateCypherFormatter();
		}
	},
	ADD_STRUCTURE("addStructure") {
		@Override
		public CypherFormatter getFormatter() {
			return new AddStructureCypherFormatter();
		}
	},
	UPDATE_STRUCTURE("updateStructure") {
		@Override
		public CypherFormatter getFormatter() {
			return new UpdateStructureCypherFormatter();
		}
	},
	UPDATE_ALL("updateAll") {
		@Override
		public CypherFormatter getFormatter() {
			return new UpdateAllCypherFormatter();
		}
	};

	private String value;

	CypherFormat(String value) {
		this.value = value;
	}

	public static CypherFormat fromString(String value) {
		if (value != null && !"".equals(value)) {
			for (CypherFormat formatType : CypherFormat.values()) {
				if (formatType.value.equalsIgnoreCase(value)) {
					return formatType;
				}
			}
		}
		return CREATE;
	}

	public abstract CypherFormatter getFormatter();

	public String toString() {
		return this.value;
	}
}
