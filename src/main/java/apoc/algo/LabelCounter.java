package apoc.algo;

public class LabelCounter {
    
	public LabelCounter(String label, int count) {
		super();
		this.label = label;
		this.count = count;
	}
	
	private String label;
	private int count;
	public String getLabel() {
		return label;
	}
	public int getCount() {
		return count;
	}
}
