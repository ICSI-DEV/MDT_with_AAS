package plantpulse.cep.engine.pipeline.dataflow.strategy;

public class Robin {
	private int i;

	public Robin(int i) {
		this.i = i;
	}

	public int call() {
		return i;
	}
}
