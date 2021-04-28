package plantpulse.cep.engine.pipeline.dataflow.strategy;

import java.util.*;

public class RoundRobin {
	
	private Iterator<Robin> it;
	private List<Robin> list;

	public RoundRobin(List<Robin> list) {
		this.list = list;
		it = list.iterator();
	}

	public int next() {
// if we get to the end, start again
		if (!it.hasNext()) {
			it = list.iterator();
		}
		Robin robin = it.next();

		return robin.call();
	}
}