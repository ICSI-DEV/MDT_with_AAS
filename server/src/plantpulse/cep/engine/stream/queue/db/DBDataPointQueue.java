package plantpulse.cep.engine.stream.queue.db;

public interface DBDataPointQueue<T> {
	
	public T getQueue();
	
	public String getSource();

}
