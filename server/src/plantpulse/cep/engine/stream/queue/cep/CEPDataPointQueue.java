package plantpulse.cep.engine.stream.queue.cep;

public interface CEPDataPointQueue<T> {
	
	public T getQueue();
	
	public String getSource();

}
