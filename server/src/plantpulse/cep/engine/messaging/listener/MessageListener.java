package plantpulse.cep.engine.messaging.listener;

public interface MessageListener<T> {
	
	public Object startListener() throws Exception;
	
	public void stopListener() ;
	
	public T getConnection();

}
