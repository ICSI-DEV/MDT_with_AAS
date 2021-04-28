package plantpulse.cep.engine.async;

/**
 * <B>Worker</B>
 * <pre> run and expire</pre>
 * @author leesangboo
 * 
 */
public interface Worker {

	
	public String getName();
	
	/**
	 * 
	 */
	public void execute();

}
