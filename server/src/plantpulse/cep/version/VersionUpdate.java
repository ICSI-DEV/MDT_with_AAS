package plantpulse.cep.version;

public interface VersionUpdate {
	
	public long getVersion();
	
	public void upgrade() throws Exception;
	

}
