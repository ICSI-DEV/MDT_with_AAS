package plantpulse.server.filter.compressor;

public interface Compressor {
	/**
	 * The main method that compresses the given source and returns a compressed result.
	 * 
	 * @param source The source to compress.
	 * @return Compressed result.
	 */
	public abstract String compress(String source);
}