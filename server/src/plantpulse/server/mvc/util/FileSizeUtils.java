package plantpulse.server.mvc.util;

import java.text.DecimalFormat;

/**
 * FileSizeUtils
 * @author leesa
 *
 */
public class FileSizeUtils {

	private static final long K = 1024;
	private static final long M = K * K;
	private static final long G = M * K;
	private static final long T = G * K;
	


    public static String units = "BKMGTPEZY";


	public static String convertToStringRepresentation(final long value){
	    final long[] dividers = new long[] { T, G, M, K, 1 };
	    final String[] units = new String[] { "TB", "GB", "MB", "KB", "B" };
	    if(value < 1)
	        throw new IllegalArgumentException("Invalid file size: " + value);
	    String result = null;
	    for(int i = 0; i < dividers.length; i++){
	        final long divider = dividers[i];
	        if(value >= divider){
	            result = format(value, divider, units[i]);
	            break;
	        }
	    }
	    return result;
	}

	private static String format(final long value,
	    final long divider,
	    final String unit){
	    final double result =
	        divider > 1 ? (double) value / (double) divider : (double) value;
	    return new DecimalFormat("#,##0.#").format(result) + " " + unit;
	}
	
	
    public static long parseToBytes(String arg0) {
        int spaceNdx = arg0.indexOf(" ");    
        double ret = Double.parseDouble(arg0.substring(0, spaceNdx));
        String unitString = arg0.substring(spaceNdx+1);
        int unitChar = unitString.charAt(0);
        int power = units.indexOf(unitChar);
        boolean isSi = unitString.indexOf('i')!=-1;
        int factor = 1024;
        if (isSi) 
        {
            factor = 1000;
        }

        return new Double(ret * Math.pow(factor, power)).longValue();
    };
	
}
