package plantpulse.plugin.aas.utils;

public class CommonUtils {

	public static boolean hasSameString(String[] array,String ele) {
		boolean result = false;

		for(String tmp : array) {
			if(tmp!=null && tmp.equals(ele)) {
				result = true;
				break;
			}
		}
		return result;
	}

	private static String hexFormat(String s){
		if(s.length() == 0){
			s = "00";
		}else if(s.length() == 1){
    		s = "0"+s;
    	}
		return s;
	}

	public static String toString(Object o){
		String rtn = "";
		if(o != null){
			rtn = String.valueOf(o);
		}

		return rtn;
	}

	public static double byteSum(byte preByte, byte aftByte) {
		String pre = toHex(preByte&0xff);
    	String aft = toHex(aftByte&0xff);

    	pre = hexFormat(pre);
    	aft = hexFormat(aft);
    	String value = pre+aft;

    	double v = getDecimal(value)*1.0;
    	return v;
	}

	public static String toHex(int decimal){
	     int rem;
	     String hex="";
	     char hexchars[]={'0','1','2','3','4','5','6','7','8','9','A','B','C','D','E','F'};
	    while(decimal>0)
	     {
	       rem=decimal%16;
	       hex=hexchars[rem]+hex;
	       decimal=decimal/16;
	     }
	    return hex;
	}

	public static int getDecimal(String hex){
	    String digits = "0123456789ABCDEF";
	             hex = hex.toUpperCase();
	             int val = 0;
	             for (int i = 0; i < hex.length(); i++)
	             {
	                 char c = hex.charAt(i);
	                 int d = digits.indexOf(c);
	                 val = 16*val + d;
	             }
	             return val;
	}

	public static byte[] hexStringToByteArray(String s) {
	    int len = s.length();
	    byte[] data = new byte[len / 2];
	    for (int i = 0; i < len; i += 2) {
	        data[i / 2] = (byte) ((Character.digit(s.charAt(i), 16) << 4)
	                             + Character.digit(s.charAt(i+1), 16));
	    }
	    return data;
	}


}
