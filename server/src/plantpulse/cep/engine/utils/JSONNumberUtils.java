package plantpulse.cep.engine.utils;

public class JSONNumberUtils {
	
	public static Double convertInfiniteDouble(Double value){
		if (value.isInfinite()) {
			String num = String.format("%.20f", value);
			value = new Double(num);
			return value;
		}else{
			String num = String.format("%.20f", value);
			value = new Double(num);
			return value;
		}
	}

}
