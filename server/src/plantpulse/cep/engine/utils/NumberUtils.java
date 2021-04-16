package plantpulse.cep.engine.utils;

import java.text.NumberFormat;

public class NumberUtils {
	
	
	public static String numberToString(long value) {
		 NumberFormat nf = NumberFormat.getInstance();
		 nf.setGroupingUsed(false);
		 nf.setMinimumIntegerDigits(0);
		 nf.setMaximumIntegerDigits(22);
		 nf.setMaximumFractionDigits(22);
		 return nf.format(value);
	};
	
	public static String numberToString(double value) {
		 NumberFormat nf = NumberFormat.getInstance();
		 nf.setGroupingUsed(false);
		 nf.setMinimumIntegerDigits(0);
		 nf.setMaximumIntegerDigits(22);
		 nf.setMaximumFractionDigits(22);
		 return nf.format(value);
	};

}
