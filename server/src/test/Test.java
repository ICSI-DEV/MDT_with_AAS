package test;

import java.math.RoundingMode;
import java.text.DecimalFormat;
import java.text.NumberFormat;

public class Test {

	public static void main(String[] args) {
		double d = 123456.1231231231321231; 
		NumberFormat nf = NumberFormat.getInstance();
		 nf.setGroupingUsed(false);
		 nf.setMinimumIntegerDigits(0);
		 nf.setMaximumIntegerDigits(22);
		 nf.setMaximumFractionDigits(22);
		 
		 //
		 System.out.println(nf.format(d));
		 //
		 
		 DecimalFormat df = new DecimalFormat("#####################.######################"); 
		 System.out.println(df.format(d));

		 System.out.println(d);
	}

}
