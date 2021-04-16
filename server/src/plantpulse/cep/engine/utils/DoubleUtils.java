package plantpulse.cep.engine.utils;

/**
 * DoubleUtils
 * @author leesa
 *
 */
public class DoubleUtils {
	
	/**
	 * 더블 유형의 소숫점 자르기
	 * @param value
	 * @param sosutjum
	 * @return
	 */
	public static double toFixed(double value, int sosutjum) {
		 return Double.parseDouble(String.format("%." + sosutjum + "f", (value)));
	}

}
