package plantpulse.cep.engine.utils;

public class JavaMemoryUtils {

	public static double getUsedJavaRamPercent() {
		final long RAM_TOTAL = Runtime.getRuntime().totalMemory();
		final long RAM_FREE = Runtime.getRuntime().freeMemory();
		final long RAM_USED = RAM_TOTAL - RAM_FREE;
		final long RAM_TOTAL_MB = RAM_TOTAL / 8 / 1024;
		final long RAM_FREE_MB = RAM_FREE / 8 / 1024;
		final long RAM_USED_MB = RAM_USED / 8 / 1024;
		final double RAM_USED_PERCENTAGE = (RAM_USED / RAM_TOTAL) * 100;
		return RAM_USED_PERCENTAGE;
	}

}
