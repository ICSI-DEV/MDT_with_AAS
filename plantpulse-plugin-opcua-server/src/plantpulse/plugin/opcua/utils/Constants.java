package plantpulse.plugin.opcua.utils;

public class Constants {

	private static String LOGPATH;
	private static String KAFKA_PORT;
	private static String KAFKA_HOST;

	public static String getKAFKA_HOST() {
		return KAFKA_HOST;
	}

	public static void setKAFKA_HOST(String kAFKA_HOST) {
		KAFKA_HOST = kAFKA_HOST;
	}
	public static String getKAFKA_PORT() {
		return KAFKA_PORT;
	}

	public static void setKAFKA_PORT(String kAFKA_PORT) {
		KAFKA_PORT = kAFKA_PORT;
	}

	public static String getLOGPATH() {
		return LOGPATH;
	}

	public static void setLOGPATH(String lOGPATH) {
		LOGPATH = lOGPATH;
	}



}
