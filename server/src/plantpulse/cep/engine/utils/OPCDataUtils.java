package plantpulse.cep.engine.utils;

import java.util.Date;

/**
 * OPCDataUtils
 * 
 * @author lsb
 *
 */
public class OPCDataUtils {


	public static Class<?> getJavaTypeClass(String typeName) {

		if (typeName.equals("Integer") == true) {
			return Integer.class;
		} else if (typeName.equals("Boolean") == true) {
			return Boolean.class;
		} else if (typeName.equals("Character") == true) {
			return Integer.class;
		} else if (typeName.equals("Short") == true) {
			return Integer.class;
		} else if (typeName.equals("Float") == true) {
			return Float.class;
		} else if (typeName.equals("Double") == true) {
			return Double.class;
		} else if (typeName.equals("Long") == true) {
			return Long.class;
		} else if (typeName.equals("Date") == true) {
			return Date.class;
		} else if (typeName.equals("JIString") == true) {
			return String.class;
		} else if (typeName.equals("JIUnsignedInteger") == true) {
			return Long.class;
		} else if (typeName.equals("JIUnsignedShort") == true) {
			return Integer.class;
		} else if (typeName.equals("JIUnsignedByte") == true) {
			return Integer.class;
		} else {
			return String.class;
		}

	}

	public static Object getConvertValue(String typeName, plantpulse.event.opc.Point point) {

		if (point == null) {
			return null;
		}

		String value = point.getValue();
		if (value == null) {
			return null;
		}

		if (typeName.equals("Integer") == true) {
			return Integer.parseInt(value);
		} else if (typeName.equals("Boolean") == true) {
			return Boolean.parseBoolean(value);
		} else if (typeName.equals("Character") == true) {
			return Integer.parseInt(value);
		} else if (typeName.equals("Short") == true) {
			return Integer.parseInt(value);
		} else if (typeName.equals("Float") == true) {
			return Float.parseFloat(value);
		} else if (typeName.equals("Double") == true) {
			return Double.parseDouble(value);
		} else if (typeName.equals("Long") == true) {
			return Long.parseLong(value);
		} else if (typeName.equals("Date") == true) {
			return new Date(Long.parseLong(value));
		} else if (typeName.equals("JIString") == true) {
			return value;
		} else if (typeName.equals("JIUnsignedInteger") == true) {
			return Long.parseLong(value);
		} else if (typeName.equals("JIUnsignedShort") == true) {
			return Integer.parseInt(value);
		} else if (typeName.equals("JIUnsignedByte") == true) {
			return Integer.parseInt(value);
		} else {
			return value;
		}

	}

}
