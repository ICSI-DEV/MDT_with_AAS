package plantpulse.cep.engine.utils;

/**
 * JavaTypeUtils
 * 
 * @author lenovo
 *
 */
public class JavaTypeUtils {

	@SuppressWarnings("rawtypes")
	public static Class getJavaType(String type) {
		if (type.equals("string")) {
			return java.lang.String.class;
		} else if (type.equals("int")) {
			return java.lang.Integer.class;
		} else if (type.equals("long")) {
			return java.lang.Long.class;
		} else if (type.equals("float")) {
			return java.lang.Float.class;
		} else if (type.equals("double")) {
			return java.lang.Double.class;
		} else {
			return null;
		}
	}

	public static String convertJavaTypeToDataType(String typeName) {
		if (typeName.equals("Integer") == true) {
			return "int";
		} else if (typeName.equals("Boolean") == true) {
			return "boolean";
		} else if (typeName.equals("Character") == true) {
			return "int";
		} else if (typeName.equals("Short") == true) {
			return "int";
		} else if (typeName.equals("Float") == true) {
			return "float";
		} else if (typeName.equals("Double") == true) {
			return "double";
		} else if (typeName.equals("Long") == true) {
			return "long";
		} else if (typeName.equals("Date") == true) {
			return "timestamp";
		} else if (typeName.equals("JIString") == true || typeName.equals("String") == true) {
			return "string";
		} else if (typeName.equals("JIUnsignedInteger") == true) {
			return "long";
		} else if (typeName.equals("JIUnsignedShort") == true) {
			return "int";
		} else if (typeName.equals("JIUnsignedByte") == true) {
			return "int";
		} else{
			return "unsupport";
		}
	}
	
	
}
