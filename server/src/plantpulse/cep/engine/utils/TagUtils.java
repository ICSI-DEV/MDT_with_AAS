package plantpulse.cep.engine.utils;

import plantpulse.domain.Tag;

/**
 * 태그 유틸
 * @author leesa
 *
 */
public class TagUtils {
	
	
		/**
		 * 태그의 데이터타입이 넘버릭인지 확인한다.
		 * @param tag
		 * @return
		 */
	public static boolean isNumbericDataType(Tag tag) {
		String java_type = tag.getJava_type();
		if (getJavaType(java_type).equals("int") 
				|| getJavaType(java_type).equals("long")
				|| getJavaType(java_type).equals("float") 
				|| getJavaType(java_type).equals("double")) {
			return true;
		}else {
			return false;
		}
	}
	
	public static boolean isBooleanDataType(Tag tag) {
		String java_type = tag.getJava_type();
		if (getJavaType(java_type).equals("boolean")) {
			return true;
		}else {
			return false;
		}
	}
	
	public static boolean isStringDataType(Tag tag) {
		String java_type = tag.getJava_type();
		if (getJavaType(java_type).equals("string")) {
			return true;
		}else {
			return false;
		}
	}
	
	public static boolean isTimestampDataType(Tag tag) {
		String java_type = tag.getJava_type();
		if (getJavaType(java_type).equals("timestamp")) {
			return true;
		}else {
			return false;
		}
	}

		
		/**
		 * OPCDataTypeConverter clone
		 * 
		 * @param typeName
		 * @return
		 */
		public static String getJavaType(String typeName) {
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
			} else if (typeName.equals("Integer") == true) {
				return "int";
			} else if (typeName.equals("Double") == true) {
				return "double";
			} else if (typeName.equals("Long") == true) {
				return "long";
			} else if (typeName.equals("Date") == true) {
				return "timestamp";
			} else if (typeName.equals("String") == true) {
				return "string";
			} else if (typeName.equals("JIString") == true) {
				return "string";
			} else if (typeName.equals("JIUnsignedInteger") == true) {
				return "long";
			} else if (typeName.equals("JIUnsignedShort") == true) {
				return "int";
			} else if (typeName.equals("JIUnsignedByte") == true) {
				return "int";
			} else {
				return "unsupport[" + typeName + "]";
			}
		}
		
		public static String fromTagTypeToPointType(String typeName) {
			return getJavaType(typeName);
		}
}
