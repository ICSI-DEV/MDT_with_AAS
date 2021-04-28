package plantpulse.plugin.aas.utils;

import org.apache.commons.lang.StringUtils;

import plantpulse.domain.Tag;

/**
 * AliasUtils
 * @author lsb
 *
 */
public class AliasUtils {
	
	
	public static String getAliasName(Tag tag) {
		if (StringUtils.isNotEmpty(tag.getAlias_name())) {
			return tag.getAlias_name();
		};

		return tag.getTag_id();
		
		/*
		if (name.indexOf(".") > -1) {
			name = name.substring(name.lastIndexOf(".") + 1, name.length());
		} else {

		}
		return name.toLowerCase();
		*/
	}


}
