package plantpulse.cep.service.support.alarm.eql;

import org.apache.commons.lang.StringUtils;

import plantpulse.domain.Tag;

public class NumbericTypeEQLGenerator {
	
	
	public static String getEQL(Tag tag) {
		StringBuffer eql = new StringBuffer();
		eql.append("SELECT timestamp, tag_id, tag_name, value,  ");
		eql.append("\n CASE ");
		if (StringUtils.isNotEmpty(tag.getTrip_hi()))
			eql.append("\n    WHEN to_double(value) > " + tag.getTrip_hi() + " THEN 'TRIP-HIGH'  ");
		if (StringUtils.isNotEmpty(tag.getHi_hi()))
			eql.append("\n    WHEN to_double(value) > " + tag.getHi_hi() + " THEN 'HIGH-HIGH'  ");
		if (StringUtils.isNotEmpty(tag.getHi()))
			eql.append("\n    WHEN to_double(value) > " + tag.getHi() + " THEN 'HIGH'  ");
		if (StringUtils.isNotEmpty(tag.getLo()))
			eql.append("\n    WHEN to_double(value) < " + tag.getLo() + " THEN 'LOW'  ");
		if (StringUtils.isNotEmpty(tag.getLo_lo()))
			eql.append("\n    WHEN to_double(value) < " + tag.getLo_lo() + " THEN 'LOW-LOW'  ");
		if (StringUtils.isNotEmpty(tag.getTrip_lo()))
			eql.append("\n    WHEN to_double(value) < " + tag.getTrip_lo() + " THEN 'TRIP-LOW'  ");
		eql.append("\n ELSE 'NORMAL' END AS band  ");
		eql.append("\n FROM Point(tag_id = '" + tag.getTag_id() + "').win:length(1)  ");
		eql.append("\n WHERE 'NORMAL' !=  ( ");
		eql.append("\n CASE ");
		if (StringUtils.isNotEmpty(tag.getTrip_hi()))
			eql.append("\n     WHEN to_double(value) > " + tag.getTrip_hi() + " THEN 'TRIP-HIGH'  ");
		if (StringUtils.isNotEmpty(tag.getHi_hi()))
			eql.append("\n     WHEN to_double(value) > " + tag.getHi_hi() + " THEN 'HIGH-HIGH'  ");
		if (StringUtils.isNotEmpty(tag.getHi()))
			eql.append("\n     WHEN to_double(value) > " + tag.getHi() + " THEN 'HIGH'  ");
		if (StringUtils.isNotEmpty(tag.getLo()))
			eql.append("\n     WHEN to_double(value) < " + tag.getLo() + " THEN 'LOW'  ");
		if (StringUtils.isNotEmpty(tag.getLo_lo()))
			eql.append("\n     WHEN to_double(value) < " + tag.getLo_lo() + " THEN 'LOW-LOW'  ");
		if (StringUtils.isNotEmpty(tag.getTrip_lo()))
			eql.append("\n     WHEN to_double(value) < " + tag.getTrip_lo() + " THEN 'TRIP-LOW'  ");
		eql.append("\n ELSE 'NORMAL' END )  ");
		//
		return eql.toString();
	}

}
