package plantpulse.cep.service.support.alarm.eql;



import plantpulse.domain.Tag;

public class BooleanTypeEQLGenerator  {
	
	public static String getEQL(Tag tag) {
		
		StringBuffer eql = new StringBuffer();
		eql.append("SELECT timestamp, tag_id, tag_name, value,  ");
		eql.append("\n CASE ");
		if ("Y".equals(tag.getBool_true())) {
			eql.append("\n  WHEN value = 'true'  THEN '" + tag.getBool_true_priority() + "'  ");
		}
		if ("Y".equals(tag.getBool_false())) {
			eql.append("\n  WHEN value = 'false' THEN '" + tag.getBool_false_priority() + "'  ");
		};
		eql.append("\n ELSE 'NORMAL' END AS band  ");
		eql.append("\n FROM Point(tag_id = '" + tag.getTag_id() + "').win:length(1)  ");
		//
		return eql.toString();
	}

}
