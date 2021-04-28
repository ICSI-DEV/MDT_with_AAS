package plantpulse.cep.engine.scheduling.job;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.JSONObject;

import plantpulse.cep.dao.TagDAO;
import plantpulse.cep.engine.scheduling.JobDateUtils;
import plantpulse.cep.engine.snapshot.PointMapSnapshot;
import plantpulse.cep.service.client.StorageClient;
import plantpulse.domain.Tag;
import plantpulse.event.opc.Point;

/**
 * 
 * @author lsb
 * @deprecated CEP 분석으로 변경
 */
public class TagPointAggregationJob extends SchedulingJob {

	private static final Log log = LogFactory.getLog(TagPointAggregationJob.class);

	private String term;

	public TagPointAggregationJob() {
	}

	@Override
	public void run() {
		//
		try {
			
			log.debug("Point aggregation job starting... : term=[" + term + "]");

			term = context.getJobDetail().getJobDataMap().getString("term");
			
			long start = System.currentTimeMillis();
			
			final long current_timestamp = (System.currentTimeMillis()/1000) * 1000;
			final String from = JobDateUtils.from(current_timestamp, term);
			final String to = JobDateUtils.to(current_timestamp);
			final Map<String, Point> point_map = new PointMapSnapshot().snapshot(); //메모리
			
			//
			//AggregationTaskRunCount.count.incrementAndGet();
			final StorageClient client = new StorageClient();
			
			//1. 넘버릭 유형의 집계 먼저 처리
			
			/*
			String[] types_array = new String[]{"int", "long", "float", "double"};
			for(int i=0; i < types_array.length; i++){
				JSONArray array = client.getSelectDAO().selectPointDataForAggregatationByType(types_array[i], current_timestamp, from, to);
				for(int j=0; array != null && j < array.size(); j++){
					//마지막 값 및 속성 추가
					
					JSONObject json = array.getJSONObject(j);
					log.debug("agg---------" + json.toString());
					Point point = point_map.get(json.getString("tag_id"));
					if (point != null) {
						if(StringUtils.isNotEmpty(point.getValue())){
							json.put("last",      point.getValue());
						}else{
							json.put("last", "");
						};
						if(MapUtils.isNotEmpty(point.getAttribute())){
							json.put("attribute", point.getAttribute());
						}else{
							json.put("attribute", new HashMap<String,String>());
						}
					} else {
						json.put("last", "");
						json.put("attribute", new HashMap<String,String>());
					}
					;
					Tag tag = new Tag();
					tag.setTag_id(json.getString("tag_id"));
					client.getInsertDAO().insertAggregation(tag, term, current_timestamp, json);
				}
			};
			*/
			
			//2. 넘버릭 이외의 스트링, 불린, 타임스탬프, 데이트, 
			TagDAO tag_dao = new TagDAO();
			List<Tag> tag_list = tag_dao.selectTagAll();
			for (int i = 0; tag_list != null && i < tag_list.size(); i++) {
				final Tag tag = tag_list.get(i);
				
				//if (tag.getJava_type().equals("String") || tag.getJava_type().equals("Boolean")) {

					try {
						
						log.debug("Point aggregation paremeter : id=" + tag.getTag_id() + ", type=" + tag.getJava_type() + ", from=" + from + ", to=" + to);
					
						
						//마지막 값 및 속성 추가
						JSONObject json = new JSONObject();
						json.put("tag_id", tag.getTag_id());
						json.put("count", 0);
						
						Point point = point_map.get(tag.getTag_id());
						if (point != null) {
							if(StringUtils.isNotEmpty(point.getValue())){
								json.put("last",      point.getValue());
							}else{
								json.put("last", "");
							};
							if(MapUtils.isNotEmpty(point.getAttribute())){
								json.put("attribute", point.getAttribute());
							}else{
								json.put("attribute", new HashMap<String,String>());
							}
						} else {
							json.put("last", "");
							json.put("attribute", new HashMap<String,String>());
						}
						;

						
						client.forInsert().insertPointAggregation(tag, term, current_timestamp, json);

					} catch (Exception e) {
						log.error("Point aggregation error : tag_id=[" + tag.getTag_id() + "], error=[" + e.getMessage() + "]", e);
					}
					
				//}//if

			}
			;

			//AggregationTaskRunCount.count.decrementAndGet();

			long end = System.currentTimeMillis() - start;
			log.debug("Point aggregation job completed : term=[" + term + "], exec_time=[" + end + "]ms");
			//

		} catch (Exception e) {
			log.error("Point aggregation data insert failed : " + e.getMessage(), e);
		}
		;
	}

}
