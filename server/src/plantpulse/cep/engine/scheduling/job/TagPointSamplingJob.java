package plantpulse.cep.engine.scheduling.job;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import plantpulse.cep.dao.TagDAO;
import plantpulse.cep.engine.scheduling.JobDateUtils;
import plantpulse.cep.engine.snapshot.PointMapSnapshot;
import plantpulse.cep.service.client.StorageClient;
import plantpulse.domain.Tag;
import plantpulse.event.opc.Point;

/**
 * 
 * @author lsb
 *
 */
public class TagPointSamplingJob extends SchedulingJob {

	private static final Log log = LogFactory.getLog(TagPointSamplingJob.class);

	private String term;

	public TagPointSamplingJob() {
	}

	@Override
	public void run() {
		//
		try {
			
			log.debug("Point sampling job starting... : term=[" + term + "]");

			term = context.getJobDetail().getJobDataMap().getString("term");
			
			long start = System.currentTimeMillis();
			
			final long current_timestamp = ((long) (System.currentTimeMillis() / 1000)) * 1000 ; //MS단위 절삭
			final String from = JobDateUtils.from(current_timestamp, term);
			final String to = JobDateUtils.to(current_timestamp);
			final Map<String, Point> point_map = new PointMapSnapshot().snapshot(); //메모리
			
			//
			final StorageClient client = new StorageClient();
			
			List<Point> point_list = new ArrayList<>();
			
			//
			TagDAO tag_dao = new TagDAO();
			List<Tag> tag_list = tag_dao.selectTagAll();
			for (int i = 0; tag_list != null && i < tag_list.size(); i++) {
				final Tag tag = tag_list.get(i);
					try {
						log.debug("Point sampling paremeter : id=" + tag.getTag_id() + ", type=" + tag.getJava_type() + ", from=" + from + ", to=" + to);
					    //마지막 값 및 속성 추가
						Point point = point_map.get(tag.getTag_id());
						if(point != null){
							point_list.add(point);
						}
					} catch (Exception e) {
						log.error("Point sampling error : tag_id=[" + tag.getTag_id() + "], error=[" + e.getMessage() + "]", e);
					}
			}
			
			if(point_list != null && point_list.size() > 0) {
				client.forInsert().insertPointSampling(term, current_timestamp, point_list);
			};

			long end = System.currentTimeMillis() - start;
			log.debug("Point sampling job completed : term=[" + term + "], point_list=[" + point_list.size() + "], exec_time=[" + end + "]ms");
			//

		} catch (Exception e) {
			log.error("Point sampling data insert failed : " + e.getMessage(), e);
		}
		;
	}

}
