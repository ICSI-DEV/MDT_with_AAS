package plantpulse.cep.engine.scheduling.job;

import java.io.File;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.JSONArray;

import plantpulse.cep.dao.TagDAO;
import plantpulse.cep.engine.CEPEngineManager;
import plantpulse.cep.engine.pipeline.DataFlowPipe;
import plantpulse.cep.engine.timer.TimerExecuterPool;
import plantpulse.cep.engine.utils.JavaTypeUtils;
import plantpulse.cep.service.client.StorageClient;
import plantpulse.domain.Tag;
import plantpulse.event.opc.Point;
import plantpulse.server.mvc.util.DateUtils;
import plantpulse.timeseries.analytics.Algorithms;
import plantpulse.timeseries.analytics.PredictedValue;
import plantpulse.timeseries.analytics.TimeseriesForecater;

/**
 * TagPointForecastJob
 * 
 * @author leesa
 *
 */
public class TagPointForecastJob  {

	private static final Log log = LogFactory.getLog(TagPointForecastJob.class);

	private ScheduledFuture<?> timer;
	
	private TagDAO tag_dao = new TagDAO();
	
	private DataFlowPipe dataflow = CEPEngineManager.getInstance().getData_flow_pipe();
	
	private StorageClient storage_service = new StorageClient();
	
	private String tag_id;
	private String learning_tag_id;
	private int learning_before_minutes; //지금으로부터의 이전 몇분가지 학습 :  EX) - 1M
	private int term; //데이터와 데이터사이의 간격
	private int predict_count; //60 
	private long timestamp_fit;
	
	//
	public TagPointForecastJob(String tag_id, String learning_tag_id, int learning_before_minutes, int term, int predict_count, long timestamp_fit) {
		super();
		
		this.tag_id = tag_id;
		this.learning_tag_id = learning_tag_id;
		this.learning_before_minutes = learning_before_minutes; //10
		this.term = term; //1
		this.predict_count = predict_count; //60
		this.timestamp_fit = timestamp_fit;
	};

	public void start(){
		
		long schedule_interval = (term * predict_count) / 2;
		
		timer = TimerExecuterPool.getInstance().getPool().scheduleAtFixedRate(new Runnable() {
			@Override
			public void run() {
				//
				try {
					
					log.debug("Tag point forecast job start...");

					//
					long start = (System.currentTimeMillis()/1000) * 1000;
					Tag tag = tag_dao.selectTagInfoWithSiteAndOpc(tag_id);
					if(tag == null){
						log.warn("Tag is NULL.  : tag_id=" + tag_id);
						return;
					}
					Tag learning_tag = tag_dao.selectTag(learning_tag_id);
					if(learning_tag == null){
						log.warn("Learning tag is NULL. : learning_tag_id=" + learning_tag_id);
						return;
					}
					//
					
					//
					LocalDateTime  to  = LocalDateTime.now().plusMinutes(1);
					LocalDateTime  from    = LocalDateTime.now().minusMinutes(learning_before_minutes);
					DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");//
					
					log.debug("Learning data : tag_id=[" + learning_tag_id + "], from=[" + formatter.format(from) + "], to=[" + formatter.format(to) + "]");
					
					JSONArray a_array = storage_service.forSelect().selectPointValueList(learning_tag, formatter.format(from) + "", formatter.format(to) + "", null, null);
					if (a_array == null || a_array.size() < 1) {
						log.warn("Learning data not found : tag_id=[" + learning_tag_id + "], learning_before_minutes=[" + learning_before_minutes+ "], from=[" + formatter.format(from) + "], to=[" + formatter.format(to) + "]");
						return;
					}

					//long learn_count = a_array.size();
					//
					String file_name = "FORECAST_" + learning_tag_id + "_" + System.currentTimeMillis() + ".arff";
					File file = makeARFF(file_name, a_array);
					TimeseriesForecater tf = new TimeseriesForecater();
					long last_learn_timestamp = a_array.getJSONArray(a_array.size()-1).getLong(0);
					String algorithm = Algorithms.FORECAST_ALOGRITHM_SMO_REG;
					List<PredictedValue> list = tf.forecast(file.getAbsolutePath(), algorithm,  predict_count, term, last_learn_timestamp, timestamp_fit);
					if (list == null || list.size() < 1) {
						log.warn("Predict data is not exist : tag_id=[" + tag_id + "]");
						return;
					};
					
					long prc_forecast = (System.currentTimeMillis() - start) + (1000);
					
					log.debug("Predict data size : size=[" + list.size() + "]");
					//
					for (int i = 0; i < list.size(); i++) {
						//
						Point point = new Point();
						PredictedValue predict = list.get(i);
						long timestamp = predict.getTimestamp();
						long timestamp_sec_f = TimeUnit.MILLISECONDS.toSeconds(timestamp - prc_forecast) * 1000;
						point.setTimestamp(timestamp_sec_f);
						String value = ((Double)predict.getValue()).toString();
						point.setValue(value);
						point.setType(JavaTypeUtils.convertJavaTypeToDataType(tag.getJava_type()));
						point.setSite_id(tag.getSite_id());
						point.setOpc_id(tag.getOpc_id());
						point.setTag_id(tag.getTag_id());
						point.setTag_name(tag.getTag_name());
						point.setQuality(192);
						point.setError_code(0);
						//
						Map<String,String> attribute = new HashMap<>();
						attribute.put("learn_tag_id", learning_tag_id + "");
						attribute.put("learn_from",   formatter.format(from) + "");
						attribute.put("learn_to",     formatter.format(to)   + "");
						//
						attribute.put("actual", predict.getActual() + "");
						attribute.put("weight", predict.getWeight() + "");
						attribute.put("limit_min", predict.getLimit_min() + "");
						attribute.put("limit_max", predict.getLimit_max() + "");
						//
						attribute.put("last_learn_timestamp", last_learn_timestamp + "");
						point.setAttribute(attribute);
						//
						log.debug("Predicted value : timestamp=[" + new Date(timestamp).toString() + "], value=[" + value + "]");
						//
						
						dataflow.flow(point);
					};

					file.delete();

					//
					long end = System.currentTimeMillis() - start;
					
					log.debug("Tag point forecast job completed : tag_id=[" + tag.getTag_id() + "], learning_tag=[" + learning_tag.getTag_id() + "], learning_before_minutes=[" + learning_before_minutes+ "], predicted_size=[" + list.size() + "], process_time=[" + end + "]ms");

				}catch(Exception ex){
					log.error("Point forecast job error : " + ex.getMessage(), ex);
				}
			}
		}, 1000 * 30, schedule_interval, TimeUnit.MILLISECONDS);
	}

	public void stop(){
		timer.cancel(true);
	}
	
	
	public File makeARFF(String file_name, JSONArray array) throws Exception {
		File file = null;
		try {
			long start = System.currentTimeMillis();

			StringBuffer buffer = new StringBuffer();
			buffer.append("@relation tag").append("\n");
			buffer.append("@attribute Date DATE 'yyyy-MM-ddHH:mm:ss'").append("\n");
			buffer.append("@attribute Value NUMERIC").append("\n");
			buffer.append("@data").append("\n");

			//
			String prev_date = "";
			for (int i = 0; i < array.size(); i++) {
				JSONArray data = array.getJSONArray(i);
				String date = DateUtils.fmtARFF((Long) data.get(0));
				String value = (new Double(data.get(1).toString())).toString();
				if (!prev_date.equals(date)) {
					buffer.append("" + date + "," + value).append("\n");
					prev_date = date;
				}
			}

			file = new File(System.getProperty("java.io.tmpdir") + "/plantpulse/" + file_name);
			FileUtils.writeStringToFile(file, buffer.toString());

			log.debug("ARFF file Write completed. time=" + (System.currentTimeMillis() - start));

		} catch (IOException e) {
			log.error("ARFF file write error : " + e.getMessage(), e);
		}

		return file;
	}

	

}
