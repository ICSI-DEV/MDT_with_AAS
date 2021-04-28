package plantpulse.server.mvc.forecast;

import java.io.File;
import java.util.List;

import javax.servlet.http.HttpServletRequest;

import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.JSONArray;
import org.json.JSONObject;
import org.realdisplay.framework.util.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.ModelAttribute;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

import plantpulse.cep.domain.Forecast;
import plantpulse.cep.engine.utils.JSONNumberUtils;
import plantpulse.cep.service.SiteService;
import plantpulse.cep.service.StorageService;
import plantpulse.cep.service.TagService;
import plantpulse.cep.stats.PointArrayStatistics;
import plantpulse.domain.Tag;
import plantpulse.server.mvc.util.ARRFUtils;
import plantpulse.server.mvc.util.DateUtils;
import plantpulse.server.mvc.util.JSONSortUtils;
import plantpulse.timeseries.analytics.AbnomalValue;
import plantpulse.timeseries.analytics.PredictedValue;
import plantpulse.timeseries.analytics.TimeseriesAbnomalDetection;
import plantpulse.timeseries.analytics.TimeseriesForecater;

@Controller
public class ForecastController {

	private static final Log log = LogFactory.getLog(ForecastController.class);

	@Autowired
	private StorageService storage_service;

	@Autowired
	private TagService tag_service;
	@Autowired
	private SiteService site_service;

	@RequestMapping(value = "/forecast/index", method = RequestMethod.GET)
	public String view(Model model, HttpServletRequest request) throws Exception {

		String learning_date_from = DateUtils.currDateByMinus10Minutes();
		String learning_date_to = DateUtils.currDateBy24();
		model.addAttribute("learning_date_from", learning_date_from);
		model.addAttribute("learning_date_to", learning_date_to);

		Forecast forecast = new Forecast();
		forecast.setForecast_count(60);        //60건 예측
		forecast.setProbability_filter(0.0);   //0.0 이상
		model.addAttribute("forecast", forecast);
		return "/forecast/index";
	}

	@RequestMapping(value = "/forecast/process", method = RequestMethod.POST)
	public @ResponseBody String process(@ModelAttribute Forecast forecast, Model model) {
		File arrf = null;
		try {
			//
			log.info("Parameters = " + forecast.toString());
			//
			long start = System.currentTimeMillis();
			Tag tag = tag_service.selectTag(forecast.getTag_id());
			//
			JSONArray a_array = storage_service.search( tag, forecast.getLearning_date_from() + ":00", forecast.getLearning_date_to() + ":59", forecast.getSampling(), forecast.getTerm());
			if (a_array == null || a_array.size() < 1) {
				throw new Exception("학습기간에 해당하는 데이터가 없습니다. 검색기간을 더 길게 조정하십시오.");
			};

			//
			int term = 1000;
			if (StringUtils.isEmpty(forecast.getTerm())) {
				term = 1000;
			} else if (forecast.getTerm().equals("1_MINUTES")) {
				term = 1000 * 60;
			} else if (forecast.getTerm().equals("10_MINUTES")) {
				term = 1000 * 60 * 10;
			} else if (forecast.getTerm().equals("30_MINUTES")) {
				term = 1000 * 60 * 30;
			} else if (forecast.getTerm().equals("1_HOURS")) {
				term = 1000 * 60 * 60;
			}
			;
			
			
			//log.info("1. FORECAST VALUES =------------------- ");
			arrf = ARRFUtils.createFileARFF(a_array);
			TimeseriesForecater tf = new TimeseriesForecater();
			List<PredictedValue> forecast_list = tf.forecast(arrf.getAbsolutePath(), forecast.getForecast_algorithm(), forecast.getForecast_count(), term, a_array.getJSONArray(a_array.size()-1).getLong(0), 0);
			if (forecast_list == null || forecast_list.size() < 1) {
				throw new Exception("예측된 결과 목록이 없습니다.");
			};
			
			JSONArray forecast_data = new JSONArray();
			JSONArray b_array = new JSONArray();
			JSONArray b_ranges = new JSONArray();
			
			//
			for (int i = 0; i < forecast_list.size(); i++) {
				JSONArray one = new JSONArray();
				PredictedValue predicted = forecast_list.get(i);
				long timestamp = predicted.getTimestamp();
				Double value = JSONNumberUtils.convertInfiniteDouble(predicted.getValue());
				one.add(timestamp);
				one.add(value);
				one.add(DateUtils.fmtISO(timestamp));
				b_array.add(one);
				
				JSONArray range = new JSONArray();
				range.add(timestamp);
				range.add(JSONNumberUtils.convertInfiniteDouble(predicted.getLimit_min()));
				range.add(JSONNumberUtils.convertInfiniteDouble(predicted.getLimit_max()));
				b_ranges.add(range);
				
				JSONObject row = new JSONObject();
				row.put("timestamp", timestamp);
				row.put("value",JSONNumberUtils.convertInfiniteDouble( predicted.getValue()));
				row.put("actual", 0);
				row.put("weight", JSONNumberUtils.convertInfiniteDouble( predicted.getWeight()));
				row.put("limit_min", JSONNumberUtils.convertInfiniteDouble(predicted.getLimit_min()));
				row.put("limit_max", JSONNumberUtils.convertInfiniteDouble(predicted.getLimit_max()));
				forecast_data.add(row);
			}
			
			//log.info("3. ABNORMAL VALUES =------------------- ");
			TimeseriesAbnomalDetection abnomal = new TimeseriesAbnomalDetection();
			List<AbnomalValue> abnmal_list = abnomal.detection(arrf.getAbsolutePath(), forecast.getAbnomal_algorithm());
			JSONArray abnomal_data = new JSONArray();
			JSONArray c_array = new JSONArray();
			for (int i = 0; abnmal_list != null && i < abnmal_list.size(); i++) {
				AbnomalValue value = abnmal_list.get(i);
				if(forecast.getProbability_filter() <= value.getProbability()) { //개연성 필터링
					JSONObject row = new JSONObject();
					row.put("timestamp",     value.getTimestamp());
					row.put("value",         JSONNumberUtils.convertInfiniteDouble(value.getValue()));
					row.put("probability",   JSONNumberUtils.convertInfiniteDouble(value.getProbability()));
					abnomal_data.add(row);
					//
					JSONArray one = new JSONArray();
					long timestamp = value.getTimestamp();
					Double d_value = JSONNumberUtils.convertInfiniteDouble(value.getValue());
					one.add(timestamp);
					one.add(d_value);
					one.add(DateUtils.fmtISO(timestamp));
					c_array.add(one);
				};
			};

			abnomal_data = JSONSortUtils.sortArrayByInJSONKey(abnomal_data, "timestamp");
			c_array      = JSONSortUtils.sortArrayByInArrayIndex(c_array,  0);
			
			//
			JSONObject json = new JSONObject();
			json.put("status", "SUCCESS");
			
			json.put("forecast_data",     forecast_data);
			json.put("a_array",  a_array);  //러닝 데이터
			json.put("b_array",  b_array);  //예측 데이터
			json.put("b_ranges", b_ranges); //예측 레인지 데이터
			
			json.put("abnomal_data",  abnomal_data);  //이상진단 데이터
			json.put("c_array",  c_array);  //이상진단 데이터
			
			PointArrayStatistics stats = new PointArrayStatistics(a_array);
			json.put("min_value",  stats.getMin());  
			json.put("max_value",  stats.getMax());  
			//
			long end = System.currentTimeMillis() - start;

			json.put("result_learning_count", (a_array != null) ? a_array.size() : 0);
			json.put("result_abnomal_count",  (c_array != null) ? c_array.size() : 0);
			json.put("result_time", end);

			return json.toString();
			
		} catch (Exception ex) {
			log.error("Forecast error : " + ex.getMessage(), ex);
			JSONObject json = new JSONObject();
			json.put("status", "ERROR");
			json.put("message", ex.getMessage());
			return json.toString();
			
		} finally {
			if(arrf != null && arrf.exists()) FileUtils.deleteQuietly(arrf);
		}
	}

	

}
