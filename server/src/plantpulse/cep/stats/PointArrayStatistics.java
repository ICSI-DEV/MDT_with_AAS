package plantpulse.cep.stats;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.json.JSONArray;

/**
 * <B>포인트 통계<B>
 * 
 * <P>
 * 통계의 경우 데이터를 불러오고 다시 CQL로 계산 쿼리를 던져서 처리하지말고, 불러온 데이터를 기반으로 자바내에서 연산한다. 이렇게
 * 처리하면 성능이 비약적으로 향상 된다. 카산ㄷ드라의 CQL의 COUNT,AVG,STDDEV 등의 함수는 매우 느리다.
 * </P>
 * 
 * @author leesangboo
 *
 */
public class PointArrayStatistics {
	
	private static final Log log = LogFactory.getLog(PointArrayStatistics.class);

	private int    count;
	private double first;
	private double last;
	private double min;
	private double max;
	private double sum;
	private double avg;
	private double stddev;

	/**
	 * PointStatistics 생성자 
	 * 
	 * @param data JSONArray  [[18283883818, 12.1], [18283884818, 12.1], [18283885818, 12.1], ...]
	 */
	public PointArrayStatistics(JSONArray data) {
		long start = System.currentTimeMillis();
		if (data != null && data.size() > 0) {
			int data_size = data.size();
			
			DescriptiveStatistics stats = new DescriptiveStatistics();
			for (int i = 0; i < data_size; i++) {
				stats.addValue(data.getJSONArray(i).getDouble(1));
			}
			//
			count = data_size;
			first = data.getJSONArray(0).getDouble(1);
			last  = data.getJSONArray(data_size - 1).getDouble(1);
			min   = stats.getMin();
			max   = stats.getMax();
			sum   = stats.getSum();
			avg   = stats.getMean();
			stddev = stats.getStandardDeviation();
		} else {
			//
			count = 0;
		};
		//
		long end = System.currentTimeMillis() - start;
		log.debug("Point array statistics process completed : process_time_ms=[" + end + "]");
	}

	public void setSum(double sum) {
		this.sum = sum;
	}

	public double getAvg() {
		return avg;
	}

	public void setAvg(double avg) {
		this.avg = avg;
	}

	public double getStddev() {
		return stddev;
	}

	public void setStddev(double stddev) {
		this.stddev = stddev;
	}

	public int getCount() {
		return count;
	}

	public double getFirst() {
		return first;
	}

	public double getLast() {
		return last;
	}

	public double getMin() {
		return min;
	}

	public double getMax() {
		return max;
	}

	public double getSum() {
		return sum;
	}


}
