package plantpulse.cep.domain;

public class Forecast {

	private String tag_id;
	private String sampling;
	private String term;
	private String learning_date_from;
	private String learning_date_to;
	//
	private String forecast_algorithm;
	private int forecast_count;
	//
	private String abnomal_algorithm;
	private double probability_filter;

	public String getTag_id() {
		return tag_id;
	}

	public void setTag_id(String tag_id) {
		this.tag_id = tag_id;
	}

	public String getSampling() {
		return sampling;
	}

	public void setSampling(String sampling) {
		this.sampling = sampling;
	}

	public String getTerm() {
		return term;
	}

	public void setTerm(String term) {
		this.term = term;
	}

	public String getLearning_date_from() {
		return learning_date_from;
	}

	public void setLearning_date_from(String learning_date_from) {
		this.learning_date_from = learning_date_from;
	}

	public String getLearning_date_to() {
		return learning_date_to;
	}

	public void setLearning_date_to(String learning_date_to) {
		this.learning_date_to = learning_date_to;
	}

	public int getForecast_count() {
		return forecast_count;
	}

	public void setForecast_count(int forecast_count) {
		this.forecast_count = forecast_count;
	}

	
	public double getProbability_filter() {
		return probability_filter;
	}

	public void setProbability_filter(double probability_filter) {
		this.probability_filter = probability_filter;
	};
	
	public String getForecast_algorithm() {
		return forecast_algorithm;
	}

	public void setForecast_algorithm(String forecast_algorithm) {
		this.forecast_algorithm = forecast_algorithm;
	}

	public String getAbnomal_algorithm() {
		return abnomal_algorithm;
	}

	public void setAbnomal_algorithm(String abnomal_algorithm) {
		this.abnomal_algorithm = abnomal_algorithm;
	}

	@Override
	public String toString() {
		return "Forecast [tag_id=" + tag_id + ", sampling=" + sampling + ", term=" + term + ", learning_date_from="
				+ learning_date_from + ", learning_date_to=" + learning_date_to + ", forecast_algorithm="
				+ forecast_algorithm + ", forecast_count=" + forecast_count + ", abnomal_algorithm=" + abnomal_algorithm
				+ ", probability_filter=" + probability_filter + "]";
	}

	

	
}
