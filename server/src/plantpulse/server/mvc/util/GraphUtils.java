package plantpulse.server.mvc.util;

import java.util.ArrayList;
import java.util.List;

import org.json.JSONObject;

import plantpulse.cep.domain.Graph;

public class GraphUtils {

	public static String getGraphJSON(Graph graph) {

		//
		if (graph.getLine_goals() != null) {
			graph.setLine_goals(toFitDoubleArray(graph.getLine_goals()));
		}
		if (graph.getArea_goals() != null) {
			graph.setArea_goals(toFitDoubleArray(graph.getArea_goals()));
		}

		return JSONObject.fromObject(graph).toString();
	}

	private static Double[] toFitDoubleArray(Double[] inputArray) {
		//
		if (inputArray != null) {
			List<Double> list = new ArrayList<Double>();
			for (int i = 0; i < inputArray.length; i++) {
				Double value = inputArray[i];
				if (value != null) {
					list.add(value);
				}
			}
			//
			Double[] array = list.toArray(new Double[list.size()]);

			return array;
		} else {
			return null;
		}
	}

}
