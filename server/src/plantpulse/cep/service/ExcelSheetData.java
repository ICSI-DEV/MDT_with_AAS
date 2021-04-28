package plantpulse.cep.service;

import java.util.List;
import java.util.Map;

public class ExcelSheetData {

	private String name;
	private String[] headerLabelArray;
	private String[] valueKeyArray;
	private List<Map<String, Object>> mapList;

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String[] getHeaderLabelArray() {
		return headerLabelArray;
	}

	public void setHeaderLabelArray(String[] headerLabelArray) {
		this.headerLabelArray = headerLabelArray;
	}

	public String[] getValueKeyArray() {
		return valueKeyArray;
	}

	public void setValueKeyArray(String[] valueKeyArray) {
		this.valueKeyArray = valueKeyArray;
	}

	public List<Map<String, Object>> getMapList() {
		return mapList;
	}

	public void setMapList(List<Map<String, Object>> mapList) {
		this.mapList = mapList;
	}

}
