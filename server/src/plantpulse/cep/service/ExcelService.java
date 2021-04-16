/*
 *  JDesigner (R) Web Application Development Platform 
 *  Copyright (C) 2010 JDesigner / SangBoo Lee (sangboo.lee@kopens.com)
 * 
 *  http://www.jdesigner.org/
 *  
 *  Please contact if you require licensing terms other
 *  than GPL-3 for this software.
 *
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or
 *  (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package plantpulse.cep.service;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.poi.hssf.usermodel.HSSFCell;
import org.apache.poi.hssf.usermodel.HSSFCellStyle;
import org.apache.poi.hssf.usermodel.HSSFDataFormat;
import org.apache.poi.hssf.usermodel.HSSFFont;
import org.apache.poi.hssf.usermodel.HSSFRichTextString;
import org.apache.poi.hssf.usermodel.HSSFRow;
import org.apache.poi.hssf.usermodel.HSSFSheet;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.hssf.util.HSSFColor;
import org.apache.poi.ss.usermodel.CellStyle;
import org.apache.poi.ss.usermodel.Font;
import org.springframework.stereotype.Service;

import plantpulse.cep.engine.utils.DateUtils;

/**
 * ExcelService
 * 
 * @author SangBoo Lee
 * @date 2009. 11. 17.
 * @since 1.0
 * 
 */
@Service
public class ExcelService {

	private static final Log log = LogFactory.getLog(ExcelService.class);

	private static final String FONT_FAMILY = "맑은 고딕";
	private static final short HEADER_BACKGROUND_COLOR = HSSFColor.BLUE.index;
	private static final short HEADER_FONT_COLOR = HSSFColor.WHITE.index;
	private static final short CELL_BACKGROUND_COLOR = HSSFColor.WHITE.index;
	private static final short CELL_BORDER_COLOR = HSSFColor.GREY_50_PERCENT.index;
	private static final short CELL_FONT_COLOR = HSSFColor.BLACK.index;
	private static final String CELL_NUMBER_FORMAT = "###,###,###,###,###,###,###,##0";

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.jdesigner.application.template.service.ExcelService#readExcel(
	 * java.lang.String[], java.lang.String)
	 */
	public List readExcel(String[] valueKeyArray, String inputPath) throws Exception {
		List result = null;
		FileInputStream fis = null;
		try {

			log.info("POI Excel read start...");

			//
			fis = new FileInputStream(inputPath);
			HSSFWorkbook workbook = new HSSFWorkbook(fis);

			//
			HSSFSheet sheet = workbook.getSheetAt(0);

			int lastRowNum = sheet.getLastRowNum();
			log.debug("lastRowNum=[" + lastRowNum + "]");

			//
			result = new ArrayList();
			for (int rIndex = 1; rIndex < (lastRowNum + 1); rIndex++) {
				HSSFRow valueRow = sheet.getRow(rIndex);
				// Map map = new OrderedHashMap();
				Map map = new HashMap();
				for (int vkIndex = 0; vkIndex < valueKeyArray.length; vkIndex++) {
					HSSFCell valueCell = valueRow.getCell(vkIndex);
					String value = "";
					// try{
					// value = (new
					// Double(valueCell.getNumericCellValue()).intValue()) + "";
					// log.debug("getNumericCellValue=" + value);
					// }catch(Exception ex){
					value = valueCell.getRichStringCellValue().getString();
					// log.debug("getRichStringCellValue=" + value);
					// }
					map.put(valueKeyArray[vkIndex], value);
				}
				result.add(map);
			}

			log.info("POI Excel read complete : inputPath=[" + inputPath + "]");

		} catch (Exception e) {
			String msg = "POI Excel read error : " + e.getMessage();
			log.error(msg, e);
			throw new Exception(msg, e);
		} finally {
			try {
				fis.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return result;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.jdesigner.application.template.service.ExcelService#writeExcel
	 * (java.lang.String[], java.lang.String[], java.util.List,
	 * java.lang.String)
	 */
	public File writeExcel(String[] headerLabelArray, String[] valueKeyArray, List mapList, String outputPath) throws Exception {
		File saveFile = null;
		FileOutputStream fos = null;

		try {

			log.info("POI Excel write start...");

			//
			HSSFWorkbook wb = new HSSFWorkbook();

			//
			HSSFSheet sheet = wb.createSheet();

			// sheet.setDefaultColumnWidth(18);
			// sheet.setDefaultRowHeight((short)20);
			// sheet.autoSizeColumn((short)1);
			// sheet.setColumnWidth(1, 2000);
			// for(int ax=0; ax < 6; ax++) sheet.autoSizeColumn((short)ax);

			//
			HSSFCellStyle headerStyle = createHeaderCellStyle(wb);
			HSSFCellStyle stringStyle = createStringCellStyle(wb);
			HSSFCellStyle numberStyle = createNumberCellStyle(wb);
			HSSFCellStyle dateStyle = createDateCellStyle(wb);

			// 헤더 행 생성
			HSSFRow hRow = sheet.createRow(0);
			for (int hcIndex = 0; hcIndex < headerLabelArray.length; hcIndex++) {
				HSSFCell headerCell = hRow.createCell(hcIndex);
				headerCell.setCellStyle(headerStyle);
				headerCell.setCellValue(new HSSFRichTextString(headerLabelArray[hcIndex]));
			}

			//
			log.info("POI Excel header label create success.");

			//
			log.info("POI Excel : valueKeyArray = " + Arrays.asList(valueKeyArray));

			// 값 셀 생성
			for (int vrIndex = 1; vrIndex < (mapList.size() + 1); vrIndex++) {
				HSSFRow valueRow = sheet.createRow(vrIndex);

				Map map = (Map) mapList.get(vrIndex - 1);
				for (int vkIndex = 0; vkIndex < valueKeyArray.length; vkIndex++) {

					// if(!map.containsKey((valueKeyArray[vkIndex]))) throw new
					// Exception("Not found value key : " +
					// (valueKeyArray[vkIndex]));

					Object value = map.get(valueKeyArray[vkIndex]);

					HSSFCell valueCell = valueRow.createCell(vkIndex);
					if (value != null) {
						Class typeClass = value.getClass();
						if (typeClass == String.class) {
							try {
								double parsedDouble = new Double(value.toString()).doubleValue();
								valueCell.setCellValue(parsedDouble);//
								valueCell.setCellStyle(numberStyle);
							} catch (Exception ex) {
								valueCell.setCellValue(new HSSFRichTextString(value.toString())); // 문자열
								valueCell.setCellStyle(stringStyle);
							}

						} else if (typeClass == int.class || typeClass == Integer.class) {
							valueCell.setCellValue(new Integer(value.toString()).intValue());//
							valueCell.setCellStyle(numberStyle);

						} else if (typeClass == long.class || typeClass == Long.class) {
							valueCell.setCellValue(new Long(value.toString()).longValue());//
							valueCell.setCellStyle(numberStyle);

						} else if (typeClass == float.class || typeClass == Float.class) {
							valueCell.setCellValue(new Float(value.toString()).floatValue());//
							valueCell.setCellStyle(numberStyle);

						} else if (typeClass == double.class || typeClass == Double.class) {
							valueCell.setCellValue(new Double(value.toString()).doubleValue());//
							valueCell.setCellStyle(numberStyle);

						} else if (typeClass == BigDecimal.class) {
							valueCell.setCellValue(((BigDecimal) value).doubleValue());//
							valueCell.setCellStyle(numberStyle);

						} else if (typeClass == Date.class) {
							valueCell.setCellValue(new HSSFRichTextString(DateUtils.format((Date) value)));//
							valueCell.setCellStyle(dateStyle);

						} else if (typeClass == Timestamp.class) {
							valueCell.setCellValue(new HSSFRichTextString(DateUtils.format(new Date(((Timestamp) value).getTime()))));//
							valueCell.setCellStyle(dateStyle);
						}
					} else {
						valueCell.setCellValue(new HSSFRichTextString(""));
						valueCell.setCellStyle(stringStyle);
					}
				}
			}
			//
			log.info("POI Excel row/cell value create success.");

			// 쉬트 사이즈 조정
			for (int ax = 0; ax < valueKeyArray.length; ax++) {
				sheet.autoSizeColumn((short) ax);
			}

			// 엑셀 파일 쓰기 시작
			log.info("POI Excel create file start...");
			saveFile = new File(outputPath);

			// 멀티 유저의 파일 쓰기 작업시 쓰레드당 파일 안정성을 위해 디렉토리에 이미 같은 파일이 존재하면,
			// 새로운 파일명으로 저장한다.
			if (saveFile.exists()) {
				log.info("Same file name already exists, so the new name of the file write operation is executed.");
				String uniqueSuffix = System.currentTimeMillis() + "";
				outputPath = outputPath + "_" + uniqueSuffix;
				saveFile = new File(outputPath);
			}
			;

			//
			fos = new FileOutputStream(saveFile);
			wb.write(fos);
			fos.flush();
			fos.close();

			//
			log.info("POI Excel create file complete : outputPath=[" + outputPath + "]");

		} catch (Exception e) {
			String msg = "POI Excel write error : " + e.getMessage();
			log.error(msg, e);
			throw new Exception(msg, e);
		} finally {
			try {
				if (fos != null)
					fos.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return saveFile;
	}

	public File writeExcel(ExcelSheetData[] sheetArray, String outputPath) throws Exception {
		File saveFile = null;
		FileOutputStream fos = null;

		try {

			log.info("POI Excel write start...");

			//
			HSSFWorkbook wb = new HSSFWorkbook();

			for (int si = 0; si < sheetArray.length; si++) {
				//
				ExcelSheetData sheetData = sheetArray[si];

				HSSFSheet sheet = wb.createSheet(sheetData.getName());

				log.info("POI Sheet created : name=[" + sheetData.getName() + "]");

				// sheet.setDefaultColumnWidth(18);
				// sheet.setDefaultRowHeight((short)20);
				// sheet.autoSizeColumn((short)1);
				// sheet.setColumnWidth(1, 2000);
				// for(int ax=0; ax < 6; ax++) sheet.autoSizeColumn((short)ax);

				//
				HSSFCellStyle headerStyle = createHeaderCellStyle(wb);
				HSSFCellStyle stringStyle = createStringCellStyle(wb);
				HSSFCellStyle numberStyle = createNumberCellStyle(wb);
				HSSFCellStyle dateStyle = createDateCellStyle(wb);

				// 헤더 행 생성
				HSSFRow hRow = sheet.createRow(0);
				for (int hcIndex = 0; hcIndex < sheetArray[si].getHeaderLabelArray().length; hcIndex++) {
					HSSFCell headerCell = hRow.createCell(hcIndex);
					headerCell.setCellStyle(headerStyle);
					headerCell.setCellValue(new HSSFRichTextString(sheetArray[si].getHeaderLabelArray()[hcIndex]));
				}

				//
				log.info("POI Excel header label create success.");

				//
				log.info("POI Excel : valueKeyArray = " + Arrays.asList(sheetArray[si].getValueKeyArray()));

				// 값 셀 생성
				for (int vrIndex = 1; vrIndex < (sheetArray[si].getMapList().size() + 1); vrIndex++) {
					HSSFRow valueRow = sheet.createRow(vrIndex);

					Map map = sheetArray[si].getMapList().get(vrIndex - 1);
					for (int vkIndex = 0; vkIndex < sheetArray[si].getValueKeyArray().length; vkIndex++) {

						// if(!map.containsKey((valueKeyArray[vkIndex]))) throw
						// new
						// Exception("Not found value key : " +
						// (valueKeyArray[vkIndex]));

						Object value = map.get(sheetArray[si].getValueKeyArray()[vkIndex]);

						HSSFCell valueCell = valueRow.createCell(vkIndex);
						if (value != null) {
							Class typeClass = value.getClass();
							if (typeClass == String.class) {
								try {
									double parsedDouble = new Double(value.toString()).doubleValue();
									valueCell.setCellValue(parsedDouble);//
									valueCell.setCellStyle(numberStyle);
								} catch (Exception ex) {
									valueCell.setCellValue(new HSSFRichTextString(value.toString())); // 문자열
									valueCell.setCellStyle(stringStyle);
								}

							} else if (typeClass == int.class || typeClass == Integer.class) {
								valueCell.setCellValue(new Integer(value.toString()).intValue());//
								valueCell.setCellStyle(numberStyle);

							} else if (typeClass == long.class || typeClass == Long.class) {
								valueCell.setCellValue(new Long(value.toString()).longValue());//
								valueCell.setCellStyle(numberStyle);

							} else if (typeClass == float.class || typeClass == Float.class) {
								valueCell.setCellValue(new Float(value.toString()).floatValue());//
								valueCell.setCellStyle(numberStyle);

							} else if (typeClass == double.class || typeClass == Double.class) {
								valueCell.setCellValue(new Double(value.toString()).doubleValue());//
								valueCell.setCellStyle(numberStyle);

							} else if (typeClass == BigDecimal.class) {
								valueCell.setCellValue(((BigDecimal) value).doubleValue());//
								valueCell.setCellStyle(numberStyle);

							} else if (typeClass == Date.class) {
								valueCell.setCellValue(new HSSFRichTextString(DateUtils.format((Date) value)));//
								valueCell.setCellStyle(dateStyle);

							} else if (typeClass == Timestamp.class) {
								valueCell.setCellValue(new HSSFRichTextString(DateUtils.format(new Date(((Timestamp) value).getTime()))));//
								valueCell.setCellStyle(dateStyle);
							}
						} else {
							valueCell.setCellValue(new HSSFRichTextString(""));
							valueCell.setCellStyle(stringStyle);
						}
					}
				}
				//
				log.info("POI Excel row/cell value create success.");

				// 쉬트 사이즈 조정
				for (int ax = 0; ax < sheetArray[si].getValueKeyArray().length; ax++) {
					sheet.autoSizeColumn((short) ax);
				}

			}

			// 엑셀 파일 쓰기 시작
			log.info("POI Excel create file start...");
			saveFile = new File(outputPath);

			// 멀티 유저의 파일 쓰기 작업시 쓰레드당 파일 안정성을 위해 디렉토리에 이미 같은 파일이 존재하면,
			// 새로운 파일명으로 저장한다.
			if (saveFile.exists()) {
				log.info("Same file name already exists, so the new name of the file write operation is executed.");
				String uniqueSuffix = System.currentTimeMillis() + "";
				outputPath = outputPath + "_" + uniqueSuffix;
				saveFile = new File(outputPath);
			}
			;

			//
			fos = new FileOutputStream(saveFile);
			wb.write(fos);
			fos.flush();
			fos.close();

			//
			log.info("POI Excel create file complete : outputPath=[" + outputPath + "]");

		} catch (Exception e) {
			String msg = "POI Excel write error : " + e.getMessage();
			log.error(msg, e);
			throw new Exception(msg, e);
		} finally {
			try {
				if (fos != null)
					fos.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return saveFile;
	}

	/**
	 * 엑셀 헤더 셀의 스타일을 반환한다.
	 * 
	 * @param aWorkbook
	 * @return
	 */
	private static HSSFCellStyle createHeaderCellStyle(final HSSFWorkbook aWorkbook) {
		final HSSFCellStyle cellStyle = aWorkbook.createCellStyle();

		cellStyle.setFillForegroundColor(HEADER_BACKGROUND_COLOR);
		cellStyle.setFillBackgroundColor(HEADER_BACKGROUND_COLOR);
		cellStyle.setFillPattern(CellStyle.SOLID_FOREGROUND);

		cellStyle.setVerticalAlignment(CellStyle.VERTICAL_CENTER);
		cellStyle.setAlignment(CellStyle.ALIGN_CENTER);

		cellStyle.setBorderBottom(CellStyle.BORDER_THIN);
		cellStyle.setBorderLeft(CellStyle.BORDER_THIN);
		cellStyle.setBorderRight(CellStyle.BORDER_THIN);
		cellStyle.setBorderTop(CellStyle.BORDER_THIN);

		HSSFFont font = aWorkbook.createFont();
		font.setBoldweight(Font.BOLDWEIGHT_BOLD);
		font.setColor(HEADER_FONT_COLOR);
		font.setFontName(FONT_FAMILY);
		cellStyle.setFont(font);
		return cellStyle;
	}

	/**
	 * 엑셀 문자형 값 셀의 스타일을 반환한다.
	 * 
	 * @param aWorkbook
	 * @return
	 */
	private static HSSFCellStyle createStringCellStyle(final HSSFWorkbook aWorkbook) {
		final HSSFCellStyle cellStyle = aWorkbook.createCellStyle();

		cellStyle.setFillForegroundColor(CELL_BACKGROUND_COLOR);
		cellStyle.setFillBackgroundColor(CELL_BACKGROUND_COLOR);
		cellStyle.setFillPattern(CellStyle.SOLID_FOREGROUND);

		cellStyle.setBottomBorderColor(CELL_BORDER_COLOR);
		cellStyle.setLeftBorderColor(CELL_BORDER_COLOR);
		cellStyle.setRightBorderColor(CELL_BORDER_COLOR);
		cellStyle.setTopBorderColor(CELL_BORDER_COLOR);

		cellStyle.setAlignment(CellStyle.ALIGN_CENTER);
		cellStyle.setBorderBottom(CellStyle.BORDER_THIN);
		cellStyle.setBorderLeft(CellStyle.BORDER_THIN);
		cellStyle.setBorderRight(CellStyle.BORDER_THIN);
		cellStyle.setBorderTop(CellStyle.BORDER_THIN);

		cellStyle.setVerticalAlignment(CellStyle.VERTICAL_CENTER);
		cellStyle.setAlignment(CellStyle.ALIGN_LEFT);

		HSSFFont font = aWorkbook.createFont();
		font.setColor(CELL_FONT_COLOR);
		font.setFontName(FONT_FAMILY);
		cellStyle.setFont(font);

		return cellStyle;
	}

	/**
	 * 엑셀 숫자형 값 셀의 스타일을 반환한다.
	 * 
	 * @param aWorkbook
	 * @return
	 */
	private static HSSFCellStyle createNumberCellStyle(final HSSFWorkbook aWorkbook) {

		final HSSFCellStyle cellStyle = aWorkbook.createCellStyle();

		cellStyle.setFillForegroundColor(CELL_BACKGROUND_COLOR);
		cellStyle.setFillBackgroundColor(CELL_BACKGROUND_COLOR);
		cellStyle.setFillPattern(CellStyle.SOLID_FOREGROUND);

		cellStyle.setBottomBorderColor(CELL_BORDER_COLOR);
		cellStyle.setLeftBorderColor(CELL_BORDER_COLOR);
		cellStyle.setRightBorderColor(CELL_BORDER_COLOR);
		cellStyle.setTopBorderColor(CELL_BORDER_COLOR);

		cellStyle.setAlignment(CellStyle.ALIGN_CENTER);
		cellStyle.setBorderBottom(CellStyle.BORDER_THIN);
		cellStyle.setBorderLeft(CellStyle.BORDER_THIN);
		cellStyle.setBorderRight(CellStyle.BORDER_THIN);
		cellStyle.setBorderTop(CellStyle.BORDER_THIN);

		cellStyle.setVerticalAlignment(CellStyle.VERTICAL_CENTER);
		cellStyle.setAlignment(CellStyle.ALIGN_RIGHT);

		HSSFFont font = aWorkbook.createFont();

		font.setColor(CELL_FONT_COLOR);
		font.setFontName(FONT_FAMILY);
		cellStyle.setFont(font);

		HSSFDataFormat df = aWorkbook.createDataFormat();
		cellStyle.setDataFormat(df.getFormat(CELL_NUMBER_FORMAT));
		return cellStyle;
	}

	/**
	 * 엑셀 날자형 값 셀의 스타일을 반환한다.
	 * 
	 * @param aWorkbook
	 * @return
	 */
	private static HSSFCellStyle createDateCellStyle(final HSSFWorkbook aWorkbook) {
		final HSSFCellStyle cellStyle = aWorkbook.createCellStyle();

		cellStyle.setFillForegroundColor(CELL_BACKGROUND_COLOR);
		cellStyle.setFillBackgroundColor(CELL_BACKGROUND_COLOR);
		cellStyle.setFillPattern(CellStyle.SOLID_FOREGROUND);

		cellStyle.setBottomBorderColor(CELL_BORDER_COLOR);
		cellStyle.setLeftBorderColor(CELL_BORDER_COLOR);
		cellStyle.setRightBorderColor(CELL_BORDER_COLOR);
		cellStyle.setTopBorderColor(CELL_BORDER_COLOR);

		cellStyle.setAlignment(CellStyle.ALIGN_CENTER);
		cellStyle.setBorderBottom(CellStyle.BORDER_THIN);
		cellStyle.setBorderLeft(CellStyle.BORDER_THIN);
		cellStyle.setBorderRight(CellStyle.BORDER_THIN);
		cellStyle.setBorderTop(CellStyle.BORDER_THIN);

		cellStyle.setVerticalAlignment(CellStyle.VERTICAL_CENTER);
		cellStyle.setAlignment(CellStyle.ALIGN_CENTER);

		HSSFFont font = aWorkbook.createFont();
		font.setColor(CELL_FONT_COLOR);
		font.setFontName(FONT_FAMILY);
		cellStyle.setFont(font);

		return cellStyle;
	}

}
