package com.facebook.datafetcher;

import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.math.BigDecimal;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Formatter;
import java.util.Iterator;
import java.util.List;

import au.com.bytecode.opencsv.CSVReader;
import au.com.bytecode.opencsv.CSVWriter;

import com.restfb.Connection;
import com.restfb.DefaultFacebookClient;
import com.restfb.FacebookClient;
import com.restfb.Parameter;
import com.restfb.json.JsonObject;
import com.restfb.types.Insight;
import com.restfb.types.Page;

public class PageFansCountFetcher {

	/**
	 * @param args
	 * @throws IOException
	 */
	public final static DateFormat dateFormat = new SimpleDateFormat(
			"yyyy-MM-dd-HH-mm");
	private static final int FETCH_LIMIT = 1;
	private static final String END_TIME = "end_time";
	private static final String VALUE = "value";
	private static final String PAGE_FANS_QUERY = "/insights/page_fans_country";
	private static final String PAGE_FANS_AUTHENTICATED_QUERY = "/insights/page_fans";

	public static List<String[]> dataDaily = new ArrayList<String[]>();

	public static void main(String[] args) throws IOException, ParseException {
		String absolutePath = args[0];
		String csvFilename = absolutePath+"ids.csv";
		String csv = "output" + dateFormat.format(new Date()).toString()
				+ ".csv";
		CSVWriter writer = new CSVWriter(new FileWriter(absolutePath+csv));
		CSVReader csvReader = new CSVReader(new FileReader(csvFilename));
		String[] row = null;
		while ((row = csvReader.readNext()) != null) {
			String token = "XXXXXXXXXXXXXXXXXXXXX|XXXXXXXXXXXXXXXXXXXX";//Token
			if (row.length > 1) {
				token = row[1];
			}
			FacebookClient facebookClient = new DefaultFacebookClient(token);
			fetchPageFansHistoricCount(row[0], facebookClient);
		}
		writer.writeAll(dataDaily);
		writer.close();

		csvReader.close();

	}

	private static void fetchPageFansHistoricCount(String pageName,
			FacebookClient facebookClient) throws ParseException {
		String method = "fetchPageFans";

		Date untilDate = new Date();

		Connection<Insight> insights = null;

		insights = facebookClient.fetchConnection(pageName + PAGE_FANS_QUERY,
				Insight.class,
				Parameter.with(FacebookFetcherConstants.LIMIT, FETCH_LIMIT),
				Parameter.with(FacebookFetcherConstants.UNTIL, "now"));
		try {
			Thread.sleep(1000);
		} catch (InterruptedException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

		Insight insight = insights.getData().get(insights.getData().size() - 1);
		List<JsonObject> jsonObjectList = insight.getValues();
		int count = 0;
		int index = jsonObjectList.size() - 1;

		String metricFetchTimeString = (String) jsonObjectList.get(index).get(
				END_TIME);
		// Specific handling required for date conversion
		metricFetchTimeString = metricFetchTimeString.replaceAll("T", "");

		Double totalCount = 0.0;
		if (jsonObjectList.get(index).get(VALUE) instanceof JsonObject) {
			JsonObject jsonObject = (JsonObject) jsonObjectList.get(index).get(
					VALUE);
			Iterator<String> keys = (Iterator<String>) jsonObject.keys();
			while (keys.hasNext()) {
				String key = keys.next();
				Integer value = (Integer) jsonObject.get(key);
				totalCount += value;
			}
		}
		
		 
		BigDecimal bg = new BigDecimal(totalCount);
		Formatter fmt = new Formatter();
		fmt.format("%." + bg.scale() + "f", bg);
		System.out.println(pageName + " : " + metricFetchTimeString + " : "
				+ fmt.toString());
		dataDaily.add(new String[] { pageName, metricFetchTimeString,
				fmt.toString() });

	}

	private static void fetchFansCount(String pageName, FacebookClient facebookClient) {
		Page page = facebookClient.fetchObject(pageName, Page.class);
		try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		String metricFetchTimeString = dateFormat.format(new Date());
		Double pageLikes = page.getLikes().doubleValue();
		BigDecimal bg = new BigDecimal(pageLikes);
		Formatter fmt = new Formatter();
		fmt.format("%." + bg.scale() + "f", bg);
		System.out.println(pageName + " : " + metricFetchTimeString + " : "
				+ fmt.toString());
		dataDaily.add(new String[] { pageName, metricFetchTimeString,
				fmt.toString() });

	}

}
