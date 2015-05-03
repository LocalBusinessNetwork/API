package com.rw.APItest;

import static org.junit.Assert.*;

import java.util.Date;

import org.json.JSONException;
import org.json.JSONObject;
import org.junit.Test;

import com.rw.API.AnalyticsMgr;
import com.rw.API.SecMgr;

public class Trackker_tests {

	@Test
	public void test() throws Exception {
		
		
		SecMgr sm = new SecMgr();
		
		JSONObject data = new JSONObject();
	 	System.setProperty("PARAM3","STN");
	    
		data.put("act", "login");
		data.put("login", "phil@referralwiretest.biz");
		data.put("password","123456");
		
		JSONObject retVal = null;
		try {
			retVal = sm.handleRequest(data);
		} catch (Exception e) {
			e.printStackTrace();
			fail("Login Failed");
		}
		
		String id = retVal.get("data").toString();

		AnalyticsMgr a = new AnalyticsMgr(sm);	
		
		JSONObject data_in = new JSONObject();

		data_in.put("name", "UserActivity");
		data_in.put("event", "UM_LOGIN");
				
		long TwoWeeksAgo = 20160; //minutes
		Date compareToDate = new Date(System.currentTimeMillis() - TwoWeeksAgo * 60000);
		data_in.put("since_date",compareToDate);
	
		System.out.println(a.getMasterChartData(data_in));

		data_in.put("name", "MyMetrics");
		data_in.put("since_date",compareToDate);
	
		System.out.println(a.getMasterChartData(data_in));

		data_in.put("name", "EventSeries");
		data_in.put("event", "UM_LOGIN");
		data_in.put("unit", "dayOfMonth");
				
		data_in.put("since_date",compareToDate);
	
		System.out.println(a.getMasterChartData(data_in));
	
		data_in.put("name", "EventSeries");
		data_in.put("event", "UM_LOGIN");
		data_in.put("unit", "month");
				
		data_in.put("since_date",compareToDate);
	
		System.out.println(a.getMasterChartData(data_in));

		data_in.put("name", "EventSeries");
		data_in.put("event", "UM_LOGIN");
		data_in.put("unit", "year");
				
		data_in.put("since_date",compareToDate);
	
		System.out.println(a.getMasterChartData(data_in));
			
	}

}
