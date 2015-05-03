package com.rw.APItest;

import static org.junit.Assert.*;

import java.util.Date;

import org.json.JSONException;
import org.json.JSONObject;
import org.junit.Before;
import org.junit.Test;

import com.rw.API.AnalyticsMgr;
import com.rw.API.SecMgr;

public class AnalyticsMgr_tests {
	public String id = null;
	
	@Before
	public void setUp() throws Exception {

		String rwhome = "/Users/sudhakarkaki/RND";

		System.setProperty("PARAM1", "localhost");
		System.setProperty("JDBC_CONNECTION_STRING", "localhost");
		System.setProperty("PARAM3", "STN");
		System.setProperty("PARAM5", "rwstaging");
		System.setProperty("EMAIL", "production");
		System.setProperty("WebRoot", rwhome + "/web/src/main/webapp");

		SecMgr sm = new SecMgr();
		JSONObject data = new JSONObject();
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
		id = retVal.get("data").toString();
		

	}

	//@Test
	public void ratingAvg() throws Exception {
		AnalyticsMgr a = new AnalyticsMgr(id);	
		
		JSONObject data_in = new JSONObject();		
		
		data_in.put("bo", "Party");
		data_in.put("bc", "Party");
		data_in.put("FK", "53000856300489c51bbae57b");
		data_in.put("summarizer", "totalSpeakerEngagements");
		data_in.put("chartName", "SpeakerRatingAvg");
		data_in.put("Speaker1_Id", "53000856300489c51bbae57b");
		try {
		System.out.println(a.summarize(data_in));
		}
		catch(Exception e) {
			e.printStackTrace();
		}
		
	}

	//@Test
	public void summarize() throws Exception {
			AnalyticsMgr a = new AnalyticsMgr(id);	
		
		JSONObject data_in = new JSONObject();		
		
		data_in.put("bo", "Party");
		data_in.put("bc", "Party");
		data_in.put("FK", "53000856300489c51bbae57b");
		data_in.put("summarizer", "totalSpeakerEngagements");
		data_in.put("chartName", "SpeakerEngagement");
		data_in.put("Speaker1_Id", "53000856300489c51bbae57b");
		
		System.out.println(a.summarize(data_in));
		
	}
	
	@Test
	public void test() throws Exception {
		
		AnalyticsMgr a = new AnalyticsMgr(id);	
		
		JSONObject data_in = new JSONObject();		
		data_in.put("name", "InvitesByChapter");
		
		
		// System.out.print("Party : ");
		System.out.println(a.getChartData(data_in));
		
		/*
			
		data_in.put("name", "AverageReferralActivity");
		data_in.put("partytype", "PARTNER");
		
		// System.out.print("Party : ");
		System.out.println(a.getChart(data_in));
	
		data_in.put("name", "AllReferralsByType");
		System.out.print("AllReferralsByType : ");
		System.out.println(a.getChart(data_in));

		data_in.put("name", "AllReferralsByStatus");
		System.out.print("AllReferralsByStatus : ");
		System.out.println(a.getChart(data_in));

		data_in.put("name", "AllReferralsByChapter");
		System.out.print("AllReferralsByChapter : ");
		System.out.println(a.getChart(data_in));
	
		data_in.put("name", "ReferralsSent");
		System.out.print("ReferralsSent : ");
		long TwoWeeksAgo = 20160; //minutes
		Date compareToDate = new Date(System.currentTimeMillis() - TwoWeeksAgo * 60000);
		data_in.put("since_date",compareToDate);
		System.out.println(a.getChart(data_in));
		
		data_in.put("name", "ReferralsRecieved");
		System.out.print("ReferralsRecieved : ");
		System.out.println(a.getChart(data_in));

		data_in.put("name", "ReferralsByProfession");
		System.out.print("ReferralsByProfession : ");
		System.out.println(a.getChart(data_in));
		
		
		//data_in.put("name", "ReferralsByAssociation");
		//System.out.print("ReferralsByAssociation : ");
		//System.out.println(a.getChart(data_in));

		data_in.put("name", "AllInvitations");
		System.out.print("AllInvitations : ");
		System.out.println(a.getChart(data_in));

		data_in.put("name", "MyInvitationsSent");
		System.out.print("MyInvitationsSent : ");
		System.out.println(a.getChart(data_in));

		data_in.put("name", "MyInvitationsRcvd");
		System.out.print("MyInvitationsRcvd : ");
		System.out.println(a.getChart(data_in));

		data_in.put("name", "mostInvitesInChapter1");
		System.out.print("mostInvitesInChapter1 : ");
		System.out.println(a.getChart(data_in));
		*/
	
	}

}
