package com.rw.APItest;

import static org.junit.Assert.*;

import org.json.JSONException;
import org.json.JSONObject;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.rw.API.RfrlMgr;
import com.rw.API.SecMgr;

public class Rfrl_AggregationTests {

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void test() throws JSONException {
		SecMgr sm = new SecMgr();
		
		JSONObject data = new JSONObject();

		data.put("act", "login");
		data.put("login", "john.green@yahoo.com");
		data.put("password","123456");
		
		try {
			data = sm.handleRequest(data);
		} catch (Exception e) {
			e.printStackTrace();
		}
		

		String fromId = data.get("data").toString();
		
		data = new JSONObject();

		data.put("act", "ReferralsSent");
		data.put("userid", fromId);
	
		RfrlMgr rfm = new RfrlMgr(fromId);
		
		try {
			data = rfm.handleRequest(data);
		} catch (Exception e) {
			e.printStackTrace();
		}

		System.out.println("======== Referrals Sent =======");

		System.out.println(data);

		data = new JSONObject();

		data.put("act", "ReferralsRecieved");
		data.put("userid", fromId);
			
		try {
			data = rfm.handleRequest(data);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		System.out.println("======== Referrals Recieved =======");

		System.out.println(data);
	
	}

}
