package com.rw.APItest;

import static org.junit.Assert.*;

import java.util.Calendar;

import org.json.JSONException;
import org.json.JSONObject;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.rw.API.RfrlMgr;
import com.rw.API.SecMgr;
import com.rw.persistence.RandomDataGenerator;

public class RfrlMgr_ReadTest {

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

		data.put("act", "query");
		data.put("userid", fromId);
		
		RfrlMgr rfm = new RfrlMgr(fromId);
		
		try {
			data = rfm.handleRequest(data);
		} catch (Exception e) {
			e.printStackTrace();
		}
	
		System.out.println(data);
	}

}
