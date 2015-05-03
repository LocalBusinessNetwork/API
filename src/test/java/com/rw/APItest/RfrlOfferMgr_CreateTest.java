package com.rw.APItest;

import static org.junit.Assert.*;

import org.json.JSONException;
import org.json.JSONObject;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.rw.API.RfrlOfferMgr;
import com.rw.API.SecMgr;
import com.rw.persistence.RandomDataGenerator;

public class RfrlOfferMgr_CreateTest {

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
		
		JSONObject retVal = null;
		try {
			retVal = sm.handleRequest(data);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		String id = retVal.get("data").toString();
    
    
		data = new JSONObject();

		data.put("act", "create");
		data.put("userid",id);
	
		data.put("title",RandomDataGenerator.getTagLine());
		data.put("details",RandomDataGenerator.getMessage());
		data.put("value", RandomDataGenerator.random.nextInt(100));
	
		RfrlOfferMgr rm = new RfrlOfferMgr(id);
	
		try {
			retVal = rm.handleRequest(data);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
