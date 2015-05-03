package com.rw.APItest;

import static org.junit.Assert.*;

import org.json.JSONException;
import org.json.JSONObject;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.mongodb.BasicDBObject;
import com.mongodb.DBRef;
import com.rw.API.ContactsMgr;
import com.rw.API.PartnerMgr;
import com.rw.API.SecMgr;
import com.rw.persistence.RWJApplication;
import com.rw.persistence.RWJBusComp;
import com.rw.persistence.RWJBusObj;
import com.rw.persistence.RandomDataGenerator;
import com.rw.persistence.mongoStore;

public class PartnerMgr_CreatePartnersTest {

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
		data.put("login", "john.green_test@yahoo.com");
		data.put("password","123456");
		
		JSONObject retVal = null;
		try {
			retVal = sm.handleRequest(data);
		} catch (Exception e) {
			e.printStackTrace();
			fail("Login Failed");
		}
		
		String id = retVal.get("data").toString();
		
		PartnerMgr cm = new PartnerMgr(id);
		
		for (int i = 0; i < 1; i++ ) {
			data = new JSONObject();
			data.put("userid", id);
			data.put("act", "create");
			data.put("partnerId","50ce6d183004f33509593d87"); 
		
			try {
				cm.handleRequest(data);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
}
