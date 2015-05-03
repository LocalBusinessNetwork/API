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
import com.rw.API.OrgMgr;
import com.rw.API.SecMgr;
import com.rw.API.UserMgr;
import com.rw.persistence.RWJApplication;
import com.rw.persistence.RWJBusComp;
import com.rw.persistence.RWJBusObj;
import com.rw.persistence.RandomDataGenerator;
import com.rw.persistence.mongoStore;

public class OrgMgr_CreateOrgTest {

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
			fail("Login Failed");
		}
		
		String id = retVal.get("data").toString();

		data = new JSONObject(id);

		data.put("act", "create");
		data.put("userid",id);
		
		data.put("partytype","BUSINESS");
		data.put("businessName","ACME Industries");
		data.put("segment","Animated Movies");
		data.put("workPhone",RandomDataGenerator.getPhoneNumber(true));
		data.put("faxPhone",RandomDataGenerator.getPhoneNumber(true));
		data.put("emailAddress","acme@yahoo.com");
		data.put("streetAddress1",RandomDataGenerator.getStreetAddress());
		data.put("cityAddress",RandomDataGenerator.getCity());
		data.put("stateAddress",RandomDataGenerator.getState());
		data.put("postalCodeAddress",RandomDataGenerator.getZip());

		OrgMgr om = new OrgMgr(id);
		
		try {
			retVal = om.handleRequest(data);
		} catch (Exception e) {
			e.printStackTrace();
			fail("Org Registration Failed");
		}
	}

}
