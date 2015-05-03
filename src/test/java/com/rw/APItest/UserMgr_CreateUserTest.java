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
import com.rw.API.SecMgr;
import com.rw.API.UserMgr;
import com.rw.persistence.RWJApplication;
import com.rw.persistence.RWJBusComp;
import com.rw.persistence.RWJBusObj;
import com.rw.persistence.RandomDataGenerator;
import com.rw.persistence.mongoStore;

public class UserMgr_CreateUserTest {

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

		data = new JSONObject();

		data.put("act", "create");
		data.put("userid",id);
		
		data.put("partytype","PARTNER");
		data.put("firstName","john");
		data.put("lastName","green_test");
		data.put("gender","MALE");
		data.put("jobTitle",RandomDataGenerator.getTitle());
		data.put("segment",RandomDataGenerator.getBusinessType());
		data.put("jobTitle",RandomDataGenerator.getTitle());
		data.put("workPhone",RandomDataGenerator.getPhoneNumber(true));
		data.put("mobilePhone",RandomDataGenerator.getPhoneNumber(true));
		data.put("faxPhone",RandomDataGenerator.getPhoneNumber(true));
		data.put("emailAddress","john.green_test@yahoo.com");
		data.put("streetAddress1",RandomDataGenerator.getStreetAddress());
		data.put("cityAddress",RandomDataGenerator.getCity());
		data.put("stateAddress",RandomDataGenerator.getState());
		data.put("postalCodeAddress",RandomDataGenerator.getZip());
		data.put("photoUrl",RandomDataGenerator.getPhotoURL());

		UserMgr um = new UserMgr(id);
		
		try {
			retVal = um.handleRequest(data);
		} catch (Exception e) {
			e.printStackTrace();
			fail("User Registration Failed");
		}
	}

}
