package com.rw.APItest;

import static org.junit.Assert.*;

import java.util.Calendar;

import org.json.JSONException;
import org.json.JSONObject;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.mongodb.BasicDBObject;
import com.mongodb.DBRef;
import com.rw.API.ActivityStreamMgr;
import com.rw.API.AssocMgr;
import com.rw.API.ContactsMgr;
import com.rw.API.OrgMgr;
import com.rw.API.SecMgr;
import com.rw.API.UserMgr;
import com.rw.persistence.RWJApplication;
import com.rw.persistence.RWJBusComp;
import com.rw.persistence.RWJBusObj;
import com.rw.persistence.RandomDataGenerator;
import com.rw.persistence.mongoStore;

public class ActivityStreamMgr_CreateMessageTest {

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

		data = new JSONObject();;

		data.put("act", "create");
		data.put("userid",id);
		
		data.put("type","MESSAGE");
		data.put("message",RandomDataGenerator.getMessage());
		data.put("photo",RandomDataGenerator.getPhotoURL());
		
        Calendar cal = Calendar.getInstance();
		data.put("timestamp",cal.getTime());

		ActivityStreamMgr am = new ActivityStreamMgr(id);
		
		try {
			retVal = am.handleRequest(data);
		} catch (Exception e) {
			e.printStackTrace();
			fail("ActivityStreamMgr Create Failed");
		}
	}

}
