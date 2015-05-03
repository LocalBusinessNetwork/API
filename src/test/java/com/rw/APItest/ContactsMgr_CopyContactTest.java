package com.rw.APItest;

import static org.junit.Assert.*;

import org.bson.types.ObjectId;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.mongodb.BasicDBObject;
import com.mongodb.DBRef;
import com.mongodb.util.JSON;
import com.rw.API.ContactsMgr;
import com.rw.API.SecMgr;
import com.rw.persistence.RWJApplication;
import com.rw.persistence.RWJBusComp;
import com.rw.persistence.RWJBusObj;
import com.rw.persistence.RandomDataGenerator;
import com.rw.persistence.mongoStore;

public class ContactsMgr_CopyContactTest {

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

		ContactsMgr cm = new ContactsMgr(id);
	
		data = new JSONObject();

		data.put("userid", id);
		data.put("act", "create");
		data.put("firstName", RandomDataGenerator.getFirstName());
		data.put("lastName", RandomDataGenerator.getSurname());
		data.put("jobTitle", RandomDataGenerator.getTitle());
		data.put("postalCodeAddress", RandomDataGenerator.getZip());
	
		try {
			data = cm.handleRequest(data);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		System.out.println(data);

		BasicDBObject contactObj = (BasicDBObject) JSON.parse(data.get("data").toString()); 
		ObjectId idObj = (ObjectId) contactObj.get("_id");

		data = new JSONObject();

	   	data.put("act", "login");
		data.put("login", "adam.sandler@yahoo.com");
		data.put("password","123456");
	   
		try {
			data = sm.handleRequest(data);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		id = data.get("data").toString();
	
		data = new JSONObject();

		data.put("userid", id);
		data.put("act", "copy");
		data.put("id",idObj.toString());

		try {
			data = cm.handleRequest(data);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		System.out.println(data);
		
	}
}
