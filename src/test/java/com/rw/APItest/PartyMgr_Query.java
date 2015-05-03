package com.rw.APItest;

import static org.junit.Assert.*;

import org.json.JSONException;
import org.json.JSONObject;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.mongodb.BasicDBObject;
import com.rw.API.ContactsMgr;
import com.rw.API.PartyMgr;
import com.rw.persistence.RWJApplication;
import com.rw.persistence.RWJBusComp;
import com.rw.persistence.RWJBusObj;
import com.rw.persistence.RandomDataGenerator;

public class PartyMgr_Query extends TestSetup{

	String testSubject = null ;
	@Before
	public void setUp() throws Exception {
		createTestUser();
		loginTestUser();
		createTestContacts();

	}

	@After
	public void tearDown() throws Exception {
		deleteTestContacts();
		deleteTestUser();
	}
	void createTestContacts() throws JSONException {
		ContactsMgr cm = new ContactsMgr(loginid);
		
		for (int i = 0; i < 10; i++ ) {
			JSONObject data = new JSONObject();

			data.put("userid", loginid);
			data.put("act", "create");
			testSubject = RandomDataGenerator.getFirstName();
			data.put("firstName", testSubject);
			data.put("lastName", RandomDataGenerator.getSurname());
			data.put("jobTitle", RandomDataGenerator.getTitle());
			data.put("postalCodeAddress", RandomDataGenerator.getZip());
		
			try {
				cm.handleRequest(data);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

	}

	void deleteTestContacts() throws JSONException {
		ContactsMgr cm = new ContactsMgr(loginid);
        
		JSONObject data= new JSONObject();
		data.put("userid", loginid);
		data.put("act", "deleteAll");
		
		try {
			data = cm.handleRequest(data);
		} catch (Exception e) {
			e.printStackTrace();
			fail("Query Failed");
		}	
	}

	
	@Test
	public void test() throws JSONException {
		RWJApplication app = new RWJApplication();

		PartyMgr mgr = new PartyMgr(loginid);
		
		JSONObject data = new JSONObject();

		// regular get all contact search
		data.put("act", "read");
		data.put("userid", loginid);
		data.put("sortby", "firstName");
		// data.put("skip", "2");
		// data.put("limit", "10");
		
		
		try {
			System.out.println(mgr.handleRequest(data).toString());
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
