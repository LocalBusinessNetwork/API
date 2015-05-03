package com.rw.APItest;

import static org.junit.Assert.*;

import org.bson.types.ObjectId;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.mongodb.BasicDBObject;
import com.mongodb.util.JSON;
import com.rw.API.ContactsMgr;
import com.rw.API.RfrlMgr;
import com.rw.persistence.RandomDataGenerator;

public class Rfrl_OOTNRfrlTest extends TestSetup {

	@Before
	public void setUp() throws Exception {
		createTestUser();
		loginTestUser();
	}

	@After
	public void tearDown() throws Exception {
		deleteTestUser();
	}

	@Test
	public void test() throws JSONException {
		
		JSONObject data = new JSONObject();
		
		// Create a soon to be member for OOTN rfrl
		ContactsMgr cm = new ContactsMgr(loginid);
		data.put("userid", loginid);
		data.put("act", "create");
		data.put("firstName", RandomDataGenerator.getFirstName());
		data.put("lastName", RandomDataGenerator.getSurname());
		data.put("jobTitle", RandomDataGenerator.getTitle());
		data.put("postalCodeAddress", RandomDataGenerator.getZip());
		data.put("emailAddress", "john.member@vc.com");
	
		try {
			data = cm.handleRequest(data);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		BasicDBObject ContactObj = (BasicDBObject) JSON.parse(data.get("data").toString()); 
		String toId = ContactObj.get("_id").toString();

		// Create a referralContact for OOTN rfrl

		data = new JSONObject();
		
		data.put("userid", loginid);
		data.put("act", "create");
		data.put("firstName", RandomDataGenerator.getFirstName());
		data.put("lastName", RandomDataGenerator.getSurname());
		data.put("jobTitle", RandomDataGenerator.getTitle());
		data.put("postalCodeAddress", RandomDataGenerator.getZip());
		data.put("emailAddress", "mary.contact@vc.com");

		try {
			data = cm.handleRequest(data);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		ContactObj = (BasicDBObject) JSON.parse(data.get("data").toString()); 
		String contactId = ContactObj.get("_id").toString();

		data = new JSONObject();
		
		data.put("act", "SendOutOfNetworkReferral");
		data.put("userid", loginid);
		data.put("toId", toId);
		data.put("contactId", contactId);
		data.put("has_budget","Yes");
		data.put("can_contact","Yes");
		data.put("decision_maker","Yes");
		data.put("source_comments",RandomDataGenerator.getTagLine());
		data.put("comments",RandomDataGenerator.getMessage());

		RfrlMgr rfm = new RfrlMgr(loginid);
		
		try {
			data = rfm.handleRequest(data);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
