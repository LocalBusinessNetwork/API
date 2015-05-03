package com.rw.APItest;

import static org.junit.Assert.*;

import java.util.Calendar;

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
import com.rw.API.RfrlOfferMgr;
import com.rw.API.SecMgr;
import com.rw.persistence.RandomDataGenerator;

public class RfrlMgr_CreateTest {

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

		ContactsMgr cm = new ContactsMgr(fromId);
		
		data = new JSONObject();
		data.put("act", "create");
		
		data.put("userid", fromId);
		data.put("firstName", RandomDataGenerator.getFirstName());
		data.put("lastName", RandomDataGenerator.getSurname());
		data.put("jobTitle", RandomDataGenerator.getTitle());
		
		try {
			data = cm.handleRequest(data);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		BasicDBObject contactObj = (BasicDBObject) JSON.parse(data.get("data").toString()); 
		ObjectId idObj = (ObjectId) contactObj.get("_id");
		
		String contactId = idObj.toString();

		data = new JSONObject();
		
		data.put("act", "login");
		data.put("login", "john.green2@yahoo.com");
		data.put("password","123456");
		
		try {
			data = sm.handleRequest(data);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		String toId = data.get("data").toString();

		
		data = new JSONObject();

		data.put("act", "create");
		data.put("userid", toId);
	
		data.put("title",RandomDataGenerator.getTagLine());
		data.put("details",RandomDataGenerator.getMessage());
		data.put("value", RandomDataGenerator.random.nextInt(100));
	
		RfrlOfferMgr rm = new RfrlOfferMgr(toId);
	
		try {
			data = rm.handleRequest(data);
		} catch (Exception e) {
			e.printStackTrace();
		}

		BasicDBObject rfrlObj = (BasicDBObject) JSON.parse(data.get("data").toString()); 
		idObj = (ObjectId) rfrlObj.get("_id");
		
		String rfrlId = idObj.toString();

	
		data = new JSONObject();
		
		data.put("act", "GetPartyId");
		data.put("userid", toId);
		
		try {
			data = sm.handleRequest(data);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		toId = data.get("id").toString();
		
		data = new JSONObject();

		data.put("act", "create");
		data.put("userid", fromId);

		data.put("toId",toId);
		data.put("contactId",contactId);
		data.put("ref_offerId",contactId);
		data.put("subject",RandomDataGenerator.getTagLine());

		Calendar cal = Calendar.getInstance();
		data.put("date",cal.getTime());

		data.put("status","PENDING");
		data.put("has_budget","Yes");
		data.put("can_contact","Yes");
		data.put("decision_maker","Yes");
		data.put("source_comments",RandomDataGenerator.getTagLine());
		data.put("comments",RandomDataGenerator.getMessage());
		
		RfrlMgr rfm = new RfrlMgr(toId);
		
		try {
			data = rfm.handleRequest(data);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}

}
