package com.rw.API.Event.Test;

import com.mongodb.BasicDBObject;
import com.rw.API.ContactDupsMgr;
import com.rw.API.EventMgr;
import com.rw.API.PartyMgr;
import com.rw.API.SecMgr;
import com.rw.APItest.*;

import static org.junit.Assert.*;

import org.json.JSONException;
import org.json.JSONObject;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestDelete {
	
	public String id = null;
	
	@Before
	public void setUp() throws Exception {

		String rwhome = "/Users/sudhakarkaki/RND";

		System.setProperty("PARAM1", "localhost");
		System.setProperty("JDBC_CONNECTION_STRING", "localhost");
		System.setProperty("PARAM3", "STN");
		System.setProperty("PARAM5", "rwstaging");
		System.setProperty("EMAIL", "production");
		System.setProperty("WebRoot", rwhome + "/web/src/main/webapp");

		SecMgr sm = new SecMgr();
		JSONObject data = new JSONObject();
		data.put("act", "login");
		data.put("login", "phil@referralwiretest.biz");
		data.put("password","123456");
		
		JSONObject retVal = null;
		try {
			retVal = sm.handleRequest(data);
		} catch (Exception e) {
			e.printStackTrace();
			fail("Login Failed");
		}
		id = retVal.get("data").toString();
		

	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void test() throws JSONException {
		EventMgr em = new EventMgr(id);
		try {
			JSONObject data = new JSONObject();
			data.put("id","535f3bc5300404a96e04cfe6");
			em.delete(data);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
