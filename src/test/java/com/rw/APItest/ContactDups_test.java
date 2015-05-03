package com.rw.APItest;

import com.rw.API.ContactDupsMgr;
import com.rw.API.SecMgr;
import com.rw.APItest.*;

import static org.junit.Assert.*;

import org.json.JSONException;
import org.json.JSONObject;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ContactDups_test {

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
		data.put("login", "phil@referralwiretest.biz");
		data.put("password","123456");
		
		JSONObject retVal = null;
		try {
			retVal = sm.handleRequest(data);
		} catch (Exception e) {
			e.printStackTrace();
			fail("Login Failed");
		}
		
		String id = retVal.get("data").toString();

		ContactDupsMgr cdm = new ContactDupsMgr(id);
		data = new JSONObject();
		
		data.put("act", "read");
		data.put("userid", id);
		
		try {
			retVal = cdm.handleRequest(data);
			System.out.println(retVal);
		} catch (Exception e) {
			e.printStackTrace();
			fail("Login Failed");
		}
	}

}
