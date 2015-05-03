package com.rw.APItest;

import static org.junit.Assert.*;

import org.json.JSONException;
import org.json.JSONObject;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.rw.API.SecMgr;

public class SecMgr_deleteLogin {

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
		
		try {
			data = sm.handleRequest(data);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		String id = data.get("data").toString();
		
		data = new JSONObject();
		
		data.put("act", "delete");
		data.put("userid", id);

		try {
			System.out.println(sm.handleRequest(data));
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
