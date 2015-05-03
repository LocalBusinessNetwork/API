package com.rw.APItest;

import com.rw.API.SecMgr;
import com.rw.APItest.*;

import static org.junit.Assert.*;

import org.json.JSONException;
import org.json.JSONObject;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class SecMgr_login {

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
		data.put("tenant","STN");
		
		try {
			System.out.println(sm.handleRequest(data));
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	
	}

}
