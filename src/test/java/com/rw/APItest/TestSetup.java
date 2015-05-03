package com.rw.APItest;

import org.json.JSONException;
import org.json.JSONObject;

import com.rw.API.SecMgr;

public class TestSetup {
	
	public String loginid = null ;

	static String testUserLogin = "phil@referralwiretest.biz";
	
	public void createTestUser() throws JSONException {
		SecMgr sm = new SecMgr();
		
		JSONObject data = new JSONObject();
		
		data.put("act", "create");
		data.put("login", testUserLogin);
		data.put("password","123456");
		data.put("passwordHint", "123456") ;
		data.put("type", "PARTNER") ;
		data.put("firstName","John");
		data.put("lastName","Green");
		data.put("postalCodeAddress", "94526");
		
		try {
			System.out.println(sm.handleRequest(data));
		} catch (Exception e) {
			e.printStackTrace();
		}

	}
	
	public void deleteTestUser() throws JSONException {
		SecMgr sm = new SecMgr();
		
		JSONObject data = new JSONObject();

		data.put("act", "login");
		data.put("login", testUserLogin);
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
	public String loginTestUser() throws JSONException {
		SecMgr sm = new SecMgr();
		
		JSONObject data = new JSONObject();

		data.put("act", "login");
		data.put("login", testUserLogin);
		data.put("password","123456");
		
		try {
			data = sm.handleRequest(data);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		loginid = data.get("data").toString(); 
		return loginid;

	}
}
