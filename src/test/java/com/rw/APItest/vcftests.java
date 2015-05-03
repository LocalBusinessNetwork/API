package com.rw.APItest;

import static org.junit.Assert.*;

import java.io.File;

import org.json.JSONException;
import org.json.JSONObject;
import org.junit.Test;

import com.rw.API.ContactsMgr;
import com.rw.API.PartnerMgr;
import com.rw.API.SecMgr;

public class vcftests {

	@Test
	public void test() throws Exception {
		SecMgr sm = new SecMgr();
		
		JSONObject data = new JSONObject();
	 	System.setProperty("PARAM3","STN");
	    
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
		
		ContactsMgr pm = new ContactsMgr(sm);
		
		data = new JSONObject();
		data.put("filename", "src/test/java/com/rw/APItest/test.vcf");
		
		pm.ImportVCards(data);
		
	}

}
