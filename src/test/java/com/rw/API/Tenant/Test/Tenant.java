package com.rw.API.Tenant.Test;

import com.mongodb.BasicDBObject;
import com.rw.API.ContactDupsMgr;
import com.rw.API.EventMgr;
import com.rw.API.PartyMgr;
import com.rw.API.SecMgr;
import com.rw.API.TenantMgr;
import com.rw.APItest.*;

import static org.junit.Assert.*;

import org.json.JSONException;
import org.json.JSONObject;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class Tenant {
	
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
	}

	@After
	public void tearDown() throws Exception {
		TenantMgr t = new TenantMgr();
		JSONObject data = new JSONObject();
		
		data.put("act", "delete");
		data.put("tenant", "LETIP");
		JSONObject retVal = null;
		try {
			retVal = t.handleRequest(data);
		} catch (Exception e) {
			e.printStackTrace();
			fail("Failed");
		}
				
	}

	@Test
	public void test() throws JSONException {
		TenantMgr t = new TenantMgr();
		JSONObject data = new JSONObject();
		
		data.put("act", "create");

		data.put("loginDisplayName", "Le Tip");
		data.put("tagLine","Business to Business Networking...Enhanced by Technology");
		data.put("splashImage","http://www.sanramonletip.com/images/logo.png");
		data.put("pageTitle","LeTip Connect");
		data.put("ver", "1.2");
		data.put("tenant", "LETIP");
		data.put("homepage","http://www.letip.com");
		data.put("domainName","letip.com");
		data.put("rootUrl","http://ec2-23-22-176-243.compute-1.amazonaws.com:8080");
		data.put("tenantLogo","http://www.sanramonletip.com/images/logo.png");
		data.put("SupportEmailAddress","support@letip.com");
		data.put("AdminEmailAddress","admin@letip.com");
		data.put("invitationsEmailAddress","invitations@letip.com");
		data.put("memberEmailAddress","member@letip.com");
		data.put("IPhoneAppLocation","https://itunes.apple.com/us/app/letip/id725335442?mt=8");
		data.put("AndroidAppLocation","/rw.jsp#home");
		data.put("WebAppLocation","/rw.jsp#home");
		data.put("FaceBookAppId","242425209253707"); 
		data.put("LinkedInAppId","75qpirkfeswa97");  
		
		JSONObject retVal = null;
		try {
			retVal = t.handleRequest(data);
			System.out.println(retVal);
		} catch (Exception e) {
			e.printStackTrace();
			fail("Failed");
		}		
	}
}
