package com.rw.APItest;

import static org.junit.Assert.*;

import java.io.IOException;

import org.json.JSONException;
import org.json.JSONObject;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.rw.API.Mail;

public class SendMailTests {
	@Before
	public void setUp() throws Exception {
		System.setProperty("PARAM4", "production");
		System.setProperty("WebRoot", "src/test/java/com/rw/APItest/EmailTemplates");
		System.out.println(System.getProperty("WebRoot"));
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void test() throws Exception {
		
		Mail m = new Mail();
		
		JSONObject header = new JSONObject();
		
		header.put("from", "admin@referralwire.com");
		header.put("fromDescription", "John Smith via SuccessfulThinkersNetwork.com");
		
		header.put("replyTo", "john.smith@referralwire.com");
		header.put("replyToDescription", "John Smith");
	
		// header.put("to", "peter.thorson@referralwire.com");
		header.put("to", "skaki@yahoo.com");
		
		header.put("subject", "Invitation to Join STN");
		
		JSONObject body = new JSONObject();
		
		body.put("heading", "This is the heading part: Hi bro..");
		body.put("message", "This is the message part: this is cool stuff, just check out the email headers..and hit reply.. see where it goes.");
		body.put("closing", "This is the closingt: Talkto you soon, best regards,");
		body.put("signature", "This is the signature part: John Smith, Accountant");
		
		// m.SendSES(header, body, null);
		
		JSONObject data = new JSONObject();
		
		data.put("to_firstname", "Peter");
		data.put("to_laststname", "Peter");		
		data.put("from_name", "John Smith");
		data.put("from_title", "Accountant");
		data.put("from_company", "Zebra Tax Consultants, Inc");
		
		body.put("heading", m.vmSnippet(System.getProperty("WebRoot"), "/heading.vm", data).getString("snippet"));
		body.put("message", m.vmSnippet(System.getProperty("WebRoot"), "/message.vm", data).getString("snippet"));
		body.put("closing", m.vmSnippet(System.getProperty("WebRoot"), "/closing.vm", data).getString("snippet"));
		body.put("signature", m.vmSnippet(System.getProperty("WebRoot"), "/signature.vm", data).getString("snippet"));
		
		m.SendSES(header, body, null);
		
		
	}

}
