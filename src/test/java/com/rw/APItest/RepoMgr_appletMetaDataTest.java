package com.rw.APItest;

import static org.junit.Assert.*;

import org.json.JSONException;
import org.json.JSONObject;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.rw.API.RepoMgr;

public class RepoMgr_appletMetaDataTest {

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void test() throws Exception {
		RepoMgr sm = new RepoMgr();
		JSONObject data = new JSONObject();
		data.put("act", "GetAppletMetaData");
		data.put("AppletName", "PartyAppletForm");
		try {
			System.out.println(sm.handleRequest(data));
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
