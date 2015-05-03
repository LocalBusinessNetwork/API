package com.rw.API;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.regex.Pattern;

import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.BasicResponseHandler;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.log4j.Logger;
import org.bson.types.ObjectId;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONStringer;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBRef;
import com.mongodb.QueryBuilder;
import com.rw.Recommender.DupFinder;
import com.rw.persistence.RWJApplication;
import com.rw.persistence.RWJBusComp;
import com.rw.persistence.RWJBusObj;
import com.rw.persistence.RWObjectMgr;
import com.rw.persistence.mongoStore;
import com.rw.repository.RWAPI;

/*
 * Parties are created either thru User Manager - for Members
 * or ContactMgr - for contacts.
 * PartyMgr gives different read abilities to Party Data.
 * Enrichment and Recommendations work off PartyMgr as well
 * 
 * App references Parties thru relationships
 * 
 */


public class ContactDupsMgr extends RWReqHandle  {
	static final Logger log = Logger.getLogger(ContactDupsMgr.class.getName());
	
	public ContactDupsMgr(String userid) {
		super(userid);
	}
	
	public ContactDupsMgr(RWObjectMgr context) throws Exception {
		super(context);
	}

	@SuppressWarnings("unchecked")
	@RWAPI
	public JSONObject read(JSONObject data) throws Exception {
		log.trace( "read");

	    DupFinder dr = new DupFinder(getUserId());
	    dr.FindDuplicates();

		RWJBusComp bc = app.GetBusObject("ContactDups").getBusComp("ContactDups");
		JSONObject retVal = new JSONObject() ;

		BasicDBObject query = new BasicDBObject();
		query.put("userid", getUserId());

		BasicDBObject sortSpec = new BasicDBObject();
		sortSpec.put("orig_id",  1);

		int nRecs = bc.ExecQuery(query,sortSpec);

		BasicDBList cList = bc.GetCompositeRecordList();
		retVal.put("data", StringifyDBList(cList));

		return retVal;	
	}
	
	@SuppressWarnings("unchecked")
	@RWAPI
	public JSONObject autoMerge(JSONObject data) throws Exception {
		log.trace( "read");
		RWJBusComp bc = app.GetBusObject("ContactDups").getBusComp("ContactDups");
		JSONObject retVal = new JSONObject() ;

		BasicDBObject query = new BasicDBObject();
		query.put("userid", getUserId());

		BasicDBObject sortSpec = new BasicDBObject();
		sortSpec.put("orig_id",  1);

		int nRecs = bc.ExecQuery(query,sortSpec);
		String prev_orig_id = null;
		RWJBusComp prev_orig_bc = null;
		
		for ( int i = 0; i < nRecs ; i++) {
			String orig_id = bc.GetFieldValue("orig_id").toString();
			String dup_id = bc.GetFieldValue("dup_id").toString();

			// TODO : Implement Auto merge algorithm for successive record of the same orig_id.
			
			bc.NextRecord();
		}
		
		return retVal;	

	}
	
	@RWAPI
	public JSONObject KickoffDupFinder(JSONObject data) {
		// TODO: 
		return new JSONObject();
	}

}
