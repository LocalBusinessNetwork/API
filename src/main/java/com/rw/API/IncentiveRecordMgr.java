package com.rw.API;

import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;

import org.apache.log4j.Logger;
import org.bson.types.ObjectId;
import org.json.JSONObject;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBRef;
import com.mongodb.util.JSON;
import com.rw.persistence.RWJApplication;
import com.rw.persistence.RWJBusComp;
import com.rw.persistence.RWJBusObj;
import com.rw.persistence.RWObjectMgr;
import com.rw.persistence.mongoStore;
import com.rw.repository.RWAPI;


public class IncentiveRecordMgr extends RWReqHandle  {
	static final Logger log = Logger.getLogger(IncentiveRecordMgr.class.getName());

	public IncentiveRecordMgr() {
	}

	public IncentiveRecordMgr(String userid) {
		super(userid);
	}
	
	public IncentiveRecordMgr(RWObjectMgr context) throws Exception {
		super(context);
	}
	
	@RWAPI
	public JSONObject payRequest(JSONObject data) throws Exception {
		log.trace( "create");
		JSONObject retVal = new JSONObject() ;

		UserMgr um = new UserMgr(this);
		
		JSONObject payload = new JSONObject();		
		payload.put("act", "read");

		JSONObject ret_data = um.handleRequest(payload);

		BasicDBObject userdata = (BasicDBObject) JSON.parse(ret_data.get("data").toString()); 
		
		payload = new JSONObject();
		payload.put("id", data.get("pay_to").toString());
		payload.put("act", "read");
		ret_data = um.handleRequest(payload);

		BasicDBObject payto_data = (BasicDBObject) JSON.parse(ret_data.get("data").toString()); 
		
		JSONObject model = new JSONObject() ;
		
		model.put("from_FullName", userdata.get("fullName"));
		model.put("to_FullName", payto_data.get("fullName"));
		model.put("from_PaypalId", userdata.get("emailAddress"));
		model.put("to_PaypalId", payto_data.get("emailAddress"));

		retVal.put("data", (String) model.toString());

		return retVal;
	}	

	@RWAPI
	public JSONObject pay(JSONObject data) throws Exception {
		log.trace( "create");
	
		JSONObject retVal = new JSONObject() ;
		
		JSONObject paypalData = new JSONObject();
	
		String toId = data.get("toId").toString();
		String amount = data.get("amount").toString();

		// TODO :
		
		/* 
		 * 1. get the users paypal id - this will be encrypted
		 * 2. get the recipients pay id - this will be encrypted
		 * 3. Challenge for PIN - this will be encrypted, challenge machines 
		 * 4. process payment
		 * 5. send email and text
		 */ 
		
		retVal.put("data", paypalData);
		return retVal;
	
	}
	
	@RWAPI
	public JSONObject create(JSONObject data) throws Exception {
		log.trace( "create");
		
		RWJBusComp bc = app.GetBusObject("IncentiveRecord").getBusComp("IncentiveRecord");

		bc.NewRecord();
		bc.SetFieldValue("ownerId", getUserId());
		super.create(data);
		bc.SaveRecord(data);

		JSONObject retVal = new JSONObject() ;
		retVal.put("data", StringifyDBObject(bc.currentRecord));
		return retVal;
	}
	
	@RWAPI
	public JSONObject read(JSONObject data) throws Exception {
		log.trace( "read");

		JSONObject retVal = new JSONObject() ;
		
		RWJBusComp bc = app.GetBusObject("IncentiveRecord").getBusComp("IncentiveRecord");

		BasicDBObject query = new BasicDBObject();
		
		// Make sure the logged in user has access to the record asking for..
		query.put("ownerId", getUserId());
		query.putAll(bc.constructQuery(data).toMap());

		int nRecs = bc.ExecQuery(query,null);
		if ( nRecs > 0 ) {
			retVal.put("data", StringifyDBList(bc.recordSet));
		}
		else {
			String errorMessage = getMessage("RECORD_NOTFOUND"); 
			log.debug ( errorMessage);
			throw new Exception(errorMessage);
		}
		return retVal;
	}

	@RWAPI
	public JSONObject query(JSONObject data) throws Exception {
		log.trace( "query");
		return read(data);
	}
}
