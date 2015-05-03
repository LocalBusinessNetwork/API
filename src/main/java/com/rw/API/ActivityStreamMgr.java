package com.rw.API;

import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;
import java.util.Date;

import org.apache.log4j.Logger;
import org.bson.types.ObjectId;
import org.json.JSONObject;

import redis.clients.jedis.Jedis;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBRef;
import com.rw.persistence.RWJApplication;
import com.rw.persistence.RWJBusComp;
import com.rw.persistence.RWJBusObj;
import com.rw.persistence.RWObjectMgr;
import com.rw.persistence.mongoStore;
import com.rw.repository.RWAPI;


public class ActivityStreamMgr extends RWReqHandle  {
	static final Logger log = Logger.getLogger(ActivityStreamMgr.class.getName());
	
	public ActivityStreamMgr() {
	}
	
	public ActivityStreamMgr(String userid) {
		super(userid);
	}
	
	public ActivityStreamMgr(RWObjectMgr context) throws Exception {
		super(context);
	}
	
	@RWAPI
	public JSONObject NewMsgCount(JSONObject data) throws Exception {
		log.trace("NewMsgCount");
		JSONObject retVal = new JSONObject();
		BasicDBObject query = new BasicDBObject();
		query.put("timestamp", new BasicDBObject().put("$gte", this.getExecutionContextItem("lastlogin_date"))); 
		RWJBusComp bc = app.GetBusObject("ActivityStream").getBusComp("ActivityStream");
		int nRec = bc.ExecQuery(query,null);
		JSONObject count = new JSONObject();
		count.put("NewMsgs", nRec);
		retVal.put("data", count);
		return retVal;
	}	

	@RWAPI
	public org.json.JSONObject create(JSONObject data) throws Exception {
		log.trace("create");
		JSONObject retVal = new JSONObject() ;
		return retVal;
		/*
		RWJBusComp bc = app.GetBusObject("ActivityStream").getBusComp("ActivityStream");

		bc.NewRecord();
		bc.SetFieldValue("ownerId", getUserId());
		bc.SetFieldValue("timestamp", new Date());
		data.remove("timestamp");
		
		String partyId = GetPartyId();
		bc.SetFieldValue("partyId", partyId);
		bc.SetFieldValue("owner_fullName", GetSelfAttr("fullName"));
		bc.SetFieldValue("owner_photoUrl", GetSelfAttr("photoUrl"));
		
		BasicDBList recipients = new BasicDBList();
		recipients.add(partyId);
		
		RWJBusComp bc_partner = app.GetBusObject("Partner").getBusComp("Partner");
		
		BasicDBObject query = new BasicDBObject();
		query.put("userId", getUserId());
		query.put("type", "REFERRAL_PARTNER");

		int nRecs = bc_partner.ExecQuery(query);
		
		for ( int i = 0; i < nRecs ; i++) {
			Object partnerId = bc_partner.GetFieldValue("partnerId");
			if (partnerId != null) {
				recipients.add(partnerId.toString());
			}
		}
		
		bc.SetFieldValue("recipients", recipients);
		
		super.create(data);
		bc.SaveRecord(data);

		JSONObject retVal = new JSONObject() ;
		String data_out = StringifyDBObject(bc.currentRecord);
		retVal.put("data", data_out);
		Long _tmsg = new Long(0);
		if ( jedisMt.get("_tmsg") != null ) {
			_tmsg = Long.parseLong(jedisMt.get("_tmsg"));
			_tmsg++;
		}	
		
		jedisMt.set("_tmsg", _tmsg.toString() );
		return retVal;
		*/
	}
	
	@RWAPI
	public JSONObject read(JSONObject data) throws Exception {
		log.trace("read");

		JSONObject retVal = new JSONObject() ;
		
		RWJBusComp bc = app.GetBusObject("ActivityStream").getBusComp("ActivityStream");

		BasicDBObject query = new BasicDBObject();
		
		// Make sure the logged in user has access to the record asking for..
		//query.put("ownerId", userid.toString());
		query.putAll(getSearchSpec(data).toMap());
		String partyId = GetPartyId();
		query.put("recipients", partyId );
		
		int nRecs = bc.ExecQuery(query,getSortSpec(data), 
					!data.isNull("limit") ? data.getInt("limit") : -1 ) ;
		String _tmsg = jedisMt.get("_tmsg");
		jedisMt.set(getUserId() + "_msgs_read", _tmsg == null? "0": _tmsg );
		retVal.put("data", StringifyDBList(bc.recordSet));
		return retVal;
	}
	@RWAPI
	public JSONObject query(JSONObject data) throws Exception {
		log.trace( "query");
		return read(data);
	}

}
