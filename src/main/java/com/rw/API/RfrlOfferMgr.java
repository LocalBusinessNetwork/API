package com.rw.API;

import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;
import java.util.Date;
import java.util.HashMap;

import org.apache.log4j.Logger;
import org.bson.types.ObjectId;
import org.json.JSONObject;

import com.mongodb.BasicDBObject;
import com.mongodb.DBRef;
import com.rw.persistence.RWJApplication;
import com.rw.persistence.RWJBusComp;
import com.rw.persistence.RWJBusObj;
import com.rw.persistence.RWObjectMgr;
import com.rw.persistence.mongoStore;
import com.rw.repository.RWAPI;


public class RfrlOfferMgr extends RWReqHandle  {
	static final Logger log = Logger.getLogger(RfrlOfferMgr.class.getName());
	
	public RfrlOfferMgr() {
	}

	public RfrlOfferMgr(String userid) {
		super(userid);
	}
	
	public RfrlOfferMgr(RWObjectMgr context) throws Exception {
		super(context);
	}
	
	@SuppressWarnings("unchecked")
	@RWAPI
	public JSONObject create(JSONObject data) throws Exception {
		log.trace( "create");
		RWJBusComp bc = app.GetBusObject("ReferralOffer").getBusComp("ReferralOffer");
		// TODO: Look for duplicates, avoid accidental duplicate records.
		// search criteria could be first name, last name, email address
		bc.NewRecord();
		bc.SetFieldValue("ownerId", getUserId());
		bc.SetFieldValue("timestamp", new Date());
		data.remove("timestamp") ;
		
		bc.SetFieldValue("partyId", GetPartyId());
		super.create(data);


		// TODO: Add rest of the fields
		bc.SaveRecord(data);
		JSONObject retVal = new JSONObject() ;
		retVal.put("data", StringifyDBObject(bc.GetCompositeRecord()));
		return retVal;
	}
	
	@SuppressWarnings("unchecked")
	@RWAPI
	public JSONObject read(JSONObject data) throws Exception {
		log.trace( "read");

		RWJBusComp bc = app.GetBusObject("ReferralOffer").getBusComp("ReferralOffer");

		BasicDBObject query = new BasicDBObject();
		query.put("ownerId", getUserId());

		JSONObject retVal = new JSONObject() ;

		if ( !data.isNull("id")) {
			Object id = data.get("id");
			query.put("_id", new ObjectId(id.toString())); 
			int nRecs = bc.ExecQuery(query,null);
			if ( nRecs > 0 ) {
				retVal.put("data", StringifyDBObject(bc.GetCompositeRecord()));
			}
			else {
				String errorMessage = getMessage("RECORD_NOTFOUND"); 
				log.debug( errorMessage);
				throw new Exception(errorMessage);
			}
		}
		else { // request for a set of records, filtered by a criteria
			// Add the additional query criteria
			query.putAll(getSearchSpec(data).toMap());
			int nRecs = bc.ExecQuery(query,getSortSpec(data));
			retVal.put("data", StringifyDBList(bc.GetCompositeRecordList()));
		}
		return retVal;
	}
	
	@RWAPI
	public JSONObject update(JSONObject data) throws Exception {
		log.trace( "update");
		
		super.update(data);
		
		Object id = data.get("id");
		
		JSONObject retVal = new JSONObject() ;
		RWJBusComp bc = app.GetBusObject("ReferralOffer").getBusComp("ReferralOffer");
		
		BasicDBObject query = new BasicDBObject();
		query.put("ownerId", getUserId());
        query.put("_id", new ObjectId(id.toString())); 
	
		if (bc.UpsertQuery(query) == 0 ) {
			String errorMessage = getMessage("RECORD_NOTFOUND"); 
			log.debug( errorMessage);
			throw new Exception(errorMessage);
		}
		
		// avoid accidental resetting of creation time.
		data.remove("timestamp");
		bc.SaveRecord(data);
		retVal.put("data", StringifyDBObject(bc.GetCompositeRecord()));
		return retVal;
	}
	
	@RWAPI
	public JSONObject query(JSONObject data) throws Exception {
		return read(data);
	}
	
	@RWAPI
	public JSONObject delete(JSONObject data) throws Exception {
		log.trace( "delete");

		super.delete(data);
		Object id = data.get("id");
	
		JSONObject retVal = new JSONObject() ;
		RWJBusComp bc = app.GetBusObject("ReferralOffer").getBusComp("ReferralOffer");
		
		BasicDBObject query = new BasicDBObject();
		query.put("ownerId", getUserId());
        query.put("_id", new ObjectId(id.toString())); 
	
		if (bc.ExecQuery(query,null) == 0 ) {
			String errorMessage = getMessage("RECORD_NOTFOUND"); 
			log.debug( errorMessage);
			throw new Exception(errorMessage);
		}
		
		retVal.put("data", StringifyDBObject(bc.currentRecord));
		bc.DeleteRecord();
		return retVal;
	}

	@RWAPI
	public JSONObject deleteAll(JSONObject data) throws Exception {
		log.trace( "delete all");

		RWJBusComp bc = app.GetBusObject("ReferralOffer").getBusComp("ReferralOffer");

		BasicDBObject query = new BasicDBObject();
		query.putAll(bc.constructQuery(data).toMap());

		query.put("ownerId", getUserId());
		query.putAll(bc.constructQuery(data).toMap());
	
		JSONObject retVal = new JSONObject() ;

		if (bc.ExecQuery(query,null) > 0 ) {
			retVal.put("data", StringifyDBList(bc.recordSet));
			while ( bc.DeleteRecord() ) ;
		}
		else {
			String errorMessage = getMessage("RECORD_NOTFOUND"); 
			log.debug( errorMessage);
			throw new Exception(errorMessage);
		}
		return retVal;
	}
}
