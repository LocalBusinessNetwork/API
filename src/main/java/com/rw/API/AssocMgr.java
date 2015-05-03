package com.rw.API;

import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;

import org.apache.log4j.Logger;
import org.bson.types.ObjectId;
import org.json.*;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBRef;
import com.rw.persistence.RWJApplication;
import com.rw.persistence.RWJBusComp;
import com.rw.persistence.RWJBusObj;
import com.rw.persistence.RWObjectMgr;
import com.rw.persistence.mongoStore;
import com.rw.repository.RWAPI;


public class AssocMgr extends RWReqHandle  {
	static final Logger log = Logger.getLogger(AssocMgr.class.getName());
	
	public AssocMgr() {
	}

	public AssocMgr(String userid) {
		super(userid);
	}
	
	public AssocMgr(RWObjectMgr context) throws Exception {
		super(context);
	}
	
	@RWAPI
	public JSONObject create(JSONObject data) throws Exception {
		log.trace("create");
		
		RWJBusComp bc = app.GetBusObject("Association").getBusComp("Association");

		// TODO duplicate assocs..
		
		bc.NewRecord();
		bc.SetFieldValue("credId", getUserId());
		bc.SetFieldValue("partytype", "ASSOCIATION");
		super.create(data);
		bc.SaveRecord(data);
		JSONObject retVal = new JSONObject() ;
		retVal.put("data", StringifyDBObject(bc.GetCompositeRecord()));
	
		return retVal;
	}
	
	@RWAPI
	public JSONObject read(JSONObject data) throws Exception {
		log.trace( "read");

		JSONObject retVal = new JSONObject() ;
		
		RWJBusComp bc = app.GetBusObject("Association").getBusComp("Association");
		BasicDBObject query = new BasicDBObject();

		if ( !data.isNull("id") ) { // request for a particular record 
			Object id = data.get("id");
	        query.put("_id", new ObjectId(id.toString())); 
			int nRecs = bc.ExecQuery(query,null);
			if ( nRecs > 0 ) {
				retVal.put("data", StringifyDBObject(bc.GetCompositeRecord()));
			}
			else {
				String errorMessage = getMessage("RECORD_NOTFOUND"); 
				log.debug(errorMessage);
				throw new Exception(errorMessage);
			}
		}
		else { // request for a set of records, filtered by a criteria
			query.put("partytype", "ASSOCIATION");
			// Add the additional query criteria
			query.putAll(getSearchSpec(data).toMap());
			int nRecs = bc.ExecQuery(query,getSortSpec(data));
			BasicDBList cList = bc.GetCompositeRecordList();
			retVal.put("data", StringifyDBList(cList));
		}
		return retVal;
	}
	
	@RWAPI
	public JSONObject query(JSONObject data) throws Exception {
		log.trace( "query");
		return read(data);
	}
	
	@RWAPI
	public JSONObject update(JSONObject data) throws Exception  {
		log.trace( "update");
		super.update(data);

		Object id = data.get("id");
		
		JSONObject retVal = new JSONObject() ;
		RWJBusComp bc = app.GetBusObject("Organization").getBusComp("Organization");
		
		BasicDBObject query = new BasicDBObject();
		query.put("credId", getUserId());
        query.put("_id", new ObjectId(id.toString())); 
	
		if (bc.UpsertQuery(query) == 0 ) {
			String errorMessage = getMessage("RECORD_NOTFOUND"); 
			log.debug ( errorMessage);
			throw new Exception(errorMessage);
		}
		
		bc.SaveRecord(data);
		retVal.put("data", StringifyDBObject(bc.GetCompositeRecord()));
		return retVal;
	}

	@RWAPI
	public JSONObject delete(JSONObject data) throws Exception {
		log.trace( "delete");
		super.delete(data);

		Object id = data.get("id");
		
		JSONObject retVal = new JSONObject() ;
		RWJBusComp bc = app.GetBusObject("Association").getBusComp("Association");
		
		BasicDBObject query = new BasicDBObject();
		query.put("credId", getUserId());
        query.put("_id", new ObjectId(id.toString())); 
	
		if (bc.ExecQuery(query,null) == 0 ) {
			String errorMessage = getMessage("RECORD_NOTFOUND"); 
			log.debug(errorMessage);
			throw new Exception(errorMessage);
		}
		
		retVal.put("data", StringifyDBObject(bc.GetCompositeRecord()));
		bc.DeleteRecord();
		return retVal;
	}

	@RWAPI
	public JSONObject joinAssociation(JSONObject data) throws Exception {
		log.trace("addMember");
		
		RWJBusComp bc = app.GetBusObject("AssociationMembership").getBusComp("AssociationMembership");

		BasicDBObject query = new BasicDBObject();
		query.put("assocId", data.get("id").toString());
		query.put("memberId", GetPartyId());

		int nRecs = bc.ExecQuery(query);
		JSONObject retVal = new JSONObject() ;

		if (nRecs == 1) {
			retVal.put("data", StringifyDBObject(bc.GetCompositeRecord()));
			return retVal;
		}
		
		bc.NewRecord();
		bc.SetFieldValue("memberId", GetPartyId());
		bc.SetFieldValue("assocId",data.get("id").toString());
		
		super.create(data);
		bc.SaveRecord(data);

		retVal.put("data", StringifyDBObject(bc.GetCompositeRecord()));
	
		return retVal;
	}

	@RWAPI
	public JSONObject getAssocMembers(JSONObject data) throws Exception {
		log.trace( "getAssocMembers");

		JSONObject retVal = new JSONObject() ;
		
		RWJBusComp bc = app.GetBusObject("AssociationMembership").getBusComp("AssociationMembership");
		BasicDBObject query = new BasicDBObject();

		query.put("assocId", data.get("id").toString());

		query.putAll(getSearchSpec(data).toMap());
		int nRecs = bc.ExecQuery(query,getSortSpec(data));
		BasicDBList cList = bc.GetCompositeRecordList();
		retVal.put("data", StringifyDBList(cList));
		return retVal;
	}
	
	@RWAPI
	public JSONObject leaveAssociation(JSONObject data) throws Exception {
		log.trace("leaveAssociation");
		
		RWJBusComp bc = app.GetBusObject("AssociationMembership").getBusComp("AssociationMembership");

		BasicDBObject query = new BasicDBObject();
		query.put("assocId", data.get("id").toString());
		query.put("memberId", GetPartyId());

		int nRecs = bc.ExecQuery(query);
		JSONObject retVal = new JSONObject() ;

		if (nRecs == 1) {
			retVal.put("data", StringifyDBObject(bc.GetCompositeRecord()));
			bc.DeleteRecord() ;
		}
		return retVal;
	}
	
}
