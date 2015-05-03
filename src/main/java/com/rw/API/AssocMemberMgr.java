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


public class AssocMemberMgr extends RWReqHandle  {
	static final Logger log = Logger.getLogger(AssocMemberMgr.class.getName());
	
	public AssocMemberMgr() {
	}

	public AssocMemberMgr(String userid) {
		super(userid);
	}
	
	public AssocMemberMgr(RWObjectMgr context) throws Exception {
		super(context);
	}
	
	@RWAPI
	public JSONObject create(JSONObject data) throws Exception {
		log.trace("create");

		RWJBusComp bc = app.GetBusObject("AssociationMembership").getBusComp("AssociationMembership");

		BasicDBObject query = new BasicDBObject();
		
		String assocId = data.get("id").toString();
		query.put("assocId",assocId );
		query.put("memberId", GetPartyId());

		int nRecs = bc.ExecQuery(query);
		JSONObject retVal = new JSONObject() ;

		if (nRecs == 1) {
			retVal.put("data", StringifyDBObject(bc.GetCompositeRecord()));
			return retVal;
		}
		
		bc.NewRecord();
		bc.SetFieldValue("memberId", GetPartyId());
		bc.SetFieldValue("assocId",assocId);
		
		super.create(data);
		bc.SaveRecord(data);

		retVal.put("data", StringifyDBObject(bc.GetCompositeRecord()));
	
		return retVal;
	}
	
	@RWAPI
	public JSONObject read(JSONObject data) throws Exception {
		log.trace( "read");

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
	public JSONObject delete(JSONObject data) throws Exception {
		log.trace( "delete");
		super.delete(data);

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
