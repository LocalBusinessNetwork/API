package com.rw.API;

import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;
import java.util.HashMap;

import org.apache.log4j.Logger;
import org.bson.types.ObjectId;
import org.json.JSONArray;
import org.json.JSONObject;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBRef;
import com.mongodb.QueryBuilder;
import com.rw.persistence.RWJApplication;
import com.rw.persistence.RWJBusComp;
import com.rw.persistence.RWJBusObj;
import com.rw.persistence.RWObjectMgr;
import com.rw.persistence.mongoStore;
import com.rw.repository.RWAPI;


@SuppressWarnings("unused")
public class CriteriaMgr extends RWReqHandle  {
	static final Logger log = Logger.getLogger(CriteriaMgr.class.getName());
	
	public CriteriaMgr(String userid) {
		super(userid);
	}
	
	public CriteriaMgr(RWObjectMgr context) throws Exception {
		super(context);
	}
	
	@SuppressWarnings("unchecked")
	@RWAPI
	public JSONObject create(JSONObject data) throws Exception {
		log.trace( "create Criteria");
		
		RWJBusComp bc = app.GetBusObject("Criteria").getBusComp("Criteria");
		
		// How we do find the duplicate Orgs ?
		
		bc.NewRecord();
		bc.SetFieldValue("credId", getUserId());
		bc.SetFieldValue("partytype", "BUSINESS");
		bc.SetFieldValue("MLID", 0L);
		
		super.create(data);
		bc.SaveRecord(data);
		JSONObject retVal = new JSONObject() ;
		retVal.put("data", StringifyDBObject(bc.GetCompositeRecord()));
		return retVal;
	}
	
	@SuppressWarnings({ "unchecked", "unchecked" })
	@RWAPI
	public JSONObject read(JSONObject data) throws Exception {
		log.trace( "read Criteria");

		JSONObject retVal = new JSONObject() ;
		
		RWJBusComp bc = app.GetBusObject("Criteria").getBusComp("Criteria");
		BasicDBObject query = new BasicDBObject();

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
		} else if ( !data.isNull("partyId")) {
			String partyId = data.getString("partyId");
			
			//BasicDBObject
			//QueryBuilder qm = new QueryBuilder().start("partyId").is(partyId).and(new BasicDBObject(getSearchSpec(data).toMap()));
			QueryBuilder qm = new QueryBuilder().and(new QueryBuilder().start().put("partyId").is(partyId).get(),new BasicDBObject(getSearchSpec(data).toMap()));

			
					//getSearchSpec(data).toMap());
			//query.putAll(qm.get());
			
			
			
			
	        //query.put("partyId", partyId); 
	        //query.putAll(getSearchSpec(data).toMap());	
			query.putAll(qm.get());
	        int nRecs = bc.ExecQuery(query,getSortSpec(data));
			
	        //if ( nRecs > 0 ) {
	        	retVal.put("data", StringifyDBList(bc.GetCompositeRecordList()));
			///}
			
			
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
	public JSONObject query(JSONObject data) throws Exception {
		log.trace( "query");
		return read(data);
	}
	
	@SuppressWarnings("unchecked")
	@RWAPI
	public JSONObject update(JSONObject data) throws Exception  {
		log.trace( "update");

		super.update(data);
		
		Object id = data.get("id");

		JSONObject retVal = new JSONObject() ;
		RWJBusComp bc = app.GetBusObject("Criteria").getBusComp("Criteria");
		
		BasicDBObject query = new BasicDBObject();
		
		// Updatable by the creator only.
		query.put("credId", getUserId());
        query.put("_id", new ObjectId(id.toString())); 
	
		if (bc.UpsertQuery(query) == 0 ) {
			String errorMessage = getMessage("RECORD_NOTFOUND"); 
			log.debug( errorMessage);
			throw new Exception(errorMessage);
		}
		
		bc.SaveRecord(data);
		retVal.put("data", StringifyDBObject(bc.GetCompositeRecord()));
		return retVal;
	}

	@SuppressWarnings("unchecked")
	@RWAPI
	public JSONObject delete(JSONObject data) throws Exception {
		log.trace( "delete");

		super.delete(data);
		
		Object id = data.get("id");
		
		JSONObject retVal = new JSONObject() ;
		RWJBusComp bc = app.GetBusObject("Criteria").getBusComp("Criteria");
		
		BasicDBObject query = new BasicDBObject();
		// Deletable by the creator only.
		query.put("credId", getUserId());
        query.put("_id", new ObjectId(id.toString())); 
	
		if (bc.ExecQuery(query,null) == 0 ) {
			String errorMessage = getMessage("RECORD_NOTFOUND"); 
			log.debug( errorMessage);
			throw new Exception(errorMessage);
		}
		
		retVal.put("data", StringifyDBObject(bc.GetCompositeRecord()));
		bc.DeleteRecord();
		return retVal;
	}
	
	@SuppressWarnings("unchecked")
	@RWAPI
	public JSONObject associate(JSONObject data) throws Exception {
		log.trace( "create Criteria");
		JSONObject retVal = new JSONObject() ;
		
		
		JSONArray dataSet = data.getJSONArray("data");
		RWJBusComp bc = app.GetBusObject("Criteria").getBusComp("Criteria");
		String partyId = (!data.isNull("partyId"))?data.getString("partyId"): getUserId();
		/* SK ??????? if (userid == null && !data.isNull("userId")){
			userid = data.getString("userId");
		}*/
		
		// should only matter to demo setup
		String partyRelation = (!data.isNull("partyRelation"))?data.getString("partyRelation"):"DESCRIPTOR";
		
		//QueryBuilder qm = new QueryBuilder().and(new QueryBuilder().start().put("partyId").is("PROFFESSION").get(),new QueryBuilder().start().put("partyRelation").is(partyRelation).get());
		QueryBuilder qm = new QueryBuilder().and(new QueryBuilder().start().put("partyId").is(partyId).get(),new QueryBuilder().start().put("partyRelation").is(partyRelation).get());
		BasicDBObject query = new BasicDBObject();
		query.putAll(qm.get());
		
		BasicDBList currentCriteria = null;
		HashMap<String, BasicDBObject> recordIndex = new HashMap();
		int nRecs = bc.ExecQuery(query,null);
		
		if ( nRecs > 0 ) {
			currentCriteria = bc.recordSet;
			for (int i = 0;i < nRecs;i++){
				BasicDBObject dbo = (BasicDBObject)currentCriteria.get(i);
				ObjectId dboId = (ObjectId)dbo.get("_id");
				String recordId = dboId.toString();
				recordIndex.put(recordId, dbo);
			}
		}
		
		for (int i=0;i< dataSet.length();i++){
			//Object thisDocObj = dataSet.get(i);
			
			
			//JSONObject thisDoc = null;
			//if (thisDocObj.getClass().getName() == "com.mongodb.BasicDBObject"){
			//	BasicDBObject BDBthisDoc = (BasicDBObject)dataSet.get(i);
			//	thisDoc = new JSONObject(BDBthisDoc.toMap());
			//} else {
				JSONObject thisDoc = dataSet.getJSONObject(i);
			//}
			if ( thisDoc.isNull("id")){
				bc.NewRecord();
				bc.SetFieldValue("partyId", partyId);
				super.create(thisDoc);
				bc.SaveRecord(thisDoc);
			} else {
				
				recordIndex.remove(thisDoc.get("id"));

			}
			
		}
		
		Object removeKeys[] = recordIndex.keySet().toArray();
		for (int i=0;i<removeKeys.length;i++){
			
			BasicDBObject removeRecord = (BasicDBObject)recordIndex.get(removeKeys[i]);
			bc.currentRecord = removeRecord;
			bc.DeleteRecord();
		}
		
		nRecs = bc.ExecQuery(query,null);
		if ( nRecs > 0 ) {
			retVal.put("data", StringifyDBList(bc.recordSet));
		}
		
		return retVal;
	}
}
