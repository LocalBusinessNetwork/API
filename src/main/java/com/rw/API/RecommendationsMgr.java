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
public class RecommendationsMgr extends RWReqHandle  {
	static final Logger log = Logger.getLogger(RecommendationsMgr.class.getName());

	public RecommendationsMgr() {
	}

	public RecommendationsMgr(String userid) {
		super(userid);
	}

	public RecommendationsMgr(RWObjectMgr context) throws Exception {
		super(context);
	}

	@SuppressWarnings("unchecked")
	@RWAPI
	public JSONObject create(JSONObject data) throws Exception {
		log.trace( "create Recommendations");
		
		RWJBusComp bc = app.GetBusObject("Recommendations").getBusComp("Recommendations");
		
		// How we do find the duplicate Orgs ?
		bc.NewRecord();
		//bc.SetFieldValue("credId", userid.toString());
		//bc.SetFieldValue("partytype", "BUSINESS");
		super.create(data);
		bc.SaveRecord(data);
		JSONObject retVal = new JSONObject() ;
		retVal.put("data",StringifyDBObject(bc.currentRecord));
		//retVal.put("data", StringifyDBObject(bc.GetCompositeRecord()));
		return retVal;
	}
	
	@SuppressWarnings({ "unchecked", "unchecked" })
	@RWAPI
	public JSONObject read(JSONObject data) throws Exception {
		log.trace( "read Recommendations");

		JSONObject retVal = new JSONObject() ;
		
		RWJBusComp bc = app.GetBusObject("Recommendations").getBusComp("Recommendations");
		BasicDBObject query = new BasicDBObject();

		if ( !data.isNull("partnerId")) {
			String partnerId = data.getString("partnerId");
			//Object id = data.get("id");
	        //query.put("partnerId", partnerId);			
			
			QueryBuilder qm = new QueryBuilder().and(new QueryBuilder().start().put("partnerId").is(partnerId).get(),
					new QueryBuilder().start().put("score").greaterThan(new Double(.2)).get());

			query.putAll(qm.get());
	        
	        query.putAll(getSearchSpec(data).toMap());
	        int limit = !data.isNull("limit")? Integer.parseInt(data.getString("limit")) :-1;
	    	int skip = !data.isNull("skip") ? Integer.parseInt(data.getString("skip")) : -1;
			int nRecs = bc.ExecQuery(query,getSortSpec(data), limit, skip);
			if ( nRecs > 0 ) {
				retVal.put("data", StringifyDBList(bc.GetCompositeRecordList()));
			}
			else {
				String errorMessage = getMessage("RECORD_NOTFOUND"); 
				log.debug( errorMessage);
				throw new Exception(errorMessage);
			}
		} else if ( !data.isNull("memberId")) {
			String userId = data.getString("memberId");
			//Object id = data.get("id");
	        //query.put("partnerId", partnerId);			
			
			QueryBuilder qm = new QueryBuilder().and(new QueryBuilder().start().put("memberId").is(userId).get(),
					new QueryBuilder().start().put("score").greaterThan(new Double(.2)).get());

			query.putAll(qm.get());
	        
	        query.putAll(getSearchSpec(data).toMap());
	        int limit = !data.isNull("limit")? Integer.parseInt(data.getString("limit")) :-1;
	    	int skip = !data.isNull("skip") ? Integer.parseInt(data.getString("skip")) : -1;
			int nRecs = bc.ExecQuery(query,getSortSpec(data), limit, skip);
			if ( nRecs > 0 ) {
				retVal.put("data", StringifyDBList(bc.GetCompositeRecordList()));
			}
			else {
				String errorMessage = getMessage("RECORD_NOTFOUND"); 
				log.debug( errorMessage);
				throw new Exception(errorMessage);
			}
		}
		else { // request for a set of records, filtered by a Recommendations
			
			// Add the additional query Recommendations
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
		RWJBusComp bc = app.GetBusObject("Recommendations").getBusComp("Recommendations");
		
		BasicDBObject query = new BasicDBObject();
		
		// Updatable by the creator only.
		//query.put("credId", userid.toString());
        query.put("_id", new ObjectId(id.toString())); 
	
		if (bc.UpsertQuery(query) == 0 ) {
			String errorMessage = getMessage("RECORD_NOTFOUND"); 
			log.debug( errorMessage);
			throw new Exception(errorMessage);
		}
		

		if ( !data.isNull("score")){
			data.put("score",data.getDouble("score"));
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
		RWJBusComp bc = app.GetBusObject("Recommendations").getBusComp("Recommendations");
		
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
		log.trace( "create Recommendations");
		JSONObject retVal = new JSONObject() ;
		
		JSONArray dataSet = data.getJSONArray("data");
		RWJBusComp bc = app.GetBusObject("Recommendations").getBusComp("Recommendations");
		
		BasicDBObject query = new BasicDBObject();
		if ( !data.isNull("partyId")){query.put("partyId", data.getString("partyId"));}
		else {query.put("partyId", getUserId());}
		
		BasicDBList currentRecommendations = null;
		HashMap<String, BasicDBObject> recordIndex = new HashMap();
		int nRecs = bc.ExecQuery(query,null);
		if ( nRecs > 0 ) {
			currentRecommendations = bc.recordSet;
			for (int i = 0;i < nRecs;i++){
				BasicDBObject dbo = (BasicDBObject)currentRecommendations.get(i);
				ObjectId dboId = (ObjectId)dbo.get("_id");
				String recordId = dboId.toString();
				recordIndex.put(recordId, dbo);
			}
		}
		
		for (int i=0;i< dataSet.length();i++){
			JSONObject thisDoc = dataSet.getJSONObject(i);
			if ( thisDoc.isNull("id")){
				bc.NewRecord();
				bc.SetFieldValue("partyId", getUserId());
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
		
		/*
		RWJApplication app = new RWJApplication();
		RWJBusComp bc = app.GetBusObject("Recommendations").getBusComp("Recommendations");
		
		// How we do find the duplicate Orgs ?
		
		bc.NewRecord();
		bc.SetFieldValue("credId", userid.toString());
		bc.SetFieldValue("partytype", "BUSINESS");
		super.create(data);
		bc.SaveRecord(data);
		JSONObject retVal = new JSONObject() ;
		retVal.put("data", StringifyDBObject(bc.GetCompositeRecord()));
		return retVal;
		*/
		return retVal;
	}
}
