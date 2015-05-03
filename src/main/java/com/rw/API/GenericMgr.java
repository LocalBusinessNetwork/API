package com.rw.API;

import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;
import java.util.HashMap;
import java.util.regex.Pattern;

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
public class GenericMgr extends RWReqHandle  {
	static final Logger log = Logger.getLogger(GenericMgr.class.getName());

	public GenericMgr() {
	}
	
	public GenericMgr(String userid) {
		super(userid);
	}
	
	public GenericMgr(RWObjectMgr context) throws Exception {
		super(context);
	}
	
	@SuppressWarnings("unchecked")
	@RWAPI
	public JSONObject create(JSONObject data) throws Exception {
		
		String bcName = data.getString("bc");
		String boName = data.getString("bo");
		log.trace( "create " +  bcName);
		
		RWJBusComp bc = app.GetBusObject(bcName).getBusComp(boName);
		
		// How we do find the duplicate Orgs ?
		
		bc.NewRecord();
		bc.SetFieldValue("credId", getUserId());
		//bc.SetFieldValue("partytype", "BUSINESS");
		super.create(data);
		bc.SaveRecord(data);
		JSONObject retVal = new JSONObject() ;
		retVal.put("data", StringifyDBObject(bc.GetCompositeRecord()));
		return retVal;
	}
	
	@SuppressWarnings("unchecked")
	@RWAPI
	public JSONObject basecreate(JSONObject data) throws Exception {
		super.create(data);
		return data;
	}
	
	public JSONObject baseupdate(JSONObject data) throws Exception {
		return super.update(data);
		
	}
	
	@SuppressWarnings({ "unchecked", "unchecked" })
	@RWAPI
	public JSONObject read(JSONObject data) throws Exception {
		
		String bcName = data.getString("bc");
		String boName = data.getString("bo");
		log.trace( "read " + bcName);

		JSONObject retVal = new JSONObject() ;
		
		RWJBusComp bc = app.GetBusObject(boName).getBusComp(getShapedBC(data,bcName));
		
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
		}
		else { // request for a set of records, filtered by a criteria
			// Add the additional query criteria
			
			query.putAll(getSearchSpec(data).toMap());
			
			if (data.has("excludeId")) {
				BasicDBObject excludeId = new BasicDBObject();
				excludeId.put("$ne", new ObjectId(data.getString("excludeId")));
				query.put("_id", excludeId);
			}
			
			// RULE3: Paging parameters 
			int skip = !data.isNull("skip") ? Integer.parseInt(data.getString("skip")) : -1;
			int limit = !data.isNull("limit")? Integer.parseInt(data.getString("limit")) :-1;

			int nRecs = bc.ExecQuery(query,getSortSpec(data), limit, skip);
			retVal.put("data", StringifyDBList(bc.GetCompositeRecordList()));
		}
		
		/*
					query.putAll(getSearchSpec(data).toMap());

			// RULE2: search parameters 
			if ( !data.isNull("searchText")) {
				Pattern text = Pattern.compile(data.get("searchText").toString(), Pattern.CASE_INSENSITIVE);
				QueryBuilder qm = new QueryBuilder().
						or(new QueryBuilder().start().put("fullName").regex(text).get(),
						new QueryBuilder().put("jobTitle").regex(text).get(),
						new QueryBuilder().put("tags").regex(text).get(),
						new QueryBuilder().put("business").regex(text).get(),
						new QueryBuilder().put("speakingTopics").regex(text).get());
				
				query.putAll(qm.get());
			}
			
			
			if (data.has("excludeId")) {
				BasicDBObject excludeId = new BasicDBObject();
				excludeId.put("$ne", new ObjectId(data.getString("excludeId")));
				query.put("_id", excludeId);
			}
			
			// RULE3: Paging parameters 
			int skip = !data.isNull("skip") ? Integer.parseInt(data.getString("skip")) : -1;
			int limit = !data.isNull("limit")? Integer.parseInt(data.getString("limit")) :-1;

			// Run the query. 
			int nRecs = bc.ExecQuery(query,getSortSpec(data), limit, skip);

			// Party Object may have composite fields.
			BasicDBList cList = bc.GetCompositeRecordList();
			retVal.put("data", StringifyDBList(cList));
		
		*/
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

		String id = data.getString("id");
		String bcName = data.getString("bc");
		String boName = data.getString("bo");
		
		super.update(data);
		
		

		JSONObject retVal = new JSONObject() ;
		RWJBusComp bc = app.GetBusObject(boName).getBusComp(bcName);
		
		BasicDBObject query = new BasicDBObject();
		
		// Updatable by the creator only.
		//query.put("credId", userid.toString());
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

		//super.delete(data);
		
		Object id = data.get("id");
		String bcName = data.getString("bc");
		String boName = data.getString("bo");
		log.trace( "create " +  bcName);
		
		JSONObject retVal = new JSONObject() ;
		RWJBusComp bc = app.GetBusObject(boName).getBusComp(bcName);
		
		BasicDBObject query = new BasicDBObject();
		// Deletable by the creator only.
		//query.put("credId", getUserId());
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
		String boName = data.getString("bo");
		String bcName = data.getString("bc");
		RWJBusComp bc = app.GetBusObject(boName).getBusComp(bcName);
		String parentKey = data.getString("parentKey");
		String parentKeyVal = data.getString("parentKeyVal");
		/* SK ?????? if (userid == null && !data.isNull("userId")){
			userid = data.getString("userId");
		}*/
		// should only matter to demo setup
		//String partyRelation = (!data.isNull("partyRelation"))?data.getString("partyRelation"):"DESCRIPTOR";
		
		//QueryBuilder qm = new QueryBuilder().and(new QueryBuilder().start().put("partyId").is("PROFFESSION").get(),new QueryBuilder().start().put("partyRelation").is(partyRelation).get());
		//QueryBuilder qm = new QueryBuilder().and(new QueryBuilder().start().put(parentKey).is(parentKeyVal).get(),new QueryBuilder().start().put("partyRelation").is(partyRelation).get());
		
		BasicDBObject query = new BasicDBObject();
		query.put(parentKey, parentKeyVal);
		//query.putAll(qm.get());
		
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
				bc.SetFieldValue(parentKey, parentKeyVal);
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
	
	@SuppressWarnings("unchecked")
	@RWAPI
	public JSONObject vmSnippet(JSONObject data) throws Exception {
		
		String bcName = data.getString("bc");
		String boName = data.getString("bo");
		log.trace( "vmSnippet " + bcName);

		JSONObject retVal = new JSONObject() ;
		
		RWJBusComp bc = app.GetBusObject(boName).getBusComp(bcName);
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
		}
		else { // request for a set of records, filtered by a criteria
			query.putAll(getSearchSpec(data).toMap());
			int nRecs = bc.ExecQuery(query,getSortSpec(data));
		}

		return super.vmSnippet(System.getProperty("WebRoot"), data.getString("template"), bc);
		
	}
}
