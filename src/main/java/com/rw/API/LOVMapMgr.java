package com.rw.API;

import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.log4j.Logger;
import org.bson.types.ObjectId;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.DBRef;
import com.mongodb.QueryBuilder;
import com.rw.persistence.RWJApplication;
import com.rw.persistence.RWJBusComp;
import com.rw.persistence.RWJBusObj;
import com.rw.persistence.RWObjectMgr;
import com.rw.persistence.mongoStore;
import com.rw.repository.RWAPI;


@SuppressWarnings("unused")
public class LOVMapMgr extends RWReqHandle  {
	static final Logger log = Logger.getLogger(LOVMapMgr.class.getName());

	public LOVMapMgr() {
	}

	public LOVMapMgr(String userid) {
		super(userid);
	}
	
	public LOVMapMgr(RWObjectMgr context) throws Exception {
		super(context);
	}
	
	@SuppressWarnings("unchecked")
	@RWAPI
	public JSONObject create(JSONObject data) throws Exception {
		log.trace( "create LOVMap");
		
		RWJBusComp bc = app.GetBusObject("LOVMap").getBusComp("LOVMap");
		
		// How we do find the duplicate Orgs ?
		
		bc.NewRecord();
		bc.SetFieldValue("credId", getUserId());
		bc.SetFieldValue("partytype", "BUSINESS");
		super.create(data);
		bc.SaveRecord(data);
		JSONObject retVal = new JSONObject() ;
		retVal.put("data", StringifyDBObject(bc.GetCompositeRecord()));
		return retVal;
	}
	
	@SuppressWarnings({ "unchecked", "unchecked" })
	@RWAPI
	public JSONObject read(JSONObject data) throws Exception {
		log.trace( "read LOVMap");

		JSONObject retVal = new JSONObject() ;
		
		RWJBusComp bc = app.GetBusObject("LOVMap").getBusComp("LOVMap");
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
		} else if ( !data.isNull("parentId")) {
			String parentId = data.getString("parentId");
			
			//BasicDBObject
			//QueryBuilder qm = new QueryBuilder().start("partyId").is(partyId).and(new BasicDBObject(getSearchSpec(data).toMap()));
			QueryBuilder qm = new QueryBuilder().and(new QueryBuilder().start().put("parentId").is(parentId).get(),new BasicDBObject(getSearchSpec(data).toMap()));

			
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
		RWJBusComp bc = app.GetBusObject("LOVMap").getBusComp("LOVMap");
		
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
		RWJBusComp bc = app.GetBusObject("LOVMap").getBusComp("LOVMap");
		
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
		log.trace( "create LOVMap");
		JSONObject retVal = new JSONObject() ;
		
		String associationParentIdField = "parentId"; //this will vary by [Object]Mgr. It's the field on the intersection table that points to the parent record
		
		JSONArray dataSet = data.getJSONArray("data");
		RWJBusComp bc = app.GetBusObject("LOVMap").getBusComp("LOVMap");
		String parentId = data.getString(associationParentIdField);
		String parent_GlobalVal = data.getString("parent_GlobalVal");
		String parent_DisplayVal = data.getString("parent_DisplayVal");
		String parent_LovType = data.getString("parent_LovType");
		JSONArray childLovTypes = data.getJSONArray("childLovTypes");
		
		// The association gesture enables both the addition and the removal of items from a 'shopping cart'
		// In order to detect whether items have been removed we must compare what's in the list when this method is invoked with 
		// what was saved there before.  If some of the items that were associated before are no longer in the
		// cart, we'll delete them.  We start with a query to see what was there before...
		BasicDBObject query = new BasicDBObject();
		query.put("parentId", parentId);
		
		if (childLovTypes != null && childLovTypes.length() > 0){
			
			BasicDBObject in  = new BasicDBObject();
            BasicDBList list = new BasicDBList();
	
			for (int i = 0; i < childLovTypes.length(); i++){
				String thisVal = childLovTypes.getString(i);
				list.add(thisVal);
				//QueryBuilder qm = new QueryBuilder().and(new QueryBuilder().start().put("parentId").is(parentId).get(),new BasicDBObject(getSearchSpec(data).toMap()));
			}
			in.put("$in", list);
			BasicDBObject lovTypeFilter = new BasicDBObject();
			lovTypeFilter.put("child_LovType", in);
			query.putAll(lovTypeFilter.toMap());
		}
		
		
		
		BasicDBList currentLOVMap = null;
		HashMap<String, BasicDBObject> recordIndex = new HashMap();
		int nRecs = bc.ExecQuery(query,null);
		if ( nRecs > 0 ) {
			currentLOVMap = bc.recordSet;
			for (int i = 0;i < nRecs;i++){//initiallly all existing associations are tentatively targeted for deletion.  
				BasicDBObject dbo = (BasicDBObject)currentLOVMap.get(i);
				ObjectId dboId = (ObjectId)dbo.get("_id");
				String recordId = dboId.toString();
				recordIndex.put(recordId, dbo);
			}
		}
		
		for (int i=0;i< dataSet.length();i++){
			JSONObject thisDoc = dataSet.getJSONObject(i);
			if ( thisDoc.isNull("id")){
				bc.NewRecord(); //create new association records for cart items being added for the first time
				bc.SetFieldValue(associationParentIdField, parentId);
				//System.out.println("parent_GlobalVal = " +parent_GlobalVal);
				bc.SetFieldValue("parent_GlobalVal", parent_GlobalVal);
				bc.SetFieldValue("parent_DisplayVal", parent_DisplayVal);
				bc.SetFieldValue("parent_LovType", parent_LovType);
				super.create(thisDoc);
				bc.SaveRecord(thisDoc);
			} else {
				
				recordIndex.remove(thisDoc.get("id")); //if an item is still in the cart that was there previously, we want to leave it.
				//remove it from the list of associations that will be deleted. 

			}
			
		}
		
		Object removeKeys[] = recordIndex.keySet().toArray();
		for (int i=0;i<removeKeys.length;i++){//records that are still in the "to delete" list have been taken out of the shopping cart by the user.  These assocaitons need to be deleted
			
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
	
	public JSONObject getDefaultPermissions(String role) throws Exception {
		
		JSONObject retVal = new JSONObject();
		JSONArray permissoins = new JSONArray();
		
		RWJBusComp bc = app.GetBusObject("LOVMap").getBusComp("LOVMap");
		
		BasicDBObject query = new BasicDBObject();
		query.put("parent_GlobalVal", role);

		int nRecs = bc.ExecQuery(query,null);
		
		for ( int i = 0; i < nRecs; i++ )	{
			String permission = bc.GetFieldValue("child_GlobalVal").toString();
			permissoins.put(permission);
			bc.NextRecord();
		}
		
		retVal.put("permissions", permissoins);
		return retVal;
	}
}
