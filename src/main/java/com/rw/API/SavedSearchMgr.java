package com.rw.API;

import java.util.Date;
import java.util.HashMap;
import java.util.Random;

import org.apache.log4j.Logger;
import org.bson.types.ObjectId;
import org.json.JSONObject;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.util.JSON;
import com.rw.persistence.RWJApplication;
import com.rw.persistence.RWJBusComp;
import com.rw.persistence.RWObjectMgr;
import com.rw.repository.RWAPI;


	@SuppressWarnings("unused")
	public class SavedSearchMgr extends GenericMgr  {
	static final Logger log = Logger.getLogger(SavedSearchMgr.class.getName());

	public SavedSearchMgr() {
	}

	public SavedSearchMgr(String userid) {
		super(userid);
	}
	
	public SavedSearchMgr(RWObjectMgr context) throws Exception {
		super(context);
	}
	
	
	@SuppressWarnings("unchecked")
	@RWAPI
	public JSONObject create(JSONObject data) throws Exception {
		
		
		
		log.trace( "create " +  "SavedSearch");
		
		RWJBusComp bc = app.GetBusObject("SavedSearch").getBusComp("SavedSearch");
		
		// How we do find the duplicate Orgs ?
		super.basecreate(data);
		bc.NewRecord();
		bc.SetFieldValue("credId", getUserId());
		String defn = data.getString("definition");
		BasicDBList defnList = (BasicDBList) JSON.parse(defn);
		data.remove("definition");
		bc.SetFieldValue("definition", defnList);
		bc.SaveRecord(data);
		JSONObject retVal = new JSONObject() ;
		retVal.put("data", StringifyDBObject(bc.GetCompositeRecord()));
		return retVal;
		
	}
	
	@SuppressWarnings("unchecked")
	@RWAPI
	public JSONObject read(JSONObject data) throws Exception {
		data.put("bo", "SavedSearch");
		data.put("bc", "SavedSearch");
		return super.read(data);
		
	}
	
	@SuppressWarnings("unchecked")
	@RWAPI
	public JSONObject update(JSONObject data) throws Exception {
		
		log.trace( "update");

		String id = data.getString("id");
		String bcName = "SavedSearch";
		String boName = "SavedSearch";
		
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
		String defn = data.getString("definition");
		BasicDBList defnList = (BasicDBList) JSON.parse(defn);
		data.remove("definition");
		super.baseupdate(data);
		bc.SetFieldValue("definition", defnList);
		bc.SaveRecord(data);
		retVal.put("data", StringifyDBObject(bc.GetCompositeRecord()));
		return retVal;
	}		
		
	
	@SuppressWarnings("unchecked")
	@RWAPI
	public JSONObject delete(JSONObject data) throws Exception {
		return super.delete(data);
		
	}

}
