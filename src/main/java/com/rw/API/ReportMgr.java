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
	public class ReportMgr extends GenericMgr  {
	static final Logger log = Logger.getLogger(ReportMgr.class.getName());

	public ReportMgr() {
	}

	public ReportMgr(String userid) {
		super(userid);
	}
	
	public ReportMgr(RWObjectMgr context) throws Exception {
		super(context);
	}
	
	
	@SuppressWarnings("unchecked")
	@RWAPI
	public JSONObject create(JSONObject data) throws Exception {
		
		
		
		log.trace( "create " +  "Report");
		
		RWJBusComp bc = app.GetBusObject("Report").getBusComp("Report");
		
		// How we do find the duplicate Orgs ?
		super.basecreate(data);
		bc.NewRecord();
		bc.SetFieldValue("credId", getUserId());
		//String fExecDef = data.getString("fExecDef");
		String definition = data.getString("definition");
		//BasicDBObject fExecList = (BasicDBObject) JSON.parse(fExecDef);
		BasicDBList fStoredList = (BasicDBList) JSON.parse(definition);
		fStoredList = tranformMultiField(fStoredList);
		//data.remove("fExecDef");
		data.remove("definition");
		//bc.SetFieldValue("fExecDef", fExecList);
		bc.SetFieldValue("definition", fStoredList);
		bc.SaveRecord(data);
		JSONObject retVal = new JSONObject() ;
		retVal.put("data", StringifyDBObject(bc.GetCompositeRecord()));
		return retVal;
		//fExecDef
		//fStoredDef

	}
	
	@SuppressWarnings("unchecked")
	@RWAPI
	public JSONObject read(JSONObject data) throws Exception {
		data.put("bo", "Report");
		data.put("bc", "Report");
		return super.read(data);
		
	}
	
	@SuppressWarnings("unchecked")
	@RWAPI
	public JSONObject update(JSONObject data) throws Exception {
		
		log.trace( "update");

		String id = data.getString("id");
		String bcName = "Report";
		String boName = "Report";
		
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
		//String fExecDef = data.getString("fExecDef");
		String definition = data.getString("definition");
		//BasicDBObject fExecList = (BasicDBObject) JSON.parse(fExecDef);
		BasicDBList fStoredList = (BasicDBList) JSON.parse(definition);
		fStoredList = tranformMultiField(fStoredList);
		//data.remove("fExecDef");
		data.remove("definition");
		//bc.SetFieldValue("fExecDef", fExecList);
		bc.SetFieldValue("definition", fStoredList);
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
