package com.rw.API;

import java.util.regex.Pattern;

import org.apache.log4j.Logger;
import org.bson.types.ObjectId;
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


public class LovMgr extends RWReqHandle {
	
	static final Logger log = Logger.getLogger(LovMgr.class.getName());

	public LovMgr() {
	}

	public LovMgr(String userid) {
		super(userid);
	}
	
	public LovMgr(RWObjectMgr context) throws Exception {
		super(context);
	}

	@SuppressWarnings("unchecked")
	@RWAPI
	public JSONObject create(JSONObject data) throws Exception {
		log.trace( "create ");
		
		if( !hasRole("ADMINISTRATOR") ) {
			String errorMessage = getMessage("UNAUTHORIZED_ACCESS"); 
			log.debug( errorMessage);
			throw new Exception(errorMessage);							
		}

		RWJBusComp bc = app.GetBusObject("Lov").getBusComp("Lov");

		BasicDBObject query = new BasicDBObject();
		query.put("LovType", data.get("LovType")); 
		query.put("GlobalVal", data.get("GlobalVal")); 
		
		if (bc.UpsertQuery(query) == 0 ) {
			bc.NewRecord();
		}

		super.create(data);
		bc.SaveRecord(data);
		
		JSONObject retVal = new JSONObject() ;
		retVal.put("data", StringifyDBObject(bc.GetCompositeRecord()));
		return retVal;
	}
	

	public JSONObject update(JSONObject data) throws Exception {
		log.trace( "update");
		
		if( !hasRole("ADMINISTRATOR") ) {
			String errorMessage = getMessage("UNAUTHORIZED_ACCESS"); 
			log.debug( errorMessage);
			throw new Exception(errorMessage);							
		}

		RWJBusComp bc = app.GetBusObject("Lov").getBusComp("Lov");

		BasicDBObject query = new BasicDBObject();
		query.put("_id", new ObjectId(data.get("id").toString())); 
		
		if (bc.UpsertQuery(query) == 0 ) {
			String errorMessage = getMessage("RECORD_NOTFOUND"); 
			log.debug( errorMessage);
			throw new Exception(errorMessage);
		}

		super.update(data);
		bc.SaveRecord(data);
		
		JSONObject retVal = new JSONObject() ;
		retVal.put("data", StringifyDBObject(bc.GetCompositeRecord()));
		return retVal;
	}

	@RWAPI
	public JSONObject delete(JSONObject data) throws Exception {
		log.trace( "delete");
		
		if( !hasRole("ADMINISTRATOR") ) {
			String errorMessage = getMessage("UNAUTHORIZED_ACCESS"); 
			log.debug( errorMessage);
			throw new Exception(errorMessage);							
		}
		
		JSONObject retVal = new JSONObject() ;
		RWJBusComp bc = app.GetBusObject("Lov").getBusComp("Lov");
		BasicDBObject query = new BasicDBObject();
		query.put("_id", new ObjectId(data.get("id").toString())); 
		
		if (bc.UpsertQuery(query) > 0 ) {
			retVal.put("data", StringifyDBObject(bc.GetCompositeRecord()));
			bc.DeleteRecordMaster();
		}
		return retVal;
	}

	@RWAPI
	public JSONObject read(JSONObject data) throws Exception {
		log.trace( "read");

		RWJBusComp bc = app.GetBusObject("Lov").getBusComp("Lov");

		BasicDBObject query = new BasicDBObject();
		JSONObject retVal = new JSONObject() ;
		
		if ( !data.isNull("id")) {
			Object id = data.get("id");
			query.put("_id", new ObjectId(id.toString())); 
			query.putAll(getSearchSpec(data).toMap());
		
			int nRecs = bc.ExecQuery(query,null);
			if ( nRecs > 0 ) {
				// Lovs may have composite fields
				BasicDBObject cRec = bc.GetCompositeRecord();
				retVal.put("data", StringifyDBObject(cRec));
			}
		} else {
			if ( !data.isNull("LovType"))
				query.put("LovType", data.get("LovType").toString());  
			if ( !data.isNull("Locale"))
				query.put("Locale", data.get("Locale").toString());
			if ( !data.isNull("Group"))
				query.put("Group", data.get("Group").toString());
			if ( !data.isNull("GlobalVal"))
				query.put("GlobalVal", data.get("GlobalVal").toString());
			if ( !data.isNull("constraint")){
				JSONObject ss = data.getJSONObject("constraint");
				String searchField = JSONObject.getNames(ss)[0];
				String searchValue = ss.getString(searchField);
				BasicDBObject baseFilter = new BasicDBObject();
				baseFilter.put(searchField,java.util.regex.Pattern.compile(searchValue));
				BasicDBObject catchAll = new BasicDBObject();
				catchAll.put(searchField,"CatchAll");
				BasicDBList OR = new BasicDBList();
				OR.add(baseFilter);
				OR.add(catchAll);
				query.put("$or",OR);
			}
			
			if ( !data.isNull("searchText")) {
				Pattern text = Pattern.compile(data.get("searchText").toString(), Pattern.CASE_INSENSITIVE);
				QueryBuilder qm = new QueryBuilder().
						or(new QueryBuilder().start().put("DisplayVal").regex(text).get(),
						new QueryBuilder().put("Group").regex(text).get(),
						new QueryBuilder().put("Tags").regex(text).get());
				query.putAll(qm.get());
			}
			
			
			
			query.putAll(getSearchSpec(data).toMap());
			
			//JSONObject retVal = new JSONObject() ;
			
			int skip = !data.isNull("skip") ? Integer.parseInt(data.getString("skip")) : -1;
			int limit = !data.isNull("limit")? Integer.parseInt(data.getString("limit")) :-1;
			int nRecs = bc.ExecQuery(query,getSortSpec(data), limit, skip);
		
			//bc.ExecQuery(query,getSortSpec(data));
			retVal.put("data", StringifyDBList(bc.recordSet));
		}
			
		return retVal;
	}
}
