package com.rw.API;

import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;
import java.text.ParseException;
import java.util.Date;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;
import org.bson.types.ObjectId;
import org.json.JSONException;
import org.json.JSONObject;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBRef;
import com.mongodb.QueryBuilder;
import com.rw.persistence.RWAlgorithm;
import com.rw.persistence.RWChartItemData;
import com.rw.persistence.RWJApplication;
import com.rw.persistence.RWJBusComp;
import com.rw.persistence.RWJBusObj;
import com.rw.persistence.RWObjectMgr;
import com.rw.persistence.mongoStore;
import com.rw.repository.Permissions;
import com.rw.repository.RWAPI;
import com.rw.repository.Roles;


@SuppressWarnings("unused")
public class BusinessMgr extends RWReqHandle  {
	static final Logger log = Logger.getLogger(BusinessMgr.class.getName());

	public BusinessMgr() {
	}
	
	public BusinessMgr(String userid) {
		super(userid);
	}
	
	public BusinessMgr(RWObjectMgr context) throws Exception {
		super(context);
	}

	
	@SuppressWarnings("unchecked")
	@RWAPI
	@Roles({"ADMINISTRATOR"})
	public JSONObject create(JSONObject data) throws Exception {
		log.trace( "create Business");
		
		RWJBusComp bc = app.GetBusObject("Business").getBusComp("Business");
		
		// How we do find the duplicate Orgs ?
		
		if (data.has("dateFounded")){
			long FoundedLong = getLongForDate(data,"dateFounded");
			data.put("dateFounded",FoundedLong);
		}
		
		bc.NewRecord();
		bc.SetFieldValue("credId", getUserId());
		bc.SetFieldValue("partytype", "BUSINESS_DIR");
		super.create(data);
		bc.SaveRecord(data);
		JSONObject retVal = new JSONObject() ;

		String OrgId = bc.GetFieldValue("_id").toString();
		
		EnrichParty(OrgId);
		SetLeaderBusiness(bc);
		retVal.put("data", StringifyDBObject(bc.GetCompositeRecord()));		
		
		JSONObject trackingData = new JSONObject();
		trackingData.put("event", "UM_CREATEORG");         			
		trackMetrics(trackingData);

		return retVal;
	}

	@SuppressWarnings({ "unchecked", "unchecked" })
	@RWAPI
	public JSONObject read(JSONObject data) throws Exception {
		log.trace( "read");
		
		JSONObject retVal = new JSONObject() ;
		
		RWJBusComp bc = app.GetBusObject("Business").getBusComp("Business");
		
		BasicDBObject query = new BasicDBObject();
		
		
		// case 1
		if ( !data.isNull("id")) {
			Object id = data.get("id"); 
	        query.put("_id", new ObjectId(id.toString())); 
			int nRecs = bc.ExecQuery(query,null);
			if ( nRecs > 0 ) {
				BasicDBObject cRec = bc.GetCompositeRecord();
				cRec.put("employees", getEmployees(id.toString()));
				retVal.put("data", StringifyDBObject(cRec));
				
			}
			else {
				String errorMessage = getMessage("RECORD_NOTFOUND"); 
				log.debug( errorMessage);
				throw new Exception(errorMessage);
			}
		} // case 2
		
		else { // request for a set of records, filtered by a criteria
			return textSearch(data);
		}
		
		
		
		
		return retVal;
	}
	
	@SuppressWarnings({ "unchecked", "unchecked" })
	@RWAPI
	public JSONObject readAll(JSONObject data) throws Exception {
		log.trace( "read");
		JSONObject retVal = new JSONObject() ;
		RWJBusComp bc = app.GetBusObject("Business").getBusComp("Business");
		BasicDBObject query = new BasicDBObject();
		
		return textSearch(data);
	}
	
	@RWAPI
	public JSONObject textSearch(JSONObject data) throws Exception {
		log.trace( "textSearch");
		RWJBusComp bc = app.GetBusObject("Business").getBusComp("Business");
		BasicDBObject query = new BasicDBObject();
		//begin new
		JSONObject retVal = new JSONObject() ;
		
		query.putAll(getSearchSpec(data).toMap());
		if ( !data.isNull("searchText")) {
			Pattern text = Pattern.compile(data.get("searchText").toString(), Pattern.CASE_INSENSITIVE);
			QueryBuilder qm = new QueryBuilder().
					or(new QueryBuilder().start().put("businessName").regex(text).get(),
					new QueryBuilder().put("services").regex(text).get(),
					new QueryBuilder().put("category").regex(text).get());
					//new QueryBuilder().put("postalCodeAddress_work").regex(text).get());
			query.putAll(qm.get());
		}
		query.put("partytype", "BUSINESS_DIR");
		
		if (data.has("excludeId")) {
			BasicDBObject excludeId = new BasicDBObject();
			excludeId.put("$ne", new ObjectId(data.getString("excludeId")));
			query.put("_id", excludeId);
		}

		int skip = !data.isNull("skip") ? Integer.parseInt(data.getString("skip")) : -1;
		int limit = !data.isNull("limit")? Integer.parseInt(data.getString("limit")) :-1;

		// Run the query. 
		int nRecs = bc.ExecQuery(query,getSortSpec(data), limit, skip);

		// Party Object may have composite fields.
		BasicDBList cList = bc.GetCompositeRecordList();
		retVal.put("data", StringifyDBList(cList));
	
		return retVal;
	}
	
	
	@RWAPI
	public JSONObject query(JSONObject data) throws Exception {
		log.trace( "query");
		return read(data);
	}
	
	@SuppressWarnings("unchecked")
	@RWAPI
	//@Permissions({"ADMIN_CHAPTER_HOUSE", "UPDATE_CHAPTER_HOUSE", "ADMIN_CHAPTER", "UPDATE_CHAPTER"})
	public JSONObject update(JSONObject data) throws Exception  {
		log.trace( "update");
		
		if (data.has("dateFounded")){
			long FoundedLong = getLongForDate(data,"dateFounded");
			data.put("dateFounded",FoundedLong);
		}
		super.update(data);	
		
		Object id = data.get("id");

		JSONObject retVal = new JSONObject() ;
		RWJBusComp bc = app.GetBusObject("Business").getBusComp("Business");
		
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
		SetLeaderBusiness(bc);
		EnrichParty(id.toString());
		retVal.put("data", StringifyDBObject(bc.GetCompositeRecord()));
		
		JSONObject trackingData = new JSONObject();
		trackingData.put("event", "UM_UPDATEORG");         			
		trackMetrics(trackingData);
		
		return retVal;
	}

	@SuppressWarnings("unchecked")
	@RWAPI
	@Roles({"ADMINISTRATOR"})
	public JSONObject delete(JSONObject data) throws Exception {
		log.trace( "delete");

		super.delete(data);
		
		Object id = data.get("id");
		
		JSONObject retVal = new JSONObject() ;
		RWJBusComp bc = app.GetBusObject("Business").getBusComp("Business");
		
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
	public void SetLeaderBusiness(RWJBusComp BizBC) throws Exception{ 
	
		String OrgId = BizBC.GetFieldValue("_id").toString();
		String leaderId = (String)BizBC.GetFieldValue("leaderId");
		
		
		RWJBusComp partybc = app.GetBusObject("Party").getBusComp("Party");
		BasicDBObject query = new BasicDBObject();
		query.put("_id", new ObjectId(leaderId));
		String oldOrgId = null;
		
		if (partybc.UpsertQuery(query) > 0 ) {
			oldOrgId = (String)partybc.GetFieldValue("OrgId");
			partybc.SetFieldValue("employerId", OrgId);
			partybc.SaveRecord();
			String strParty = StringifyDBObject(partybc.GetCompositeRecord());
		}
		
	}
	
	private BasicDBList getEmployees(String businessId) throws Exception{
		BasicDBList retVal = null;
		RWJBusComp party = app.GetBusObject("Party").getBusComp("PartySkinny");
		BasicDBObject query = new BasicDBObject();
		query.put("employerId", businessId);
		
		if (party.ExecQuery(query) > 0){
			retVal = party.recordSet;
		}
		
		return retVal;
		
	}
	
	public void cron() {
		try {
			RWJBusComp bc = app.GetBusObject("Business").getBusComp("Business");
			BasicDBObject query = new BasicDBObject();
			query.put("partytype", "BUSINESS_DIR");
			int nRecs = bc.UpsertQuery(query);
			
			for ( int i = 0; i < nRecs ; i++ ) {
				try {								
					if ( bc.currentRecord.containsField("leaderId")) {
						String leaderId = bc.GetFieldValue("leaderId").toString();
						Double memberRankScore =  (Double) GetPartyAttr(leaderId,"memberRankScore");					
						if ( memberRankScore != null ) {
							Double businessRankScore = (Double) bc.GetFieldValue("businessRankScore");
							
							if (businessRankScore == null || businessRankScore != memberRankScore) {
								bc.SetFieldValue("businessRankScore", memberRankScore);
								bc.SaveRecord();
							}
							
						}
					}
				}
				catch(Exception e) {
					log.debug(e.getStackTrace());
				}
				bc.NextRecord();
			}
		}
		catch( Exception e) {
			log.debug(e.getStackTrace());
		}
	}	

}
