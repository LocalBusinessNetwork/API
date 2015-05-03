package com.rw.API;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.regex.Pattern;

import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.BasicResponseHandler;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.log4j.Logger;
import org.bson.types.ObjectId;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONStringer;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBRef;
import com.mongodb.QueryBuilder;
import com.rw.persistence.RWAlgorithm;
import com.rw.persistence.RWJApplication;
import com.rw.persistence.RWJBusComp;
import com.rw.persistence.RWJBusObj;
import com.rw.persistence.RWObjectMgr;
import com.rw.persistence.mongoStore;
import com.rw.repository.RWAPI;

/*
 * Parties are created either thru User Manager - for Members
 * or ContactMgr - for contacts.
 * PartyMgr gives different read abilities to Party Data.
 * Enrichment and Recommendations work off PartyMgr as well
 * 
 * App references Parties thru relationships
 * 
 */


public class PartyMgr extends RWReqHandle  {
	static final Logger log = Logger.getLogger(PartyMgr.class.getName());
	
	public PartyMgr() {
	}

	public PartyMgr(String userid) {
		super(userid);
	}
	
	public PartyMgr(RWObjectMgr context) throws Exception {
		super(context);
	}

	/*
	 * (non-Javadoc)
	 * @see com.rw.API.RWReqHandle#read(org.json.JSONObject)
	 * PartyMgr reads either a specific party record or all members
	 * Otherwise, Parties are referenced thru relationships.
	 */
	@SuppressWarnings("unchecked")
	@RWAPI
	public JSONObject read(JSONObject data) throws Exception {
		log.trace( "read");
		
		RWJBusComp bc = app.GetBusObject("Party").getBusComp( getShapedBC(data,"Party") );
		
		JSONObject retVal = new JSONObject() ;

		BasicDBObject query = new BasicDBObject();
		 
		
		if ( !data.isNull("id")) {
			String partyId = data.get("id").toString();
			
			String strParty = null ; // Cache(partyId);
			
			if ( strParty != null ) {
				retVal.put("data", strParty);
				return retVal;
			}
			ObjectId pId = new ObjectId(partyId);
			query.put("_id", pId); 
			int nRecs = bc.ExecQuery(query,null);
			if ( nRecs > 0 ) {
				String partytype = bc.GetFieldValue("partytype").toString();
				// If user is asking for a contact information
				// Let us make sure the user has access rights to this info thru the current relationship.
				if ( partytype.equals("CONTACT")) {
					query.clear();
					query.put("userid", getUserId());
					query.put("partnerId", partyId);
					RWJBusComp bc2 = app.GetBusObject("Partner").getBusComp("Partner");
					nRecs = bc2.ExecQuery(query,null);
				
					if( nRecs> 0 ) { // Yes, the user has relationships with this party
						retVal.put("data", StringifyDBObject(bc.currentRecord));
					}
					else { // No, User does not have access, so through Unauthorized access violation
						String errorMessage = getMessage("UNAUTHORIZED_ACCESS"); 
						log.debug( errorMessage);
						throw new Exception(errorMessage);
					}
				}
				else { // PARTNER info can be accessed by anyone.
					//  this is a rare case. Parties are all cached.
					retVal.put("data", StringifyDBObject(bc.GetCompositeRecord()));
				}
			}
			else {
				String errorMessage = getMessage("RECORD_NOTFOUND"); 
				log.debug( errorMessage);
				throw new Exception(errorMessage);
			}
		} else {

			// RULE1 : We allow only PARTNER objects to be searched
			// Contacts are private to the users, they need to be referenced thru relationships
			/*
			QueryBuilder qb = new QueryBuilder().or(new QueryBuilder().start().put("partytype").is("PARTNER").get(),
					new QueryBuilder().start().put("partytype").is("SYNDICATED").get());
			//query.put("partytype", "PARTNER");
			query.putAll(qb.get());
			
			if ( !data.isNull("searchText")) {
			Pattern text = Pattern.compile(data.get("searchText").toString(), Pattern.CASE_INSENSITIVE);
			QueryBuilder qm = new QueryBuilder().
					or(new QueryBuilder().start().put("businessName").regex(text).get(),
					new QueryBuilder().put("services").regex(text).get(),
					new QueryBuilder().put("category").regex(text).get());
					//new QueryBuilder().put("postalCodeAddress_work").regex(text).get());
			query.putAll(qm.get());
		}
			
			*/
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

		}
		return retVal;	
	}
	
	@SuppressWarnings("unchecked")
	@RWAPI
	public JSONObject update(JSONObject data) throws Exception  { //this is for an administrator
		log.trace( "update");

		super.update(data);
		
		Object id = data.get("id");

		JSONObject retVal = new JSONObject() ;
		RWJBusComp bc = app.GetBusObject("Party").getBusComp("Party");
		
		BasicDBObject query = new BasicDBObject();
		
		// Updatable by the creator only.
		// query.put("credId", userid.toString());
        query.put("_id", new ObjectId(id.toString())); 
	
		if (bc.UpsertQuery(query) == 0 ) {
			String errorMessage = getMessage("RECORD_NOTFOUND"); 
			log.debug( errorMessage);
			throw new Exception(errorMessage);
		}
		
		if ( !data.isNull("firstName") || !data.isNull("lastName") ) { 
			
			String fullName =  (!data.isNull("firstName"))?data.getString("firstName").concat(" "):"";
			fullName = (!data.isNull("lastName"))?fullName.concat(data.getString("lastName")):fullName;
			//fullName = fullName.toUpperCase();
			//bc.SetFieldValue("fullName", fullName);
			data.put("fullName", fullName);
		}
		
		String currentOrgId = bc.currentRecord.getString("OrgId");
		
		if (data.has("joinChapter")){
			data.put("oldOrgId",currentOrgId);
			pushToBackground("addUserToMeeting", data);
		}
		
		if (data.has("invitedBy_Id") && bc.currentRecord.containsField("invitedBy_Id") &&
			!data.getString("invitedBy_Id").equals(bc.GetFieldValue("invitedBy_Id").toString())) {
			String prevIid = bc.GetFieldValue("invitedBy_Id").toString();
			bc.SaveRecord(data);
			updateStat("totalInvited", "totalInvited", "Party",  data.getString("invitedBy_Id"));
			updateStat("totalInvited", "totalInvited", "Party",  prevIid);
		}
		else {
			bc.SaveRecord(data);
		}
		JSONObject trackingData = new JSONObject();
		trackingData.put("event", "UM_UPDATEPROFILE");         			
		trackMetrics(trackingData);
		
		// String strParty = StringifyDBObject(bc.GetCompositeRecord());
		// Cache(id.toString(), strParty);
		retVal.put("data", StringifyDBObject(bc.GetCompositeRecord()));
		
		EnrichParty((String)id);
		JSONObject data_metric = new JSONObject();
		data_metric.put("metric", "speakerProfileScore");
		data_metric.put("field", "speakerProfileScore");
		pushToBackground("computeMetric", data_metric);
		
		// Decache(bc.GetFieldValue("_id").toString());

		//pre_OrgId, pre_isPrimaryAmbassador
	
		
		return retVal;
	}

	@RWAPI
	public JSONObject RunRecommendationsEngine(JSONObject data) {

		//MongoRecommenderModel m = new MongoRecommenderModel();
		//MongoRecommender r = new MongoRecommender("euclidean");
		//r.build(m);
		//r.RunRecommendations();
		
		return new JSONObject();

	}
	
	@RWAPI
	public JSONObject KickoffRecommendations(JSONObject data) throws Exception {
		RunOffline("PartyMgr:RunRecommendationsEngine:" + getUserId());
		return new JSONObject();
	}
	
	@RWAPI
	public JSONObject NearMe(JSONObject data) throws Exception {

		log.trace( "NearMe");

		JSONObject retVal = new JSONObject() ;
		RWJBusComp bc = app.GetBusObject("Party").getBusComp("Party");
		
		BasicDBObject query = new BasicDBObject();
		query.put("credId", getUserId());
		query.put("partytype", "PARTNER");
		
		if (bc.UpsertQuery(query) == 0 ) {
			String errorMessage = getMessage("RECORD_NOTFOUND"); 
			log.debug( errorMessage);
			throw new Exception(errorMessage);
		}
		
		BasicDBObject loc = (BasicDBObject) bc.GetFieldValue("GeoLocation");	
		BasicDBList cList = null;

		if ( loc == null ) {
			cList = new BasicDBList();
			retVal.put("data", StringifyDBList(cList));	
			return retVal;
		}
		
		BasicDBList coordinates  = (BasicDBList) loc.get("coordinates");
		
		if ((((Double) coordinates.get(0)) == 0 ) || (((Double) coordinates.get(1)) == 0) ) {
			cList = new BasicDBList();
		}
		else {
			// Default : 100 meters
			Double distance = data.has("distance") ? Double.parseDouble(data.getString("distance")) : 100.0;
			BasicDBObject near = new BasicDBObject();
			near.put("$geometry", loc);
			near.put("$maxDistance", distance);			
			BasicDBObject GeoLocation = new BasicDBObject();
			GeoLocation.put("$near", near);
			
			query = new BasicDBObject();
			query.put("partytype", "PARTNER");
			query.put("GeoLocation", GeoLocation);
						
			// Default: 120 minutes
			Long duration  = data.has("duration") ? Long.parseLong(data.getString("duration")) : 120L;
			Date time_ago = new Date(System.currentTimeMillis() - duration * 60000); // in the last 120 minutes.
			QueryBuilder qm = new QueryBuilder().start()
					.put("GeoTimeStamp").greaterThanEquals(time_ago)
					.put("credId").notEquals( getUserId());
			query.putAll(qm.get());
			int skip = !data.isNull("skip") ? Integer.parseInt(data.getString("skip")) : -1;
			int limit = !data.isNull("limit")? Integer.parseInt(data.getString("limit")) :-1;
			int nRecs = bc.ExecQuery(query,getSortSpec(data), limit, skip);
			log.info("Near Me Debug: nRecs = " + nRecs + ", Query : "+ query.toString());
			cList = bc.GetCompositeRecordList();
		}
		retVal.put("data", StringifyDBList(cList));	
		return retVal;
	}
	
	JSONObject computeMetric(JSONObject data) throws Exception {
		JSONObject retVal = new JSONObject();
		
		RWJBusComp bc = app.GetBusObject("Party").getBusComp("Party");
		BasicDBObject query = new BasicDBObject();
		query.put("credId", getUserId());
		if (bc.UpsertQuery(query) == 0 ) {
			String errorMessage = getMessage("RECORD_NOTFOUND"); 
			log.debug( errorMessage);
			throw new Exception(errorMessage);
		}

		RWAlgorithm a = new RWAlgorithm ( this );
		String metric = data.getString("metric");
		double l = a.compute(bc.currentRecord, metric ) ;
		
		bc.SetFieldValue(data.getString("field"), l);
		log.info("compute Metric:" +  metric + ":" + Double.toString(l));
		bc.SaveRecord(data);
		// Decache(bc.GetFieldValue("_id").toString());

		return retVal;		
	}
	
	
}
