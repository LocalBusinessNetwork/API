package com.rw.API;

import java.util.Date;
import java.util.HashMap;
import java.util.Random;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;
import org.bson.types.ObjectId;
import org.json.JSONObject;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.QueryBuilder;
import com.rw.persistence.RWJApplication;
import com.rw.persistence.RWJBusComp;
import com.rw.persistence.RWObjectMgr;
import com.rw.repository.RWAPI;


	@SuppressWarnings("unused")
	public class AttendeeMgr extends GenericMgr  {
	static final Logger log = Logger.getLogger(AttendeeMgr.class.getName());

	public AttendeeMgr() {
	}

	public AttendeeMgr(String userid) {
		super(userid);
	}
	
	public AttendeeMgr(RWObjectMgr context) throws Exception {
		super(context);
	}
	
	@SuppressWarnings("unchecked")
	@RWAPI
	public JSONObject delete(JSONObject data) throws Exception {
		log.trace( "delete");
		data.put("bo", "Attendee");
		data.put("bc", "Attendee");
		return super.delete(data);
	}
	
	@SuppressWarnings("unchecked")
	@RWAPI
	public JSONObject create(JSONObject data) throws Exception {
		
		String guestType = (data.has("guestType"))?data.getString("guestType"):"MEMBER";
		String OrgId = data.getString("OrgId");		
		String eventId = data.getString("eventId");
		String status = (data.has("status"))?data.getString("status"):"CHECKEDIN";
		data.put("bo", "Attendee");
		data.put("bc", "Attendee");
		data.put("status", status);
		
		RWJBusComp eventBC = getDocument("Event", "Event", eventId);
		
		if (eventBC != null){
			Date meetingDate = new Date(eventBC.GetFieldValue("datetime").toString());
			//Date meetingDate = new Date(eventBC.GetFieldValue("datetime").toString());
			data.put("denorm_datetime", meetingDate);
		}
		
		
		JSONObject retVal = null;
		
		if (guestType.equals("FLOATER")){
			String fullName = data.getString("fullName");
			data.put("fullName_denorm", fullName);
		}
		
		if (status.equals("CHECKEDIN") && data.has("partyId")){
			String pId = data.getString("partyId");
			JSONObject data_in = new JSONObject();
			data_in.put("partyId", pId);
			updateStatExp("totalMtgsAttended", "totalMtgsAttended", "Party",  pId,data_in);
		}

			
			if (guestType.equals("NEW_MEMBER")){
				
				
				JSONObject invitationData = new JSONObject();
				String newContactFirst = data.getString("newContactFirst");
				String newContactLast = data.getString("newContactLast");
				String newContactEmail = data.getString("newContactEmail");
				
				if (data.has("newContactPhone") && !data.getString("newContactPhone").equals("") && data.getString("newContactPhone") != null){
					String newContactPhone = data.getString("newContactPhone");
					invitationData.put("newContactPhone", newContactPhone);
				}
				
				String whoGetsCreditId = data.has("newMemberSrc_partyId")?data.getString("newMemberSrc_partyId"):null;
				
				
				invitationData.put("act", "create");
				invitationData.put("userid", getUserId());
				invitationData.put("newContactFirst", newContactFirst);
				invitationData.put("newContactLast", newContactLast);
				invitationData.put("newContactEmail", newContactEmail);
				invitationData.put("newContactOrgId", OrgId);
				invitationData.put("fromId",whoGetsCreditId);
				invitationData.put("fromId",whoGetsCreditId);
				invitationData.put("referralType","PART_INVITE");
				
				RfrlMgr rm = new RfrlMgr(this);
				invitationData = rm.handleRequest(invitationData);
				
				//String contactId = new JSONObject(new JSONObject(invitationData.getString("data")).getString("toId")).getString("$oid");
				String partyId = new JSONObject(invitationData.getString("data")).getString("toId");
				data.put("partyId", partyId);
				retVal = super.create(data);
				
			} 
			else {
				retVal = super.create(data);
			}
		
		return retVal;
	}
	
	@SuppressWarnings({ "unchecked", "unchecked" })
	@RWAPI
	public JSONObject read(JSONObject data) throws Exception {
		
		String bcName = "Attendee";
		String boName = "Attendee" ;
		
		if ( data.has("bc")) {
			bcName = data.getString("bc");
		}
		
		if ( data.has("bo") ) {
			boName = data.getString("bo");
		}
		
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
			
			if ( !data.isNull("searchText")) {
				Pattern text = Pattern.compile(data.get("searchText").toString(), Pattern.CASE_INSENSITIVE);
				QueryBuilder qm = new QueryBuilder().
						or(new QueryBuilder().start().put("fullName_denorm").regex(text).get());
/*				
						new QueryBuilder().put("jobTitle").regex(text).get(),
						new QueryBuilder().put("tags").regex(text).get(),
						new QueryBuilder().put("business").regex(text).get(),
						new QueryBuilder().put("speakingTopics").regex(text).get());
*/				
				query.putAll(qm.get());
			}

			int skip = !data.isNull("skip") ? Integer.parseInt(data.getString("skip")) : -1;
			int limit = !data.isNull("limit")? Integer.parseInt(data.getString("limit")) :-1;

			// Run the query. 
			int nRecs = bc.ExecQuery(query,getSortSpec(data), limit, skip);

			// Party Object may have composite fields.
			BasicDBList cList = bc.GetCompositeRecordList();
			retVal.put("data", StringifyDBList(cList));
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

	
	
	@SuppressWarnings("unchecked")
	@RWAPI
	public JSONObject getNextEvent(JSONObject data) throws Exception {	
		String bcName = data.getString("bc");
		String boName = data.getString("bo");
		log.trace( "getNextEvent " + bcName);

		JSONObject retVal = new JSONObject() ;
		
		RWJBusComp bc = app.GetBusObject(boName).getBusComp(getShapedBC(data,bcName));
		BasicDBObject query = new BasicDBObject();
		query.putAll(getSearchSpec(data).toMap());

		int limit = 1;

		// Run the query. 
		int nRecs = bc.ExecQuery(query,getSortSpec(data), limit, -1);

		// Party Object may have composite fields.
		BasicDBList cList = bc.GetCompositeRecordList();
		retVal.put("data", StringifyDBList(cList));
		return retVal;
		
	}
	
	@SuppressWarnings("unchecked")
	@RWAPI
	public JSONObject update(JSONObject data) throws Exception {		
	
		JSONObject retVal = super.update(data);
		
		if ( data.has("status") ) {
			RWJBusComp adoc = getDocument("Attendee", "Attendee", data.getString("id"));			
			RWJBusComp pdoc = getDocument("Party", "Party", adoc.get("partyId").toString());
		
			BasicDBObject pLoc = new BasicDBObject();
			pLoc.put("type", "Point");

			BasicDBList coordinates = new BasicDBList();

			if ( data.getString("status").equals("CHECKEDIN")) {
				BasicDBObject loc = (BasicDBObject)getDocument("Event", "Event", adoc.get("eventId").toString()).get("GeoWorkLocation");
				if (loc != null  ) {
					coordinates.add(0, loc.get("Longitude"));
					coordinates.add(1, loc.get("Latitude"));
				}
				else {
					coordinates.add(0, 0L);
					coordinates.add(1, 0L);
				}
				
			}
			else {
				coordinates.add(0, 0L);
				coordinates.add(1, 0L);
			}

			pLoc.put("coordinates", coordinates );				
			pdoc.SetFieldValue("GeoLocation", pLoc);
			pdoc.SetFieldValue("GeoTimeStamp", new Date() );	
			pdoc.SaveRecord();
			
			JSONObject data_in = new JSONObject();
			data_in.put("partyId", adoc.get("partyId").toString());
			updateStatExp("totalMtgsAttended", "totalMtgsAttended", "Party",  adoc.get("partyId").toString(),data_in);
			
			
	
		}
		
		return retVal;
		
	}
	
}
