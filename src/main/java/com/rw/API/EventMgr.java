package com.rw.API;

import java.net.URLEncoder;
import java.text.DateFormat;
import java.text.Format;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Locale;
import java.util.Random;

import org.apache.log4j.Logger;
import org.bson.types.ObjectId;
import org.joda.time.DateTime;
import org.json.JSONException;
import org.json.JSONObject;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.QueryBuilder;
import com.mongodb.util.JSON;
import com.rw.persistence.RWAlgorithm;
import com.rw.persistence.RWChartItemData;
import com.rw.persistence.RWJApplication;
import com.rw.persistence.RWJBusComp;
import com.rw.persistence.RWObjectMgr;
import com.rw.persistence.mongoStore;
import com.rw.repository.RWAPI;



	@SuppressWarnings("unused")
	public class EventMgr extends GenericMgr  {
	static final Logger log = Logger.getLogger(EventMgr.class.getName());
	
	public EventMgr(String userid) {
		super(userid);
	}
	
	public EventMgr(RWObjectMgr context) throws Exception {
		super(context);
	}
	
	public EventMgr() {
		super();
	}
	
	
	@SuppressWarnings("unchecked")
	@RWAPI
	public JSONObject create(JSONObject data) throws Exception {
		
		String OrgId = data.getString("OrgId");		
		data.put("bo", "Event");
		data.put("bc", "Event");
		long EventLong = getLongForDate(data,"datetime");
		data.put("datetime",EventLong);
		data.put("reminderSent","false");
		data.put("meetingNotesSent","false");
		
		if ( !data.has("reminder"))
			data.put("reminder",6);
		
		data = updateDateVals(data,EventLong);
		//Long EventLong = combineDateAndTime(data,"datetime","time");
		//data = updateDateVals(data);

		JSONObject retVal = super.create(data);
		
		JSONObject eventIdObj = new JSONObject(retVal.getString("data")).getJSONObject("_id");
		String eventOrgId = new JSONObject(retVal.getString("data")).getString("OrgId");

		

		long EventTime = getLongForDate(data,"datetime");
		//long EventTime = new JSONObject(retVal.getString("data")).getLong("datetime");
		String EventId = eventIdObj.getString("$oid");
		
		RWJBusComp orgBC = getOrg(OrgId);
		BasicDBObject loc = (BasicDBObject)orgBC.get("GeoWorkLocation");
		RWJBusComp eventBC = getEvent(EventId);
		eventBC.SetFieldValue("GeoWorkLocation", loc);
		eventBC.SaveRecord();
		
		RWJBusComp chapterMemberBC = app.GetBusObject("Party").getBusComp("Party");
		RWJBusComp attendeeBC = app.GetBusObject("Attendee").getBusComp("Attendee");

		BasicDBObject query = new BasicDBObject();
		query.put("OrgId", OrgId);
		query.put("partytype", "PARTNER");
		//query.putAll(qm.get());
	
		
		BasicDBList chapterMembers = null;
		HashMap<String, BasicDBObject> recordIndex = new HashMap();
		int nRecs = chapterMemberBC.ExecQuery(query,null);

		final Random random = new Random();
		if ( nRecs > 0 ) {
			chapterMembers = chapterMemberBC.recordSet;
			for (int i = 0;i < nRecs;i++){
				BasicDBObject dbo = (BasicDBObject)chapterMembers.get(i);
				ObjectId dboId = (ObjectId)dbo.get("_id");
				String memberPartyId = dboId.toString();
				String fullName = dbo.getString("fullName");
				
				String[] statusVals = {"INVITED","CHECKEDIN","CHECKEDIN","CHECKEDIN","CHECKEDIN","CHECKEDIN"};
				String status = "INVITED";
				
				if (data.has("demosetup")){
					
					status = statusVals[random.nextInt(statusVals.length)];
				}

				attendeeBC.NewRecord();
				attendeeBC.SetFieldValue("credId", getUserId());
				attendeeBC.SetFieldValue("status", status);
				attendeeBC.SetFieldValue("OrgId", eventOrgId);
				attendeeBC.SetFieldValue("partyId", memberPartyId);
				attendeeBC.SetFieldValue("eventId", EventId);
				attendeeBC.SetFieldValue("guestType", "MEMBER");
				attendeeBC.SetFieldValue("fullName_denorm", fullName);
				attendeeBC.SetFieldValue("denorm_datetime", new Date(EventLong));
				attendeeBC.SetFieldValue("denorm_datelong", new Date(EventLong).getTime());
				attendeeBC.SaveRecord();
			}
		}
		if (data.has("Speaker1_Id")){
			String Speaker1_Id = data.getString("Speaker1_Id"); 
			updateSpeakerEngageStats(null,Speaker1_Id);
		}
		if (data.has("speakerRating")){updateSpeakerRatingStats(data.getString("Speaker1_Id"));}
		
		

		return retVal;
	}
	
	@SuppressWarnings("unchecked")
	@RWAPI
	public JSONObject read(JSONObject data) throws Exception {
		data.put("bo", "Event");
		data.put("bc", "Event");
		return super.read(data);
		
	}
	
	@SuppressWarnings("unchecked")
	@RWAPI
	public JSONObject readphone(JSONObject data) throws Exception {
		JSONObject retVal = new JSONObject() ;
		
		RWJBusComp bc = app.GetBusObject("Event").getBusComp(getShapedBC(data,"Event"));
		
		BasicDBObject query = new BasicDBObject();

		Object id = data.get("id"); // we always assume there's a record id for this act
        query.put("_id", new ObjectId(id.toString())); 
		int nRecs = bc.ExecQuery(query,null);
		if ( nRecs > 0 ) {
			
			RWJBusComp attendeeBC = getAttendee(id.toString());
			if (attendeeBC.recordSet.size() > 0){
				String attendeeId = attendeeBC.get("_id").toString();
				String status = (String)attendeeBC.get("status");
				String guestType = (String)attendeeBC.get("guestType");
				bc.SetFieldValue("attendeeId", attendeeId);
				bc.SetFieldValue("status", status);
				bc.SetFieldValue("guestType", guestType);
			}
			retVal.put("data", StringifyDBObject(bc.GetCompositeRecord()));
		}
		else {
			String errorMessage = getMessage("RECORD_NOTFOUND"); 
			log.debug( errorMessage);
			throw new Exception(errorMessage);
		}

		
		return retVal;
		
	}
	
	@SuppressWarnings("unchecked")
	@RWAPI
	public JSONObject selfcheckin(JSONObject data) throws Exception {
		JSONObject retVal = new JSONObject() ;
		
		RWJBusComp bc = app.GetBusObject("Event").getBusComp(getShapedBC(data,"Event"));
		
		BasicDBObject query = new BasicDBObject();

		Object eventId = data.get("id"); // we always assume there's a record id for this act
		
        query.put("_id", new ObjectId(eventId.toString())); 
		int nRecs = bc.ExecQuery(query,null);
        String partyId = GetPartyId();
    	BasicDBList coordinates = new BasicDBList();
	
		RWJBusComp attendeeBC = getAttendee(eventId.toString());
		RWJBusComp party = getDocument("Party", "Party",partyId);
		
		BasicDBObject pLoc = new BasicDBObject();

		if (attendeeBC.recordSet.size() > 0){
			attendeeBC.SetFieldValue("status", "CHECKEDIN");
			
			String attendeeId = attendeeBC.get("_id").toString();
			String status = (String)attendeeBC.get("status");
			attendeeBC.SaveRecord();
			bc.SetFieldValue("attendeeId", attendeeId);
			bc.SetFieldValue("status", status);
			bc.SetFieldValue("guestType", "FLOATER");
		} else {
			/*
			AttendeeMgr am = new AttendeeMgr();
			JSONObject d = new JSONObject();
			d.put("act", "create");
			d.put("eventId", eventId);
			//d.put("OrgId", GetPartyAttr(partyId, "OrgId"));
			d.put("guestType","FLOATER");
			d.put("status","CHECKEDIN");
			d.put("OrgId", data.getString("OrgId"));
			am.handleRequest(d);
			*/
			String fullName = (String)party.get("fullName");
			attendeeBC.NewRecord();
			attendeeBC.SetFieldValue("OrgId", data.getString("OrgId"));
			attendeeBC.SetFieldValue("partyId", partyId);
			attendeeBC.SetFieldValue("fullName_denorm", fullName);
			attendeeBC.SetFieldValue("eventId", eventId);
			attendeeBC.SetFieldValue("guestType", "FLOATER");
			attendeeBC.SetFieldValue("status", "CHECKEDIN");
			attendeeBC.SetFieldValue("denorm_datetime", bc.GetFieldValue("datetime"));
			attendeeBC.SaveRecord();
			bc.SetFieldValue("status", "CHECKEDIN");
		}
		
		BasicDBObject loc = (BasicDBObject) bc.get("GeoWorkLocation");
		if (loc != null  ) {
			coordinates.add(0, loc.get("Longitude"));
			coordinates.add(1, loc.get("Latitude"));
		}
		else {
			coordinates.add(0, 0L);
			coordinates.add(1, 0L);
		}
		
		pLoc.put("type", "Point");
		pLoc.put("coordinates", coordinates );				
		
		party.SetFieldValue("GeoLocation", pLoc);
		party.SetFieldValue("GeoTimeStamp", new Date() );	
		party.SaveRecord();				
		
		retVal.put("data", StringifyDBObject(bc.GetCompositeRecord()));

		return retVal;
		
	}
	
	@SuppressWarnings("unchecked")
	@RWAPI
	public JSONObject update(JSONObject data) throws Exception {
		if (data.has("selfcheckin")){
			data.remove("selfcheckin");
			return selfcheckin(data);
		} else {
			data.put("bo", "Event");
			data.put("bc", "Event");
			
			JSONObject c = read(data);
			DBObject currentRec = (DBObject) JSON.parse(c.getString("data"));
			
			String eventId = data.getString("id");
			long EventLong = getLongForDate(data,"datetime");
			data.put("datetime",EventLong);
			data = updateDateVals(data,EventLong);
			updateAttendeeDenormDate(eventId,EventLong);
			JSONObject retVal = super.update(data);
			
			String updatedSpeaker1_Id = (data.has("Speaker1_Id"))?data.getString("Speaker1_Id"):null;
			String originalSpeaker1_Id = (currentRec.containsField("Speaker1_Id"))?(String)currentRec.get("Speaker1_Id"):null;
			if (!updatedSpeaker1_Id.equals(originalSpeaker1_Id)){
				updateSpeakerEngageStats(originalSpeaker1_Id,updatedSpeaker1_Id);
			}
			if (data.has("speakerRating")){updateSpeakerRatingStats(updatedSpeaker1_Id);}
			return retVal;
		}
		
	}

	
	@SuppressWarnings("unchecked")
	@RWAPI
	public JSONObject delete(JSONObject data) throws Exception {		
		
		RWJBusComp EventBc = getDocument("Event", "Event",data.getString("id"));
		
		String credId = EventBc.GetFieldValue("credId").toString();
		
		// can be deleted by the creator/owner or the administrator
		/*
		if ( !credId.equals(getUserId()) ||  !hasRole("ADMINISTRATOR")) {
			String errorMessage = getMessage("UNAUTHORIZED_ACCESS"); 
			log.debug( errorMessage);
			throw new Exception(errorMessage);							
		}
		*/
		

		RWJBusComp attendeeBC = app.GetBusObject("Attendee").getBusComp("Attendee");
		BasicDBObject query = new BasicDBObject();
		query.put("eventId", data.getString("id"));
		query.put("status", "CHECKEDIN");
		int nRecs = attendeeBC.ExecQuery(query);
		if ( nRecs > 0 )
		{
			String errorMessage = getMessage("ATTENDANT_EXISTS"); 
			log.debug( errorMessage);
			throw new Exception(errorMessage);
		}
		
		query.remove("status");
		nRecs = attendeeBC.UpsertQuery(query);

		for ( int i = 0 ; i < nRecs; i++) {
			attendeeBC.DeleteRecord();
			attendeeBC.NextRecord();
		}
		
		JSONObject retVal = new JSONObject();
		retVal.put("data", StringifyDBObject(EventBc.GetCompositeRecord()));
		EventBc.DeleteRecord();
		return retVal;
		
	}
	
	private JSONObject updateDateVals(JSONObject data, Long EventLong) throws JSONException{
		//Long EventLong = combineDateAndTime(data,"datetime","time");
		final Calendar c = Calendar.getInstance();
		Format formatter = new SimpleDateFormat("MMMMM"); 
	    String monthName = formatter.format(new Date(EventLong));
	    
	    formatter = new SimpleDateFormat("EEEE");
	    String day = formatter.format(new Date(EventLong));
	    
        c.setTime(new Date(EventLong));
		 
		data.put("datelong", EventLong);
		data.put("datetime", new Date(EventLong));
		data.put("time", new Date(EventLong));
		
		data.put("monthName", monthName);
		data.put("day", day);
		data.put("date", c.get(Calendar.DATE));
		return data;
	}
	
	private void updateAttendeeDenormDate(String eventId, Long EventLong) throws Exception{

		BasicDBObject find = new BasicDBObject();
    	find.put("eventId",eventId);

    	BasicDBObject update = new BasicDBObject();    	
    	BasicDBObject rwc =  new BasicDBObject();
    	rwc.put("denorm_datetime", new Date(EventLong));
    	rwc.put("denorm_datelong", new Date(EventLong).getTime());
    	update.put("$set",rwc);
    	
    	//update.put("rw_created_on", past);
    	mongoStore s = new mongoStore(getTenantKey());
    	s.getColl("rwAttendee").updateMulti(find, update);
		
	}
	
	
	
	public void updateSpeakerStat(String speakerId, String totalField, String chartName) throws Exception {
		AnalyticsMgr a = new AnalyticsMgr(this);	
		
		JSONObject data_in = new JSONObject();		
		
		data_in.put("bo", "Party");
		data_in.put("bc", "Party");
		data_in.put("FK", speakerId);
		data_in.put("summarizer", totalField); //"totalSpeakerEngagements"
		data_in.put("chartName", chartName);//"SpeakerEngagement"
		data_in.put("Speaker1_Id", speakerId);
		a.summarize(data_in);
	}
	
	public void updateSpeakerEngageStats(String originalSpeakerId, String updatedSpeakerId) throws Exception{
		if (originalSpeakerId != null){
			updateSpeakerStat(originalSpeakerId,"totalSpeakerEngagements","SpeakerEngagement");
			//do another call to update speaker rating
		}
		updateSpeakerStat(updatedSpeakerId,"totalSpeakerEngagements","SpeakerEngagement");
	}
	private void updateSpeakerRatingStats(String speakerId) throws Exception{
		updateSpeakerStat(speakerId,"speakerRating","SpeakerRatingAvg");
	}
	
	private RWJBusComp getAttendee(String eventId) throws Exception{
		RWJBusComp bc = app.GetBusObject("Attendee").getBusComp("Attendee");
		BasicDBObject query = new BasicDBObject();
		query.put("eventId", eventId);
		query.put("partyId",GetPartyId());
		bc.UpsertQuery(query);
		return bc;
	}
	private RWJBusComp getOrg(String OrgId) throws Exception{
		RWJBusComp bc = app.GetBusObject("Organization").getBusComp("Organization");
		BasicDBObject query = new BasicDBObject();
		query.put("_id", new ObjectId(OrgId));
		bc.ExecQuery(query);
		return bc;
	}
	private RWJBusComp getEvent(String eventId) throws Exception{
		RWJBusComp bc = app.GetBusObject("Event").getBusComp("Event");
		BasicDBObject query = new BasicDBObject();
		query.put("_id", new ObjectId(eventId));
		bc.UpsertQuery(query);
		return bc;
	}
	
	public void cron() {
		sendMeetingReminders();
		// sendMeetingNotes();
	}
	
	public void sendMeetingReminders() {		
		try {
			
			RWJBusComp events = app.GetBusObject("Event").getBusComp("Event");
			BasicDBObject query = new BasicDBObject();
			
			long t1 = System.currentTimeMillis();
			long t2 = t1 + 86400000; // + 24hrs.
			
			Date tStart = new Date(t1);
			Date tEnd = new Date(t2);

			query.putAll(QueryBuilder.start().put("datetime").greaterThanEquals(tStart).lessThanEquals(tEnd).get());
			query.putAll(QueryBuilder.start().put("reminderSent").is("false").get());

			//scan the events scheduled for the next 24 hrs.
			int nEvents = events.UpsertQuery(query);
			if ( nEvents == 0)	return;

			Mail m = new Mail(this);
			JSONObject tenantObj = getTenant(null);


			for ( int i = 0; i < nEvents ; i++ ) {
				try {					
					
					// This is for the backward compatibility mode.
					// We should update the data with a default of 6hrs, before pushing this feature.
					if ( !events.has("reminder")) continue; /* TODO : REMOVE THIS LINE */

					Date mtgTime = (Date) events.GetFieldValue("datetime");
					Long reminder = Long.parseLong(events.GetFieldValue("reminder").toString());
					
					// no reminders sent if the reminder time was set to zero.
					if ( reminder <= 0L ) continue;
					
					// max reminder are 24hrs or less.
					if ( reminder > 24L ) reminder = 24L;
					
					long h = mtgTime.getTime();
					long r = reminder * 3600000;
					h = h - r;
					
					// Look for the reminders that fall into next 60 minutes
					if ( h < (t1 + 3600000) ) {
						// Send reminder email
						JSONObject emailData = new JSONObject();
						
						BasicDBObject cRecEvent = events.GetCompositeRecord();
						
						String eventId = cRecEvent.get("_id").toString();
						
						emailData.put("title", cRecEvent.get("title").toString());
						Date d = (Date) cRecEvent.get("datetime");
						SimpleDateFormat df = new SimpleDateFormat();
						
						emailData.put("datetime", df.getDateTimeInstance().format(d));
						emailData.put("locationName", cRecEvent.get("locationName").toString());
						emailData.put("businessName", cRecEvent.get("businessName").toString());
						emailData.put("streetAddress1_work", cRecEvent.get("streetAddress1_work").toString());
						emailData.put("cityAddress_work", cRecEvent.get("cityAddress_work").toString());
						emailData.put("stateAddress_work", cRecEvent.get("stateAddress_work").toString());
						emailData.put("postalCodeAddress_work", cRecEvent.get("postalCodeAddress_work").toString());
							
						BasicDBObject q2 = new BasicDBObject();
						q2.put("eventId", eventId);
						
						RWJBusComp attendees = app.GetBusObject("Attendee").getBusComp("Attendee");
						int nAttendees = attendees.ExecQuery(q2);
						
						for ( int j = 0; j < nAttendees ; j++ ) {
							BasicDBObject cRecAttendee = attendees.GetCompositeRecord();
							
							String emailAddress = cRecAttendee.get("emailAddress").toString();
							emailData.put("firstName", cRecAttendee.get("firstName").toString());
							emailData.put("lastName", cRecAttendee.get("lastName").toString());
											
							JSONObject header = m.header( tenantObj.getString("SupportEmailAddress"), 
									cRecEvent.get("businessName").toString(), 
									tenantObj.getString("SupportEmailAddress"), tenantObj.getString("pageTitle"),
									"Meeting reminder from " + cRecEvent.get("businessName").toString(),
									emailAddress, null);
							
							m.SendHtmlMailMessage(header, getMessage("ET_MEETINGREMINDER"), emailData, null); 
							log.debug("Sent meeting remainder to " + emailAddress);
							attendees.NextRecord();
						}
						
						events.SetFieldValue("reminderSent", "true");
						events.SaveRecord();
					}						
				}
				
				catch(Exception e) {
					log.debug(e.getStackTrace());
				}
				events.NextRecord();
			}
		}
		catch( Exception e) {
			log.debug(e.getStackTrace());
		}
	}


	public void sendMeetingNotes() {		
		try {
			
			RWJBusComp events = app.GetBusObject("Event").getBusComp("Event");
			BasicDBObject query = new BasicDBObject();
			
			long t1 = System.currentTimeMillis();
			long t2 = t1 + 7200000; // + 2hrs.
			
			Date tStart = new Date(t1);
			Date tEnd = new Date(t2);
			
			query.putAll(QueryBuilder.start().put("datetime").greaterThanEquals(tStart).lessThanEquals(tEnd).get());
			query.putAll(QueryBuilder.start().put("meetingNotesSent").is("false").get());
			
			//scan the events starting in the next 2 hrs, where we not sent the meeting Notes
			int nEvents = events.UpsertQuery(query);
			if ( nEvents == 0)	return;
			
			Mail m = new Mail(this);
			JSONObject tenantObj = getTenant(null);

			for ( int i = 0; i < nEvents ; i++ ) {
				try {					
					JSONObject emailData = new JSONObject();
					BasicDBObject cRecEvent = events.GetCompositeRecord();
					String eventId = cRecEvent.get("_id").toString();
					
					emailData.put("title", cRecEvent.get("title").toString());
					Date d = (Date) cRecEvent.get("datetime");
					SimpleDateFormat df = new SimpleDateFormat();
					emailData.put("datetime", df.getDateTimeInstance().format(d));
					
					emailData.put("locationName", cRecEvent.get("locationName").toString());
					emailData.put("businessName", cRecEvent.get("businessName").toString());
					emailData.put("streetAddress1_work", cRecEvent.get("streetAddress1_work").toString());
					emailData.put("cityAddress_work", cRecEvent.get("cityAddress_work").toString());
					emailData.put("stateAddress_work", cRecEvent.get("stateAddress_work").toString());
					emailData.put("postalCodeAddress_work", cRecEvent.get("postalCodeAddress_work").toString());
					emailData.put("totalInvitations", 7);
					emailData.put("totalGuests", 3);
					emailData.put("totalReferrals", 2);
					emailData.put("totalNewMembers", 2);

						
					BasicDBObject q2 = new BasicDBObject();
					q2.put("eventId", eventId);
						
					RWJBusComp attendees = app.GetBusObject("Attendee").getBusComp("Attendee");
					int nAttendees = attendees.ExecQuery(q2);
					
					for ( int j = 0; j < nAttendees ; j++ ) {
						
						BasicDBObject cRecAttendee = attendees.GetCompositeRecord();
						
						String emailAddress = "peter.thorson@referralwire.com" ; // cRecAttendee.get("emailAddress").toString();
						emailData.put("firstName", cRecAttendee.get("firstName").toString());
						emailData.put("lastName", cRecAttendee.get("lastName").toString());
							
						JSONObject header = m.header( tenantObj.getString("SupportEmailAddress"), 
								cRecEvent.get("businessName").toString(), 
								tenantObj.getString("SupportEmailAddress"), tenantObj.getString("pageTitle"),
								"News from " + cRecEvent.get("businessName").toString(),
								emailAddress, null);
						
						m.SendHtmlMailMessage(header,getMessage("ET_MEETINGNOTES"), emailData, null); 
						attendees.NextRecord();
					}
					
					events.SetFieldValue("meetingNotesSent", "true");
					events.SaveRecord();
				}										
				catch(Exception e) {
					log.debug(e.getStackTrace());
				}
				events.NextRecord();
			}
		}
		catch( Exception e) {
			log.debug(e.getStackTrace());
		}
	}

}
