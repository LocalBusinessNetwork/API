package com.rw.API;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Locale;
import java.util.ResourceBundle;
import java.util.Set;
import java.util.UUID;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.log4j.Logger;
import org.bson.types.ObjectId;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import redis.clients.jedis.Jedis;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBRef;
import com.mongodb.QueryBuilder;
import com.rw.Enricher.Agency;
import com.rw.Enricher.EnrichmentFactory;
import com.rw.persistence.JedisMT;
import com.rw.persistence.RWAlgorithm;
import com.rw.persistence.RWChartItemData;
import com.rw.persistence.RWJApplication;
import com.rw.persistence.RWJBusComp;
import com.rw.persistence.RWJBusObj;
import com.rw.persistence.RWObjectMgr;
import com.rw.persistence.mongoStore;
import com.rw.repository.RWAPI;


public class UserMgr extends RWReqHandle  {
	static final Logger log = Logger.getLogger(UserMgr.class.getName());

	public UserMgr() {
	}

	public UserMgr(String userid) {
		super(userid);
	}

	public UserMgr(RWObjectMgr context) throws Exception {
		super(context);
	}

	@SuppressWarnings("unchecked")
	@RWAPI
	public JSONObject create(JSONObject data) throws Exception {
		log.trace( "Create Partner");
		
		RWJBusComp bc = app.GetBusObject("Party").getBusComp("Party");
		
		BasicDBObject query = new BasicDBObject();

		// emailAddress is a must.
		String emailAddress = data.getString("emailAddress");
		
		query.put("emailAddress", emailAddress);

		// See if someone with this email address already exists in the system
		// This has to be better than just email Look up. This should Contact Deduper to see if there is
		// a match.
		
		if (bc.UpsertQuery(query) == 0 ) {
			bc.NewRecord();
		}
		else {
			// TODO: clean up
			
			// Really,? is it freaky to see your address, etc, already populated.
			// Why can't ReferralWire enrich your data as you register ?
			
		}
		
		// reset the creator. From here on this record will be owned by this user.
		super.create(data);

		// Associate the login creds
		bc.SetFieldValue("credId", getUserId());
		
		bc.SetFieldValue("partytype", "PARTNER");
		bc.SetFieldValue("billing_day", 1);
		bc.SetFieldValue("rfrl_txn_fees", 0.75);
		bc.SetFieldValue("joinedFullDate", new Date());
		bc.SetFieldValue("joinedDate", Calendar.getInstance().DATE);
		bc.SetFieldValue("joinedMonth", new SimpleDateFormat("MMM").format(Calendar.getInstance().getTime()));
		bc.SetFieldValue("joinedYear", new SimpleDateFormat("yyyy").format(Calendar.getInstance().getTime()));

		String firstName = data.getString("firstName");
		String lastName = data.getString("lastName");

		data.put("firstName", getProperName(firstName));
		data.put("lastName", getProperName(lastName));

		String fullName = firstName.concat(" ").concat(lastName);
		//fullName = fullName.toUpperCase();
		bc.SetFieldValue("fullName", fullName);
		
		bc.SetFieldValue("MLID", 0L);
		JSONArray FaceBookFriends = null;
		JSONArray LinkedInConnections = null;


		UpdateSocialIds(data);

		
		if ( data.has("FaceBookFriends")) {
			FaceBookFriends = data.getJSONArray("FaceBookFriends");
			data.remove("FaceBookFriends");
		}
		
		if ( data.has("LinkedInConnections")) {
			LinkedInConnections = data.getJSONArray("LinkedInConnections");
			data.remove("LinkedInConnections");
		}
		
		if ( data.has("GeoLocation")) {
			data.remove("GeoLocation");
		}
		
		
		BasicDBList coordinates = new BasicDBList();
		coordinates.add(0, 0L);
		coordinates.add(1, 0L);
		
		BasicDBObject loc = new BasicDBObject();
		
		loc.put("type", "Point");
		loc.put("coordinates", coordinates );
		
		bc.SetFieldValue("GeoLocation", loc);
		bc.SetFieldValue("welcomePage", "SHOW");
		
		bc.SaveRecord(data);

		SetupOpenInvitaton(bc);

		// We need this info for fb/ln triangulation emails, which will be done as part of enrichment
		// Triangulation is an Enrichment Agency
		
		// post a triagulation request for this user
		// TODO: jedisMt.publish(JedisMT.PARTY_TRIANGULATE_CHANNEL, getUserId());
		
		
		
		String partyId = bc.GetFieldValue("_id").toString();
		if (data.has("profession")){
			String profCategory = data.getString("profession");
			if (profCategory != null){
				associateDefaultTargetValues(profCategory, getUserId(),partyId);
			}
		}

		// PT: this may be a poor man's triangulation.  It just looks for people who have the same
		// contact in their address book and updates partytype_denorm to PARTNER.  This allows us to 
		// show users to search for other people they know who  are already STN members.  It's not part of
		// EnrichParty because it doesn't have to be done every time the user updates his/her profile 

		//JSONObject  jsonPartyId = new JSONObject();
		//pushToBackground("updateOtherUsersAddressBooks", data_metric);  // at some point, may want to push to back ground
		updateOtherUsersAddressBooks(partyId);
		
		// kick off Enrichment for this new party
		// Several things happen in the back ground - FullContact, Google, Triagulation, etc.		
		EnrichParty(partyId);
		
		// compute stats off line
		if ( bc.currentRecord.containsField("invitedBy_Id")) {
			String invitedBy_Id = bc.GetFieldValue("invitedBy_Id").toString();
			if (!invitedBy_Id.isEmpty())
				updateStat("totalInvited", "totalInvited", "Party",  invitedBy_Id);

			JSONObject tenantObj = getTenant(null);
			
			BasicDBObject invitedByParty = GetParty(invitedBy_Id);
			
			JSONObject emailData = new JSONObject();
			
			emailData.put("invitedBy_firstName", invitedByParty.getString("firstName"));
			emailData.put("invitedBy_lastName", invitedByParty.getString("lastName"));
			emailData.put("newMember_firstName", bc.get("firstName").toString());
			emailData.put("newMember_lastName", bc.get("lastName").toString());
			
			Mail m = new Mail(this);

			JSONObject header2 = m.header( tenantObj.getString("SupportEmailAddress"), 
					tenantObj.getString("pageTitle"), tenantObj.getString("SupportEmailAddress"), tenantObj.getString("pageTitle"),
					getMessage("STNCONNECT_CONFIRM"),invitedByParty.getString("emailAddress"), null);
			m.SendHtmlMailMessage(header2, getMessage("ET_REGCONFIRM"), emailData, null); 
			
		}
		
		JSONObject data_metric = new JSONObject();
		data_metric.put("metric", "memberProfileScore");
		data_metric.put("field", "memberProfileScore");
		pushToBackground("computeMetric", data_metric);
		
		data_metric.put("metric", "speakerProfileScore");
		data_metric.put("field", "speakerProfileScore");
		pushToBackground("computeMetric", data_metric);
		
		
		JSONObject retVal = new JSONObject();
		retVal.put("data",StringifyDBObject(bc.GetCompositeRecord()));		
		
		return retVal;

	}
	
	@SuppressWarnings("unchecked")
	@RWAPI
	public JSONObject read(JSONObject data) throws Exception {
		log.trace( "read partner");

		String partyId = null ;

		if (!data.isNull("id")) 
			partyId = data.get("id").toString();
		else
			partyId = GetPartyId();
		
		String strParty  = null ; // Cache(partyId);	

		if ( strParty == null ) {
			// This has be rare exception. Otherwise, all party objects stay in the cache
			RWJBusComp bc = app.GetBusObject("Party").getBusComp(getShapedBC(data,"Party"));
			BasicDBObject query = new BasicDBObject();
			
	        query.put("_id", new ObjectId(partyId));
			//query.put("partytype", "PARTNER");
			int nRecs = bc.ExecQuery(query,null);
			if ( nRecs > 0 ) {
				strParty = StringifyDBObject(bc.GetCompositeRecord());
			}
			else {
				String errorMessage = getMessage("RECORD_NOTFOUND"); 
				log.debug( errorMessage);
				throw new Exception(errorMessage);
			}
		}
		
		JSONObject retVal = new JSONObject() ;
		retVal.put("data",strParty);
		return retVal;
		
	}
	
	@RWAPI
	public JSONObject update(JSONObject data) throws Exception  {
		log.trace( "update");


		super.update(data);
		Object id = data.get("id");

		JSONObject retVal = new JSONObject() ;
		RWJBusComp bc = app.GetBusObject("Party").getBusComp("Party");
		
		BasicDBObject query = new BasicDBObject();
		query.put("credId", getUserId());		
        query.put("_id", new ObjectId(id.toString())); 
        
	
		if (bc.UpsertQuery(query) == 0 ) {
			String errorMessage = getMessage("UNAUTHORIZED_ACCESS"); 
			log.debug( errorMessage);
			throw new Exception(errorMessage);
		}
		String fullName = "";
		if ( !data.isNull("firstName") || !data.isNull("lastName") ) { 
			
			fullName =  (!data.isNull("firstName"))?data.getString("firstName").concat(" "):"";
			fullName = (!data.isNull("lastName"))?fullName.concat(data.getString("lastName")):fullName;
			//fullName = fullName.toUpperCase();
			//bc.SetFieldValue("fullName", fullName);
			data.put("fullName", fullName);
		}
		
		if (data.has("joinedFullDate[$date]")){
			Date joinedFullDate = new Date(data.getLong("joinedFullDate[$date]"));
			data.put("joinedFullDate", joinedFullDate);
			//bc.SetFieldValue("joinedFullDate", joinedFullDate);
		}
		
		String currentProfCategory = bc.currentRecord.getString("profession");
		String currentOrgId = bc.currentRecord.getString("OrgId");
		
		if (data.has("joinChapter")){
			data.put("oldOrgId",currentOrgId);
			addUserToMeeting(data);
			//pushToBackground("addUserToMeeting", data);
		}
		
		String partyId = bc.GetFieldValue("_id").toString();
		
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
	    
		String FaceBookId = (String)bc.GetFieldValue("FaceBookId");
		if (FaceBookId != null && !FaceBookId.isEmpty()) {
			jedisMt.sadd("FACEBOOK_MEMBERS",FaceBookId );
		}

		String LNProfileId = (String)bc.GetFieldValue("LNProfileId");
		if (LNProfileId != null && !LNProfileId.isEmpty() ) {
			jedisMt.sadd("LINKEDIN_MEMBERS",LNProfileId );
		}
		
		if (data.has("profession")){
			String newProfCategory = data.getString("profession");
			if (newProfCategory != null && newProfCategory.equals(currentProfCategory) == false){
				associateDefaultTargetValues(newProfCategory, getUserId(),partyId);
			}
		}
		
		if (data.has("OrgId")){
			String newOrgId = data.getString("OrgId");			
			if (!newOrgId.equals(currentOrgId)){ UpdateInvitedByOrgId(id.toString(),newOrgId); }			
		}
		
		// kick off Enrichment for this updated party
		EnrichParty(partyId);

		// Update metrics
		JSONObject data_metric = new JSONObject();
		data_metric.put("metric", "memberProfileScore");
		data_metric.put("field", "memberProfileScore");
		pushToBackground("computeMetric", data_metric);
		
		data_metric.put("metric", "speakerProfileScore");
		data_metric.put("field", "speakerProfileScore");
		pushToBackground("computeMetric", data_metric);
		
		retVal.put("data",StringifyDBObject(bc.GetCompositeRecord()));
		return retVal;

	}
	
	@RWAPI
	public JSONObject setupPaypal(JSONObject data) throws Exception {
		log.trace( "setupPaypal");
	
		JSONObject retVal = new JSONObject();
	
		String billing_day = data.getString("billing_day");
		String paypalId = data.getString("paypalId");
		
		BasicDBObject query = new BasicDBObject();
		query.put("credId", getUserId());		

		RWJBusComp bc = app.GetBusObject("Party").getBusComp("Party");
		
		bc.UpsertQuery(query);

		bc.SetFieldValue("billing_day",billing_day);
		bc.SetFieldValue("paypalId",paypalId);

		bc.SaveRecord();
		return new JSONObject().put("data", StringifyDBObject(query));
			
	}
	public void associateDefaultTargetValues(String profCatDisplayVal,String userId,String partyId) throws Exception{
		
		JSONArray dataList = getDefaultTargetCriteria(profCatDisplayVal);

			if (dataList != null && dataList.length() > 0){
				
			CriteriaMgr cm = new CriteriaMgr(this);
			JSONObject data = new JSONObject();
			data.put("act", "associate");
			data.put("userId", userId);
			data.put("partyId", partyId);
			data.put("data", dataList);
			data.put("partyRelation", "TARGET");
			
			try {
				data = cm.handleRequest(data);
			} catch (Exception e) {
				log.debug("DemoSetup Error: ", e);
			}
		}
	}
	
	@RWAPI
	public JSONObject MarkGeoLocation(JSONObject data) throws Exception {

		log.trace( "MarkGeoLocation");

		JSONObject retVal = new JSONObject() ;
		RWJBusComp bc = app.GetBusObject("Party").getBusComp("Party");
		
		BasicDBObject query = new BasicDBObject();
		query.put("credId", getUserId());
	
		if (bc.UpsertQuery(query) == 0 ) {
			String errorMessage = getMessage("RECORD_NOTFOUND"); 
			log.debug( errorMessage);
			throw new Exception(errorMessage);
		}

		BasicDBList coordinates = new BasicDBList();
		coordinates.add(0, Double.parseDouble(data.get("longitude").toString()));
		coordinates.add(1, Double.parseDouble(data.get("latitude").toString()));
		log.info(getUserId() + ": Mark Geolocation " + coordinates.toString() + " from device : " + data.getString("userAgent"));
		
		BasicDBObject loc = new BasicDBObject();
		
		loc.put("type", "Point");
		loc.put("coordinates", coordinates );
		bc.SetFieldValue("GeoLocation",loc );
		bc.SaveRecord(data);
		retVal.put("data", StringifyDBObject(bc.GetCompositeRecord()));
		
		JSONObject trackingData = new JSONObject();
		trackingData.put("longitude",data.get("longitude"));
		trackingData.put("latitude",data.get("latitude"));
		trackingData.put("device", data.getString("userAgent"));
		trackingData.put("event", "UM_LOCATION");         			
		trackMetrics(trackingData);
	
		return retVal;

	}

	public void UpdateSocialIds(JSONObject data) throws Exception {
		
		log.trace( "UpdateSocialIds");
		
		if ( inUIMode() ) { 
			// This job can be done in the background. so push it to the background
			pushToBackground("UpdateSocialIds", data );
			return; 
		}
		
		// grab social network data of this user for future mining
		RWJBusComp bc2 = app.GetBusObject("SocialNetworkData").getBusComp("SocialNetworkData");
		
		BasicDBObject query = new BasicDBObject();
		query.put("credId", getUserId());
		
		
		if ( bc2.UpsertQuery(query) == 0 ) {
			bc2.NewRecord();
			bc2.SetFieldValue("credId", getUserId());
		}
		
		String FaceBookId = data.has("FaceBookId") ? data.getString("FaceBookId") : null;
		if ( FaceBookId != null && !FaceBookId.isEmpty() ) {
			bc2.SetFieldValue("FaceBookId", FaceBookId);
			jedisMt.sadd("FACEBOOK_MEMBERS",FaceBookId );
			JSONArray facebookfriends = data.has("facebookfriends") ? data.getJSONArray("facebookfriends") : null;
			if ( facebookfriends != null ) {
				BasicDBList flist = new BasicDBList();
				for (int i = 0; i < facebookfriends.length(); i++) {
					flist.put(i, facebookfriends.getJSONObject(i).getString("id"));
				}
				bc2.SetFieldValue("FaceBookFriends", flist);
			}
		}
		
		String LNProfileId = data.has("LNProfileId") ? data.getString("LNProfileId") : null;
		
		if ( LNProfileId != null && !LNProfileId.isEmpty() ) {
			bc2.SetFieldValue("LNProfileId", LNProfileId);
			jedisMt.sadd("LINKEDIN_MEMBERS",LNProfileId );
			JSONArray linkedinconnectons = data.has("linkedinconnectons") ? data.getJSONArray("linkedinconnectons") : null;

			if ( linkedinconnectons != null ) {
				BasicDBList flist = new BasicDBList();
				for (int i = 0; i < linkedinconnectons.length(); i++) {
					flist.put(i, linkedinconnectons.getJSONObject(i).getString("id"));
				}
				bc2.SetFieldValue("LinkedInConnections", flist);
			}
		}
		
		bc2.SaveRecord();
		
		// Kick off triangulation
		jedisMt.publish(JedisMT.PARTY_TRIANGULATE_CHANNEL, getUserId());

	}
	
	private void UpdateInvitedByOrgId(String memberId, String newOrgId) throws Exception{
		
		
    	mongoStore s = new mongoStore(getTenantKey());
    	//db.rwReferral.update( {subject: "past" }, { $set: { rw_created_on:new ISODate("2012-01-10") } }, { multi: true } )
		BasicDBObject find = new BasicDBObject();
    	find.put("invitedBy_Id", memberId);
		//find.put("invitedBy_OrgId","past");

    	BasicDBObject update = new BasicDBObject();
    	
    	BasicDBObject rwc =  new BasicDBObject();
    	rwc.put("invitedBy_OrgId", newOrgId);
    	update.put("$set",rwc);
    	
    	//update.put("rw_created_on", past);
    	s.getColl("rwParty").updateMulti(find, update);
		
	}
	/*
		Story for Triangulation:

		Phil is an STN member
		Laura is an STN member

		Raja is phil's facebook contact
		Raja is Laura's Facebook or linkedin contact

		Phil sends an invitation to Raja to join STN thru his Invite Facebook Friends page.

		Sub Story 1: Raja accepts invitation and joins STN. Phil gets credit for inviting Raja.
		 Phils gets notified that Raja has joined STN. System notifies Raja that 15 of his Facebook 
		 friends are already STN members and 20 of his linked in contacts are already STN members. 

		Sub Story 2 : System recognizes Laura is Raja's facebook contact. System sends an email to 
		Laura informing her that Phil invited Raja to STN and Raja has just joined STN. System suggests 
		Laura to invite her Facebook friends like Raja to STN.

		Sub Story 3 : System did not find Laura in Raja's Facebook friends list. Then system locates 
		Laura in Raja's LinkedIn contacts. System sends an email to Laura informing her that Phil invited Raja 
		to STN and Raja has just joined STN. System Suggests Laura to invite her LinkedIn Contacts.

		Sub Story 4 : System does not find Laura in Raja's Facebook or LinkedIn contacts. System locates 
		Raja in Laura's Contact List. System sends an email to Laura informing her that Phil invited Raja to 
		STN and Raja has just joined STN. System Suggests Laura to invite her Contacts.

		Sub Story 5: System limits a maximum of 5 invitation suggests to Laura per day.  
	
	*/
	
	public void SocialTriangulation(JSONObject data) throws Exception {
		
		log.trace( "SocialTriangulation");
		
		if ( inUIMode() ) { 
			// This job can be done in the background. so push it to the background
			pushToBackground("SocialTriangulation", data );
			return; 
		}
		
		
		
		String credId = data.getString("credId");
		
		RWJBusComp bc = app.GetBusObject("SocialNetworkData").getBusComp("SocialNetworkData");
		
		BasicDBObject query = new BasicDBObject();
		query.put("credId", getUserId());
		
		
		if ( bc.ExecQuery(query) == 0 ) {
				return;
		}
		
		BasicDBList fblist = (BasicDBList) bc.GetFieldValue("FaceBookFriends");
		
		if ( fblist != null && fblist.size() > 0)
		{
			String myFbFSet = "myFbFSet" + credId;
			
			jedisMt.del(myFbFSet);
			
			for (int i = 0 ; i < fblist.size(); i++ ) {
				jedisMt.sadd(myFbFSet, fblist.get(i).toString());
			}
			
			jedisMt.cacheFaceBookIds(false);
			Set<String> members = jedisMt.sinter("FACEBOOK_MEMBERS", myFbFSet );
			
			if ( members.size() > 0 ) {
				// send email to this user about how many of his friends are already members.
				// TODO : fire away emails to his friends as well, except the invitor
			}
			
			jedisMt.del(myFbFSet);
			
		}

		BasicDBList lnlist = (BasicDBList) bc.GetFieldValue("linkedinconnectons");
		
		if ( fblist != null && fblist.size() > 0)
		{
			String myLNCSet = "myLNCSet" + credId;
			jedisMt.del(myLNCSet);
			
			for (int i = 0 ; i < lnlist.size(); i++ ) {
				jedisMt.sadd(myLNCSet, fblist.get(i).toString());
			}
			
			jedisMt.cacheLinkedInIds(false);
			Set<String> members = jedisMt.sinter("LINKEDIN_MEMBERS", myLNCSet );
			
			if ( members.size() > 0 ) {
				// send email to this user about how many of his friends are already members.
				// TODO : fire away emails to his friends as well, except the invitor
			}			
		}

		
		// TODO: That should do for OCT. Implement Contact triangulation here..		
		
	}
	
	
	@SuppressWarnings("unchecked")
	@RWAPI
	public JSONObject publicProfile(JSONObject data) throws Exception {
		log.trace( "publicProfile");
		String partyId = data.get("id").toString() ;
		String strParty  = null; //Cache(partyId);	

		if ( strParty == null ) {
			// This has be rare exception. Otherwise, all party objects stay in the cache
			RWJBusComp bc = app.GetBusObject("Party").getBusComp("Party");
			BasicDBObject query = new BasicDBObject();
			
	        query.put("_id", new ObjectId(partyId));
			query.put("partytype", "PARTNER");
			int nRecs = bc.UpsertQuery(query);
			if ( nRecs > 0 ) {
				
				JSONObject body = new JSONObject();
				String webRoot = System.getProperty("WebRoot");
				log.info("WebRoot : " + webRoot);
				if ( webRoot == null || webRoot.isEmpty()) {
					webRoot = new String ("/var/lib/tomcat7/webapps/ROOT");
				}
				
				String publicId = bc.GetFieldValue("InvitationCode").toString();
				
				data.put("fullName", bc.GetFieldValue("fullName").toString());
				data.put("jobTitle", bc.GetFieldValue("jobTitle").toString());
				data.put("cityAddress_work", bc.has("cityAddress_work") ? bc.GetFieldValue("cityAddress_work").toString() : "City");
				data.put("stateAddress_work", bc.has("stateAddress_work") ? bc.GetFieldValue("stateAddress_work").toString() : "State");
				data.put("profession", bc.GetFieldValue("profession").toString());
				data.put("bio", bc.GetFieldValue("bio").toString());
				data.put("InvitationCode", bc.GetFieldValue("InvitationCode").toString());
					
				JSONObject tenantObj = getTenant(null);
				
				data.put("domainName", tenantObj.getString("domainName"));
				data.put("tenant", tenantObj.getString("tenant"));
				data.put("TrackingID", tenantObj.getString("GoogleAnalyticsPropertyCode"));
				
				String htmlString = vmSnippet(System.getProperty("WebRoot"), getMessage("MEM_PUBLICPROFILE"), data).getString("snippet");
				
				
				
				String existingBucketName = tenantObj.getString("publicProfilesBaseUrl") + "." + tenantObj.getString("domainName") ;
                String fileName = publicId.toLowerCase() +  ".html";
         	    
                File htmlFileFile = File.createTempFile(new File(RandomStringUtils.randomAlphabetic(24)).getName(),null);
                FileUtils.writeStringToFile(htmlFileFile,htmlString );
                
                S3DataVault dataVault = new S3DataVault();            	
            	InputStream htmlStream = new FileInputStream(htmlFileFile);
            	String publicProfile = dataVault.staticWeb(existingBucketName, fileName, htmlStream, htmlFileFile.length());
            	bc.SetFieldValue("publicProfile", publicProfile);
            	bc.SaveRecord();
				strParty = StringifyDBObject(bc.GetCompositeRecord());
		}
			else {
				String errorMessage = getMessage("RECORD_NOTFOUND"); 
				log.debug( errorMessage);
				throw new Exception(errorMessage);
			}
		}
		
		JSONObject retVal = new JSONObject() ;
		retVal.put("data",strParty);
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
		
		log.info("compute Metric:" +  metric + ":" + Double.toString(l));

		bc.SetFieldValue(data.getString("field"), l);
		bc.SaveRecord();
		// Decache(bc.GetFieldValue("_id").toString());

		return retVal;		

	}
	
	public void cron() {
		try {
			
			RWJBusComp bc = app.GetBusObject("Party").getBusComp("Party");
			BasicDBObject query = new BasicDBObject();
			query.put("partytype", "PARTNER");
			//query.put("emailAddress", "jimb@yesitworks.com");
			int nRecs = bc.UpsertQuery(query);
			RWAlgorithm a = new RWAlgorithm ( this );
			
			for ( int i = 0; i < nRecs ; i++ ) {
				try {					
					double l = a.compute(bc.currentRecord, "memberProfileScore" ) ;
					bc.SetFieldValue("memberProfileScore", l);
					
					l = a.compute(bc.currentRecord, "speakerProfileScore" ) ;
					bc.SetFieldValue("speakerProfileScore", l);

					JSONObject data = new JSONObject();
					String pId = bc.GetFieldValue("_id").toString();
					data.put("userid",bc.GetFieldValue("credId").toString());
					data.put("partyId", pId);
					
					RWChartItemData d = app.GetChartObject("totalProspects");

					BasicDBList dl = d.getSeries(data);
					if ( dl.size() > 0 ) {
						BasicDBObject dbo = (BasicDBObject) dl.get(0);
						String summarizer = dbo.getString("_id");
						bc.SetFieldValue("totalProspects", dbo.get(summarizer));
					}
					
					d = app.GetChartObject("totalProspectsConverted");

					dl = d.getSeries(data);
					if ( dl.size() > 0 ) {
						BasicDBObject dbo = (BasicDBObject) dl.get(0);
						String summarizer = dbo.getString("_id");
						bc.SetFieldValue("totalProspectsConverted", dbo.get(summarizer));
					}
					
					d = app.GetChartObject("totalIntroductions");
					dl = d.getSeries(data);
					if ( dl.size() > 0 ) {
						BasicDBObject dbo = (BasicDBObject) dl.get(0);
						String summarizer = dbo.getString("_id");
						bc.SetFieldValue("totalIntroductions", dbo.get(summarizer));
					}
					
					d = app.GetChartObject("totalInvited");
					
					dl = d.getSeries(data);
					if ( dl.size() > 0 ) {
						BasicDBObject dbo = (BasicDBObject) dl.get(0);
						String summarizer = dbo.getString("_id");
						bc.SetFieldValue("totalInvited", dbo.get(summarizer));
					}
					d = app.GetChartObject("totalConnections");
					
					dl = d.getSeries(data);
					if ( dl.size() > 0 ) {
						BasicDBObject dbo = (BasicDBObject) dl.get(0);
						String summarizer = dbo.getString("_id");
						bc.SetFieldValue("totalConnections", dbo.get(summarizer));
					}
					d = app.GetChartObject("totalMtgsAttended");
					
					
					
					dl = d.getSeries(data);
					if ( dl.size() > 0 ) {
						BasicDBObject dbo = (BasicDBObject) dl.get(0);
						String summarizer = dbo.getString("_id");
						bc.SetFieldValue("totalMtgsAttended", dbo.get(summarizer));
					}
				
					l = a.compute(bc.currentRecord, "MemberRankScore" ) ;
					bc.SetFieldValue("memberRankScore", l);
					bc.SaveRecord();
					
					/* 
					 * We are already sending emails when a meesage is posted.
					 * We need to think thru this notifications email a bit.
					 * May be send it only when user chooses to not get email from members.
					 *
					
					Object emailPref1 = GetPartyAttr(pId, "EmailPref1");
					if ( emailPref1 == null || emailPref1.toString().equals("YES") ){
					
						JSONArray notifications = new JSONArray();
						
						while ( true ) {
							String note = jedisMt.lpop(pId + "_notify");
							if ( note != null ) {
								JSONObject item = new JSONObject(note);
								notifications.put(item);
							}
							else 
								break;
						} 
						
						if (notifications.length() > 0 ) {
							
							Mail m = new Mail(this);
							
							JSONObject tenantObj = getTenant(null);
			
							BasicDBObject party = GetParty(pId);
							
							JSONObject emailData = new JSONObject();
							
							emailData.put("firstName", party.getString("firstName"));
							emailData.put("lastName", party.getString("lastName"));
							emailData.put("Alerts", notifications );

							JSONObject header = m.header( tenantObj.getString("SupportEmailAddress"), 
								tenantObj.getString("pageTitle"), tenantObj.getString("SupportEmailAddress"), tenantObj.getString("pageTitle"),
								getMessage("NOTIFICATIONS_SUBJECT"),party.getString("emailAddress"), null);
							m.SendHtmlMailMessage(header, getMessage("ET_NOTIFICATIONS"), emailData, null); 

						}
						
					} */
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
	
	public void SetupOpenInvitaton( RWJBusComp bc ) throws Exception {
		log.info( "OpenInvitaton");

		String invitationId = UUID.randomUUID().toString().substring(0, 8);
		invitationId = "OI" + invitationId.replace('-', 'X').toUpperCase();
		bc.SetFieldValue("InvitationCode", invitationId);

		JSONObject data = new JSONObject();

		data.put("fromId", bc.get("_id").toString());
    	data.put("fromFullName", bc.get("fullName").toString());
      	data.put("invitationType", "OI");
    	data.put("invitationLifeSpan", "PERPETUAL");
		jedisMt.set(invitationId, data.toString());				
		bc.SaveRecord();
		
		log.info("Invitation for " +  bc.get("fullName").toString() + " = " + invitationId);
		
	}
	
	private void updateOtherUsersAddressBooks(String partyId) throws Exception{
		BasicDBObject find = new BasicDBObject();
    	find.put("eventId",partyId);

    	BasicDBObject update = new BasicDBObject();    	
    	BasicDBObject rwc =  new BasicDBObject();
    	rwc.put("partytype_denorm", "PARTNER");
    	update.put("$set",rwc);
    	
    	//update.put("rw_created_on", past);
    	mongoStore s = new mongoStore(getTenantKey());
    	s.getColl("rwPartner").updateMulti(find, update);
	}
	

}
