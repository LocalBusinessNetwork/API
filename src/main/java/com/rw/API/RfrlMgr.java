package com.rw.API;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.ResourceBundle;
import java.util.UUID;

import org.apache.log4j.Logger;
import org.bson.types.ObjectId;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.mongodb.AggregationOutput;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.DBRef;
import com.mongodb.QueryBuilder;
import com.rw.persistence.RWChartItemData;
import com.rw.persistence.RWJApplication;
import com.rw.persistence.RWJBusComp;
import com.rw.persistence.RWJBusObj;
import com.rw.persistence.RWObjectMgr;
import com.rw.persistence.RandomDataGenerator;
import com.rw.persistence.mongoStore;
import com.rw.repository.Permissions;
import com.rw.repository.RWAPI;
import com.rw.API.Mail;


@SuppressWarnings("unused")
public class RfrlMgr extends RWReqHandle  {
	static final Logger log = Logger.getLogger(RfrlMgr.class.getName());
	private final String[] statusLOVs = {"ACCEPTED","CONFIRMED","CONVERTED","UNREAD" } ;

	public RfrlMgr() {
	}
	public RfrlMgr(String userid) {
		super(userid);
	}
	
	public RfrlMgr(RWObjectMgr context) throws Exception {
		super(context);
	}

	@SuppressWarnings("unchecked")
	@RWAPI
	public JSONObject ReferralsByMyAssociation(JSONObject data) throws Exception {
		log.trace( "ReferralsByMyProfession");
		data.put("primaryAssocId", GetPartyAttr(GetPartyId(), "primaryAssocId"));
		return ReferralsByAssociation(data);
	}
	
	@SuppressWarnings("unchecked")
	@RWAPI
	public JSONObject ReferralsByAssociation(JSONObject data) throws Exception {

		log.trace( "ReferralsByAssociation");

		log.trace( "ReferralsByAssociation");
		AnalyticsMgr a = new AnalyticsMgr(this);
		data.put("name", "ReferralsByProfession" );
		return a.getChart(data);
	}	
	
	@SuppressWarnings("unchecked")
	@RWAPI
	public JSONObject ReferralsByProfession(JSONObject data) throws Exception {
		log.trace( "ReferralsByProfession");
		AnalyticsMgr a = new AnalyticsMgr(this);
		data.put("name", "ReferralsByProfession" );
		return a.getChart(data);
	}	

	@SuppressWarnings({ "unchecked" })
	@RWAPI
	public JSONObject PartnerTransactions(JSONObject data) throws Exception {

		log.trace( "PartnerTransactions");

		JSONObject retVal = new JSONObject() ;
		RWJBusComp bc = app.GetBusObject("Referral").getBusComp("Referral");
	
		String party_id = null;
		if (!data.isNull("PartyId"))
			party_id = data.getString("PartyId");
		else
			party_id = GetPartyId();
		
		Long period =  Long.parseLong(data.getString("period"));

		BasicDBObject match = new BasicDBObject();
		
		QueryBuilder qm = new QueryBuilder().
				or(new QueryBuilder().start().put("fromId").is(party_id).get(),
				new QueryBuilder().put("toId").is(party_id).get(), 
				new QueryBuilder().put("toId2").is(party_id).get());

		match.putAll(qm.get());
		Date past = new Date(System.currentTimeMillis()  -  period * 60000);

		qm = new QueryBuilder().start().put("statusChangeDate").greaterThanEquals(past);

		match.putAll(qm.get());
		
		qm = new QueryBuilder().start().put("status").is(new QueryBuilder().put("$in").
				is(statusLOVs).get());
		match.putAll(qm.get());

		JSONObject metrics = new JSONObject();

		int nRecs = bc.ExecQuery(match,null);
		BasicDBList cList = bc.GetCompositeRecordList();
		
		LinkedHashSet<String> partnerIds = new LinkedHashSet<String>();
		LinkedHashSet<String> sameProfIds = new LinkedHashSet<String>();

		String idStr = null;

		Object viewerProfObject = GetSelfAttr("profession"); 
		String viewerProf = viewerProfObject != null ? viewerProfObject.toString() : "NONE";
		
		for ( int i = 0; i < nRecs; i++) {
			BasicDBObject rfrl = (BasicDBObject) cList.get(i);

			idStr = rfrl.get("fromId").toString(); 
			if ( idStr.equals(party_id) ) { 
				idStr = rfrl.get("toId").toString(); 
				if (rfrl.get("to_profession").toString().equals(viewerProf)) 
					sameProfIds.add(idStr);
			}
			else { 
				if ( rfrl.get("from_profession").toString().equals(viewerProf) )
					sameProfIds.add(idStr);
			}	
			partnerIds.add(idStr);
		}
		
		metrics.put("PI_total", partnerIds.size() );
		metrics.put("PI_sameprof", sameProfIds.size() );
		retVal.put("data", metrics);
		return retVal;
	}



	@SuppressWarnings("unchecked")
	@RWAPI
	public JSONObject LastTwoWeeksReferralsRcvd(JSONObject data) throws Exception {
		log.trace( "LastTwoWeeksReferralsRcvd");
		long TwoWeeksAgo = 20160; //minutes
		Date compareToDate = new Date(System.currentTimeMillis() - TwoWeeksAgo * 60000);		
		data.put("since_date",compareToDate);
		return ReferralsRecieved(data);
	}	
	
	@SuppressWarnings("unchecked")
	@RWAPI
	public JSONObject LastTwoWeeksReferralsSent(JSONObject data) throws Exception {
		log.trace( "LastTwoWeeksReferralsSent");
		long TwoWeeksAgo = 20160; //minutes
		Date compareToDate = new Date(System.currentTimeMillis() - TwoWeeksAgo * 60000);		
		data.put("since_date",compareToDate);
		return ReferralsSent(data);
	}
	
	
	@SuppressWarnings("unchecked")
	@RWAPI
	public JSONObject NewReferralsRcvd(JSONObject data) throws Exception {
		log.trace( "NewReferralsRcvd");
		Object d  =  this.getExecutionContextItem("lastlogin_date");
		data.put("since_date",this.getExecutionContextItem("lastlogin_date"));
		return ReferralsRecieved(data);
	}	
	
	@SuppressWarnings("unchecked")
	@RWAPI
	public JSONObject NewReferralsSent(JSONObject data) throws Exception {
		log.trace( "NewReferralsSent");
		data.put("since_date",this.getExecutionContextItem("lastlogin_date"));
		JSONObject retVal = ReferralsSent(data);
		return retVal;
	}
	
	@SuppressWarnings("unchecked")
	@RWAPI
	public JSONObject latestUpdates(JSONObject data) throws Exception {
		log.trace( "latestUpdates");
		AnalyticsMgr a = new AnalyticsMgr(this);
		data.put("name", "latestUpdates" );
		return a.getChart(data);		
	}	
	
	@SuppressWarnings("unchecked")
	@RWAPI
	public JSONObject ReferralsSent(JSONObject data) throws Exception {
		log.trace( "ReferralsSent");
		AnalyticsMgr a = new AnalyticsMgr(this);
		data.put("name", "ReferralsSent" );
		return a.getChart(data);
	}	

	@SuppressWarnings("unchecked")
	@RWAPI
	public JSONObject ReferralsRecieved(JSONObject data) throws Exception {
		log.trace( "ReferralsRecieved");
		AnalyticsMgr a = new AnalyticsMgr(this);
		data.put("name", "ReferralsRecieved" );
		return a.getChart(data);		
	}	

	@SuppressWarnings("unchecked")
	@RWAPI
	public JSONObject ReferralActivity(JSONObject data) throws Exception {
		
		log.trace( "ReferralActivity");
		AnalyticsMgr a = new AnalyticsMgr(this);
		data.put("name", "ReferralActivity" );

		String party_id = null;
		if (!data.isNull("PartyId"))
			party_id = data.getString("PartyId");
		else
			party_id = GetPartyId();

		data.put("partyId", party_id);

		Long period =  Long.parseLong(data.getString("period"));
		Date past = new Date(System.currentTimeMillis()  -  period * 60000);
		data.put("since_date", past);
		
		BasicDBList d = a.getChartData(data);	

		JSONObject refActivity = new JSONObject();

		refActivity.put("myRefActivity", d);
		
		data.put("name", "PartnerRefActivity" );
		BasicDBList d2 = a.getChartData(data);	

		refActivity.put("PartnerRefActivity", d2);
		JSONObject retVal = new JSONObject();

		retVal.put("data", refActivity);
		return retVal;

	}	

	@SuppressWarnings("unchecked")
	@RWAPI
	@Permissions({"ADMIN_REFERRALS"})
	public JSONObject create(JSONObject data) throws Exception {
		log.trace( "create");

		String party_id = data.isNull("fromId") ? GetPartyId() : data.getString("fromId");
		String toId = null;
		String fromId = null;

		if ( !data.isNull("referralType") ) {
			String referralType = data.getString("referralType");
			if ( referralType.equals("FB") || referralType.equals("LN") || referralType.equals("OI") ) {
				return ProcessSocialReferral(data);
			}
			else if ( referralType.equals("INMAIL_REFERRAL") ) {
				return SendInMail(data);
			}			
			else if ( referralType.equals("CUST_FOR_PART") ) {
				toId = data.getString("toId");
				fromId = party_id;
				BasicDBObject toParty = GetParty(toId);
				if ( toParty.getString("partytype").equals("CONTACT") )
					return ProcessInvitationReferral(data);
				// else fall into default case
				updateStat("totalProspects", "totalProspects", "Party",  fromId);
			}
			else if (referralType.equals("PART_INVITE") && !data.isNull("newContactEmail")){
				JSONObject contactData = new JSONObject();
				String newContactFirst = data.getString("newContactFirst");
				String newContactLast = data.getString("newContactLast");
				String newContactEmail = data.getString("newContactEmail");
				String newContactOrgId = data.has("newContactOrgId")? data.getString("newContactOrgId") : GetSelfAttr("OrgId").toString();
				if (data.has("newContactPhone") && !data.getString("newContactPhone").equals("") && data.getString("newContactPhone") != null){
					String newContactPhone = data.getString("newContactPhone");
					contactData.put("workPhone", newContactPhone);
				}
				contactData.put("act", "create");
				contactData.put("userid", getUserId());
				contactData.put("firstName", newContactFirst);
				contactData.put("lastName", newContactLast);
				contactData.put("emailAddress", newContactEmail);
				contactData.put("partytype", "CONTACT");
				contactData.put("OrgId", newContactOrgId);
				ContactsMgr cm = new ContactsMgr(this);
				contactData = cm.handleRequest(contactData);
				
				String contactId = new JSONObject(new JSONObject(contactData.getString("data")).getString("_id")).getString("$oid");
				data.put("toId", contactId);
				String comments = (data.has("comments"))?data.getString("comments"):"Thanks attending our meeting!  We'd love to have you as a Successful Thinkers member!";
				data.put("comments",comments);


				return ProcessInvitationReferral(data);
				
			}
			else { // PART_FOR_PART or PART_INVITE
				if (referralType.equals("PART_FOR_PART")){

					updateStat("totalIntroductions", "totalIntroductions", "Party",  party_id);
				}
				
				if (!data.isNull("demosetup")){
					return CreateDemoInvitation(data);
				} else {
					return ProcessInvitationReferral(data);
					
				}
				
			}
		}

		// else fall into default case
		// this is the default case, customer exists, memeber exists.
		
		RWJBusComp bc = app.GetBusObject("Referral").getBusComp("Referral");
		
		BasicDBObject query = new BasicDBObject();

		bc.NewRecord();
		bc.SetFieldValue("fromId", party_id );
		bc.SetFieldValue("status", "UNREAD");
		bc.SetFieldValue("GeoWorkLocation", GetPartyAttr(party_id, "GeoWorkLocation"));
		bc.SetFieldValue("from_profession", GetPartyAttr(party_id, "profession"));
		bc.SetFieldValue("to_profession", GetPartyAttr(toId, "profession"));
		bc.SetFieldValue("fromBusinessId", GetPartyAttr(party_id, "OrgId"));
		bc.SetFieldValue("statusChangeDate", new Date());
		bc.SetFieldValue("referralSubType", "P2P2C"); 

		super.create(data);
		bc.SaveRecord(data);
		
		JSONObject trackingData = new JSONObject();
		trackingData.put("event", "UM_REFERRAL");         			
		trackMetrics(trackingData);

		
		BasicDBObject cRec = bc.GetCompositeRecord();

		JSONObject tenantObj = getTenant(null);
		
		Mail m = new Mail(this);
		
		JSONObject emailData = new JSONObject();
		
		emailData.put("memberFirstName", cRec.getString("to_firstName"));
		emailData.put("memberLastName", cRec.getString("to_lastName"));
		emailData.put("partnerFullName", cRec.getString("from_fullName"));
		emailData.put("partnerFirstName", cRec.getString("from_firstName"));
		emailData.put("loginEmail", cRec.getString("to_emailAddress"));
		String fromEmailAddress = GetPartyAttr(party_id, "emailAddress").toString();
		
		JSONObject header = m.header( tenantObj.getString("memberEmailAddress"),
				tenantObj.getString("domainName"), fromEmailAddress,
				GetPartyAttr(party_id, "fullName").toString(),
				getMessage("NEWRFRL_EMAILTEMPLATE_SUBJECT",cRec.getString("from_fullName") ), cRec.getString("to_emailAddress"),null);					
		
		m.SendHtmlMailMessage(header, getMessage("ET_NEWREFERRAL"), emailData, null);
		
		
		JSONObject header2 = m.header( tenantObj.getString("SupportEmailAddress"), 
				tenantObj.getString("pageTitle"), tenantObj.getString("SupportEmailAddress"), tenantObj.getString("pageTitle"),
				getMessage("STNCONNECT_CONFIRM"),fromEmailAddress, null);
		
		emailData.put("loginEmail", fromEmailAddress);
		m.SendHtmlMailMessage(header2, getMessage("ET_NEWREFERRAL_CONFIRM"), emailData, null); 
				
		JSONObject retVal = new JSONObject() ;
		retVal.put("data", StringifyDBObject(cRec));
		return retVal;
	}
	
	private JSONObject SendInMail(JSONObject data) throws Exception {

		String referralSubType = data.getString("referralSubType");
		String toId = data.getString("toId");
		
		RWJBusComp bc = app.GetBusObject("Referral").getBusComp("Referral");

		String fromId = null;
		String fromFullname = null;
		
		if ( referralSubType.equals("C2PM") ) {
			fromId = data.getString("toId");
			fromFullname = data.getString("to_fullName");
		}
		else {
			fromId = data.isNull("fromId") ? GetPartyId() : data.getString("fromId");
			fromFullname = GetPartyAttr(fromId, "fullName").toString();
		}

		bc.NewRecord();
		
		bc.SetFieldValue("fromId", fromId );
		bc.SetFieldValue("status", "UNREAD");
		bc.SetFieldValue("toId", toId);
		bc.SetFieldValue("firstPartyFullName", fromFullname);
		bc.SetFieldValue("subject", data.getString("subject"));
		bc.SetFieldValue("to_profession", GetPartyAttr(toId, "profession"));
		bc.SetFieldValue("statusChangeDate", new Date());
		bc.SetFieldValue("referralType", "INMAIL_REFERRAL"); 
		bc.SetFieldValue("referralSubType", referralSubType); 
		bc.SetFieldValue("comments", data.getString("comments")); 
		bc.SetFieldValue("urgency", data.isNull("urgency") ?"NORMAL": data.getString("urgency") );
		bc.SetFieldValue("documents", data.isNull("documents") ?"": data.getString("documents") );

		super.create(data);
		bc.SaveRecord(data);
		
		JSONObject notification = new JSONObject();
		notification.put("docRef", bc.GetFieldValue("_id").toString());
		notification.put("docType", "INMAIL_REFERRAL");
		notification.put("docSubType", referralSubType);

		JSONObject emailData = new JSONObject();
		JSONObject tenantObj = getTenant(null);
		
		emailData.put("personalMessage", data.getString("comments"));

		Mail m = new Mail(this);
		
		if ( referralSubType.equals("P2PM") ) {
			notification.put("partyId", toId);			
			notification.put("message", "New Message from " + fromFullname);
			jedisMt.lpush( toId + "_notify", notification.toString() );

			Object emailPref4 = GetPartyAttr(toId, "EmailPref4");
			if ( emailPref4 == null || emailPref4.toString().equals("YES") ){
				String toEmailAddress = GetPartyAttr(toId, "emailAddress").toString();
				String fromEmailAddress = GetPartyAttr(fromId, "emailAddress").toString();
				emailData.put("to_emailAddress",toEmailAddress);
				JSONObject header = m.header( tenantObj.getString("memberEmailAddress"),
						tenantObj.getString("domainName"), fromEmailAddress,
						fromFullname,
						data.getString("subject"), toEmailAddress,null);					
				m.SendHtmlMailMessage(header, getMessage("ET_INMAIL_P2PM"), emailData, null);
			}
		}
		else {
			RWJBusComp partybc = app.GetBusObject("Party").getBusComp("Party");
			BasicDBObject query = new BasicDBObject();
			
			query.put("partytype", "PARTNER");
			query.put("OrgId", toId);
			
			notification.put("message", "New Announcement from " + fromFullname);
			
			int nRecs = partybc.ExecQuery(query);
			for ( int i = 0; i < nRecs; i++) {
				String pid = partybc.GetFieldValue("_id").toString();
				notification.put("partyId", pid);			
				jedisMt.lpush( pid + "_notify", notification.toString() );
				Object emailPref3 = GetPartyAttr(pid, "EmailPref3");
				if ( emailPref3 == null || emailPref3.toString().equals("YES") ){
					String toEmailAddress = GetPartyAttr(pid, "emailAddress").toString();
					String fromEmailAddress = tenantObj.getString("memberEmailAddress");

					emailData.put("to_emailAddress", toEmailAddress);
					
					JSONObject header = m.header( tenantObj.getString("memberEmailAddress"),
							tenantObj.getString("domainName"), fromEmailAddress,
							fromFullname,
							data.getString("subject"), toEmailAddress,null);					
					m.SendHtmlMailMessage(header, getMessage("ET_INMAIL_C2PM"), emailData, null);
				}
				partybc.NextRecord();
			}
		}

		return data;
		
	}

	public JSONObject CreateDemoInvitation(JSONObject data) throws Exception {
		String party_id = data.isNull("fromId") ? GetPartyId() : data.getString("fromId");
		String toId = data.getString("toId");
		String toId2 = (data.has("toId2"))?data.getString("toId2"):null;
		String comments = data.getString("comments");
		//String toStatus = (data.has("toStatus"))?data.getString("toStatus"):null;
		//String toStatus2 = (data.has("toStatus2"))?data.getString("toStatus2"):null;
		String waitingForId = (data.has("waitingForId"))?data.getString("waitingForId"):null;
		
		String status = data.getString("status");
		String referralType = data.getString("referralType");
		
		RWJBusComp bc = app.GetBusObject("Referral").getBusComp("Referral");
		
		BasicDBObject query = new BasicDBObject();

		bc.NewRecord();
		bc.SetFieldValue("fromId", party_id );
		bc.SetFieldValue("toId", toId );
		if (toId2 != null){bc.SetFieldValue("toId2", toId2 );}
		bc.SetFieldValue("status", status);
		bc.SetFieldValue("comments", comments);
		if (toId2 != null){bc.SetFieldValue("waitingForId", waitingForId);}
		//bc.SetFieldValue("toStatus", toStatus);
		//if (toStatus2 != null){bc.SetFieldValue("2Status", toStatus2);}
		//bc.SetFieldValue("from_profession", GetPartyAttr(party_id, "profession"));
		//bc.SetFieldValue("to_profession", GetPartyAttr(toId, "profession"));
		bc.SetFieldValue("statusChangeDate", new Date());
		bc.SetFieldValue("referralType", referralType);
		bc.SetFieldValue("referralSubType", "P2P"); 

		super.create(data);
		bc.SaveRecord(data);
		
		BasicDBObject cRec = bc.GetCompositeRecord();

		JSONObject retVal = new JSONObject() ;
		retVal.put("data", StringifyDBObject(cRec));
		return retVal;
	}

	/*
	 * (non-Javadoc)
	 * @see com.rw.API.RWReqHandle#update(org.json.JSONObject)
	 * Update on referral can be performed by 
	 * Originator of the referral : fromid
	 * First recipient : toId
	 * Second recipient : 2toId ( in case of PART_FOR_PART referrals )
	 */
	@SuppressWarnings({ "unchecked" })
	@RWAPI
	public JSONObject update(JSONObject data) throws Exception {
		log.trace( "update");
		if (data.has("protoStatus")) {
			String pStatus = data.getString("protoStatus");
			if (pStatus.equals("ARCHIVED") || pStatus.equals("NOSALE")){
				data.put("archiveFlag", "true");
			}
			if (!pStatus.equals("ARCHIVED")){
				data.put("status", pStatus);
			}
		}		
		super.update(data);
		
		Object id = data.get("id");
		
		RWJBusComp bc = app.GetBusObject("Referral").getBusComp("Referral");
		BasicDBObject query = new BasicDBObject();
        query.put("_id", new ObjectId(id.toString())); 
        
		if (bc.UpsertQuery(query) == 0 ) {
			String errorMessage = getMessage("RECORD_NOTFOUND"); 
			log.debug( errorMessage);
			throw new Exception(errorMessage);
		}
		
		String prevStatus = bc.GetFieldValue("status").toString();
		String referralType = bc.GetFieldValue("referralType").toString();
		
		String newStatus = data.get("status").toString();
		String me = GetPartyId();
		String fromId = bc.GetFieldValue("fromId").toString();
		String toId = data.has("toId") ?  data.getString("toId") : bc.GetFieldValue("toId").toString();
		Object toId2Obj = bc.GetFieldValue("toId2");
		String toId2 = data.has("toId2") ?  data.getString("toId2") : (toId2Obj != null) ? toId2Obj.toString() : "";

		if ( referralType.equals("PART_FOR_PART") && prevStatus.equals("UNREAD") && newStatus.equals("ACCEPTED")) {
			newStatus = "WAITING";
			data.put("status",newStatus);
			bc.SetFieldValue("waitingForId", me.equals(toId) ? toId2 : toId);
		}
		//else if ( referralType.equals("PART_FOR_PART") && prevStatus.equals("WAITING") && newStatus.equals("WAITING")) {
		else if ( referralType.equals("PART_FOR_PART") && prevStatus.equals("WAITING") && newStatus.equals("ACCEPTED")) {
			//newStatus = "	";
			data.put("status",newStatus);
			bc.SetFieldValue("waitingForId", "");
			
		}
		else if ( referralType.equals("INMAIL_REFERRAL") ) {
			String comments = bc.GetFieldValue("comments").toString();
			bc.SetFieldValue("comments", comments.concat(data.get("comments2").toString()));
			
		}

		// if (!prevStatus.equals(newStatus)) // there has been some update to this referral, so there is a status change.
			data.put("statusChangeDate", new Date());
		
		bc.SaveRecord(data);
		
		BasicDBObject cRec = bc.GetCompositeRecord();

		// workflow - UNREAD -> [WAITING]->ACCEPTED -> CONFIRMED -> CONVERTED
		if ( prevStatus.equals("UNREAD") && newStatus.equals("WAITING") ) {
			PartnerMgr pm = new PartnerMgr(this);
			pm.BiDirectionalEdge( fromId, me);
		}
		else if ( prevStatus.equals("WAITING") && newStatus.equals("ACCEPTED") ) {
			// this is a P2P scenario, first partner has accepted the invitation.
			PartnerMgr pm = new PartnerMgr(this);
			pm.BiDirectionalEdge( toId, toId2);
		}
		else if ( prevStatus.equals("UNREAD") && newStatus.equals("ACCEPTED") ) {
			// Kick of Accepted workflow
			
			if (referralType.equals("PART_INVITE")) {
				PartnerMgr pm = new PartnerMgr(this);
				pm.BiDirectionalEdge(fromId, me);
			}
			
			/*
			JSONObject payload = new JSONObject();
			BillingMgr bm = new BillingMgr();
			payload.put("userid", userid.toString());
			payload.put("act", "create");
			payload.put("referralId", id.toString());
			BasicDBObject rfrlObject = bc.GetCompositeRecord();
			payload.put("contact_fullName", rfrlObject.get("contact_fullName"));	
			payload.put("from_fullName", rfrlObject.get("from_fullName"));	
			payload.put("amount", GetPartyAttr(party_id,"rfrl_txn_fees"));
			bm.handleRequest(payload);
			*/
			
			if (referralType.equals("CUST_FOR_PART")) {
				// copy contact
				if ( !cRec.getString("contactId").isEmpty()) {
	
					String contactId = cRec.getString("contactId");
					JSONObject payload = new JSONObject();
					ContactsMgr cm = new ContactsMgr(this);
					payload.put("userid", getUserId());
					payload.put("act", "copy");
					payload.put("id", cRec.getString("contactId"));
					cm.handleRequest(payload);
					String customerEmail = cRec.getString("contact_email");
					if ( customerEmail != null && !customerEmail.isEmpty()) {
						CFBOutboxMgr cfbm = new CFBOutboxMgr(this);	
						cfbm.ScheduleAMessage(cRec, 10);
					}
					
				}
				
				

			}
		}	
		
		if (referralType.equals("CUST_FOR_PART")) {
			JSONObject data_in = new JSONObject();
			data_in.put("partyId", fromId);
			updateStatExp("totalProspectsConverted", "totalProspectsConverted", "Party",  fromId,data_in);
		}
		
		if (data.has("toId_fwd")){
			forwardReferral(data);
		}
		
		JSONObject retVal = new JSONObject() ;
		retVal.put("data", StringifyDBObject(cRec));
		
		if (referralType.equals("INMAIL_REFERRAL")  ) {
			
			JSONObject notification = new JSONObject();
			notification.put("docRef", cRec.getString("_id"));
			notification.put("docType", "INMAIL_REFERRAL");
			notification.put("docSubType", cRec.getString("referralSubType"));
			
			if ( toId.equals(me)) {
				notification.put("partyId", fromId);
				notification.put("message", "Reply from " + cRec.getString("to_fullName"));
				jedisMt.lpush( fromId + "_notify", notification.toString() );		
			}
			else {
				notification.put("partyId", toId);
				notification.put("message", "Reply from " + cRec.getString("from_fullName"));
				jedisMt.lpush( toId + "_notify", notification.toString() );		
			}
			
			
		}
		
		return retVal;
	}
	
	/*
	 * InBox : All referral's received by a particular member
	 */
	
	private void forwardReferral(JSONObject data) throws Exception {
		
		data.put("status", "UNREAD");
		data.remove("reasonNoOpportunity");
		data.remove("serviceProvided");
		data.remove("leadRevenue");
		data.remove("archiveNow");
		data.remove("isActive");
		data.remove("archiveReason");
		data.remove("protoStatus");
		data.put("fromId", data.get("toId"));
		data.put("from_fullName", data.get("to_fullName"));
		data.put("from_firstName", data.get("to_firstName"));
		data.put("toId",data.getString("toId_fwd"));
		data.put("to_fullName",data.getString("to_fullName_fwd"));
		data.put("to_firstName",data.getString("to_firstName_fwd"));
		data.put("recipientProfession",data.getString("recipientProfession_fwd"));
		data.put("toId",(data.has("toId_fwd"))?data.getString("toId_fwd"):"");
		data.put("to_fullName",(data.has("to_fullName_fwd"))?data.getString("to_fullName_fwd"):"");
		data.put("to_firstName",(data.has("to_firstName_fwd"))?data.getString("to_firstName_fwd"):"");
		data.put("recipientProfession",(data.has("recipientProfession_fwd"))?data.getString("recipientProfession_fwd"):"");
		data.put("referral_reason",(data.has("referral_reason_fwd"))?data.getString("referral_reason_fwd"):"");
		data.put("comments",(data.has("comments_fwd"))?data.getString("comments_fwd"):"");
		data.put("urgency",(data.has("urgency_fwd"))?data.getString("urgency_fwd"):"");
		data.put("can_contact",(data.has("can_contact_fwd"))?data.getString("can_contact_fwd"):"");
		data.put("question1",(data.has("question1_fwd"))?data.getString("question1_fwd"):"");
		data.put("answer1",(data.has("answer1_fwd"))?data.getString("answer1_fwd"):"");
		data.put("question2",(data.has("question2_fwd"))?data.getString("question2_fwd"):"");
		data.put("answer2",(data.has("answer2_fwd"))?data.getString("answer2_fwd"):"");
		data.put("question3",(data.has("question3_fwd"))?data.getString("question3_fwd"):"");
		data.put("answer3",(data.has("answer3_fwd"))?data.getString("answer3_fwd"):"");
		data.put("question4",(data.has("question4_fwd"))?data.getString("question4_fwd"):"");
		data.put("answer4",(data.has("answer4_fwd"))?data.getString("answer4_fwd"):"");
		
		JSONObject retVal = create(data);
	}	
	
	@SuppressWarnings("unchecked")
	@RWAPI
	public JSONObject InBox(JSONObject data) throws Exception {
		log.trace( "InBox");
		
		JSONObject retVal = new JSONObject() ;

		String toId = data.isNull("party_id") ? GetPartyId() : data.getString("party_id");

		RWJBusComp bc = app.GetBusObject("Referral").getBusComp("Referral");
		BasicDBObject query = new BasicDBObject();
		QueryBuilder qm = new QueryBuilder().or(new QueryBuilder().start().put("toId").is(toId).get(),
							new QueryBuilder().put("toId2").is(toId).get());

		query.putAll(qm.get());
		query.putAll(getSearchSpec(data).toMap());

		int nRecs = bc.ExecQuery(query,getSortSpec(data));
		BasicDBList cList = bc.GetCompositeRecordList();
		retVal.put("data", StringifyDBList(cList));

		return retVal;
	}

	/*
	 * OutBox : all referral sent from the a particular member.
	 * Default : logged in user. 
	 */
	
	@SuppressWarnings("unchecked")
	@RWAPI
	public JSONObject OutBox(JSONObject data) throws Exception {
		log.trace( "OutBox");
		
		JSONObject retVal = new JSONObject() ;
		String fromId = data.isNull("party_id") ? GetPartyId() : data.getString("party_id");
		BasicDBObject query = new BasicDBObject();

        query.put("fromId", fromId); 
		query.putAll(getSearchSpec(data).toMap());

        RWJBusComp bc = app.GetBusObject("Referral").getBusComp("Referral");

		int nRecs = bc.ExecQuery(query,getSortSpec(data));
		BasicDBList cList = bc.GetCompositeRecordList();
		retVal.put("data", StringifyDBList(cList));

		return retVal;
	}
	
	@SuppressWarnings("unchecked")
	@RWAPI
	public JSONObject NewsFeed(JSONObject data) throws Exception {
		log.trace( "NewsFeed");
		
		JSONObject retVal = new JSONObject() ;

		String toId = data.isNull("party_id") ? GetPartyId() : data.getString("party_id");
		String OrdId = GetPartyAttr(toId,"OrgId").toString();

		RWJBusComp bc = app.GetBusObject("Referral").getBusComp("Referral");
		BasicDBObject query = new BasicDBObject();
		/*QueryBuilder qm = new QueryBuilder().or(new QueryBuilder().start().put("toId").is(toId).get(),
							new QueryBuilder().put("toId2").is(toId).get(),new QueryBuilder().put("fromId").is(fromId).get());
		*/
		QueryBuilder qm = new QueryBuilder().or(
				new QueryBuilder().and(new QueryBuilder().put("toId").is(toId).get(),new QueryBuilder().put("archiveFlag").notEquals("true").get()).get(),
				new QueryBuilder().and(new QueryBuilder().put("toId2").is(toId).get(),new QueryBuilder().put("archiveFlag").notEquals("true").get()).get(),
				new QueryBuilder().put("fromId").is(toId).get(),
				new QueryBuilder().put("fromId").is(OrdId).get(),
				new QueryBuilder().put("fromId").is("GLOBAL_MSG").get());
		
		query.putAll(qm.get());
		query.putAll(getSearchSpec(data).toMap());

		
		//int nRecs = bc.ExecQuery(query,getSortSpec(data), 5, 0);
		int nRecs = bc.ExecQuery(query,getSortSpec(data));
		BasicDBList cList = bc.GetCompositeRecordList();
		retVal.put("data", StringifyDBList(cList));

		return retVal;
	}
	
	public JSONObject ChapterReferrals(JSONObject data) throws Exception {
		log.trace( "NewsFeed");
		
		JSONObject retVal = new JSONObject() ;

		String toId = data.isNull("party_id") ? GetPartyId() : data.getString("party_id");
		String fromId = data.isNull("party_id") ? GetPartyId() : data.getString("party_id");

		RWJBusComp bc = app.GetBusObject("Referral").getBusComp("Referral");
		BasicDBObject query = new BasicDBObject();
		QueryBuilder qm = new QueryBuilder().or(new QueryBuilder().start().put("toId").is(toId).get(),
							new QueryBuilder().put("toId2").is(toId).get(),new QueryBuilder().put("fromId").is(fromId).get());

		query.putAll(qm.get());
		query.putAll(getSearchSpec(data).toMap());

		int nRecs = bc.ExecQuery(query,getSortSpec(data));
		BasicDBList cList = bc.GetCompositeRecordList();
		retVal.put("data", StringifyDBList(cList));

		return retVal;
	}
	
	/*
	 * (non-Javadoc)
	 * @see com.rw.API.RWReqHandle#read(org.json.JSONObject)
	 * A referrals can be read by fromId, toId, or 2toId
	 * When a fromId is presented, read all referrals from this from Id
	 * When a toId is presented, read all referrals to this toId or 2toId
	 */
	@SuppressWarnings("unchecked")
	@RWAPI
	public JSONObject read(JSONObject data) throws Exception {
		log.trace( "read");

		JSONObject retVal = new JSONObject() ;

		RWJBusComp bc = app.GetBusObject("Referral").getBusComp(getShapedBC(data,"Referral"));
		BasicDBObject query = new BasicDBObject();
		
		if ( !data.isNull("id")) {
			Object id = data.get("id");
	        query.put("_id", new ObjectId(id.toString())); 
			int nRecs = bc.ExecQuery(query,null);
			if ( nRecs > 0 ) {
				BasicDBObject cRec = bc.GetCompositeRecord();
				retVal.put("data", StringifyDBObject(cRec));
			}
			else {
				String errorMessage = getMessage("RECORD_NOTFOUND"); 
				log.debug( errorMessage);
				throw new Exception(errorMessage);
			}
		}
		else {
			String partyId = data.isNull("party_id") ? GetPartyId() : data.getString("party_id");
			QueryBuilder qm = new QueryBuilder().or(new QueryBuilder().start().put("fromId").is(partyId).get(),
					new QueryBuilder().start().put("toId").is(partyId).get(),
					new QueryBuilder().put("toId2").is(partyId).get());
			query.putAll(qm.get());
			query.putAll(getSearchSpec(data).toMap());

			if (data.has("excludeId")) {
				BasicDBObject excludeId = new BasicDBObject();
				excludeId.put("$ne", new ObjectId(data.getString("excludeId")));
				query.put("_id", excludeId);
			}

			int nRecs = bc.ExecQuery(query,getSortSpec(data));
			BasicDBList cList = bc.GetCompositeRecordList();
			retVal.put("data", StringifyDBList(cList));
		}
		return retVal;
	}
	
	@SuppressWarnings("unchecked")
	@RWAPI
	public JSONObject readAll(JSONObject data) throws Exception {
		log.trace( "read");

		JSONObject retVal = new JSONObject() ;

		RWJBusComp bc = app.GetBusObject("Referral").getBusComp(getShapedBC(data,"Referral"));
		BasicDBObject query = new BasicDBObject();
		
		query.putAll(getSearchSpec(data).toMap());

		if (data.has("excludeId")) {
			BasicDBObject excludeId = new BasicDBObject();
			excludeId.put("$ne", new ObjectId(data.getString("excludeId")));
			query.put("_id", excludeId);
		}

		int nRecs = bc.ExecQuery(query,getSortSpec(data));
		BasicDBList cList = bc.GetCompositeRecordList();
		retVal.put("data", StringifyDBList(cList));
		
		return retVal;
	}
	
	@RWAPI
	public JSONObject delete(JSONObject data) throws Exception {
		log.trace( "delete");
		super.delete(data);
		Object id = data.get("id");
        RWJBusComp bc = app.GetBusObject("Referral").getBusComp("Referral");

        BasicDBObject query = new BasicDBObject();
	    
		// TODO: Could a referral be deleted by toId or fromId..
		// I think we should just soft delete the referral, so that we could analyze them later..
		// question for Peter.
		String party_id = GetPartyId();
		QueryBuilder qm = new QueryBuilder().or(new QueryBuilder().start().put("fromId").is(party_id).get(),
				new QueryBuilder().put("toId").is(party_id).get());

		query.put("_id", new ObjectId(id.toString())); 

		if (bc.ExecQuery(query,null) == 0 ) {
			String errorMessage = getMessage("RECORD_NOTFOUND"); 
			log.debug( errorMessage);
			throw new Exception(errorMessage);
		}
		
		JSONObject retVal = new JSONObject() ;
		retVal.put("data", StringifyDBObject(bc.GetCompositeRecord()));
		bc.DeleteRecord();
		return retVal;
	}
	
	@SuppressWarnings("unchecked")
	@RWAPI
	public JSONObject GetARfrl(String id) throws Exception {
		log.trace( "GetARfrl");

		RWJBusComp bc = app.GetBusObject("Referral").getBusComp("Referral");
		BasicDBObject query = new BasicDBObject();
        query.put("_id", new ObjectId(id)); 
		int nRecs = bc.ExecQuery(query,null);
		if ( nRecs > 0 ) {
			BasicDBObject cRec = bc.GetCompositeRecord();
			return new JSONObject(cRec.toMap());
		}
		else {
			String errorMessage = getMessage("RECORD_NOTFOUND"); 
			log.debug( errorMessage);
			throw new Exception(errorMessage);
		}
	}

	@SuppressWarnings("unchecked")
	@RWAPI
	public boolean UpdateCFB(String token, String id, String surveytype, String decision) throws Exception {
		log.trace( "UpdateCFB");

		RWJBusComp bc = app.GetBusObject("Referral").getBusComp("Referral");
		BasicDBObject query = new BasicDBObject();
        query.put("_id", new ObjectId(id)); 
		int nRecs = bc.UpsertQuery(query);
		if ( nRecs > 0 ) {
			if (surveytype.equals("1") ) {
				
				bc.SetFieldValue("CFB_Answer1", decision);
				bc.SaveRecord();

				RWJBusComp bc2 = app.GetBusObject("CFBOutbox").getBusComp("CFBOutbox");
				query.clear();
				query.put("token", token); 
				
				nRecs = bc2.UpsertQuery(query);				
				
				if ( nRecs > 0 ) {
					bc2.SetFieldValue("status", "COMPLETE");
					bc2.SaveRecord();	
				}
								
				BasicDBObject cRec = bc.GetCompositeRecord();
				JSONObject tenantObj = getTenant(null);
				Mail m = new Mail(this);
				String customerFullName = cRec.getString("contact_fullName");
				JSONObject header = m.header( tenantObj.getString("memberEmailAddress"),
						tenantObj.getString("domainName"), tenantObj.getString("memberEmailAddress"),
						tenantObj.getString("domainName"),
						getMessage("CFBRESPONSE_EMAILTEMPLATE_SUBJECT",customerFullName ), 
								cRec.getString("from_emailAddress") );					
				JSONObject emailData = new JSONObject();
				emailData.put("memberFirstName", bc.GetFieldValue("from_firstName"));
				emailData.put("customerFullName", bc.GetFieldValue("customerFullName"));
				emailData.put("loginEmail", cRec.getString("from_emailAddress"));
				
				m.SendHtmlMailMessage(header, getMessage("ET_CFBRESPONSE"), emailData, null);
				
			}
			return true;
		}
		else {
			String errorMessage = getMessage("RECORD_NOTFOUND"); 
			log.debug( errorMessage);
			throw new Exception(errorMessage);
		}
	}
	

	/*
	 * An Invitation Referral is sent to one or two contacts.
	 * One contact Case - User initiate an invitation from Contacts Screen, to invite him to referralwire network.
	 * Two contact case - User Initiates on invitation from Members or Contacts screen to invite two contacts to connect on the referralwire network.
	 * data: fromId : Originator of the invitation, toId : first Recipient of the invitation, 2toId : second recipient of the invitation
	 *  comments: additional comments to the invitation 
	 */

	@SuppressWarnings("unchecked")
	@RWAPI
	public JSONObject ProcessInvitationReferral(JSONObject data) throws Exception {
		log.trace( "InvitationReferral");

		RWJBusComp bc = createInvitationReferral(data); 
		String refid = bc.currentRecord.get("_id").toString();
				
		data.put("id", refid);
		data.put("fromId", bc.currentRecord.get("fromId").toString());
		SendInvitationReferral(data.getString("toId"), data);
		if (data.has("toId2")) 
			SendInvitationReferral(data.getString("toId2"), data);
		
	    JSONObject retVal = new JSONObject();
		BasicDBObject cRec = bc.GetCompositeRecord();
		retVal.put("data", StringifyDBObject(cRec));
		return retVal;
	}

	/*
	 * An Invitation Referral is sent to one or two contacts.
	 * One contact Case - User initiate an invitation from Contacts Screen, to invite him to referralwire network.
	 * Two contact case - User Initiates on invitation from Members or Contacts screen to invite two contacts to connect on the referralwire network.
	 * data: fromId : Originator of the invitation, toId : first Recipient of the invitation, 2toId : second recipient of the invitation
	 *  comments: additional comments to the invitation 
	 */
	public RWJBusComp createInvitationReferral(JSONObject data) {
	
		try {

			RWJBusComp bc = app.GetBusObject("Referral").getBusComp("Referral");
			BasicDBObject query = new BasicDBObject();
			String party_id = data.isNull("fromId") ? GetPartyId() : data.getString("fromId");
			BasicDBObject partyObj = GetParty(party_id);
			
			String toId = data.get("toId").toString();

			bc.NewRecord();
			bc.SetFieldValue("fromId", party_id );
			bc.SetFieldValue("status", "UNREAD");
			String OrgId = (String)GetPartyAttr(party_id, "OrgId");
			bc.SetFieldValue("fromBusinessId", OrgId);
			BasicDBObject toParty = GetParty(toId);
			bc.SetFieldValue("toId", toId);
			

			if ( data.has("toId2")) {
				String toId2 = data.get("toId2").toString();
				bc.SetFieldValue("toId2", toId2);
			}

			bc.SetFieldValue("from_profession", GetPartyAttr(party_id, "profession"));
			bc.SetFieldValue("statusChangeDate", new Date());
			bc.SetFieldValue("referralSubType", "P2P"); 

			super.create(data);
			bc.SaveRecord(data);

			JSONObject trackingData = new JSONObject();
			trackingData.put("event", "UM_REFERRAL");         			
			trackMetrics(trackingData);

			return bc;

		} catch (JSONException e) {
			// TODO Auto-generated catch block
			log.debug("API Error: ", e);
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			log.debug("API Error: ", e);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			log.debug("API Error: ", e);
		}
		return null;
	}

	public void SendInvitationReferral(String toId, JSONObject data)
	{
		if ( toId == null ) return;
		
		try {
			BasicDBObject toParty = GetParty(toId);
			String fromId = data.has("fromId") ? data.getString("fromId") : GetPartyId();
			BasicDBObject fromParty = GetParty(fromId);
		
			String partyType = toParty.getString("partytype");
			String referralType = data.getString("referralType");
			String emailSubjectId = referralType.equals("PART_INVITE") ? "OON_CUST_INVITATION_SUBJECT": "OON_PART_INVITATION_SUBJECT";
	    	String subject = getMessage(emailSubjectId, fromParty.getString("fullName"));
			String message = null;
			Mail m = new Mail(this);
			JSONObject tenantObj = getTenant(null);

			JSONObject header = m.header( tenantObj.getString("invitationsEmailAddress"),
					tenantObj.getString("pageTitle"), fromParty.getString("emailAddress"), fromParty.getString("fullName"),
					subject,toParty.getString("emailAddress"), null );					

			String personalMessage = data.has("comments") ? data.getString("comments") : getMessage("MSG_DEFAULTCOMMENT");
			personalMessage = personalMessage.replace("\n", "<br>");
			data.put("personalMessage", personalMessage);
			data.put("fromId", fromId);
	    	data.put("fromOrgId", fromParty.getString("OrgId"));
	    	data.put("fromFullName", fromParty.getString("fullName"));
	    	data.put("fromFirstName", fromParty.getString("firstName"));
	    	data.put("fromEmailAddress", fromParty.getString("emailAddress"));
	    	data.put("fromWorkPhone", fromParty.getString("workPhone"));
	    	data.put("fromJobTitle", fromParty.getString("jobTitle"));
	    	data.put("fromCompany", fromParty.getString("employerName"));	    	
	    	data.put("fromChapter", fromParty.getString("org_businessName"));	    	
	    	data.put("to_firstName", toParty.getString("firstName"));				
	    	data.put("to_lastName", toParty.getString("lastName"));
	    	data.put("invitationType", referralType);
	    	data.put("to_emailAddress", toParty.getString("emailAddress"));
	    	data.put("InviteeId", toId);
			
	    	if ( partyType.equals("CONTACT")) {
				String token = UUID.randomUUID().toString().replace('-', 'X');
				data.put("token", token );
				jedisMt.set(token, data.toString());				
				long expireat = System.currentTimeMillis() + 7200 * 60000;
				jedisMt.expireAt(token, expireat);
			}
			
			m.SendHtmlMailMessage(header, 					
					partyType.equals("CONTACT") ? getMessage("ET_INVITATION_REQUEST") : getMessage("ET_CONNECTION_REQUEST"),
					data, null); 
			
			JSONObject header2 = m.header( tenantObj.getString("SupportEmailAddress"), 
					tenantObj.getString("pageTitle"), tenantObj.getString("SupportEmailAddress"), tenantObj.getString("pageTitle"),
					getMessage("STNCONNECT_CONFIRM"),fromParty.getString("emailAddress"), null);

			m.SendHtmlMailMessage(header2, 
						partyType.equals("CONTACT") ? getMessage("ET_INVITATION_CONFIRM") : getMessage("ET_CONNECTION_CONFIRM"),
						data, null); 				
			
			JSONObject trackingData = new JSONObject();
			trackingData.put("event", "UM_SENTINVITATION");         			
			trackingData.put("details", toParty.getString("emailAddress"));         			
			trackMetrics(trackingData);
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			log.debug("API Error: ", e);
		} catch (JSONException e) {
			// TODO Auto-generated catch block
			log.debug("API Error: ", e);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			log.debug("API Error: ", e);
		}
	}
	
	@SuppressWarnings("unchecked")
	@RWAPI
	public JSONObject SocialInvitaton(JSONObject data) throws Exception {
		log.trace( "SocialInvitaton");

		String myId = data.getString("socialId"); // my social id
		String myConnectionId = data.getString("socialConnectionId"); // my social connection's id
		String invitationType = data.getString("invitationType");
		
		String invitationId = invitationType + myConnectionId;				
		
		BasicDBObject fromParty = GetParty(null);
		
		data.put("fromId", fromParty.getString("_id"));
    	data.put("fromOrgId", fromParty.getString("OrgId"));
    	data.put("fromFullName", fromParty.getString("fullName"));
		
        JSONObject retVal = new JSONObject();

		jedisMt.set(invitationId, data.toString());				
		data.put("token", invitationId);
		retVal.put("data", data);		

		return retVal;
	}
	
	@SuppressWarnings("unchecked")
	@RWAPI
	public JSONObject SocialInvitatonSent(JSONObject data) throws Exception {
		
		log.trace( "SocialInvitatonSent");
		
		String status = data.getString("status");
		String myConnectionId = data.getString("socialConnectionId"); // my social connection's id
		String invitationType = data.getString("invitationType");
		String invitationId = invitationType + myConnectionId;				
		
		if (status.equals("SUCCESS")) {
			long expireat = System.currentTimeMillis() + 7200 * 60000;  // expire in 5 days
			jedisMt.expireAt(invitationId, expireat);
			
			JSONObject trackingData = new JSONObject();
			trackingData.put("event", "UM_SENTINVITATION");         			
			trackingData.put("details", invitationType + ":" + myConnectionId);         			
			trackMetrics(trackingData);
		}
		else {
			jedisMt.del(invitationId);
		}
		return data;
	}

	@SuppressWarnings("unchecked")
	@RWAPI
	public JSONObject ProcessSocialReferral(JSONObject data) throws Exception {
	
		RWJBusComp bc = app.GetBusObject("Referral").getBusComp("Referral");
		
		BasicDBObject query = new BasicDBObject();
		String fromId = data.getString("fromId");
		String toId = data.getString("toId");
		bc.NewRecord();
		bc.SetFieldValue("fromId", fromId );
		bc.SetFieldValue("status", "ACCEPTED");
		bc.SetFieldValue("referralType", "PART_INVITE");
		bc.SetFieldValue("referralSubType", data.getString("referralType")); 
		data.remove("referralType"); 
		bc.SetFieldValue("from_profession", GetPartyAttr(fromId, "profession"));
		bc.SetFieldValue("to_profession", GetPartyAttr(toId, "profession"));
		bc.SetFieldValue("fromBusinessId", GetPartyAttr(fromId, "OrgId"));
		bc.SetFieldValue("statusChangeDate", new Date());
	
		super.create(data);
		
		bc.SaveRecord(data);


		JSONObject data_in = new JSONObject();
		data_in.put("partyId", fromId);
		updateStatExp("totalInvited", "totalInvited", "Party",  fromId,data_in);

		JSONObject trackingData = new JSONObject();
		trackingData.put("event", "UM_REFERRAL");         			
		trackMetrics(trackingData);

		
		BasicDBObject cRec = bc.GetCompositeRecord();
		JSONObject retVal = new JSONObject() ;
		retVal.put("data", StringifyDBObject(cRec));
		return retVal;
		
	}
	
	public JSONObject publishProspectLeaderBoard(JSONObject data) throws Exception {
		JSONObject retVal = new JSONObject() ;		
		try {
			Mail m = new Mail(this);
			JSONObject tenantObj = getTenant(null);
			RWJBusComp bc = app.GetBusObject("Party").getBusComp("Party");
			BasicDBObject query = new BasicDBObject();
			query.put("partytype", "PARTNER");
			
			BasicDBObject sort = new BasicDBObject();
			sort.put("lastName", 1);
			
			int nRecs = bc.ExecQuery(query, sort);
			log.info( "publishProspectLeaderBoard Email will be sent to  " + String.valueOf(nRecs) + " members");
	
			int j = 0; 
			for ( int i = 0; i < nRecs ; i++) {
				JSONObject header = m.header( tenantObj.getString("AdminEmailAddress"), 
					tenantObj.getString("pageTitle"), tenantObj.getString("SupportEmailAddress"), tenantObj.getString("pageTitle"),
					getMessage("LEADERBOARD_SUBJECT"), bc.get("emailAddress").toString(), null);
				m.SendHtmlMailMessage(header, getMessage("ET_PROSPECT_LEADERBOARD"), data, null); 	
				log.info( "publishProspectLeaderBoard Email Sent to " + bc.get("emailAddress").toString());
				bc.NextRecord();
				j++;
			}
			log.info( "publishProspectLeaderBoard Email has been sent to  " + String.valueOf(j) + " members");					
			
			/*
			RWChartItemData d = app.GetChartObject("ProspectsLeaderBoard");
			BasicDBList dl = d.getSeries(data);
			if ( dl.size() > 0 ) {
				retVal.put("data", StringifyDBList(dl));
			}
			*/
			
		} catch (JSONException e) {
			// TODO Auto-generated catch block
			log.debug(e.getStackTrace());
		} catch (Exception e) {
			// TODO Auto-generated catch block
			log.debug(e.getStackTrace());
		}
		return retVal;
	}
	
	
	public void cron() {
		try {
			JSONObject data = new JSONObject();
			data.put("id", "dummy");
			log.info( "publishProspectLeaderBoard Cron Job Started");
			String publishLeaderBoardStr = jedisMt.get("publishProspectLeaderBoard");
			log.info( "publishProspectLeaderBoard Trigger : " + publishLeaderBoardStr);
			if ( publishLeaderBoardStr == null || publishLeaderBoardStr.equals("Yes") ) {
				publishProspectLeaderBoard(data);
				jedisMt.set("publishProspectLeaderBoard", "No");
			}			
		}
		catch( Exception e) {
			log.debug(e.getStackTrace());
			log.info( "publishProspectLeaderBoard Cron Ended with an exception");
		}
		log.info( "publishProspectLeaderBoard Cron Ended Successfulluy");

	}
}
