package com.rw.API;

import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;

import org.apache.log4j.Logger;
import org.bson.types.ObjectId;
import org.json.JSONObject;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBRef;
import com.rw.persistence.RWJApplication;
import com.rw.persistence.RWJBusComp;
import com.rw.persistence.RWJBusObj;
import com.rw.persistence.RWObjectMgr;
import com.rw.persistence.mongoStore;
import com.rw.repository.Permissions;
import com.rw.repository.RWAPI;
import com.rw.repository.Roles;


public class HomePageMgr extends RWReqHandle  {
	static final Logger log = Logger.getLogger(HomePageMgr.class.getName());
	
	public HomePageMgr() {
	}
	
	public HomePageMgr(String userid) {
		super(userid);
	}
	
	public HomePageMgr(RWObjectMgr context) throws Exception {
		super(context);
	}
	
	@RWAPI
	public JSONObject phoneread(JSONObject data) throws Exception {
		log.trace( "read");
		JSONObject retVal = new JSONObject() ;
		
		UserMgr um = new UserMgr(this);
		data.put("act", "read");
		retVal.put("userdata", um.handleRequest(data));
		
		RfrlMgr rm = new RfrlMgr(this);
		data.put("act", "NewReferralsRcvd");
		retVal.put("NewReferralsRcvd", rm.handleRequest(data));
		
		data.put("act", "NewReferralsSent");
		retVal.put("NewReferralsSent", rm.handleRequest(data));

		SecMgr sm = new SecMgr(this);
		retVal.put("routes", StringifyDBList(sm.getViews()));		
		return retVal;
	}

	@RWAPI
	@Roles( {"MEMBER"} )
	public JSONObject read(JSONObject data) throws Exception {
		log.trace( "read");
		JSONObject retVal = new JSONObject() ;
		
		UserMgr um = new UserMgr(this);
		data.put("act", "read");
		retVal.put("userdata", um.handleRequest(data));
		
		RfrlMgr rm = new RfrlMgr(this);
		AnalyticsMgr alm = new AnalyticsMgr(this);
		/*
		data.put("act", "NewReferralsRcvd");
		retVal.put("NewReferralsRcvd", rm.handleRequest(data));
		
		data.put("act", "NewReferralsSent");
		retVal.put("NewReferralsSent", rm.handleRequest(data));
		
		data.put("act", "LastTwoWeeksReferralsRcvd");
		retVal.put("LastTwoWeeksReferralsRcvd", rm.handleRequest(data));
		
		data.put("act", "LastTwoWeeksReferralsSent");
		retVal.put("LastTwoWeeksReferralsSent", rm.handleRequest(data));
		*/
		//data.put("invitedBy_Id", GetPartyId());
		//data.put("act", "newMembersInvitedByMe");
		data.put("act", "NewMembersLastSeven");
		JSONObject result = alm.handleRequest(data);
		
		retVal.put("NewMembersLastSeven", result);
		
		data.put("act", "NewPowerPartners");
		JSONObject userdata = (JSONObject)retVal.get("userdata");
		String ds = userdata.getString("data");
		JSONObject d2 = new JSONObject(ds);
		String pp = "";
		if (d2.has("powerpartner1")){
			pp = d2.getString("powerpartner1");
		}
		data.put("powerpartner1", pp);
		result = alm.handleRequest(data);
		retVal.put("NewPowerPartners", result);
		//retVal.put("LastTwoWeeksMembersInvitedByMe", alm.handleRequest(data));
		SecMgr sm = new SecMgr(this);
		
		data.put("act", "NewUsersCount");
		retVal.put("NewUsersCount", sm.handleRequest(data));
	
		retVal.put("routes", StringifyDBList(sm.getViews()));
		
		ActivityStreamMgr am = new ActivityStreamMgr(this);
		am.cloneExecutionContext(this);
		
		data.put("act", "NewMsgCount");
		retVal.put("NewMsgCount", am.handleRequest(data));
		
		return retVal;
	}
	
	@RWAPI
	public JSONObject ProfileRead(JSONObject data) throws Exception {
		log.trace( "ProfileRead");
		JSONObject retVal = new JSONObject() ;
		
		UserMgr um = new UserMgr(this);
		data.put("act", "read");
		retVal.put("userdata", um.handleRequest(data));
		
		RfrlMgr rm = new RfrlMgr(this);
		data.put("act", "PartnerTransactions");
		
		if ( data.isNull("period") )
			data.put("period", "262974");
		
		retVal.put("PartnerTransactions", rm.handleRequest(data));

		data.put("act", "ReferralActivity");
		retVal.put("ReferralActivity", rm.handleRequest(data));
		
		data.put("act", "ReferralsByProfession");
		retVal.put("ReferralsByProfession", rm.handleRequest(data));
		
		return retVal;
	}

	@RWAPI
	public JSONObject query(JSONObject data) throws Exception {
		log.trace( "query");
		return read(data);
	}
	
}
