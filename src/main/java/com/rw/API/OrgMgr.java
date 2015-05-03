package com.rw.API;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;
import java.text.ParseException;
import java.util.Date;
import java.util.regex.Pattern;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.log4j.Logger;
import org.bson.types.ObjectId;
import org.json.JSONException;
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
import com.rw.repository.Permissions;
import com.rw.repository.RWAPI;
import com.rw.repository.Roles;


@SuppressWarnings("unused")
public class OrgMgr extends RWReqHandle  {
	static final Logger log = Logger.getLogger(OrgMgr.class.getName());

	public OrgMgr() {
	}
	
	public OrgMgr(String userid) {
		super(userid);
	}
	
	public OrgMgr(RWObjectMgr context) throws Exception {
		super(context);
	}

	
	@SuppressWarnings("unchecked")
	@RWAPI
	@Roles({"ADMINISTRATOR"})
	public JSONObject create(JSONObject data) throws Exception {
		log.trace( "create Organization");
		
		RWJBusComp bc = app.GetBusObject("Organization").getBusComp("Organization");
		
		// How we do find the duplicate Orgs ?
		
		if (data.has("dateFounded")){
			long FoundedLong = getLongForDate(data,"dateFounded");
			data.put("dateFounded",FoundedLong);
		}
		
		if (data.has("meetingHour")){
			//long meetingHour = getLongForTime(data,"meetingHour");
			//data.put("meetingHour",meetingHour);
		}	
		
		bc.NewRecord();
		bc.SetFieldValue("credId", getUserId());
		bc.SetFieldValue("partytype", "BUSINESS");
		super.create(data);
		bc.SaveRecord(data);
		JSONObject retVal = new JSONObject() ;

		String OrgId = bc.GetFieldValue("_id").toString();
		
		EnrichParty(OrgId);
		SetAmbassadorHomeChapter(bc);
		retVal.put("data", StringifyDBObject(bc.GetCompositeRecord()));		
		
		JSONObject trackingData = new JSONObject();
		trackingData.put("event", "UM_CREATEORG");         			
		trackMetrics(trackingData);

		return retVal;
	}
	/*
	@SuppressWarnings({ "unchecked", "unchecked" })
	@RWAPI
	public JSONObject read(JSONObject data) throws Exception {
		log.trace( "read organization");

		JSONObject retVal = new JSONObject() ;
		
		RWJBusComp bc = app.GetBusObject("Organization").getBusComp("Organization");
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
			query.put("partytype", "BUSINESS");
			// Add the additional query criteria
			query.putAll(getSearchSpec(data).toMap());
			int nRecs = bc.ExecQuery(query,getSortSpec(data));
			retVal.put("data", StringifyDBList(bc.GetCompositeRecordList()));
		}
		return retVal;
	}
	*/
	@SuppressWarnings({ "unchecked", "unchecked" })
	@RWAPI
	public JSONObject read(JSONObject data) throws Exception {
		log.trace( "read");
		
		JSONObject retVal = new JSONObject() ;
		
		// case 1
		if ( !data.isNull("id")) {
			RWJBusComp bc = app.GetBusObject("Organization").getBusComp(getShapedBC(data,"Organization"));
			BasicDBObject query = new BasicDBObject();
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
		} // case 2
		
		else { // request for a set of records, filtered by a criteria
			return textSearch(data);
		}
		return retVal;
	}
	
	@RWAPI
	public JSONObject textSearch(JSONObject data) throws Exception {
		log.trace( "textSearch");
		RWJBusComp bc = app.GetBusObject("Organization").getBusComp(getShapedBC(data,"Organization"));
		BasicDBObject query = new BasicDBObject();
		//begin new
		JSONObject retVal = new JSONObject();
		query.put("partytype", "BUSINESS");
		query.putAll(getSearchSpec(data).toMap());
		if ( !data.isNull("searchText")) {
			Pattern text = Pattern.compile(data.get("searchText").toString(), Pattern.CASE_INSENSITIVE);
			QueryBuilder qm = new QueryBuilder().
					or(new QueryBuilder().start().put("businessName").regex(text).get(),
					new QueryBuilder().put("ambassador_fullName").regex(text).get(),
					new QueryBuilder().put("cityAddress_work").regex(text).get(),
					new QueryBuilder().put("postalCodeAddress_work").regex(text).get());
			query.putAll(qm.get());
		}
		
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
		
		/*
		 //ORIGINAL
		JSONObject retVal = new JSONObject() ;
		if ( !data.isNull("searchText")) {
			Pattern text = Pattern.compile(data.get("searchText").toString(), Pattern.CASE_INSENSITIVE);
			QueryBuilder qm = new QueryBuilder().
					or(new QueryBuilder().start().put("businessName").regex(text).get(),
					new QueryBuilder().put("ambassador_fullName").regex(text).get(),
					new QueryBuilder().put("cityAddress_work").regex(text).get(),
					new QueryBuilder().put("postalCodeAddress_work").regex(text).get());
			query.putAll(qm.get());
		}
				
		//query.put("partytype", "BUSINESS");
		BasicDBObject filters = getSearchSpec(data);
		QueryBuilder q2 = new QueryBuilder().and(query,filters);
		BasicDBObject query2 = new BasicDBObject();
		query2.putAll(q2.get());
		query=query2;
		
		int skip = !data.isNull("skip") ? Integer.parseInt(data.getString("skip")) : -1;
		int limit = !data.isNull("limit")? Integer.parseInt(data.getString("limit")) :-1;
		int nRecs = bc.ExecQuery(query,getSortSpec(data), limit, skip);
		BasicDBList cList = bc.GetCompositeRecordList();
		retVal.put("data", StringifyDBList(cList));
		return retVal;
		*/
		
		/*
		 * query.putAll(getSearchSpec(data).toMap());			
		 * if ( !data.isNull("searchText")) {
				Pattern text = Pattern.compile(data.get("searchText").toString());
				QueryBuilder qm = new QueryBuilder().
						or(new QueryBuilder().start().put("fullName").regex(text).get(),
						new QueryBuilder().put("jobTitle").regex(text).get(),
						new QueryBuilder().put("tags").regex(text).get(),
						new QueryBuilder().put("business").regex(text).get(),
						new QueryBuilder().put("speakingTopics").regex(text).get());
				
				query.putAll(qm.get());
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
		
		if (data.has("meetingHour")){
			System.out.println("meetingHour = " + data.get("meetingHour"));
			//long meetingHour = getLongForTime(data,"meetingHour");
			//long meetingHour = getLongForDate(data,"meetingHour");
			//data.put("meetingHour",meetingHour);
		}	
		super.update(data);	
		
		Object id = data.get("id");

		JSONObject retVal = new JSONObject() ;
		RWJBusComp bc = app.GetBusObject("Organization").getBusComp("Organization");
		
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
		SetAmbassadorHomeChapter(bc);
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
		RWJBusComp bc = app.GetBusObject("Organization").getBusComp("Organization");
		
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
	public void SetAmbassadorHomeChapter(RWJBusComp OrgBC) throws Exception{ 
	
		String OrgId = OrgBC.GetFieldValue("_id").toString();
		String AmbassadorId = (String)OrgBC.GetFieldValue("ambassadorId");
		
		
		RWJBusComp partybc = app.GetBusObject("Party").getBusComp("Party");
		BasicDBObject query = new BasicDBObject();
		query.put("_id", new ObjectId(AmbassadorId));
		String oldOrgId = null;
		
		if (partybc.UpsertQuery(query) > 0 ) {
			oldOrgId = (String)partybc.GetFieldValue("OrgId");
			partybc.SetFieldValue("OrgId", OrgId);
			partybc.SaveRecord();
			String strParty = StringifyDBObject(partybc.GetCompositeRecord());
			Cache(AmbassadorId, strParty);
		}
		
		if (oldOrgId != null){
			
			JSONObject payload = new JSONObject();
			payload.put("oldOrgId", oldOrgId);
			payload.put("id", AmbassadorId);
			payload.put("OrgId",OrgId);			
			
			pushToBackground("addAmbassadorToMeetings", payload);
		}
	}
	

	
	@SuppressWarnings("unchecked")
	@RWAPI
	public JSONObject addAmbassadorToMeetings(JSONObject data) throws Exception {
		log.trace( "addAmbassadorToMeeting - clean old meetings");
		System.out.println("addAmbassadorToMeeting");
		UserMgr um = new UserMgr(this);
		data.put("act", "addUserToMeeting");
		
		try {
			data = um.handleRequest(data);
		} catch (Exception e) {
			log.debug("DemoSetup Error: ", e);
		}
		
		return data;
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
			query.put("partytype", "BUSINESS");
			int nRecs = bc.UpsertQuery(query);
			if ( nRecs > 0 ) {
				
				JSONObject body = new JSONObject();
				String webRoot = System.getProperty("WebRoot");
				log.info("WebRoot : " + webRoot);
				if ( webRoot == null || webRoot.isEmpty()) {
					webRoot = new String ("/var/lib/tomcat7/webapps/ROOT");
				}
				
				data.put("businessName", bc.GetFieldValue("businessName").toString());
				data.put("greeting", bc.GetFieldValue("greeting").toString());
				data.put("postalCodeAddress_work", bc.GetFieldValue("postalCodeAddress_work").toString());
				
				JSONObject tenantObj = getTenant(null);
				
				data.put("domainName", tenantObj.getString("domainName"));
				data.put("tenant", tenantObj.getString("tenant"));
				data.put("TrackingID", tenantObj.getString("GoogleAnalyticsPropertyCode"));
				
				String htmlString = vmSnippet(System.getProperty("WebRoot"), getMessage("ORG_PUBLICPROFILE"), data).getString("snippet");
				
				String existingBucketName = tenantObj.getString("publicProfilesBaseUrl") + "." + tenantObj.getString("domainName") ;
                String fileName = partyId + ".html";
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

}
