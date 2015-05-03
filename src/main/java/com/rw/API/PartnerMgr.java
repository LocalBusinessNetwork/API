package com.rw.API;

import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;
import java.text.ParseException;
import java.util.Set;
import java.util.regex.Pattern;

import net.sourceforge.cardme.vcard.VCard;
import net.sourceforge.cardme.vcard.VCardImpl;
import net.sourceforge.cardme.vcard.arch.VCardTypeName;
import net.sourceforge.cardme.vcard.arch.VCardVersion;
import net.sourceforge.cardme.vcard.types.NameType;
import net.sourceforge.cardme.vcard.types.VersionType;

import org.apache.log4j.Logger;
import org.bson.types.ObjectId;
import org.json.JSONException;
import org.json.JSONObject;

import redis.clients.jedis.Jedis;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBRef;
import com.mongodb.QueryBuilder;
import com.rw.Recommender.DupRecommenderModel;
import com.rw.persistence.RWJApplication;
import com.rw.persistence.RWJBusComp;
import com.rw.persistence.RWJBusObj;
import com.rw.persistence.RWObjectMgr;
import com.rw.persistence.mongoStore;
import com.rw.repository.RWAPI;


public class PartnerMgr extends RWReqHandle  {
	static final Logger log = Logger.getLogger(PartnerMgr.class.getName());

	public PartnerMgr() {
	}

	public PartnerMgr(String userid) {
		super(userid);
	}

	public PartnerMgr(RWObjectMgr context) throws Exception {
		super(context);
	}

	@SuppressWarnings("unchecked")
	@RWAPI
	public JSONObject create(JSONObject data) throws Exception {
		log.trace( "create");
		JSONObject retVal = new JSONObject() ;

		RWJBusComp bc = app.GetBusObject("Partner").getBusComp("Partner");
		
		BasicDBObject query = new BasicDBObject();
		query.put("userId", getUserId());

		String partnerId = data.get("partnerId").toString();
		query.put("partnerId", partnerId);

		if (bc.UpsertQuery(query) == 0 ) {
			bc.NewRecord();			
			bc.SetFieldValue("userId", getUserId());
			bc.SetFieldValue("status", "ACTIVE");
			super.create(data);
		
			Long dqInitScore = 2L;	//  each SetFieldValue above counts for dq.		
			if ( !data.has("type")) {
					bc.SetFieldValue("type", "REFERRAL_PARTNER");
					dqInitScore++;
					updateStat("totalConnections","totalConnections","Party",  GetPartyId());
					
			}
			bc.SaveRecord(data);
			
			
			//DupRecommenderModel df = new DupRecommenderModel(userid.toString());
			//df.AddUserDataToModel(bc);
			
		}
		retVal.put("data", StringifyDBObject(bc.GetCompositeRecord()));		
		return retVal;
	}
	
	@SuppressWarnings({ "unchecked" })
	@RWAPI
	public JSONObject update(JSONObject data) throws Exception {
		log.trace( "update");

		super.update(data);

		Object id = data.get("id");
		
		RWJBusComp bc = app.GetBusObject("Partner").getBusComp("Partner");
		
		BasicDBObject query = new BasicDBObject();
		
        query.put("_id", new ObjectId(id.toString())); 

        if (bc.UpsertQuery(query) == 0 ) {
			String errorMessage = getMessage("RECORD_NOTFOUND"); 
			log.debug( errorMessage);
			throw new Exception(errorMessage);
		}
		
		if ( !data.isNull("firstName") || !data.isNull("lastName") ) { 
			String firstNameProper = "";
			String lastNameProper = "";
			if (!data.isNull("firstName")){
				firstNameProper = getProperName(data.getString("firstName"));
			}
			if (!data.isNull("lastName")){
				lastNameProper = getProperName(data.getString("lastName"));
			}
			
			//String fullName =  (!data.isNull("firstName"))?data.getString("firstName").concat(" "):"";
			String fullName = (firstNameProper != "")?firstNameProper.concat(" ").concat(lastNameProper):lastNameProper;
			fullName = fullName.toUpperCase();
			//fullName = (!data.isNull("lastName"))?fullName.concat(data.getString("lastName")):fullName;
			//bc.SetFieldValue("fullName", fullName);
			data.put("firstName",firstNameProper);
			data.put("lastName",lastNameProper);
			data.put("fullName", fullName);
		}
		
		String partytype_denorm = (String)bc.GetFieldValue("partytype_denorm");
		if ( bc.GetFieldValue("type").toString().equals("REFERRAL_CONTACT")) {
			// setup the search spec
			query.clear();
	        query.put("_id", new ObjectId(bc.GetFieldValue("partnerId").toString()));
	        query.put("partytype", "CONTACT"); 
			// Only owner has the right to change the master record
	        query.put("parentId", GetPartyId());

	        RWJBusComp bc2 = app.GetBusObject("Contact").getBusComp("Contact");
			if ( bc2.UpsertQuery(query) == 1 ) {
				partytype_denorm = (String)bc2.GetFieldValue("partytype");
				bc2.SaveRecord(data);
			}
		}
        data.put("partytype_denorm", partytype_denorm);
		bc.SaveRecord(data);
		
		// Update master, if applicable.
		
		
		
		JSONObject retVal = new JSONObject() ;
		retVal.put("data", StringifyDBObject(bc.GetCompositeRecord()));
		return retVal;

	}

	/*
	 * (non-Javadoc)
	 * @see com.rw.API.RWReqHandle#read(org.json.JSONObject)
	 * Fetch case 1) a specific relationship by id
	 * 	case 2) a relationship with a specific partner
	 *  case 3) or text search for a set of partners within your network.
	 */
	@RWAPI
	public JSONObject read(JSONObject data) throws Exception {
		log.trace( "read");
		
		JSONObject retVal = new JSONObject() ;
		
		RWJBusComp bc = app.GetBusObject("Partner").getBusComp(getShapedBC(data,"Partner"));
		
		BasicDBObject query = new BasicDBObject();
		if ( !data.isNull("userId")){query.put("userId", data.getString("userId"));}
		else {query.put("userId", getUserId());}
		//String partnerId = null;
		
		// case 1
		if ( !data.isNull("id")) {
			Object id = data.get("id"); // this id is from rwPartners Document
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
		else if( !data.isNull("partnerId")) {	
			String partnerId = data.getString("partnerId");
			query.put("partnerId",partnerId); 
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
		} // case 3
		else { // request for a set of records, filtered by a criteria
			return textSearch(data);
		}
		return retVal;
	}

	@RWAPI
	public JSONObject delete(JSONObject data) throws Exception {
		log.trace( "delete");

		super.delete(data);

		Object id = data.get("id");
		
        RWJBusComp bc = app.GetBusObject("Partner").getBusComp("Partner");

        BasicDBObject query = new BasicDBObject();
		query.put("userId", getUserId());
        query.put("_id", new ObjectId(id.toString())); 

		if (bc.ExecQuery(query,null) == 0 ) {
			String errorMessage = getMessage("RECORD_NOTFOUND"); 
			log.debug( errorMessage);
			throw new Exception(errorMessage);
		}
		
		String type = (String)bc.get("type");
		if (type.equals("REFERRAL_PARTNER")){
			updateStat("totalConnections","totalConnections","Party", GetPartyId());
		}
	

		
		JSONObject retVal = new JSONObject() ;
		retVal.put("data", StringifyDBObject(bc.GetCompositeRecord()));
		
		//DupRecommenderModel df = new DupRecommenderModel(userid.toString());
		//df.RemoveUserDataFromModel(bc.GetFieldValue("partnerId").toString());

		bc.DeleteRecord();
		return retVal;
	}
	@SuppressWarnings("unchecked")
	
	@RWAPI
	public JSONObject textSearch(JSONObject data) throws Exception {
		log.trace( "textSearch");
		RWJBusComp bc = app.GetBusObject("Partner").getBusComp(getShapedBC(data,"Partner"));
		BasicDBObject query = new BasicDBObject();
		query.put("userId", getUserId());
		JSONObject retVal = new JSONObject() ;
		if ( !data.isNull("searchText")) {
			Pattern text = Pattern.compile(data.get("searchText").toString(), Pattern.CASE_INSENSITIVE);
			QueryBuilder qm = new QueryBuilder().
					or(new QueryBuilder().start().put("fullName").regex(text).get(),
					new QueryBuilder().put("jobTitle").regex(text).get(),
					new QueryBuilder().put("tags").regex(text).get(),
					new QueryBuilder().put("business").regex(text).get());
			query.putAll(qm.get());
		}
		

		BasicDBObject filters = getSearchSpec(data);
		QueryBuilder q2 = new QueryBuilder().and(query,filters);
		BasicDBObject query2 = new BasicDBObject();
		query2.putAll(q2.get());
		query=query2;
		
		if (data.has("excludeId")) {
			BasicDBObject excludeId = new BasicDBObject();
			excludeId.put("$ne", new ObjectId(data.getString("excludeId")));
			query.put("_id", excludeId);
		}

		int skip = !data.isNull("skip") ? Integer.parseInt(data.getString("skip")) : -1;
		int limit = !data.isNull("limit")? Integer.parseInt(data.getString("limit")) :-1;
		int nRecs = bc.ExecQuery(query,getSortSpec(data), limit, skip);
		System.out.println("cum q " + query.toString());
		BasicDBList cList = bc.GetCompositeRecordList();
		retVal.put("data", StringifyDBList(cList));
		return retVal;
	}
	
	/*
	 * partyOne and partyTwo becomes network partners to each other.
	 * Creates bidirectional edge in the relationship graph
	 */
	public void BiDirectionalEdge(String partyOne, String partyTwo) throws Exception {
		log.trace( "BiDirectionalEdge");
			UniDirectionalEdge(partyOne, partyTwo);
			UniDirectionalEdge(partyTwo, partyOne);
	}
	
	/*
	 * PartTwo becomes partOne's network partner 
	 * Creates unidirectional edge in the relationship graph
	 */
	public void UniDirectionalEdge(String partyOne, String partyTwo) throws Exception {
		log.trace( "UniDirectionalEdge");
		
		BasicDBObject p1  = GetParty(partyOne);
		BasicDBObject p2 = GetParty(partyTwo);

		RWJBusComp partnerBC = app.GetBusObject("Partner").getBusComp("Partner");
		BasicDBObject query = new BasicDBObject();
		
		query.put("userId", p1.getString("credId"));
		query.put("partnerId", partyTwo);

		if ( partnerBC.UpsertQuery(query) > 0 ) {
			try {
				
				if ( partnerBC.GetFieldValue("type").toString().equals("REFERRAL_PARTNER")) return;
				
				partnerBC.SetFieldValue("type", "REFERRAL_PARTNER");
				partnerBC.SetFieldValue("status", "ACTIVE");
				partnerBC.SetFieldValue("fullName_prv", p2.getString("fullName"));//denorming full name so we can sort on it
				partnerBC.SaveRecord();
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				log.debug("API Error: ", e);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				log.debug("API Error: ", e);
			}
		}
		else {

				Set<String> keys = partnerBC.getEmptyRecord().keySet();
				partnerBC.NewRecord();
				JSONObject data = new JSONObject();
				
				data.put("userId",p1.getString("credId"));
				data.put("partnerId",partyTwo);
				data.put("type", "REFERRAL_PARTNER");
				data.put("status", "ACTIVE");

				for ( String key : keys) {
					if ( p2.containsField(key) )
						data.put(key, p2.get(key));
				}
				
				try {
					partnerBC.SaveRecord(data);
				} catch (ParseException e) {
					// TODO Auto-generated catch block
					log.debug("API Error: ", e);
				}
			}
		
			JSONObject trackingData = new JSONObject();
			trackingData.put("event", "UM_ADDCONNECTION");         			
			trackMetrics(trackingData);

	}

	public void BuildNetworkGraph() throws Exception {
		RWJBusComp bc = app.GetBusObject("Partner").getBusComp("Partner");
		BasicDBObject query = new BasicDBObject();
		query.put("type", "REFERRAL_PARTNER");
		
		int nRecs = bc.ExecQuery(query,null);
		for ( int i = 0; i < nRecs ; i++) {
			String userId = bc.GetFieldValue("UserId").toString();
			String fromId = GetPartyIdFromUid(bc.GetFieldValue("userId").toString());
			String toId = bc.GetFieldValue("partnerId").toString();
			jedisMt.sadd2(fromId, toId);
			bc.NextRecord();
		}
	}
	
	public void ExportVCards() throws Exception {
		
		RWJBusComp bc = app.GetBusObject("Partner").getBusComp("Partner");
		BasicDBObject query = new BasicDBObject();
		query.put("userId", getUserId());
		
		int nRecs = bc.ExecQuery(query);

		for ( int i = 0; i < nRecs ; i++) {
			
			VCard vcard = new VCardImpl();
			vcard.setVersion(new VersionType(VCardVersion.V3_0));
			NameType name = new NameType();
			name.setName("John Doe");
			vcard.setName(name);

		}
	}
	
	
	
}
