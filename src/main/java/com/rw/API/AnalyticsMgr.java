package com.rw.API;

import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import org.apache.log4j.Logger;
import org.bson.types.ObjectId;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.mongodb.AggregationOutput;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.DBRef;
import com.mongodb.QueryBuilder;
import com.rw.persistence.RWChartItemData;
import com.rw.persistence.RWJApplication;
import com.rw.persistence.RWJBusComp;
import com.rw.persistence.RWJBusObj;
import com.rw.persistence.RWObjectMgr;
import com.rw.persistence.mongoStore;
import com.rw.repository.RWAPI;


@SuppressWarnings("unused")
public class AnalyticsMgr extends RWReqHandle  {
	static final Logger log = Logger.getLogger(AnalyticsMgr.class.getName());
	private final String[] validParties = {"PARTNER","CONTACT"} ;

	public AnalyticsMgr() {
	}
	
	public AnalyticsMgr(String userid) {
		super(userid);
	}

	public AnalyticsMgr(RWObjectMgr context) throws Exception {
		super(context);
	}

	@RWAPI
	public JSONObject query(JSONObject data) throws Exception {
		log.trace( "query");
		return read(data);
	}
	

		/*
		 * 		bc:'Party'
		 * 		matchExpr:[{InvitedById:{}],
		 * 		
				categoryField:'invitedBy_fullName',
            	series:[{name:'invitedBy',description:{fieldName:'id',aggregateFunction:'count'}}],
            	sortByType:'series',
            	sortBy:'numReferrals',
            	sortOrder:'DESC',
            	limit:5
            	
            	
		
		
				SELECT TOP 5 
				INV_PARTY.ID,
				INV_PARTY.FULL_NAME,
				COUNT(ACPT_PARTY.ID) TTL,
				
				FROM
				PARTY INV_PARTY JOIN PARTY ACPT_PARTY ON INV_PARTY.ID = ACPT_PARTY.INVITED_BY_ID
				GROUP BY INV_PARTY.ID,INV_PARTY.FULL_NAME
				WHERE INV_PARTY.ORG_ID = [THIS_CHAPTER]
				ORDER BY COUNT(ACPT_PARTY.ID) DESC
		
		*/
		
		/*
		mongoStore s = new mongoStore();
    	//db.rwReferral.update( {subject: "past" }, { $set: { rw_created_on:new ISODate("2012-01-10") } }, { multi: true } )
		
		String OrgId = "foo";
		DBCollection members = s.getColl("rwParty");
		BasicDBObject match = new BasicDBObject("$match", new BasicDBObject("OrgId", OrgId) ); //need to add date constraint
		
		BasicDBObject fields = new BasicDBObject("_id",0);
		fields.put("invitedBy_Id",1);
		fields.put("invitedBy_FullName",1);
		BasicDBObject project = new BasicDBObject("$project",fields);
		
		BasicDBObject groupByFields = new BasicDBObject("invitedBy_Id","$invitedBy_Id");
		groupByFields.put("invitedBy_FullName","$invitedBy_FullName");
		BasicDBObject groupById = new BasicDBObject("_id",groupByFields);
		
		groupById.put("total",new BasicDBObject("$sum",1));
		BasicDBObject group= new BasicDBObject("$group",groupById);

		AggregationOutput output = members.aggregate(match,project,group);
		//members.aggregate({"$match":{"OrgId":"foo"}},{"$group":{"_id":{"invitedBy_Id":$invitedBy_Id,"invitedBy_FullName":$invitedBy_FullName},"total":{ $sum : 1 },}}
	
		return retVal;
		*/
	

	
	@SuppressWarnings("unchecked")
	@RWAPI
	public JSONObject mostInvitesInChapter1(JSONObject data) throws Exception {
		log.trace( "InvitedBy");

		JSONObject retVal = new JSONObject() ;
		RWJBusComp bc = app.GetBusObject("Party").getBusComp("Party");
		
		BasicDBObject match = new BasicDBObject();
		String invitedBy_OrgId = data.getString("masterRecordId");  
		
		//QueryBuilder qm = new QueryBuilder().and(new QueryBuilder().start().put("invitedBy_OrgId").is(invitedBy_OrgId).get(),
		//		new QueryBuilder().put("toId2").is(party_id).get());
		
		long EightWeeksAgo = 20160 * 4; //minutes in last eight weeks
		Date compareToDate = new Date(System.currentTimeMillis() - EightWeeksAgo * 60000);
		String dateCompareField = "joinedFullDate";

		QueryBuilder qm = new QueryBuilder().and(new QueryBuilder().start().put(dateCompareField).greaterThanEquals(compareToDate).get(),
				new QueryBuilder().start().put("invitedBy_OrgId").is(invitedBy_OrgId).get(),new QueryBuilder().start().put("partytype").is("PARTNER").get());
		
		match.putAll(qm.get());
		
		
		//TODO: add more matching params from data
		
		BasicDBObject groupByFields = new BasicDBObject("invitedBy_Id","$invitedBy_Id");
		groupByFields.put("invitedBy_FullName","$invitedBy_FullName");
		BasicDBObject groupById = new BasicDBObject("_id",groupByFields);
		
		groupById.put("total",new BasicDBObject("$sum",1));
		BasicDBObject group= new BasicDBObject("$group",groupById);
		
		AggregationOutput output = bc.ExecAggregate(match, null,group, null); 
		BasicDBList outputList = new BasicDBList();
		for ( DBObject o : output.results() ) {
			outputList.add(o);
		}
		
		retVal.put("data", StringifyDBList(outputList));

		return retVal;
	}
	
	@SuppressWarnings("unchecked")
	@RWAPI
	public JSONObject mostOnlineInvitesInChapter(JSONObject data) throws Exception {

		log.trace( "mostInvitesInChapter");

		JSONObject retVal = new JSONObject() ;
		RWJBusComp bc = app.GetBusObject("Party").getBusComp("Party");
		
		
		BasicDBObject match = new BasicDBObject();
		String invitedBy_OrgId = data.getString("masterRecordId");  
		
		//QueryBuilder qm = new QueryBuilder().and(new QueryBuilder().start().put("invitedBy_OrgId").is(invitedBy_OrgId).get(),
		//		new QueryBuilder().put("toId2").is(party_id).get());
		
		long EightWeeksAgo = 20160 * 4; //minutes in last eight weeks
		Date compareToDate = new Date(System.currentTimeMillis() - EightWeeksAgo * 60000);
		String dateCompareField = "joinedFullDate";

		QueryBuilder qm = new QueryBuilder().and(new QueryBuilder().start().put(dateCompareField).greaterThanEquals(compareToDate).get(),
				new QueryBuilder().start().put("invitedBy_OrgId").is(invitedBy_OrgId).get());
		
		//match.put("invitedBy_OrgId", invitedBy_OrgId);
		match.putAll(qm.get());
		match.put("partytype","PARTNER");

		BasicDBObject group = new BasicDBObject();
		group.put("_id", "$invitedBy_Id");
		
		BasicDBObject counter = new BasicDBObject();
		counter.put("$sum", 1);
		group.put("count",counter);
		
		BasicDBObject sort = new BasicDBObject();
		sort.put("count", -1);

		AggregationOutput output = bc.ExecAggregate(match, null,group, sort); 
		BasicDBList outputList = new BasicDBList();
		int limit = 5;
		int recordCount = 0;
		for ( DBObject o : output.results() ) {
			String party_id = o.get("_id").toString();
			o.put("fullName", GetPartyAttr(party_id,"fullName")) ;
			//o.put("lastName", GetPartyAttr(party_id,"lastName")) ;
			if (recordCount < limit){
				outputList.add(o);
				recordCount++;
			}
		}
		
		retVal.put("data", StringifyDBList(outputList));

		return retVal;
	}
	@SuppressWarnings("unchecked")
	@RWAPI
	public JSONObject NewMembersLastSeven(JSONObject data) throws Exception {
		log.trace( "NewMembersLastSeven");
		long oneWeekAgo = 10080; //minutes in last week
		//long oneWeekAgo = 70080; //minutes in last week
		Date compareToDate = new Date(System.currentTimeMillis() - oneWeekAgo * 60000);		
		data.put("since_date", compareToDate);
		data.put("name", "NewMembersLastSeven" );
		return getChart(data);
		
	}
	
	@SuppressWarnings("unchecked")
	@RWAPI
	public JSONObject NewPowerPartners(JSONObject data) throws Exception {
		log.trace( "NewPowerPartners");
		long fourWeeksAgo = 40320; //minutes in last 4 weeks
		Date compareToDate = new Date(System.currentTimeMillis() - fourWeeksAgo * 60000);		
		data.put("since_date", compareToDate);
		String pp = "Z";
		if (data.has("powerpartner1")){
			pp = data.getString("powerpartner1");
		}
		
		List<String> items = Arrays.asList(pp.split("\\s*,\\s*"));
      	 BasicDBList inList = new BasicDBList();
      	 inList.addAll(items);
      	 BasicDBObject inClause = new BasicDBObject();
      	 inClause.put("$in", inList);
		
		data.put("professionlist", inList.toString());
		data.put("name", "NewPowerPartners" );
		return getChart(data);
		
	}
	
	
	@SuppressWarnings("unchecked")
	@RWAPI
	public JSONObject mostGuestInvitesInChapter(JSONObject data) throws Exception {

		log.trace( "mostInvitesInChapter");

		JSONObject retVal = new JSONObject() ;
		RWJBusComp bc = app.GetBusObject("Attendee").getBusComp("Attendee");
		
		
		BasicDBObject match = new BasicDBObject();
		String OrgId = data.getString("masterRecordId");  
		
		//QueryBuilder qm = new QueryBuilder().and(new QueryBuilder().start().put("invitedBy_OrgId").is(invitedBy_OrgId).get(),
		//		new QueryBuilder().put("toId2").is(party_id).get());
		
		long EightWeeksAgo = 20160 * 4; //minutes in last eight weeks
		Date compareToDate = new Date(System.currentTimeMillis() - EightWeeksAgo * 60000);
		String dateCompareField = "denorm_datetime";
		

		QueryBuilder qm = new QueryBuilder().and(
				new QueryBuilder().start().put(dateCompareField).greaterThanEquals(compareToDate).get(),
				new QueryBuilder().start().put("newMemberSrc_OrgId").is(OrgId).get(),
				new QueryBuilder().start().put("partytype").notEquals("PARTNER_DEMO").get()
				); //finish
		
		
		//match.put("invitedBy_OrgId", invitedBy_OrgId);
		match.putAll(qm.get());

		BasicDBObject group = new BasicDBObject();
		group.put("_id", "$newMemberSrc_partyId");
		
		BasicDBObject counter = new BasicDBObject();
		counter.put("$sum", 1);
		group.put("count",counter);
		
		BasicDBObject sort = new BasicDBObject();
		sort.put("count", -1);

		AggregationOutput output = bc.ExecAggregate(match, null,group, sort); 
		BasicDBList outputList = new BasicDBList();
		int limit = 5;
		int recordCount = 0;
		for ( DBObject o : output.results() ) {
			String party_id = o.get("_id").toString();
			o.put("fullName", GetPartyAttr(party_id,"fullName")) ;
			//o.put("lastName", GetPartyAttr(party_id,"lastName")) ;
			if (recordCount < limit){
				outputList.add(o);
				recordCount++;
			}
		}
		
		retVal.put("data", StringifyDBList(outputList));

		return retVal;
	}
	
	@SuppressWarnings("unchecked")
	@RWAPI
	public JSONObject mostPRefInChapter(JSONObject data) throws Exception {

		log.trace( "mostPRefInChapter");

		JSONObject retVal = new JSONObject() ;
		RWJBusComp bc = app.GetBusObject("Referral").getBusComp("Referral");
		
		
		BasicDBObject match = new BasicDBObject();
		String OrgId = data.getString("masterRecordId");  
		
		//QueryBuilder qm = new QueryBuilder().and(new QueryBuilder().start().put("invitedBy_OrgId").is(invitedBy_OrgId).get(),
		//		new QueryBuilder().put("toId2").is(party_id).get());
		
		long EightWeeksAgo = 20160 * 4; //minutes in last eight weeks
		Date compareToDate = new Date(System.currentTimeMillis() - EightWeeksAgo * 60000);
		String dateCompareField = "rw_created_on";
		

		QueryBuilder qm = new QueryBuilder().and(
				new QueryBuilder().start().put(dateCompareField).greaterThanEquals(compareToDate).get(),
				new QueryBuilder().start().put("fromBusinessId").is(OrgId).get(),
				new QueryBuilder().start().put("referralType").is("CUST_FOR_PART").get()
				); //finish
		
		
		//match.put("invitedBy_OrgId", invitedBy_OrgId);
		match.putAll(qm.get());
		System.out.println("ref " + match.toString());

		BasicDBObject group = new BasicDBObject();
		group.put("_id", "$fromId");
		
		BasicDBObject counter = new BasicDBObject();
		counter.put("$sum", 1);
		group.put("count",counter);
		
		BasicDBObject sort = new BasicDBObject();
		sort.put("count", -1);

		AggregationOutput output = bc.ExecAggregate(match, null,group, sort); 
		BasicDBList outputList = new BasicDBList();
		int limit = 5;
		int recordCount = 0;
		for ( DBObject o : output.results() ) {
			String party_id = o.get("_id").toString();
			o.put("fullName", GetPartyAttr(party_id,"fullName")) ;
			//o.put("lastName", GetPartyAttr(party_id,"lastName")) ;
			if (recordCount < limit){
				outputList.add(o);
				recordCount++;
			}
		}
		
		retVal.put("data", StringifyDBList(outputList));

		return retVal;
	}
	
	@SuppressWarnings("unchecked")
	@RWAPI
	public JSONObject newMembersInvitedByMe(JSONObject data) throws Exception {
		log.trace( "newMembersInvitedByMe");
		long twoWeeksAgo = 20160; //minutes in last two weeks
		Date compareToDate = new Date(System.currentTimeMillis() - twoWeeksAgo * 60000);		
		data.put("since_date", compareToDate);
		data.put("name", "MembersInvited" );
		return getChart(data);
	}

	public void addStockContext(JSONObject data) throws JSONException, Exception {
		if ( !data.has("partyId"))
			data.put("partyId", GetPartyId());

		if ( !data.has("userid"))
			data.put("userid", getUserId());

		if ( !data.has("lastlogin_date"))		
			data.put("lastlogin_date", getExecutionContextItem("lastlogin_date"));		
		
		if (!data.has("OrgId"))
			data.put("OrgId", GetSelfAttr("OrgId"));
	}
	
	@SuppressWarnings("unchecked")
	@RWAPI
	public BasicDBList getChartData(JSONObject data) throws Exception {
		log.trace( "getChart");
		RWChartItemData d = app.GetChartObject(data.getString("name"));		
		addStockContext(data);
		return d.getSeries(data);
	}
	
	@SuppressWarnings("unchecked")
	@RWAPI
	public JSONObject getChart(JSONObject data) throws Exception {
		log.trace( "getChart");
		JSONObject retVal = new JSONObject() ;
		data = addUserFilters(data);
		retVal.put("data", StringifyDBList(getChartData(data)));
		return retVal;
	}

	@SuppressWarnings("unchecked")
	@RWAPI
	public JSONObject getChart2(JSONObject data) throws Exception {
		log.trace( "getChart");
		RWChartItemData d = app.GetChartObject();
		JSONObject retVal = new JSONObject() ;	
		addStockContext(data);
		JSONObject chart = data.getJSONObject("chart");		
		data = addUserFilters(data);
		retVal.put("data", StringifyDBList(d.getSeries(data, chart)));
		return retVal;
	}

	

	@SuppressWarnings("unchecked")
	@RWAPI
	public BasicDBList getMasterChartData(JSONObject data) throws Exception {
		log.trace( "getChart");
		RWChartItemData d = app.GetChartObject(data.getString("name"));
		JSONObject retVal = new JSONObject() ;	
		addStockContext(data);
		return d.getMasterSeries(data, null, null);
	}
	
	@SuppressWarnings("unchecked")
	@RWAPI
	public JSONObject getTrackerChart(JSONObject data) throws Exception {
		log.trace( "getTrackerChart");
		JSONObject retVal = new JSONObject() ;			
		retVal.put("data", StringifyDBList(getMasterChartData(data)));
		return retVal;
	}
	
	/*
	 * requirement :	Summarize data from child records and store the result in one or more parent attributes
	 * 	 call summarize for every parent attribute that need to be summarized from the child records.
	 */
	
	@SuppressWarnings("unchecked")
	@RWAPI
	public JSONObject summarize(JSONObject data) throws Exception {
		log.trace( "summarize");
		
		String parentBO = data.getString("bo");
		String parentBC = data.getString("bc");
		String parentKey = data.getString("FK");
		String summarizer = data.getString("summarizer");
		String chart = data.getString("chartName");
		JSONObject retVal = new JSONObject() ;			
		
		addStockContext(data);
		RWChartItemData d = app.GetChartObject(chart);
		BasicDBList dl = d.getSeries(data);

		if ( dl.size() > 0 ) {
			BasicDBObject dbo = (BasicDBObject) dl.get(0);
			String summarizer2 = dbo.getString("_id");
			Object value = dbo.get(summarizer2);

			RWJBusComp bc = app.GetBusObject(parentBO).getBusComp(parentBC);	
			BasicDBObject query = new BasicDBObject();			
			query.put("_id", new ObjectId(parentKey));			
			int nRecs = bc.UpsertQuery(query);
			if ( nRecs > 0) {
				log.info("Summarizer:" + summarizer + ":" + value.toString() );
				bc.SetFieldValue(summarizer,value);
				bc.SaveRecord();
				// Decache(bc.GetFieldValue("_id").toString());

				retVal.put("data", StringifyDBObject(bc.currentRecord));
			}
			else {
				String errorMessage = getMessage("RECORD_NOTFOUND"); 
				log.warn( errorMessage);
				throw new Exception(errorMessage);
			}
		}		
		return retVal;
	}
	
	@SuppressWarnings("unchecked")
	@RWAPI
	public JSONObject summarize2(JSONObject data) throws Exception {
		log.trace( "summarize");
		
		String parentBO = data.getString("bo");
		String parentBC = data.getString("bc");
		String parentKey = data.getString("FK");
		JSONObject chart = data.getJSONObject("chart");		
		
		addStockContext(data);
		RWChartItemData d = app.GetChartObject();

		BasicDBList dl = d.getSeries(data, chart);
		JSONObject retVal = new JSONObject() ;			

		if ( dl.size() > 0 ) {
			BasicDBObject dbo = (BasicDBObject) dl.get(0);
			String summarizer = dbo.getString("_id");
			Object value = dbo.get(summarizer);

			RWJBusComp bc = app.GetBusObject(parentBO).getBusComp(parentBC);	
			BasicDBObject query = new BasicDBObject();			
			query.put("_id", new ObjectId(parentKey));			
			int nRecs = bc.UpsertQuery(query);
			if ( nRecs > 0) {
				bc.SetFieldValue(summarizer,value);
				bc.SaveRecord();
				retVal.put("data", StringifyDBObject(bc.currentRecord));
			}
			else {
				String errorMessage = getMessage("RECORD_NOTFOUND"); 
				log.warn( errorMessage);
				throw new Exception(errorMessage);
			}
		}		
		return retVal;
	}
	private JSONObject addUserFilters(JSONObject data) throws JSONException {
		JSONArray filters = new JSONArray();
		if (data.has("chart")){
			JSONObject chart = data.getJSONObject("chart");
			if (chart.has("filter") && chart.get("filter") instanceof JSONArray){
				filters = (JSONArray) chart.get("filter"); 
			} else {
				if (chart.has("filter") && chart.get("filter") instanceof JSONObject){
					filters.put(chart.get("filter"));
				}
				
			}
			
			if (!data.isNull("searchRequest")){
				
				BasicDBObject searchSpec = new BasicDBObject();
				JSONObject s_Model = data.getJSONObject("searchRequest");
				
				Iterator itr = s_Model.keys();
				boolean hasItem = false;
				while(itr.hasNext()) {
					String item = (String)itr.next();
					JSONObject spec = s_Model.getJSONObject(item);
					searchSpec.putAll(getSavedSearchItem(spec).toMap());
			        hasItem = true;
			    }
				//chart.put("filter", searchSpec);
				if (hasItem){
					BasicDBObject f1 = new BasicDBObject();
					f1.put("ftype", "expr");
					f1.put("expression", searchSpec);
					filters.put(f1);
					chart.put("filter", filters);
				}
				
			}
			data.put("chart", chart);
		}
		return data;
	}

}
