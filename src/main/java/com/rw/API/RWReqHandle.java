package com.rw.API;
import java.io.IOException;
import java.io.StringWriter;
import java.io.UnsupportedEncodingException;
import java.lang.annotation.Annotation;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URLEncoder;
import java.text.MessageFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Properties;
import java.util.ResourceBundle;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Pattern;

import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.BasicResponseHandler;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.log4j.Logger;
import org.apache.velocity.Template;
import org.apache.velocity.VelocityContext;
import org.apache.velocity.app.Velocity;
import org.apache.velocity.exception.MethodInvocationException;
import org.apache.velocity.exception.ParseErrorException;
import org.apache.velocity.exception.ResourceNotFoundException;
import org.bson.types.ObjectId;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.jayway.jsonpath.Criteria;
import com.jayway.jsonpath.Filter;
import com.jayway.jsonpath.InvalidPathException;
import com.jayway.jsonpath.JsonPath;
import com.mongodb.AggregationOutput;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.QueryBuilder;
import com.mongodb.util.JSON;
import com.mongodb.util.JSONSerializers;
import com.mongodb.util.ObjectSerializer;
import com.rw.Enricher.GoogleGeoCodeImpl;
import com.rw.persistence.JedisMT;
import com.rw.persistence.RWAlgorithm;
import com.rw.persistence.RWJApplication;
import com.rw.persistence.RWJBusComp;
import com.rw.persistence.RWObjectMgr;
import com.rw.persistence.dataMT;
import com.rw.persistence.mongoMaster;
import com.rw.persistence.mongoStore;
import com.rw.repository.Permissions;
import com.rw.repository.RWAPI;
import com.rw.repository.Roles;


public class RWReqHandle extends RWObjectMgr {
	
	static final Logger log = Logger.getLogger(RWReqHandle.class.getName());
	
	
	public static final boolean enrichment_mode = !ResourceBundle.getBundle("referralwire").getString("enrichment_mode").equals("skip");  //, new Locale("en", "US")).getString("enrichment_mode").equals("skip");
	
	public RWReqHandle() {
	}
	
	public RWReqHandle(String userid) {
		super(userid);
	}
	
	public RWReqHandle(RWObjectMgr context) throws Exception {
		super(context);
	}

	
	public String getMessage(String key, Object... args) throws Exception {
		// Decide which locale to use
		Locale currentLocale = new Locale("en", "US");
		ResourceBundle messages = ResourceBundle.getBundle(getTenantKey() + "_msgs", currentLocale);
		MessageFormat formatter = new MessageFormat("");
		formatter.setLocale(currentLocale);
		String str_key = messages.getString(key);
		formatter.applyPattern(str_key);
		String output = formatter.format(args);
		return output;
	} 
	
		
	// <filter type ='time' fieldname="timestamp" period="6400"></filter>
	BasicDBObject timeQuerySpec(JSONObject filter) throws JSONException {
		log.trace( "timeQuerySpec");
		String fieldname = filter.getString("fieldname");
		// period is in minutes
		if (filter.has("max")){
			Long max = Long.parseLong(filter.getString("max"));
			Long period =  Long.parseLong(filter.getString("period"));
			Date compateToDate = new Date(System.currentTimeMillis() - period * 60000);
			Date maxDate = new Date(System.currentTimeMillis() + max * 60000);
			QueryBuilder qm = new QueryBuilder().and(new QueryBuilder().start().put(fieldname).greaterThanEquals(compateToDate).get(),new QueryBuilder().start().put(fieldname).lessThanEquals(maxDate).get());
			
			//QueryBuilder qm = new QueryBuilder().and(new QueryBuilder().start().put("LovType").is("PROFFESSION").get(),new QueryBuilder().start().put("DisplayVal").is(profCatDisplayVal).get());
			
			return (BasicDBObject) qm.get();
		} else {
			Long period =  Long.parseLong(filter.getString("period"));
			Date compateToDate = new Date(System.currentTimeMillis() - period * 60000);
			QueryBuilder qm = new QueryBuilder().start().put(fieldname).greaterThanEquals(compateToDate);
			return (BasicDBObject) qm.get();
		}
	}
	
	BasicDBObject listQuerySpec(JSONObject filter) throws JSONException {
		BasicDBObject retVal = null;
		
		String fieldName = filter.getString("fieldname");
		String fieldval = filter.getString("fieldVal");
		String operator = filter.getString("operator");
		if (fieldval != null && !fieldval.equals("")){
			retVal = new BasicDBObject();
	       	List<String> items = Arrays.asList(fieldval.split("\\s*,\\s*"));
	       	BasicDBList inList = new BasicDBList();
	       	inList.addAll(items);
	       	BasicDBObject inClause = new BasicDBObject();
	       	inClause.put(operator, inList);
	       	retVal.put(fieldName, inClause);
		}
       	return retVal;
	}
	
	
	
	// SPECFICICATION: <filter type='self' fieldname="fromId" attr="id" opr="$ne"></filter>

	BasicDBObject selfQuerySpec(JSONObject filter) throws Exception {
		log.trace( "selfQuerySpec");


		String attr =  filter.getString("attr");
		Object attrValue = null ;
		if  ( (attrValue = GetSelfAttr(attr) ) == null ) 
			return null;
		String fieldname =filter.getString("fieldname");
		
		QueryBuilder qm = null;
		if ( filter.isNull("opr")) {   // Equality
			qm = new QueryBuilder().start().put(fieldname).is((attrValue.toString()));
		}
		else {
			qm = new QueryBuilder().start().put(fieldname).is(new QueryBuilder().put(filter.getString("opr")).
					is(attrValue.toString()).get());
		}
		
		return (BasicDBObject) qm.get();
	}

	// SPECFICICATION: <filter type='this' fieldname="fromId" attr="id" opr="$ne"></filter>

	BasicDBObject thisQuerySpec(JSONObject filter, JSONObject data) throws JSONException {
		log.trace( "selfQuerySpec");


		String attr =  filter.getString("attr");
		Object attrValue = null ;
		if  ( (attrValue = data.getString(attr) ) == null ) 
			return null;
		String fieldname =filter.getString("fieldname");
		
		QueryBuilder qm = null;
		if ( filter.isNull("opr")) {   // Equality
			qm = new QueryBuilder().start().put(fieldname).is((attrValue.toString()));
		}
		else {
			qm = new QueryBuilder().start().put(fieldname).is(new QueryBuilder().put(filter.getString("opr")).
					is(attrValue.toString()).get());
		}
		
		return (BasicDBObject) qm.get();
	}
	
	@SuppressWarnings("unchecked")
	public BasicDBObject getSearchSpecItem(JSONObject filter, JSONObject data) throws Exception {

		log.trace( "getSearchSpecItem");

		BasicDBObject searchSpec = new BasicDBObject();
		
		
		String type = filter.getString("ftype");
		if ( type.equals("self") ) {
			BasicDBObject sqs = selfQuerySpec(filter);
			if ( sqs != null)
				searchSpec.putAll(sqs.toMap());
		}
		else if (type.equals("time") ) {
			BasicDBObject tqs = timeQuerySpec(filter);
			if ( tqs != null )
				searchSpec.putAll(tqs.toMap());
		}
		if (type.equals("fieldVal_list")){
			BasicDBObject lqs = listQuerySpec(filter);
			if (lqs != null)
				searchSpec.putAll(lqs.toMap());
		}
		if ( type.equals("this") ) {
			BasicDBObject sqs = thisQuerySpec(filter, data);
			if ( sqs != null)
				searchSpec.putAll(sqs.toMap());
		}
		if (type.equals("proximity")){
			//ftype:"proximity",from:zipCode,distance:20};};
			JSONObject searchRequestTerm = new JSONObject();
			searchRequestTerm.put("searchOperator", "DISTANCE");
			searchRequestTerm.put("postalCode",filter.getString("from"));
			searchRequestTerm.put("distanceLength",filter.getString("distance"));
			searchRequestTerm.put("fieldName","GeoWorkCode");
			searchRequestTerm.put("dataType","string");
			searchSpec.putAll(getSavedSearchItem(searchRequestTerm).toMap());
		}
		
		
			
		else if (type.equals("expr") ) {
			// SPECIFICATON: <filter type='expr' expression="{status : {'$not: 'IGNORE'}"></filter>
			String expr = filter.getString("expression");
			BasicDBObject exprObj = (BasicDBObject) JSON.parse(expr); 
			searchSpec.putAll(exprObj.toMap());
		} else if(type.equals("validPartnerEmail")){//see referral reason upsert applet for an example
			//<filter ftype="regex" field="emailAddress" regex="/@/"></filter>
			String email1 = "emailAddress";
			String email2 = "emailAddress2";
			String regex = "@";
			String typeField = "type";
			String typeVal = "REFERRAL_PARTNER";
			Pattern p = java.util.regex.Pattern.compile(regex);
			QueryBuilder qm = new QueryBuilder();
			qm.or(
					QueryBuilder.start(email1).regex(p).get(),
					QueryBuilder.start(email1).regex(p).get(),
					QueryBuilder.start(typeField).is(typeVal).get()
			);
			searchSpec.putAll(qm.get().toMap());
		} else if(type.equals("objectId")){//see referral pick party applet for an example
			String field = filter.getString("field");
			String objIdVal = filter.getString("objectId");
			ObjectId thisId = new ObjectId(objIdVal);
			String operator = filter.getString("operator");
			QueryBuilder qm = null;
			if (operator.equals("$ne")){
				qm = new QueryBuilder().start(field).notEquals(thisId);
			}
			else if (operator.equals("=")){
				qm = new QueryBuilder().start(field).is(thisId);
			}
			
			searchSpec.putAll(qm.get());
			
		} 
		else if(type.equals("fieldVal")){//used for pick constraints
			//String field = filter.getString("field");
			String field = filter.getString("fieldname");
			String fieldVal = filter.getString("fieldVal");
			
			String operator = filter.getString("operator");
			QueryBuilder qm = null;
			if (operator.equals("$ne")){
				qm = new QueryBuilder().start(field).notEquals(fieldVal);
			}
			else if (operator.equals("=")){
				qm = new QueryBuilder().start(field).is(fieldVal);
			}
			searchSpec.putAll(qm.get());
		}
		
		return searchSpec;
	}	
	
	@SuppressWarnings("unchecked")
	public BasicDBObject getSavedSearchItem(JSONObject searchRequestTerm) throws JSONException{
		
		BasicDBObject newterm = new BasicDBObject();
		String fieldName = searchRequestTerm.getString("fieldName");
		String searchOperator = searchRequestTerm.getString("searchOperator");
		Object value = (searchRequestTerm.has("value"))?searchRequestTerm.get("value"):null;
		String dataType = searchRequestTerm.getString("dataType");
		
		

        if (searchOperator.equals("IN")){ // contains (",") is supposed to simulate identifying a list of terms that require an $in clause -- should be replace with something more robust
	       	 
        	 String sVal = (String)value;
	       	 List<String> items = Arrays.asList(sVal.split("\\s*,\\s*"));
	       	 BasicDBList inList = new BasicDBList();
	       	 inList.addAll(items);
	       	 BasicDBObject inClause = new BasicDBObject();
	       	 inClause.put("$in", inList);
	       	 newterm.put(fieldName, inClause);
        }
        if (searchOperator.equals("NOT_IN")){ // contains (",") is supposed to simulate identifying a list of terms that require an $in clause -- should be replace with something more robust
	       	 
       	 	String sVal = (String)value;
	       	 List<String> items = Arrays.asList(sVal.split("\\s*,\\s*"));
	       	 BasicDBList inList = new BasicDBList();
	       	 inList.addAll(items);
	       	 BasicDBObject inClause = new BasicDBObject();
	       	 inClause.put("$nin", inList);
	       	 newterm.put(fieldName, inClause);
       } 
        if (searchOperator.equals("LIKE")){
        	
        	String sVal = (String)value;
        	//String upperSuffix = "_upper";
        	//int suffixIndex = fieldName.length() - upperSuffix.length();
        	//if (fieldName.substring(suffixIndex).equals(upperSuffix)){sVal = sVal.toUpperCase();}
        	//Pattern text = Pattern.compile(data.get("searchText").toString(), Pattern.CASE_INSENSITIVE);
        	
        	Pattern text = Pattern.compile(sVal, Pattern.CASE_INSENSITIVE);
			QueryBuilder qm = new QueryBuilder().put(fieldName).regex(text);
        	newterm = (BasicDBObject)qm.get();
        }
        
        if (searchOperator.equals("EQUALS")){
        	newterm.put(fieldName, value);
        }
        
        if (searchOperator.equals("EQUALS_ID")){
        	String sVal = (String)value;
        	fieldName = searchRequestTerm.getString("equalsIdField");
        	if (sVal.equals("nothing")){
        		BasicDBObject all = new BasicDBObject();
        		BasicDBObject neInner = new BasicDBObject();
        		neInner.put("$exists", false);
        		BasicDBObject neOuter = new BasicDBObject();
        		neOuter.put(fieldName, neInner);
        		BasicDBObject emptyString = new BasicDBObject();
        		emptyString.put(fieldName, "");
        		BasicDBObject nul = new BasicDBObject();
        		nul.put(fieldName,null);
        		BasicDBList allOr = new BasicDBList();
        		allOr.add(neOuter);
        		allOr.add(emptyString);
        		allOr.add(nul);
        		newterm.put("$or", allOr);
        		
        	} else {
        		newterm.put(fieldName, sVal);
        	}

        }
        if (searchOperator.equals("LIKE_ID")){
        	String sVal = (String)value;
        	fieldName = searchRequestTerm.getString("likeIdField");
        	Pattern text = Pattern.compile(sVal, Pattern.CASE_INSENSITIVE);
			QueryBuilder qm = new QueryBuilder().put(fieldName).regex(text);
        	newterm = (BasicDBObject)qm.get();
        	
        }
        if (searchOperator.equals("LT_GT")){
        	//fieldName = searchRequestTerm.getString("compareField");
        	BasicDBObject q = new BasicDBObject();
        	Object lessThanVal = searchRequestTerm.has("LESS_THAN")?searchRequestTerm.get("LESS_THAN"):null;
        	Object greaterThanVal = searchRequestTerm.has("GREATER_THAN")?searchRequestTerm.get("GREATER_THAN"):null;
        	
        	if (lessThanVal != null && greaterThanVal != null){
        		if ((dataType.equals("date") || dataType.equals("datetime")) && lessThanVal instanceof Long){
        			q.put(fieldName, BasicDBObjectBuilder.start("$gte", new Date((Long)greaterThanVal)).add("$lte", new Date((Long)lessThanVal)).get());
        		} else {
        			if (dataType.equals("integer")){ 
        				q.put(fieldName, BasicDBObjectBuilder.start("$gte", Integer.parseInt((String) greaterThanVal)).add("$lte", Integer.parseInt((String) lessThanVal)).get());
        			} else {
        				if (dataType.equals("double")){
        					q.put(fieldName, BasicDBObjectBuilder.start("$gte", Double.parseDouble((String)greaterThanVal)).add("$lte", Double.parseDouble((String)lessThanVal)).get());
        				}
        				else {
        					q.put(fieldName, BasicDBObjectBuilder.start("$gte", greaterThanVal).add("$lte", lessThanVal).get());
        				}
        			}
        		}
        	}
        	if (lessThanVal != null && greaterThanVal == null){
        		if ((dataType.equals("date") || dataType.equals("datetime")) && lessThanVal instanceof Long){
        			q.put(fieldName, BasicDBObjectBuilder.start("$lte", new Date((Long)lessThanVal)).get());
        		} else {
        			
        			if (dataType.equals("integer")){ 
        				q.put(fieldName, BasicDBObjectBuilder.start("$lte", Integer.parseInt((String)lessThanVal)).get());
        			} else {
        				if (dataType.equals("double")){
        					q.put(fieldName, BasicDBObjectBuilder.start("$lte", Double.parseDouble((String)lessThanVal)).get());
        				}
        				else {
        					q.put(fieldName, BasicDBObjectBuilder.start("$lte", lessThanVal).get());
        				}
        			}
        		}
        		
        	}
        	if (lessThanVal == null && greaterThanVal != null){
        		if ((dataType.equals("date") || dataType.equals("datetime")) && greaterThanVal instanceof Long){
        			q.put(fieldName, BasicDBObjectBuilder.start("$gte", new Date((Long)greaterThanVal)).get());
        		} else {
        			
        			if (dataType.equals("integer")){         				
        				q.put(fieldName, BasicDBObjectBuilder.start("$gte", Integer.parseInt((String) greaterThanVal)).get());
        			} else {
        				if (dataType.equals("double")){
        					q.put(fieldName, BasicDBObjectBuilder.start("$gte", Double.parseDouble((String)greaterThanVal)).get());
        				}
        				else {
        					q.put(fieldName, BasicDBObjectBuilder.start("$gte", greaterThanVal).get());
        				}
        			}
        		}
        		
        	}
        	
        	newterm = q;
        }
        /*
        if (searchOperator.equals("GREATER_THAN")){
        	fieldName = searchRequestTerm.getString("compareField");
        	BasicDBObject ne = new BasicDBObject();
        	if ((dataType.equals("date") || dataType.equals("datetime")) && value instanceof Long){
        		ne.put("$gt", new Date((Long)value)); 
        		
        	} else {
        		ne.put("$gt", value);
        	}
    		newterm.put(fieldName, ne);
        }
        */
        if (searchOperator.equals("WEEKS_AGO")){
        	BasicDBObject q = new BasicDBObject();
        	//10080
        	Long max = new Long(0);//Long.parseLong(filter.getString("max"));
        	String v = "";
        	if (value instanceof Integer){
        		v = Integer.toString((Integer)value);
        	} else {
        		v = (String)value;
        	}
			Long period =  Long.parseLong(v) * 10080;
			Date compareToDate = new Date(System.currentTimeMillis() - period * 60000);
			Date maxDate = new Date(System.currentTimeMillis());
			//QueryBuilder qm = new QueryBuilder().and(new QueryBuilder().start().put(fieldName).greaterThanEquals(compateToDate).get(),new QueryBuilder().start().put(fieldName).lessThanEquals(maxDate).get());
			//newterm.putAll(qm.get());
			String fn = searchRequestTerm.getString("compareField");
			
			q.put(fn, BasicDBObjectBuilder.start("$gte", compareToDate).add("$lte", maxDate).get());
			newterm = q;
        }
        if (searchOperator.equals("WEEKS_BEFORE")){
        	BasicDBObject q = new BasicDBObject();
        	//10080
        	Long max = new Long(0);//Long.parseLong(filter.getString("max"));
        	String v = (String)value;
			Long period =  Long.parseLong(v) * 10080;
			Date compareToDate = new Date(System.currentTimeMillis() - period * 60000);
			//QueryBuilder qm = new QueryBuilder().and(new QueryBuilder().start().put(fieldName).greaterThanEquals(compateToDate).get(),new QueryBuilder().start().put(fieldName).lessThanEquals(maxDate).get());
			//newterm.putAll(qm.get());
			String fn = searchRequestTerm.getString("compareField");
			
			q.put(fn, BasicDBObjectBuilder.start("$lt", compareToDate).get());
			newterm = q;
        }
        //DOESNT_EXIST
        if (searchOperator.equals("DOESNT_EXIST")){
       	 	String sVal = (String)value;
	       	 List<String> items = Arrays.asList(sVal.split("\\s*,\\s*"));
	       	 BasicDBList existList = new BasicDBList();
	       	 for (int i=0;i<items.size();i++){
	       		 String thisItem = items.get(i);
	       		 BasicDBObject existTermInner = new BasicDBObject();
	       		 existTermInner.put("$exists",false);
	       		 BasicDBObject existTermOuter = new BasicDBObject();
	       		 existTermOuter.put(thisItem, existTermInner);
	       		 existList.add(existTermOuter);
	       		 //db.rwEvent.find( { "GeoWorkLocation.Latitude": { $exists: false } })
	       	 }
	       	 //BasicDBList inList = new BasicDBList();
	       	 //inList.addAll(items);
	       	 //BasicDBObject inClause = new BasicDBObject();
	       	 //inClause.put("$in", inList);
	       	 newterm.put("$or", existList);
        }
        if (searchOperator.equals("DOES_EXIST")){
       	 	String sVal = (String)value;
	       	 List<String> items = Arrays.asList(sVal.split("\\s*,\\s*"));
	       	 BasicDBList existList = new BasicDBList();
	       	 for (int i=0;i<items.size();i++){
	       		 String thisItem = items.get(i);
	       		 BasicDBObject existTermInner = new BasicDBObject();
	       		 existTermInner.put("$exists",true);
	       		 BasicDBObject existTermOuter = new BasicDBObject();
	       		 existTermOuter.put(thisItem, existTermInner);
	       		 existList.add(existTermOuter);
	       		 //db.rwEvent.find( { "GeoWorkLocation.Latitude": { $exists: false } })
	       	 }
	       	 for (int i=0;i<items.size();i++){
	       		 String thisItem = items.get(i);
	       		 BasicDBObject existTermInner = new BasicDBObject();
	       		 existTermInner.put("$ne","");
	       		 BasicDBObject existTermOuter = new BasicDBObject();
	       		 existTermOuter.put(thisItem, existTermInner);
	       		 existList.add(existTermOuter);
	       		 //db.rwEvent.find( { "GeoWorkLocation.Latitude": { $exists: false } })
	       	 }
	       	 newterm.put("$and", existList);
        }
        if (searchOperator.equals("DISTANCE")){
        	
        	GoogleGeoCodeImpl geo = new GoogleGeoCodeImpl();
        	//GeoCoderSynchronous geo = new GeoCoderSynchronous();
        	String distanceCompareTo = "GeoWorkLocation";//searchRequestTerm.getString("distanceCompareTo");
        	BasicDBObject compareToAddr = new BasicDBObject();
        	if (searchRequestTerm.has("street")){compareToAddr.put("streetAddress",searchRequestTerm.getString("street"));}
        	if (searchRequestTerm.has("city")){compareToAddr.put("city",searchRequestTerm.getString("city"));}
        	if (searchRequestTerm.has("state")){compareToAddr.put("state",searchRequestTerm.getString("state"));}
        	if (searchRequestTerm.has("postalCode")){compareToAddr.put("postalCode",searchRequestTerm.getString("postalCode"));}
        	
        	BasicDBObject loc = geo.getEnrichmentData(compareToAddr);
        	//BasicDBObject loc = geo.getCoordinateData(compareToAddr);
        	
        	if (searchRequestTerm.has("distanceLength")){
        			
        	
		        	double distance = searchRequestTerm.getDouble("distanceLength");
		        	
		        	BasicDBList coordinates  = new BasicDBList();
		        	coordinates.add(0, loc.getDouble("Longitude"));
		        	coordinates.add(1, loc.getDouble("Latitude"));
		    		BasicDBList cList = null;

		    			BasicDBObject geometry = new BasicDBObject();
		    			geometry.put("type","Point");
		    			geometry.put("coordinates", coordinates);
		    			//BasicDBObject near = new BasicDBObject();
		    			//near.put("$nearSphere", coordinates);
		    			//near.put("$maxDistance", distance/ 3959);//this is the radians to miles conversion - -we'd divide 6,371 for kilometers			
		    			//newterm.put(distanceCompareTo, near);
		    			BasicDBList points = new BasicDBList();
		    			points.add(0,coordinates);
		    			points.add(1,distance/ 3959);
		    			BasicDBObject centerSphere = new BasicDBObject();
		    			centerSphere.put("$centerSphere", points);
		    			BasicDBObject geoWithin = new BasicDBObject();
		    			geoWithin.put("$geoWithin", centerSphere);
		    			newterm.put(distanceCompareTo, geoWithin);
		    			
		    			//OLD { "GeoWorkLocation" : { "$nearSphere" : [ -121.9359339 , 38.3286205] , "$maxDistance" : 0.02525890376357666}}
		    			//NEW "GeoWorkLocation" : { "$geoWithin" : {"$centerSphere":[[ -121.9359339 , 38.3286205] , 0.02525890376357666]}}
        	}
        }
        System.out.println("new term = " + newterm.toString());
        return newterm;
	}
	
	@SuppressWarnings("unchecked")
	public BasicDBObject getSearchSpec(JSONObject payload) throws Exception {
	
		log.trace( "getSearchSpec");

		BasicDBObject searchSpec = new BasicDBObject();
		
		if ( !payload.isNull("searchSpec")){ 
			
		
			JSONObject s_obj = payload.getJSONObject("searchSpec");
			Object obj = s_obj.get("filter");
			
			if (obj instanceof JSONArray) {
			    // It's an array
				JSONArray filters = (JSONArray)obj;
				for ( int i = 0; i < filters.length(); i++) {
					JSONObject filter = filters.getJSONObject(i);
					searchSpec.putAll(getSearchSpecItem(filter, payload).toMap());
				}
			}
			else if (obj instanceof JSONObject) {
			    // It's an object
				JSONObject filter = (JSONObject) obj;
				searchSpec = getSearchSpecItem(filter, payload);
				//return getSearchSpecItem(filter, payload);
			}
		}
		
		//this block of code is contrived -- it needs to handle more use cases 
		if (!payload.isNull("searchRequest")){
			
			JSONObject s_Model = payload.getJSONObject("searchRequest");
			Iterator itr = s_Model.keys();
			while(itr.hasNext()) {
				String item = (String)itr.next();
				JSONObject spec = s_Model.getJSONObject(item);
				searchSpec.putAll(getSavedSearchItem(spec).toMap());
		         
		    }

		}
		System.out.println("searchspec " + searchSpec.toString());
		return searchSpec;
	
	}
	
	@SuppressWarnings("unchecked")
	public BasicDBObject getPickConstraints(JSONObject payload) throws Exception {
	
		log.trace( "getPickConstraints");

		BasicDBObject searchSpec = new BasicDBObject();
		
		if ( payload.isNull("searchSpec"))
			return searchSpec;
		
		JSONObject s_obj = payload.getJSONObject("searchSpec");
		Object obj = s_obj.get("filter");
		
		if (obj instanceof JSONArray) {
		    // It's an array
			JSONArray filters = (JSONArray)obj;
			for ( int i = 0; i < filters.length(); i++) {
				JSONObject filter = filters.getJSONObject(i);
				searchSpec.putAll(getSearchSpecItem(filter, payload).toMap());
			}
		}
		else if (obj instanceof JSONObject) {
		    // It's an object
			JSONObject filter = (JSONObject) obj;
			return getSearchSpecItem(filter, payload);
		}
		return searchSpec;
	
	}
	
	
	
	
	@SuppressWarnings("unchecked")
	public JSONObject postAMessage(String msg) throws Exception {
		log.trace( "postAMessage");

		JSONObject payload = new JSONObject();
		return payload;
		/*
		ActivityStreamMgr am = new ActivityStreamMgr(this);
		
		payload.put("userid", getUserId());
		payload.put("act", "create");
		payload.put("type","MESSAGE");
		payload.put("message",msg);

		return am.handleRequest(payload);
		*/
	}

	public BasicDBObject getSortSpec(JSONObject data) throws JSONException {
		log.trace( "getSortSpec");

		
		BasicDBObject sortSpec = null;

		if ( !data.isNull("sortby") ) {
			Object sortby = data.get("sortby");
			sortSpec = new BasicDBObject();
			if ( !data.isNull("sortOrder") ) {
				Object sortOrder = data.get("sortOrder");
				sortSpec.put(sortby.toString(), sortOrder.equals("ASC")? 1: -1);
				data.remove("sortOrder");
			}
			else 
				sortSpec.put(sortby.toString(), 1);
			
			data.remove("sortby");
		}
		return sortSpec;
	}
	
	
	public JSONObject handleRequest(JSONObject payload) throws Exception {
		log.trace( "handleRequest");
		
		JSONObject retJso = null;
		String act = null;
		
		try {
			act = (String) payload.get("act");
			Method m[] = this.getClass().getDeclaredMethods();
			for (int i=0; i < m.length; i++) {
				if (m[i].getName().equals(act)) {
					// If some one has permission to access an asset, then role does not matter.
					// This permission might have been delegated for a period of time.
					// TODO : drive this from XML setup
					// AssertAccess(m[i].getName(), this.getClass(), userid);

					// TODO: Hook up Role based access control
					/*
					Permissions perms = m[i].getAnnotation(Permissions.class);
					if ( perms != null ) {
						AssertPermissions(perms.value());
					}
					else {
						// If there was no permissions setup, then Role has the over all privileges.
						Roles roles = m[i].getAnnotation(Roles.class);
						if ( roles != null ) {
							AssertRoles(roles.value());
						}
					}
					Role Based access */
					
					// TODO : Hook up API counter.
					// SecMgr.apiCount(1);
					
					// now we know which act we are going to goto..
					// Shred act.
					payload.remove("act");
					retJso = (JSONObject ) m[i].invoke(this, payload);
				}
			}
			
		} catch (NumberFormatException e) {
			// TODO Auto-generated catch block
			log.debug("API Error : ", e);
		} catch (IllegalAccessException e) {
			// TODO Auto-generated catch block
			log.debug("API Error : ", e);
		} catch (IllegalArgumentException e) {
			// TODO Auto-generated catch block
			log.debug("API Error : ", e);
		} catch (InvocationTargetException e) {
			log.debug("API Error : ", e);
			throw (Exception) e.getTargetException();
			// TODO Auto-generated catch block
		}

		if ( retJso == null) {
			String errorMessage = getMessage("NOAPI", act); 
			throw new Exception(errorMessage);
		}
		return retJso;
	}
	
	@RWAPI
	public JSONObject create(JSONObject data) throws Exception {
		data.put("rw_created_on", new Date());
		String pid = GetPartyId();
		data.put("rw_created_by", pid);
		data.put("rw_lastupdated_on", new Date());
		data.put("rw_lastupdated_by",pid);
		return null;
	}
	
	@RWAPI
	public JSONObject update(JSONObject data) throws Exception {

		if ( data.isNull("id")) {
			String errorMessage = getMessage("RECORD_NOTFOUND"); 
			log.warn( errorMessage);
			throw new Exception(errorMessage);
		}
		data.put("rw_lastupdated_on", new Date());
		data.put("rw_lastupdated_by", GetPartyId());
		return null;
	}
	@RWAPI
	public JSONObject delete(JSONObject data) throws Exception {
		if ( data.isNull("id")) {
			String errorMessage = getMessage("RECORD_NOTFOUND"); 
			log.warn( errorMessage);
			throw new Exception(errorMessage);
		}
		return null;
	}
	
	public JSONArray getDefaultTargetCriteria(String profCatDisplayVal) throws Exception{
		JSONArray retVal = null;
		
		RWJBusComp lovs = app.GetBusObject("Lov").getBusComp("Lov");
		
		QueryBuilder qm = new QueryBuilder().and(new QueryBuilder().start().put("LovType").is("PROFFESSION").get(),new QueryBuilder().start().put("DisplayVal").is(profCatDisplayVal).get());
		BasicDBObject query = new BasicDBObject();
		query.putAll(qm.get());
		
		int recs = lovs.ExecQuery(query,null);
		
		String GlobalVal = null;
		if (lovs.FirstRecord()){
			GlobalVal = lovs.currentRecord.getString("GlobalVal");
		}
		
		RWJBusComp lovMapBC = app.GetBusObject("LOVMap").getBusComp("LOVMap");
    	
		qm = new QueryBuilder().and(new QueryBuilder().start().put("parent_LovType").is("PROFFESSION").get(),new QueryBuilder().start().put("parent_GlobalVal").is(GlobalVal).get());
		query = new BasicDBObject();
		query.putAll(qm.get());
		
		recs = lovMapBC.ExecQuery(query,null);
		BasicDBList cList = lovMapBC.GetCompositeRecordList();
		if (recs > 0){
			
			retVal = new JSONArray();
			
			for (int i = 0; i < recs; i++){
				BasicDBObject thisRecord = (BasicDBObject)cList.get(i);
				JSONObject Criterium = new JSONObject();
				Criterium.put("GlobalVal", thisRecord.get("child_GlobalVal"));
				Criterium.put("name",thisRecord.get("child_DisplayVal"));
				Criterium.put("DisplaySeq",thisRecord.get("child_DisplaySeq"));
				Criterium.put("category",thisRecord.get("child_Group"));
				Criterium.put("partyRelation","TARGET");
				retVal.put(Criterium);
			}
			
		}
		return retVal;
		
	}

	@RWAPI
	public JSONObject read(JSONObject data) throws Exception {
		log.trace( "read - NOT IMPLEMENTED");
		return null;
	}
	
	
	public void Edge(String start, String End) {
		jedisMt.sadd(start, End);
	}
	
	void RunOffline(String token) {
		jedisMt.publish(JedisMT.PARTY_ENRICHMENT_CHANNEL, token);
	}

	void EnrichParty(String token) {
		System.out.println("enrichment_mode = " + enrichment_mode);
		if (enrichment_mode) {
			System.out.println("enrichment_channel = " + JedisMT.PARTY_ENRICHMENT_CHANNEL);
			jedisMt.publish(JedisMT.PARTY_ENRICHMENT_CHANNEL, token);
		}		
	}
	
	public String getProperName(String name){
		name = name.trim();
		String retVal = "";
		if (name != null && name.length() > 1){
			String firstLetter = name.substring(0,1);
			String rest = name.substring(1, name.length());
			retVal = firstLetter.toUpperCase().concat(rest);
			//retVal = name.toUpperCase(); //somewhat STN specific - supports case insensitive searching and sorting of names
		}
		if (name != null && name.length() == 1){retVal = name.toUpperCase();}
		return retVal;
	}
	

	public Boolean hasRole(String role) throws Exception {
		
		RWJBusComp bc = app.GetBusObject("PartyCred").getBusComp("PartyCred");
		
		BasicDBObject query = new BasicDBObject();
        query.put("_id", new ObjectId( getUserId())); 
		query.put("roles", role);
		
		return bc.ExecQuery(query) > 0;
	}

	public Boolean hasPermission(String permission) throws Exception {
		
		RWJBusComp bc = app.GetBusObject("PartyCred").getBusComp("PartyCred");
		
		BasicDBObject query = new BasicDBObject();
        query.put("_id", new ObjectId( getUserId())); 
		query.put("permissions", permission);
		
		return bc.ExecQuery(query) > 0;
	}

	public void AssertRole(String role) throws Exception {
		if ( !hasRole(role)) {
			String errorMessage = getMessage("UNAUTHORIZED_ACCESS"); 
			log.warn( errorMessage);
			throw new Exception(errorMessage);
		}
	}
	
	public void AssertPermission(String permission) throws Exception {
		if ( !hasPermission(permission)) {
			String errorMessage = getMessage("UNAUTHORIZED_ACCESS"); 
			log.warn( errorMessage);
			throw new Exception(errorMessage);
		}
	}
	
	public void AssertRoles(String [] roles) throws Exception {
        for ( int i = 0; i < roles.length; i++) {
        	if ( hasRole(roles[i]) ) return;
        }

        String errorMessage = getMessage("UNAUTHORIZED_ACCESS"); 
		log.warn( errorMessage);
		throw new Exception(errorMessage);
	}
	
	public void AssertPermissions(String [] permissions) throws Exception {
        for ( int i = 0; i < permissions.length; i++) {
        	if ( hasPermission(permissions[i]) ) return;
        }

        String errorMessage = getMessage("UNAUTHORIZED_ACCESS"); 
		log.warn( errorMessage);
		throw new Exception(errorMessage);
	}
	
	public String pushToBackground(String api, JSONObject data) throws Exception {
		
		data.put("module", this.getClass().getName());
		data.put("act", api);	
		data.put("userid", getUserId());
		String uuid = UUID.randomUUID().toString();
		
		uuid = uuid.replace('-', '_');
		data.put("_pid", uuid);
		jedisMt.set(uuid, data.toString());
		jedisMt.publish(JedisMT.ASYNC_API_CHANNEL, uuid);

		return uuid;
		
	}
	
	public Boolean inUIMode() {
		try {
			dataMT t = (dataMT)Thread.currentThread();
			return false;
		}
		catch(Exception e){
			return true;
		}	
	}
	
	
	public Long getLongForDate(JSONObject data, String dateFieldName) throws JSONException, ParseException{
		
		Long EventDateLong = null;
		if (data.has(dateFieldName+"[$date]")){
			EventDateLong = data.getLong(dateFieldName+"[$date]");
		} else {
			
			
		
			Object EventDate = data.get(dateFieldName);
			
			if (EventDate != null && EventDate.equals("") == false){
				if (EventDate instanceof Long){
					return (Long)EventDate;
				}
				
				//System.out.println("EventMgr combineDateAndTime EventDate = " + EventDate);
				if (EventDate instanceof Date){
					EventDateLong = ((Date) EventDate).getTime();
					
				} else {
				
					try{ 
						EventDateLong = Long.parseLong((String)EventDate); 
					}
					catch(Exception e){
						//EventDateLong = new Date((String)EventDate).getTime();
						try {
							//Date d = new SimpleDateFormat( "yyyy-MM-dd'T'HH:mm:ss.SSSZ", Locale.ENGLISH).parse((String)EventDate);
							Date d = new SimpleDateFormat("EE MMM dd yyyy HH:mm:ss",Locale.US).parse((String)EventDate);
							//new SimpleDateFormat("EE MMM dd HH:mm:ss z yyyy",Locale.ENGLISH);
							EventDateLong = d.getTime();
						} catch (ParseException e1) {
							try {
							Date d = new SimpleDateFormat("MM/dd/yy hh:mm a",Locale.US).parse((String)EventDate);
							//new SimpleDateFormat("EE MMM dd HH:mm:ss z yyyy",Locale.ENGLISH);
							EventDateLong = d.getTime();
							// TODO Auto-generated catch block
							} catch (ParseException e2) {
									Date d = new SimpleDateFormat("MM/dd/yyyy hh:mm a",Locale.US).parse((String)EventDate);
									//new SimpleDateFormat("EE MMM dd HH:mm:ss z yyyy",Locale.ENGLISH);
									EventDateLong = d.getTime();
									// TODO Auto-generated catch block
							}
						}
					}
				}
			}
		//EventDateLong = convertToUTC(EventDateLong);
		}
        return EventDateLong;
	}

	public Long getLongForTime(JSONObject data, String timeFieldName) throws JSONException{
		
		Long retVal = null;
		if (data.has(timeFieldName) && data.get(timeFieldName) != null && data.get(timeFieldName).equals("") == false){
			Object timeValObj = data.get(timeFieldName);
			
			if (timeValObj instanceof Long){
				return (Long)timeValObj;
			}
			
			if (timeValObj instanceof Date){
				Date dt = (Date)timeValObj;
				
				return dt.getTime();
			}
			
			String timeVal = (String)data.get(timeFieldName);
			
			DateTime now = new DateTime();
			
			
			DateTimeFormatter formatter = DateTimeFormat.forPattern("MM/dd/yyyy h:mm:aa");
			String todayString = now.toString(formatter);
			int space = todayString.indexOf(" ");
			String dateString = todayString.substring(0, space);
			
			timeVal = timeVal.replaceAll(" am", ":AM");
			timeVal = timeVal.replaceAll(" pm", ":PM");
			String dateTimeString = dateString + " " + timeVal;
			
			DateTime newDate = formatter.parseDateTime(dateTimeString);
			retVal = newDate.getMillis();
			//DateTimeZone dtz = DateTimeZone.getDefault();
			//System.out.println(dtz.getID());
			//retVal = convertToUTC(retVal);
	
		}
		return retVal;
		
	}
	
	public Long convertToUTC(Long longDate){
		Long retVal = longDate;
		if (longDate != null){
			DateTime now = new DateTime();
			long utcOffset = now.getZone().getOffset(now.getMillis());
			
			retVal = longDate - utcOffset;
		}
		
		return retVal;
		
	}
	/*
	public void setAdjustedTime (String timeFieldName, JSONObject inputData, RWJBusComp bc) throws JSONException{
		long incomingVal = getLongForTime(inputData,timeFieldName);
		Date existingDateVal = 
		
	}

	*/

	@RWAPI
	public JSONObject trackMetrics(JSONObject data) throws Exception {		
		log.trace( "recordUserEvent");		
		mongoMaster s = new mongoMaster();
		DBCollection c = s.getColl("rwUserMetrics");
		BasicDBObject track = new BasicDBObject();
        track.put("time", new Date());
		track.put("tenant", getTenantKey());
		track.put("userid", getUserId());
		
		Iterator<?> keys = data.keys();
        while( keys.hasNext() ){
            String key = (String) keys.next();
            track.put(key, data.get(key));
        }
	    c.save(track);
		return data;
	}
	
		@RWAPI
	public JSONObject vmSnippet(String rootDir, String templateFileLocation, Object data) throws Exception {		
		log.trace( "vmSnippet");
		
		JSONObject retVal = new JSONObject();
		if ( System.getProperty("EMAIL").equals("local")) return retVal;
		
		// WebRoot : "src/main/webapp"
		try
		{
			
			Properties p = new Properties();
		    p.setProperty("file.resource.loader.path", rootDir);
		    
			Velocity.init(p);
			VelocityContext context = new VelocityContext();
			context.put( "data", data );

			String S3root = ResourceBundle.getBundle("referralwire", new Locale("en", "US")).getString("S3ROOT.codeCDN");        
		    context.put( "cdn", S3root );

			String S3root2 = ResourceBundle.getBundle("referralwire", new Locale("en", "US")).getString("S3ROOT." + System.getProperty("PARAM5") );        
		    context.put( "cdn_user", S3root2 );

		    JSONObject tenantObj = getTenant(null);
		    context.put( "com", tenantObj );


			Template template = Velocity.getTemplate( templateFileLocation );
			StringWriter sw = new StringWriter();
			template.merge( context, sw );
			retVal.put("snippet", sw.toString() );
		}
		catch( ResourceNotFoundException rnfe )
		{
			log.debug(rnfe.getStackTrace());
			throw rnfe;
		}
		catch( ParseErrorException pee )
		{
			log.debug(pee.getStackTrace());
			throw pee;
		}
		catch( MethodInvocationException mie )
		{
			log.debug(mie.getStackTrace());
			throw mie;
		}
		catch( Exception e )
		{
			log.debug(e.getStackTrace());
			throw e;
		}
		return retVal;
	}
	
	@SuppressWarnings("unchecked")
	@RWAPI
	public JSONObject addUserToMeeting(JSONObject data) throws Exception {
		log.trace( "addUserToMeeting - clean old meetings");
		
		
		String oldOrgId = data.has("oldOrgId")?data.getString("oldOrgId"):"";
		String partyId = data.getString("id");
		String OrgId = data.getString("OrgId"); // the id of the chapter the user is changing to
		Date t24hrsago = new Date(new Date().getTime()-(24*60*60*1000));
		 
		//remove user from meetings of their old chapter
		
		QueryBuilder qm = new QueryBuilder().and(new QueryBuilder().start().put("OrgId").is(oldOrgId).get(),
				new QueryBuilder().start().put("partyId").is(partyId).get(),
				new QueryBuilder().start().put("denorm_datetime").greaterThanEquals(t24hrsago).get());
		BasicDBObject query = new BasicDBObject();
		query.putAll(qm.get());
		
    	mongoStore s = new mongoStore(getTenantKey());
    	s.getColl("rwAttendee").remove(query);
    	log.trace( "addUserToMeeting - clean old meetings");
    	
    	//add user to meetings for their new chapter
		query = new BasicDBObject();
    	
		RWJBusComp bc = app.GetBusObject("Event").getBusComp("Event");
		qm = new QueryBuilder().and(new QueryBuilder().start().put("OrgId").is(OrgId).get(),
				new QueryBuilder().start().put("datetime").greaterThanEquals(t24hrsago).get());
		query = new BasicDBObject();
		query.putAll(qm.get());
		int nRecs = bc.ExecQuery(query,null);
		RWJBusComp attendeeBC = app.GetBusObject("Attendee").getBusComp("Attendee");
		System.out.println( "addUserToMeeting - event query " + query.toString());
		
		if ( nRecs > 0 ) {
			BasicDBList upcomingEvents = bc.recordSet;
			for (int i = 0;i < nRecs;i++){
				BasicDBObject dbo = (BasicDBObject)upcomingEvents.get(i);
				ObjectId dboId = (ObjectId)dbo.get("_id");
				String eventId = dboId.toString();
				Date datetime = dbo.getDate("datetime");
				RWJBusComp party = getPartyRecord(partyId);
				String fullName = (String)party.get("fullName");
				
				String[] statusVals = {"INVITED","CHECKEDIN","CHECKEDIN","CHECKEDIN","CHECKEDIN","CHECKEDIN"};
				String status = "INVITED";

				attendeeBC.NewRecord();
				attendeeBC.SetFieldValue("credId", getUserId());
				attendeeBC.SetFieldValue("status", status);
				attendeeBC.SetFieldValue("OrgId", OrgId);
				attendeeBC.SetFieldValue("partyId", partyId);
				attendeeBC.SetFieldValue("eventId", eventId);
				attendeeBC.SetFieldValue("guestType", "MEMBER");
				attendeeBC.SetFieldValue("fullName_denorm", fullName);
				attendeeBC.SetFieldValue("denorm_datetime", datetime);
				attendeeBC.SetFieldValue("denorm_datelong", datetime.getTime());
				attendeeBC.SaveRecord();
			}
		}

		return data;
	}
	public BasicDBList tranformMultiField(BasicDBList list){
		
		Iterator items = list.iterator();
        while(items.hasNext()){
            BasicDBObject field = (BasicDBObject) items.next();
            boolean isDate = (field.containsField("isDate"))?true:false;
            String fieldName = field.getString("fldName"); 
            Object fieldValue = field.get("value");
            if (isDate){
            	if (fieldValue instanceof Long){
 					fieldValue = new Date ((Long) fieldValue);
 				} else {
 					if (fieldValue instanceof String){
 						fieldValue = new Date (Long.parseLong((String) fieldValue));
 					} 
 				}
            	field.put("value", fieldValue);
            }
            	
          }  
        return list;
	}
	
	String getShapedBC(JSONObject data, String bcName ) throws JSONException {
		if( data.has("shape")) {
			String retVal = new String(bcName + data.getString("shape"));
			return retVal;
		}
		else return bcName;
	}
	
	public void  updateStat(String stat, String aggregator, String sourceBC, String key) throws Exception {
		//AnalyticsMgr a = new AnalyticsMgr(this);	
		JSONObject data_in = new JSONObject();
		/*
		data_in.put("bo", sourceBC);
		data_in.put("bc", sourceBC);
		data_in.put("FK", key);
		data_in.put("summarizer", stat);
		data_in.put("chartName", aggregator);
		a.pushToBackground("summarize", data_in);
		*/
		updateStatExp(stat,aggregator,sourceBC,key,data_in);
	}
	public void  updateStatExp(String stat, String aggregator, String sourceBC, String key, JSONObject data_in) throws Exception {
		AnalyticsMgr a = new AnalyticsMgr(this);	
		data_in.put("bo", sourceBC);
		data_in.put("bc", sourceBC);
		data_in.put("FK", key);
		data_in.put("summarizer", stat);
		data_in.put("chartName", aggregator);
		a.pushToBackground("summarize", data_in);
	}
	
	public RWJBusComp getDocument(String boName, String bcName, String id) throws Exception {		
		RWJBusComp bc = app.GetBusObject(boName).getBusComp(bcName);
		BasicDBObject query = new BasicDBObject();
        query.put("_id", new ObjectId(id.toString())); 
		int nRecs = bc.UpsertQuery(query);
		if ( nRecs <= 0 ) {
			String errorMessage = getMessage("RECORD_NOTFOUND"); 
			log.debug( errorMessage);
			throw new Exception(errorMessage);
		}		
		return bc;
	}
	
	public RWJBusComp getPartyRecord(String partyId) throws Exception{
		
    	BasicDBObject query = new BasicDBObject();
		RWJBusComp bc = app.GetBusObject("Party").getBusComp("Party");
		query.put("_id", new ObjectId(partyId));
		bc.ExecQuery(query);
		return bc;
	}
}
