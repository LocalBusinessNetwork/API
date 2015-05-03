package com.rw.API;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.ResourceBundle;

import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;
import org.bson.types.ObjectId;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.QueryBuilder;
import com.rw.API.RWReqHandle;
import com.rw.persistence.RWJApplication;
import com.rw.persistence.RWJBusComp;
import com.rw.persistence.RWJBusObj;
import com.rw.persistence.RWObjectMgr;
import com.rw.persistence.mongoMaster;
import com.rw.persistence.mongoStore;
import com.rw.repository.RWAPI;
import com.mongodb.util.JSON;
import com.mongodb.util.JSONSerializers;
import com.mongodb.util.ObjectSerializer;

public class TenantMgr extends RWReqHandle {

	static final Logger log = Logger.getLogger(TenantMgr.class.getName());
	
    public TenantMgr() {
	}

    public TenantMgr(String userid) {
		super(userid);
	}
    
	public TenantMgr(RWObjectMgr context) throws Exception {
		super(context);
	}

	@RWAPI
	public JSONObject create(JSONObject data) throws Exception {
		log.trace( "create");
		
		if ( !data.has("_demoSetup_") && !hasRole("ADMINISTRATOR")) {
			String errorMessage = getMessage("UNAUTHORIZED_ACCESS"); 
			log.debug( errorMessage);
			throw new Exception(errorMessage);							
		}
		
		RWJBusComp bc = app.GetBusObject("Tenant").getBusComp("Tenant");
		BasicDBObject query = new BasicDBObject();
		JSONObject retVal = new JSONObject() ;
		String tenant = data.getString("tenant");		
		query.put("tenant", tenant);
		if (bc.UpsertQueryMaster(query) == 0 ) {
			bc.NewRecord();
		}
		data.put("rw_created_on", new Date());
		data.put("start", new Date());
		bc.SetFieldValue("start", new Date());
		
		
		if ( !data.has("WebAppLocation"))
			bc.SetFieldValue("WebAppLocation", "/rw.jsp");
			
		bc.SaveRecordMaster(data);
		
		String cachedCopy = StringifyDBObject(bc.GetCompositeRecord());
		Cache(bc.GetFieldValue("tenant").toString(), cachedCopy);
		Cache(bc.GetFieldValue("domainName").toString(), cachedCopy);
		retVal.put("data", cachedCopy);
		
		return retVal;
	}	

	@RWAPI
	public JSONObject delete(JSONObject data) throws Exception {
		log.trace( "delete");
		
		if( !hasRole("ADMINISTRATOR") ) {
			String errorMessage = getMessage("UNAUTHORIZED_ACCESS"); 
			log.debug( errorMessage);
			throw new Exception(errorMessage);							
		}
		
		JSONObject retVal = new JSONObject() ;

		RWJBusComp bc = app.GetBusObject("Tenant").getBusComp("Tenant");

		BasicDBObject query = new BasicDBObject();
		if (!data.isNull("id")) {
			query.put("_id", new ObjectId(data.get("id").toString()));
		}

		if (bc.UpsertQueryMaster(query) > 0) {
			Decache(bc.GetFieldValue("tenant").toString());
			Decache(bc.GetFieldValue("domainName").toString());
			bc.DeleteRecordMaster();
		}

		return retVal;
	}
	
	@SuppressWarnings("unchecked")
	@RWAPI
	public JSONObject update(JSONObject data) throws Exception {
		log.trace( "update");
		JSONObject retVal = new JSONObject() ;
		RWJBusComp bc = app.GetBusObject("Tenant").getBusComp("Tenant");
		BasicDBObject query = new BasicDBObject();
		String tenant = data.getString("tenant");	

		if( !hasRole("ADMINISTRATOR") ) {
			tenant = getTenantKey();
			query.put("tenant",tenant);
		}
		else {
			query.put("_id", new ObjectId(data.get("id").toString()));
		}
		
		if (bc.UpsertQueryMaster(query) == 0 ) {
			String errorMessage = getMessage("RECORD_NOTFOUND"); 
			log.debug( errorMessage);
			throw new Exception(errorMessage);
		}

		data.put("rw_lastupdated_on", new Date());
		bc.SaveRecordMaster(data);
		String cachedCopy = StringifyDBObject(bc.GetCompositeRecord());
		Cache(bc.GetFieldValue("tenant").toString(), cachedCopy);
		Cache(bc.GetFieldValue("domainName").toString(), cachedCopy);
		retVal.put("data", cachedCopy);
		return retVal;
	}

	@SuppressWarnings("unchecked")
	@RWAPI
	public JSONObject read(JSONObject data) throws Exception {
		log.trace( "update");
		JSONObject retVal = new JSONObject() ;
		
		// TODO: Need to enforce admin roles.
		RWJBusComp bc = app.GetBusObject("Tenant").getBusComp("Tenant");
		String tenantId = null;
		BasicDBObject query = new BasicDBObject();
	
		if( !hasRole("ADMINISTRATOR") ) {
			retVal.put("data", Cache(getTenantKey()));
			return retVal;
		}
		else {
			if ( data != null && !data.isNull("id")) {
				query.put("_id", new ObjectId(data.get("id").toString()));
			}
		}

		int nRecs = bc.ExecQueryMaster(query);
		
		if ( nRecs > 0 ) {
			retVal.put("data",StringifyDBList(bc.GetCompositeRecordList()));
		}
		else {
			String errorMessage = getMessage("RECORD_NOTFOUND"); 
			log.debug( errorMessage);
			throw new Exception(errorMessage);
		}
		return retVal;
	}
	
	@RWAPI
	public JSONObject getTenantContext(JSONObject data) throws Exception {
		
		BasicDBObject found = null;
		String sn = data.getString("sn");
		log.debug(sn);
		String snproxy = System.getProperty("TENANTPROXY");
		
		if (snproxy != null )  sn = snproxy;

		String [] parts = sn.split("\\.");
		
		if ( parts.length >= 2 ) { 
			String dn = parts[parts.length-2] + "." + parts[parts.length-1]; 
			String tenantStr = Cache(dn);
			
			if ( tenantStr == null ) {
				
				mongoMaster master = new mongoMaster();
				BasicDBObject query = new BasicDBObject();
				query.put("domainName", dn);
				
				found = (BasicDBObject) master.getColl("rwAdmin").findOne(query);
						
				if ( found == null ) {
					log.debug(dn);

					String errorMessage = "Tenant not found : " + dn ;  
					log.debug( errorMessage);
					throw new Exception(errorMessage);
				}
				Cache(dn, StringifyDBObject(found));
			
			}
			else {
				found = (BasicDBObject) JSON.parse(tenantStr);
			}
			return new JSONObject(found.toMap());			
		}
		else {
			String errorMessage = "Tenant not found : " + sn ;  
			log.debug( errorMessage);
			throw new Exception(errorMessage);
		}	
	}
	
	@RWAPI
	public String getTenantKey(String sn) throws Exception {
		
		JSONObject data = new JSONObject();
		data.put("sn", sn);
		JSONObject retVal = getTenantContext(data);
		if (retVal == null ) {
			String errorMessage = "Tenant not found : " + sn ;  
			log.debug( errorMessage);
			throw new Exception(errorMessage);
		}
		return retVal.getString("tenant");
	}
	
	@RWAPI
	public BasicDBList getAPIApps(JSONObject data) throws Exception {
		if ( data.has("id")) {
			RWJBusComp bc = app.GetBusObject("Tenant").getBusComp("TenantApiApp");
			BasicDBObject query = new BasicDBObject();
			query.put("tenant_id", data.get("id").toString());
			int nRecs = bc.ExecQueryMaster(query);
			if ( nRecs > 0 ) {
				return bc.GetCompositeRecordList();
			}
			else {
				return new BasicDBList();
			}
		}
		return new BasicDBList();
	}
	
	@RWAPI
	public JSONObject putAPIAppDef(JSONObject data) throws Exception {
		RWJBusComp bc = app.GetBusObject("Tenant").getBusComp("TenantApiApp");
		
		if ( data.has("id")) {
			BasicDBObject query = new BasicDBObject();
			query.put("_id", new ObjectId(data.getString("id")));
			int nRecs = bc.UpsertQueryMaster(query);
			if ( nRecs == 0 ) {
				String errorMessage = "App not found : " + data.getString("id") ;  
				log.debug( errorMessage);
				throw new Exception(errorMessage);
			}
		}
		else {
			bc.NewRecord();
		}
		
		bc.SaveRecordMaster(data);
		return data;
	}

	@RWAPI
	public JSONObject deleteAPIAppDef(JSONObject data) throws Exception {
		RWJBusComp bc = app.GetBusObject("Tenant").getBusComp("TenantApiApp");
		BasicDBObject query = new BasicDBObject();

		if ( data.has("id"))
			query.put("_id", new ObjectId(data.get("id").toString()));
		
		if ( data.has("client_id"))
			query.put("appClientId", new ObjectId(data.get("client_id").toString()));

		int nRecs = bc.UpsertQueryMaster(query);
		if ( nRecs > 0 ) {
			bc.DeleteRecordMaster();
		}
		return data;
	}
	
	@RWAPI
	public JSONObject getAPIAppDef(JSONObject data) throws Exception {
		
		RWJBusComp bc = app.GetBusObject("Tenant").getBusComp("TenantApiApp");
		BasicDBObject query = new BasicDBObject();
		if ( data.has("id"))
			query.put("_id", new ObjectId(data.get("id").toString()));
		
		if ( data.has("client_id"))
			query.put("appClientId", data.get("client_id").toString());
			
		int nRecs = bc.ExecQueryMaster(query);
		if ( nRecs > 0 ) {
			return new JSONObject(bc.currentRecord.toMap());
		}
		else {
			String errorMessage = getMessage("RECORD_NOTFOUND"); 
			log.debug( errorMessage);
			throw new Exception(errorMessage);
		}
	}

	@RWAPI
	public boolean ValidateAppId(JSONObject data) throws Exception {
		RWJBusComp bc = app.GetBusObject("Tenant").getBusComp("TenantApiApp");
		BasicDBObject query = new BasicDBObject();
		query.put("tenant_id", data.get("tenant_id").toString());
		query.put("appClientId", data.get("appClientId").toString());
		return  bc.ExecQueryMaster(query) > 0;
	}

	public boolean ValidateAppIdAndSecret(JSONObject data) throws Exception {
		RWJBusComp bc = app.GetBusObject("Tenant").getBusComp("TenantApiApp");
		BasicDBObject query = new BasicDBObject();
		query.put("tenant_id", data.get("tenant_id").toString());
		query.put("appClientId", data.get("appClientId").toString());
		query.put("appClientSecret", data.get("appClientSecret").toString());
		return  bc.ExecQueryMaster(query) > 0;	}

}
