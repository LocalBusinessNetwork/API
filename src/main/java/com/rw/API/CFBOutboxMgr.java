package com.rw.API;

import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;
import java.util.Date;
import java.util.UUID;

import org.apache.log4j.Logger;
import org.bson.types.ObjectId;
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
import com.rw.repository.RWAPI;


@SuppressWarnings("unused")
public class CFBOutboxMgr extends RWReqHandle  {
	static final Logger log = Logger.getLogger(CFBOutboxMgr.class.getName());
	private final String[] statusLOVs = {"NOT_SENT","WAITING" } ;

	public CFBOutboxMgr(String userid) {
		super(userid);
	}
	
	public CFBOutboxMgr(RWObjectMgr context) throws Exception {
		super(context);
	}
	
	public CFBOutboxMgr() {
	}

	
	@SuppressWarnings("unchecked")
	@RWAPI
	public JSONObject ScheduleAMessage(BasicDBObject rfrl, int days) throws Exception {

		JSONObject data = new JSONObject();
		
		String token = UUID.randomUUID().toString();
		token = token.replace('-', 'X');
		
		String customerEmail = rfrl.getString("contact_email");
		
		log.debug( "CFB Token for " + customerEmail + " : " + token);
		// stick the referral id on Jedis.
		// what's visible to the public is a random UUID.  Hacker protection!!				
		// expire in 6-months.
		long expireat = System.currentTimeMillis() + 262974 * 60000;
		
		String id = rfrl.get("_id").toString();
		jedisMt.set(token, id);				
		jedisMt.expireAt(token, expireat);

		data.put("token", token);
		data.put("rfrlId", id );
		long minutes = 1440 * days; // in minutes
		Date send_on = new Date(System.currentTimeMillis() + minutes * 60000);
		data.put("send_on", send_on);

		
		return create(data);
	}
		
	@SuppressWarnings("unchecked")
	@RWAPI
	public JSONObject create(JSONObject data) throws Exception {
		log.trace( "create");
		RWJBusComp bc = app.GetBusObject("CFBOutbox").getBusComp("CFBOutbox");

		bc.NewRecord();
		bc.SetFieldValue("status", "NOT_SENT");
		
		if ( data.isNull("max_attempts"))
			bc.SetFieldValue("max_attempts", 3);  
		if ( data.isNull("freq_attempts"))
			bc.SetFieldValue("freq_attempts", 10); // number days before the next attempt

		bc.SetFieldValue("total_attempts", 0); 
	
		if ( data.isNull("send_on")) {
			long minutes = 1440 * 10; // in minutes
			Date send_on = new Date(System.currentTimeMillis() + minutes * 60000);
			bc.SetFieldValue("send_on", send_on);
		}
		
		super.create(data);
		bc.SaveRecord(data);
		JSONObject retVal = new JSONObject();
		retVal.put("data", StringifyDBObject(bc.GetCompositeRecord()));
		return retVal;
	}
	
	@SuppressWarnings({ "unchecked", "unchecked" })
	@RWAPI
	public JSONObject read(JSONObject data) throws Exception {
		log.trace( "read");

		JSONObject retVal = new JSONObject() ;
		
		RWJBusComp bc = app.GetBusObject("CFBOutbox").getBusComp("CFBOutbox");
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
			int nRecs = bc.ExecQuery(query,getSortSpec(data));
			retVal.put("data", StringifyDBList(bc.GetCompositeRecordList()));
		}
		return retVal;
	}
	
	@SuppressWarnings("unchecked")
	@RWAPI
	public JSONObject update(JSONObject data) throws Exception  {
		log.trace( "update");

		super.update(data);
		
		Object id = data.get("id");

		JSONObject retVal = new JSONObject() ;
		RWJBusComp bc = app.GetBusObject("CFBOutbox").getBusComp("CFBOutbox");
		
		BasicDBObject query = new BasicDBObject();
		
        query.put("_id", new ObjectId(id.toString())); 
	
		if (bc.ExecQuery(query,null) == 0 ) {
			String errorMessage = getMessage("RECORD_NOTFOUND"); 
			log.debug( errorMessage);
			throw new Exception(errorMessage);
		}
		
		bc.SaveRecord(data);
		retVal.put("data", StringifyDBObject(bc.GetCompositeRecord()));
		return retVal;
	}

	@SuppressWarnings("unchecked")
	@RWAPI
	public JSONObject delete(JSONObject data) throws Exception {
		log.trace( "delete");

		super.delete(data);
		
		Object id = data.get("id");
		
		JSONObject retVal = new JSONObject() ;
		RWJBusComp bc = app.GetBusObject("CFBOutbox").getBusComp("CFBOutbox");
		
		BasicDBObject query = new BasicDBObject();
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
	public void cron() throws Exception {

		log.trace( "SendMessages");

		RWJBusComp bc = app.GetBusObject("CFBOutbox").getBusComp("CFBOutbox");
		BasicDBObject query = new BasicDBObject();

		Date send_on = new Date();
	
		QueryBuilder  qm = new QueryBuilder().start().put("status").is(new QueryBuilder().put("$in").
				is(statusLOVs).get());
		query.putAll(qm.get());
		
		qm = new QueryBuilder().start().put("total_attempts").lessThan(3);
		query.putAll(qm.get());

		qm = new QueryBuilder().start().put("send_on").lessThanEquals(send_on);
		query.putAll(qm.get());
		
		int nRecs = bc.UpsertQuery(query);

		JSONObject tenantObj = getTenant(null);

		RWJBusComp rbc = app.GetBusObject("Referral").getBusComp("Referral");
		com.rw.API.Mail m = new com.rw.API.Mail();

		for (int i=0; i < nRecs; i++) {
		
			JSONObject data = new JSONObject();
			if ( bc.has("rfrlId") && bc.GetFieldValue("rfrlId") != null ) {
				String rfrlId = bc.GetFieldValue("rfrlId").toString();
				
				BasicDBObject qr = new BasicDBObject();
		        query.put("_id", new ObjectId(rfrlId)); 
				int n = rbc.ExecQuery(query,null);
				if ( n > 0 ) {
					BasicDBObject rfrl = rbc.GetCompositeRecord();
	
					JSONObject emailData = new JSONObject();
					String subject = getMessage("CFB_EMAILTEMPLATE_SUBJECT", rfrl.getString("from_fullName"));
	
					JSONObject header = m.header( tenantObj.getString("SupportEmailAddress"),
							tenantObj.getString("domainName"), rbc.GetFieldValue("from_emailAddress").toString(),
							rbc.GetFieldValue("from_fullName").toString(),
							subject, rbc.GetFieldValue("contact_email").toString() );					
				
					rfrl.put("token", bc.GetFieldValue("token").toString() );
					
					m.SendHtmlMailMessage(header, getMessage("ET_CFB"), rfrl, null);
				
					bc.SetFieldValue("attempted_on", new Date());
					Integer total_attempts = ((Integer) bc.GetFieldValue("total_attempts")) + 1;
					bc.SetFieldValue("total_attempts", total_attempts);
					Integer freq_attempts = (Integer) bc.GetFieldValue("freq_attempts");
					long minutes = 1440 * freq_attempts; // in minutes
					send_on = new Date(System.currentTimeMillis() + minutes * 60000);
					bc.SetFieldValue("send_on", send_on);
					bc.SetFieldValue("status", "WAITING");
					bc.SaveRecord();
				}			
			}
			bc.NextRecord();
		}
						
	}

}
