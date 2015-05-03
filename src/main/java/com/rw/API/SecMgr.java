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

import org.apache.log4j.Logger;
import org.apache.shiro.SecurityUtils;
import org.apache.shiro.authc.IncorrectCredentialsException;
import org.apache.shiro.authc.UnknownAccountException;
import org.apache.shiro.authc.UsernamePasswordToken;
import org.apache.shiro.subject.PrincipalCollection;
import org.apache.shiro.subject.Subject;
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
import com.rw.persistence.RWJApplication;
import com.rw.persistence.RWJBusComp;
import com.rw.persistence.RWJBusObj;
import com.rw.persistence.RWObjectMgr;
import com.rw.persistence.mongoMaster;
import com.rw.persistence.mongoStore;
import com.rw.repository.RWAPI;
import com.mongodb.util.JSONSerializers;
import com.mongodb.util.ObjectSerializer;

public class SecMgr extends RWReqHandle {

	static final Logger log = Logger.getLogger(SecMgr.class.getName());
    public static final Random random = new Random();
	
    public SecMgr() {
	}

    public SecMgr(String userid) {
		super(userid);
	}
    
	public SecMgr(RWObjectMgr context) throws Exception {
		super(context);
	}

	@RWAPI
	public JSONObject PartyId(JSONObject payload) throws Exception {
		JSONObject retVal = new JSONObject();
		retVal.put("id", super.GetPartyId());
		return retVal;
	}
	
	
	@RWAPI
	public JSONObject NewUsersCount(JSONObject data) throws Exception {
		log.trace( "NewUsers");
		JSONObject retVal = new JSONObject();
		BasicDBObject query = new BasicDBObject();
		query.put("registered_date", new BasicDBObject().put("$gte", this.getExecutionContextItem("lastlogin_date"))); 
		query.put("type", "PARTNER");
		RWJBusComp bc = app.GetBusObject("PartyCred").getBusComp("PartyCred");
		int nRec = bc.ExecQuery(query,null);
		JSONObject count = new JSONObject();
		count.put("NewUsers", nRec);
		retVal.put("data", count);
		return retVal;
	}	

	@RWAPI
	public JSONObject Eula(JSONObject data) throws NoSuchAlgorithmException, InvalidKeySpecException, Exception {
		log.trace( "Eula");
		JSONObject retVal = new JSONObject() ;

		RWJBusComp bc = app.GetBusObject("PartyCred").getBusComp("PartyCred");
		
		BasicDBObject query = new BasicDBObject();
		query.put("_id", new ObjectId( getUserId() ));
		
		if (bc.UpsertQuery(query) == 0 ) {
			String errorMessage = getMessage("USERNAME_NOT_FOUND", getUserId()); 
			log.debug( errorMessage);
			throw new Exception(errorMessage);
		}
		else {
			String Eula = data.getString("Eula");
			bc.SetFieldValue("Eula", Eula);

			if ((Eula != null) && Eula.equals("Yes")) {
				Date lastlogin_date = (Date) bc.GetFieldValue("currentlogin_date");
				if ( lastlogin_date == null) lastlogin_date = new Date();
				bc.SetFieldValue("lastlogin_date",lastlogin_date);
				this.setExecutionContextItem("lastlogin_date", lastlogin_date );
				Date currentlogin_date = new Date();
				bc.SetFieldValue("currentlogin_date",currentlogin_date);
				this.setExecutionContextItem("currentlogin_date", currentlogin_date );
				bc.SaveRecord();
				
				log.trace( bc.GetFieldValue("login").toString() + " accpeted Eula");
			}
			retVal.put("data", getUserId());
			retVal.put("data_secret", bc.currentRecord);
			
		}
		return retVal;
	}

	@RWAPI
	public JSONObject login(JSONObject data) throws NoSuchAlgorithmException, InvalidKeySpecException, Exception {
		log.trace( "login");

		RWJBusComp bc = app.GetBusObject("PartyCred").getBusComp("PartyCred");
		
		BasicDBObject query = new BasicDBObject();
		String userName = data.get("login").toString().toLowerCase();
		query.put("login", userName);
		
		int nrecs = bc.UpsertQuery(query);
		
		if ( nrecs  == 0 ) {
			RWJBusComp bc2 = app.GetBusObject("Party").getBusComp("Party");
			BasicDBObject q2 = new BasicDBObject();
			q2.put("emailAddress", userName);
			q2.put("partytype", "PARTNER");

			nrecs = bc2.ExecQuery(q2);
			
			if ( nrecs == 0 ) {
				String errorMessage = getMessage("USERNAME_NOT_FOUND", userName); 
				log.debug( errorMessage);
				throw new Exception(errorMessage);				
			}
							
			String credId = bc2.GetFieldValue("credId").toString();
			BasicDBObject q3 = new BasicDBObject();
			q3.put("_id", new ObjectId(credId));
			
			nrecs = bc.UpsertQuery(q3);
					
			if ( nrecs == 0 ) {
				String errorMessage = getMessage("USERNAME_NOT_FOUND", userName); 
				log.debug( errorMessage);
				throw new Exception(errorMessage);				
			}		
			
			userName = bc.GetFieldValue("login").toString();
		}
		
		String demomode = System.getProperty("demomode");
		
		if( demomode == null ) {
			Subject currentUser = SecurityUtils.getSubject();
	    	if ( !currentUser.isAuthenticated() ) {
	    	    UsernamePasswordToken token = new UsernamePasswordToken( getTenantKey() + ":" + userName, data.get("password").toString());
	    	    token.setRememberMe(true);
	    	    try {
	    	        currentUser.login( token );        
	    	    } catch ( UnknownAccountException uae ) {
	    	        log.debug( uae.getMessage() );
	        		String errorMessage = getMessage("USERNAME_INCORRECTPASSWORD", userName); 
					throw new Exception(errorMessage);	    	
	    	   } catch ( IncorrectCredentialsException uae ) {
	    		    log.debug( uae.getMessage() );
	    		    String errorMessage = getMessage("USERNAME_INCORRECTPASSWORD", userName); 
					throw new Exception(errorMessage);	    	
	    	   }
	    	    
	    	}
		}
        JSONObject retVal = new JSONObject() ;
		String id = bc.currentRecord.get("_id").toString();
		
		Date lastlogin_date = (Date) bc.GetFieldValue("currentlogin_date");
		if ( lastlogin_date == null) lastlogin_date = new Date();
		bc.SetFieldValue("lastlogin_date",lastlogin_date);

		Date currentlogin_date = new Date();
		bc.SetFieldValue("currentlogin_date",currentlogin_date);

		bc.SaveRecord();
		setExecutionContextItem("userid", id);
		setExecutionContextItem("lastlogin_date", lastlogin_date );
		setExecutionContextItem("currentlogin_date", currentlogin_date );
		saveExecutionContext();
		
		String errorMessage = getMessage("USERNAME_LOGGEDIN", userName); 
	    log.info( errorMessage);
	 
		retVal.put("data", id);
	    retVal.put("data_secret", bc.currentRecord);
		retVal.put("routes", StringifyDBList(getViews()));
		JSONObject trackingData = new JSONObject();
		trackingData.put("login", userName);
		trackingData.put("event", "UM_LOGIN");         			
		trackMetrics(trackingData);
		updatePartyLastLogin(currentlogin_date);
		
		return retVal;
 	}

	@RWAPI
	public JSONObject create(JSONObject data) throws Exception {
		log.trace( "create");

		RWJBusComp bc = app.GetBusObject("PartyCred").getBusComp("PartyCred");
		BasicDBObject query = new BasicDBObject();
				
		String userName = data.get("login").toString().toLowerCase();
		query.put("login", userName);
		if (bc.UpsertQuery(query) > 0 ) {
			String errorMessage = getMessage("USERNAME_EXISTS", userName); 
			log.debug( errorMessage);
			throw new Exception(errorMessage);
		}
		else {
			bc.NewRecord();
			bc.SetFieldValue("login",data.get("login").toString().toLowerCase());
  			RWPasswordEncryptionService pes = new RWPasswordEncryptionService();
      		
  			byte[] encryptedPasswordSalt = null;
  			byte[] encryptedPassword = null;
  			try {
				encryptedPasswordSalt = pes.generateSalt();
				encryptedPassword = pes.getEncryptedPassword(data.get("password").toString(), encryptedPasswordSalt);
			} catch (NoSuchAlgorithmException e) {
				// TODO Auto-generated catch block
				log.debug(e.getStackTrace());
				throw e;
			} catch (InvalidKeySpecException e) {
				// TODO Auto-generated catch block
				log.debug(e.getStackTrace());
				throw e;
			}
			bc.SetFieldValue("password",encryptedPassword);
			bc.SetFieldValue("passwordSalt",encryptedPasswordSalt);
			
			// bc.SetFieldValue("passwordHint",data.get("passwordHint").toString());
			bc.SetFieldValue("type","PARTNER");
			
			bc.SetFieldValue("registered_date",new Date());

			Date lastlogin_date = new Date();
			bc.SetFieldValue("lastlogin_date",lastlogin_date);
			Date currentlogin_date = lastlogin_date;
			bc.SetFieldValue("currentlogin_date",currentlogin_date);

			bc.SetFieldValue("rw_created_on",currentlogin_date);
			bc.SetFieldValue("Eula","Yes");
			
			if ( !data.isNull("invitationId"))
				bc.SetFieldValue("invitationId", data.getString("invitationId"));
			
			bc.SaveRecord();

			String id = bc.currentRecord.getObjectId("_id").toString();

			setExecutionContextItem("userid", id);
			setExecutionContextItem("lastlogin_date", lastlogin_date );
			setExecutionContextItem("currentlogin_date", currentlogin_date );
   			saveExecutionContext();

			JSONObject trackingData = new JSONObject();
   			trackingData.put("login", userName);
   			trackingData.put("event", "UM_REGISTER");         			
   			trackMetrics(trackingData);
   			
			data.put("role","MEMBER" );
			addRole(data);

			JSONObject retVal = new JSONObject() ;
			
			bc.currentRecord.remove("password");
			bc.currentRecord.remove("passwordSalt");
			
			retVal.put("data", StringifyDBObject(bc.currentRecord));
			UserMgr um = new UserMgr(this);
			
			JSONObject party_data = new JSONObject() ;
			    
			party_data.put("userid", bc.currentRecord.getObjectId("_id").toString());
			party_data.put("act", "create");

			// Required fields
			party_data.put("emailAddress", userName);
			party_data.put("partytype", "PARTNER");
			party_data.put("firstName", data.get("firstName").toString());
			party_data.put("lastName", data.get("lastName").toString());
		
			// optional fields
			if ( !data.isNull("postalCodeAddress_work"))
					party_data.put("postalCodeAddress_work", data.get("postalCodeAddress_work").toString());
			if ( !data.isNull("workPhone"))
				party_data.put("workPhone", data.get("workPhone").toString());
			if ( !data.isNull("LNProfileId"))
				party_data.put("LNProfileId", data.get("LNProfileId").toString());
			if ( !data.isNull("LNProfile"))
				party_data.put("LNProfile", data.get("LNProfile").toString());
			if ( !data.isNull("business"))
				party_data.put("business", data.get("business").toString());
			if ( !data.isNull("profession"))
				party_data.put("profession", data.get("profession").toString());
			if ( !data.isNull("photoUrl"))
				party_data.put("photoUrl", data.get("photoUrl").toString());
			if ( !data.isNull("gender"))
				party_data.put("gender", data.get("gender").toString());
			if ( !data.isNull("FaceBookId"))  
				party_data.put("FaceBookId", data.get("FaceBookId").toString());

			if ( !data.isNull("FaceBookFriends"))  
				party_data.put("FaceBookFriends", data.get("FaceBookFriends"));
			
			if ( !data.isNull("LinkedInConnections"))  
				party_data.put("LinkedInConnections", data.get("LinkedInConnections"));

			if ( !data.isNull("photoUrl"))  {
				party_data.put("photoUrl", data.get("photoUrl"));
				party_data.put("profilePhotoUrl", data.get("photoUrl"));
			}
			
			if ( !data.isNull("bio"))  
				party_data.put("bio", data.get("bio"));			
			if ( !data.isNull("jobTitle"))  
				party_data.put("jobTitle", data.get("jobTitle"));

			um.handleRequest(party_data);
			
			
			JSONObject tenantObj = getTenant(null);
			
			Mail m = new Mail(this);
			
			JSONObject header = m.header( tenantObj.getString("invitationsEmailAddress"),
					tenantObj.getString("domainName"), tenantObj.getString("invitationsEmailAddress"), tenantObj.getString("domainName"),
					getMessage("REGISTRATION_EMAILTEMPLATE_SUBJECT"),userName);					
			
			party_data.put("login",data.getString("login"));
			party_data.put("password",data.getString("password"));
			m.SendHtmlMailMessage(header,getMessage("ET_REGWELCOME"), party_data, null);
			String errorMessage = getMessage("USERNAME_CREATED", userName); 
			log.info( errorMessage);
			return retVal;
		}
	}
	

	@RWAPI
	public JSONObject delete(JSONObject data) throws Exception {
		log.trace( "delete");
		JSONObject retVal = new JSONObject() ;
		
		RWJBusComp bc = app.GetBusObject("PartyCred").getBusComp("PartyCred");

		BasicDBObject query = new BasicDBObject();
		query.put("_id", new ObjectId( getUserId()));
		if (bc.ExecQuery(query,null) == 0 ) {
			String errorMessage = getMessage("RECORD_NOTFOUND"); 
			log.debug( errorMessage);
			throw new Exception(errorMessage);
		}
		retVal.put("data", StringifyDBObject(bc.currentRecord));
		bc.DeleteRecord();
		return retVal;
	}
	
	@RWAPI
	public JSONObject changePasswordImpl(JSONObject data) throws Exception {
		log.trace( "changePasswordImpl");

		RWJBusComp bc = app.GetBusObject("PartyCred").getBusComp("PartyCred");
		BasicDBObject query = new BasicDBObject();		

		String userName = data.get("login").toString().toLowerCase();
		query.put("login", userName);

		int nrecs = bc.UpsertQuery(query);
		if ( nrecs == 0 ) {
			RWJBusComp bc2 = app.GetBusObject("Party").getBusComp("Party");
			BasicDBObject q2 = new BasicDBObject();
			q2.put("emailAddress", userName);
			nrecs = bc2.ExecQuery(q2);
			
			if ( nrecs == 0 ) {
				String errorMessage = getMessage("USERNAME_NOT_FOUND", userName); 
				log.debug( errorMessage);
				throw new Exception(errorMessage);				
			}
							
			String credId = bc2.GetFieldValue("credId").toString();
			BasicDBObject q3 = new BasicDBObject();
			q3.put("_id", new ObjectId(credId));
			
			nrecs = bc.UpsertQuery(q3);
					
			if ( nrecs == 0 ) {
				String errorMessage = getMessage("USERNAME_NOT_FOUND", userName); 
				log.debug( errorMessage);
				throw new Exception(errorMessage);				
			}			

		}

		RWPasswordEncryptionService pes = new RWPasswordEncryptionService();
      		
		byte[] encryptedPasswordSalt = null;
		byte[] encryptedPassword = null;
		try {
			encryptedPasswordSalt = pes.generateSalt();
			encryptedPassword = pes.getEncryptedPassword(data.get("password").toString(), encryptedPasswordSalt);
		} catch (NoSuchAlgorithmException e) {
			// TODO Auto-generated catch block
			log.debug(e.getStackTrace());
			throw e;
		} catch (InvalidKeySpecException e) {
			// TODO Auto-generated catch block
			log.debug(e.getStackTrace());
			throw e;
		}
		bc.SetFieldValue("password",encryptedPassword);
		bc.SetFieldValue("passwordSalt",encryptedPasswordSalt);
		bc.SaveRecord();
		
		JSONObject retVal = new JSONObject();
		
		retVal.put("data", StringifyDBObject(query));
		String credId = bc.GetFieldValue("_id").toString();
		retVal.put("credId", credId);
		
		executionContext.put("userid", credId);
		log.trace( userName + " changed password");
		JSONObject trackingData = new JSONObject();
		trackingData.put("login", userName);
		trackingData.put("event", "UM_CHANGEPASSWORD");         			
		trackMetrics(trackingData);
			
		return retVal;
	}

	public static String generateString(String characters, int length)
	{
	    char[] text = new char[length];
	    for (int i = 0; i < length; i++)
	    {
			text[i] = characters.charAt(random.nextInt(characters.length()));
	    }
	    return new String(text);
	}
	
	@RWAPI
	public JSONObject resetPassword(JSONObject data) throws Exception {
		log.trace( "resetPassword");
		
		String newPassword = generateString("ACDEFGHJKLMNPQRSTUVWXY23456789",6); 
		data.put("password",newPassword);
		
		JSONObject retVal = changePasswordImpl(data);
		String loginEmail = data.getString("login");

		JSONObject tenantObj = getTenant(null);
		
		Mail m = new Mail(this);
		String credId = retVal.getString("credId");
		executionContext.put("userid", credId);
		RWJBusComp bc = GetPartyFromUid(credId);
		
		String emailAddress = bc.GetFieldValue("emailAddress").toString();		
		if (emailAddress == null || emailAddress.isEmpty()) 
			emailAddress = loginEmail;
		
		JSONObject header = m.header( tenantObj.getString("AdminEmailAddress"),
				tenantObj.getString("domainName"), tenantObj.getString("AdminEmailAddress"),
				tenantObj.getString("domainName"),
				getMessage("PWDRESET_EMAILTEMPLATE_SUBJECT"),emailAddress );					
		
		data.put("fullName", bc.GetFieldValue("fullName"));
		data.put("firstName", bc.GetFieldValue("firstName"));
		
		m.SendHtmlMailMessage(header, getMessage("ET_PASSWORDRESET"), data,null );
		
		log.info( data.getString("login") + " had reset the password");
		JSONObject trackingData = new JSONObject();
		trackingData.put("login", data.getString("login"));
		trackingData.put("event", "UM_RESETPASSWORD");         			
		trackMetrics(trackingData);
			
		return retVal;
	}

	@RWAPI
	public JSONObject confirmLoginId(JSONObject data) throws Exception {
		log.trace( "confirmLoginId");
	
		RWJBusComp bc = app.GetBusObject("PartyCred").getBusComp("PartyCred");
		
		BasicDBObject query = new BasicDBObject();
		
		String userName = data.get("login").toString().toLowerCase();
		query.put("login", userName);
		int nrecs = bc.ExecQuery(query,null);
		if ( nrecs == 0 ) {

			RWJBusComp bc2 = app.GetBusObject("Party").getBusComp("Party");
			BasicDBObject q2 = new BasicDBObject();
			q2.put("emailAddress", userName);
			q2.put("partytype", "PARTNER");

			nrecs = bc2.ExecQuery(q2);
			
			if ( nrecs == 0 ) {
				String errorMessage = getMessage("USERNAME_NOT_FOUND", userName); 
				log.debug( errorMessage);
				throw new Exception(errorMessage);				
			}

		}
		
		return new JSONObject().put("data", StringifyDBObject(query));
	}

	@RWAPI
	public JSONObject changePassword(JSONObject data) throws Exception {
		log.trace( "changePassword");
	
		JSONObject retVal = new JSONObject();
	
		String currentPassword = data.getString("password");
		String newPassword = data.getString("newPassword");
		
		BasicDBObject query = new BasicDBObject();
        query.put("_id", new ObjectId( getUserId())); 

		RWJBusComp bc = app.GetBusObject("PartyCred").getBusComp("PartyCred");
		
		bc.UpsertQuery(query);

     	byte[] encryptedPasswordSalt = (byte []) bc.GetFieldValue("passwordSalt");
     	byte[] encryptedPassword = (byte []) bc.GetFieldValue("password");
  		RWPasswordEncryptionService pes = new RWPasswordEncryptionService();
  		if (pes.authenticate(currentPassword,encryptedPassword, encryptedPasswordSalt) ) {
  			try {
  				encryptedPasswordSalt = pes.generateSalt();
  				encryptedPassword = pes.getEncryptedPassword(newPassword, encryptedPasswordSalt);
  			} catch (NoSuchAlgorithmException e) {
  				// TODO Auto-generated catch block
				log.debug(e.getStackTrace());
  				throw e;
  			} catch (InvalidKeySpecException e) {
  				// TODO Auto-generated catch block
				log.debug(e.getStackTrace());
  				throw e;
  			}

  			bc.SetFieldValue("password",encryptedPassword);
  			bc.SetFieldValue("passwordSalt",encryptedPasswordSalt);

			bc.SaveRecord();
			
			JSONObject trackingData = new JSONObject();
			trackingData.put("login", bc.GetFieldValue("login"));
			trackingData.put("event", "UM_CHANGEPASSWORD");         			
			trackMetrics(trackingData);
			
			return new JSONObject().put("data", StringifyDBObject(query));
			
      	} else {
				String errorMessage = getMessage("USERNAME_INCORRECTPASSWORD", bc.GetFieldValue("login")); 
				log.debug( errorMessage);
				throw new Exception(errorMessage);
		}
	}
	
	@RWAPI
	public JSONObject healthCheck(JSONObject data) throws Exception {
		mongoMaster m = new mongoMaster();
		JSONObject retVal = new JSONObject();
		retVal.put("data", "ok");
		return retVal;
	}
	
	@RWAPI
	public JSONObject getTenant(JSONObject data) throws Exception {
		return super.getTenant(data);				
	}
	
	@RWAPI
	public JSONObject getTenantContext(JSONObject data) throws Exception {
		TenantMgr t = new TenantMgr(this);
		return t.getTenantContext(data);
	}
	
	@RWAPI
	public JSONObject addRole(JSONObject data) throws Exception {
		
		RWJBusComp bc = app.GetBusObject("PartyCred").getBusComp("PartyCred");
		
		BasicDBObject query = new BasicDBObject();
		
		String userName = data.get("login").toString().toLowerCase();
		query.put("login", userName);
		String role = data.get("role").toString();
		int	nRecs = bc.UpsertQuery(query);
		
		BasicDBList roles = null;
		
		try {
			roles = (BasicDBList) bc.GetFieldValue("roles");
			if (roles == null)
				roles = new BasicDBList();
		} catch (ClassCastException e) {
			roles = new BasicDBList();
		}
		
		if ( !roles.contains(role)) {
			roles.add(role);
			bc.SetFieldValue("roles", roles);
			bc.SaveRecord();
			LOVMapMgr lmMgr = new LOVMapMgr(this);
			
			JSONObject params = lmMgr.getDefaultPermissions(role);
			params.put("login", userName);
			return addPermissions(params);
		}
		
		return new JSONObject(bc.currentRecord.toMap());
	}

	@RWAPI
	public JSONObject deleteRole(JSONObject data) throws Exception {
		
		RWJBusComp bc = app.GetBusObject("PartyCred").getBusComp("PartyCred");
		
		BasicDBObject query = new BasicDBObject();
		
		String userName = data.get("login").toString().toLowerCase();
		query.put("login", userName);
		String role = data.get("role").toString();
		query.put("roles", role);
		
		int nRecs = bc.UpsertQuery(query);
		
		if ( nRecs > 0 ) {
			BasicDBList roles = (BasicDBList) bc.GetFieldValue("roles");
			roles.remove(role);
			bc.SetFieldValue("roles", roles);
			bc.SaveRecord();
			LOVMapMgr lmMgr = new LOVMapMgr(getUserId());
			
			JSONObject params = lmMgr.getDefaultPermissions(role);
			params.put("login", userName);
			return deletePermissions(params);
		}
		return new JSONObject(bc.currentRecord.toMap());
	}

	@RWAPI
	public JSONObject addPermissions(JSONObject data) throws Exception {
		
		RWJBusComp bc = app.GetBusObject("PartyCred").getBusComp("PartyCred");
		
		BasicDBObject query = new BasicDBObject();
		
		String userName = data.get("login").toString().toLowerCase();
		query.put("login", userName);
		JSONArray permissions = (JSONArray) data.get("permissions");
		
		int nRecs = bc.UpsertQuery(query);
		
		BasicDBList currentPermissions = null;
		
		try {
			currentPermissions = (BasicDBList) bc.GetFieldValue("permissions");
			if (currentPermissions == null )
				currentPermissions = new BasicDBList();
		} catch (ClassCastException e) {
			currentPermissions = new BasicDBList();
		}
		int nCurrentPermissions = currentPermissions.size();
		
		for ( int i = 0; i < permissions.length(); i++) {
			String permission = permissions.getString(i);
			if (!currentPermissions.contains( permission )) {
				currentPermissions.add(permission);
			}
		}
		
		if ( currentPermissions.size() >  nCurrentPermissions ) {
			bc.SetFieldValue("permissions", currentPermissions);
			bc.SaveRecord();
		}
		return new JSONObject(bc.currentRecord.toMap());
	}

	@RWAPI
	public JSONObject deletePermissions(JSONObject data) throws Exception {
		
		RWJBusComp bc = app.GetBusObject("PartyCred").getBusComp("PartyCred");
		
		BasicDBObject query = new BasicDBObject();
		String userName = data.get("login").toString().toLowerCase();
		query.put("login", userName);
		int nRecs = bc.UpsertQuery(query);
		BasicDBList currentPermissions = null;
		try {
			currentPermissions = (BasicDBList) bc.GetFieldValue("permissions");
			int nCurrentPermissions = currentPermissions.size();
			JSONArray permissions = (JSONArray) data.get("permissions");
			for ( int i = 0; i < permissions.length(); i++) {
				String permission = permissions.getString(i);
				if (!currentPermissions.contains( permission )) {
					currentPermissions.remove(permission);
				}
			}
			if ( currentPermissions.size() <  nCurrentPermissions ) {
				bc.SetFieldValue("permissions", currentPermissions);
				bc.SaveRecord();
			}
		} catch (ClassCastException e) {
			// ignore this exception
		}
		return new JSONObject(bc.currentRecord.toMap());
	}

	@RWAPI
	public BasicDBList getViews() throws Exception {
		
		try {
			RWJBusComp bc = app.GetBusObject("PartyCred").getBusComp("PartyCred");
			BasicDBObject query = new BasicDBObject();
			query.put("_id", new ObjectId( getUserId()));
			int nRecs = bc.ExecQuery(query);
			BasicDBList currentPermissions = (BasicDBList) bc.GetFieldValue("permissions");
			
			int nCurrentPermissions = (currentPermissions != null) ? currentPermissions.size() : 0;
			ArrayList<String> strPerms = new ArrayList<String>(); 
			for ( int i = 0; i < nCurrentPermissions; i++) {
				String permission = (String) currentPermissions.get(i);
				strPerms.add(permission);
			}
			query.clear();
			query.put("LovType", "PERMISSIONS");
			QueryBuilder qm = new QueryBuilder().start().put("GlobalVal").is(new QueryBuilder().put("$in").
					is(strPerms).get());
			query.putAll(qm.get());
			RWJBusComp bcViews = app.GetBusObject("Views").getBusComp("Views");
			nRecs = bcViews.ExecQuery(query);
			return  bcViews.GetCompositeRecordList();
		} catch (ClassCastException e) {
		    String errorMessage = getMessage("UNAUTHORIZED_ACCESS"); 
			log.warn( errorMessage);
			throw new Exception(errorMessage);
		}
	}
	
	public String findInvitation(String invitationId) throws Exception {
		log.trace( "findInvitation");		
		BasicDBObject query = new BasicDBObject();
		mongoStore m = new mongoStore(getTenantKey());
		query.put("invitationId", invitationId);
		DBObject found = m.getColl("rwSecurity").findOne(query);		
		return found == null ? null :  found.get("login").toString() ; 
	}
	
	public void updatePartyLastLogin(Date lastLoginDate) throws Exception{
		String partyId = super.GetPartyId();
		mongoStore m = new mongoStore(getTenantKey());
		BasicDBObject find = new BasicDBObject();
		find.put("_id", new ObjectId(partyId));
		BasicDBObject update = new BasicDBObject();
		BasicDBObject f = new BasicDBObject();
		f.put("lastlogin_date",lastLoginDate);
		update.put("$set",f);
		m.getColl("rwParty").update(find, update);
	}
	

	public boolean findUser(String login) throws Exception {
		log.trace( "findUser");		
		BasicDBObject query = new BasicDBObject();
		mongoStore m = new mongoStore(getTenantKey());
		query.put("login", login);
		DBObject found = m.getColl("rwSecurity").findOne(query);		
		return (found != null); 
	}

	public void apiCount(long usage) throws Exception {
		log.trace( "IncrApiCounter");		
		mongoMaster m = new mongoMaster();
		DBCollection rwAdmin =  m.getColl("rwAdmin");
		if ( rwAdmin != null ) {			
			BasicDBObject query = new BasicDBObject();
			query.put("tenant", getTenantKey());
			DBObject found = rwAdmin.findOne(query);
			if ( found != null ) {
				Object apiCounter = found.get("APICounter");
				if ( apiCounter != null )
					found.put("APICounter", (Long) apiCounter + usage);
				else 
					found.put("APICounter", (Long) usage);
				rwAdmin.save(found);	
			}		
		}
	}

}
