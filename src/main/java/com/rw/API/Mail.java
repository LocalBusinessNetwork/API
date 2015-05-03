package com.rw.API;

import java.io.UnsupportedEncodingException;
import java.text.MessageFormat;
import java.util.Date;
import java.util.Locale;
import java.util.Properties;
import java.util.ResourceBundle;
import java.util.UUID;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import javax.activation.DataHandler;
import javax.activation.FileDataSource;
import javax.mail.Address;
import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.Multipart;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeBodyPart;
import javax.mail.internet.MimeMessage;
import javax.mail.internet.MimeMultipart;

import org.apache.log4j.Logger;

import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.simpleemail.AWSJavaMailTransport;
import com.amazonaws.services.simpleemail.AmazonSimpleEmailService;
import com.amazonaws.services.simpleemail.AmazonSimpleEmailServiceClient;
import com.amazonaws.services.simpleemail.model.ListVerifiedEmailAddressesResult;
import com.amazonaws.services.simpleemail.model.VerifyEmailAddressRequest;
import com.rw.persistence.JedisMT;
import com.rw.persistence.RWObjectMgr;

import java.io.StringWriter;

import org.apache.velocity.VelocityContext;
import org.apache.velocity.Template;
import org.apache.velocity.app.Velocity;
import org.apache.velocity.exception.ResourceNotFoundException;
import org.apache.velocity.exception.ParseErrorException;
import org.apache.velocity.exception.MethodInvocationException;
import org.json.JSONException;
import org.json.JSONObject;

public class Mail extends RWReqHandle {
	
	private static final Logger log = Logger.getLogger(Mail.class.getName());
	private static String mode  = "local";

	public Mail(){
		setMode(System.getProperty("EMAIL"));
		if ( getMode() == null)
			setMode(ResourceBundle.getBundle("referralwire", new Locale("en", "US")).getString("ses_mode"));
	};

	public Mail(String userid) {
		super(userid);
		setMode(System.getProperty("EMAIL"));
	}

	public Mail(RWObjectMgr context) throws Exception {
		super(context);
		setMode(System.getProperty("EMAIL"));
	}

	public Mail(String m, String userid){
		super(userid);		
		mode = new String(m);
	};

	
	public void setMode(String p) {
		mode = new String(p);
	}
	
	public String getMode() {
		return mode;
	}
	
	private static void verifyEmailAddress(AmazonSimpleEmailService ses, String address) {
        ListVerifiedEmailAddressesResult verifiedEmails = ses.listVerifiedEmailAddresses();
        if (verifiedEmails.getVerifiedEmailAddresses().contains(address)) return;
        ses.verifyEmailAddress(new VerifyEmailAddressRequest().withEmailAddress(address));
        log.info("Please check the email address " + address + " to verify it");
    }
	
	public void SendHtmlMailMessage(JSONObject header, String templateFileLocation, Object data, String[] attachments) throws Exception {	
		if ( getMode().equals("local")) return;
		JSONObject body = new JSONObject();
		String webRoot = System.getProperty("WebRoot");
		log.info("WebRoot : " + webRoot);
		if ( webRoot == null || webRoot.isEmpty()) {
			webRoot = new String ("/var/lib/tomcat7/webapps/ROOT");
		}
		log.info("WebRoot : " + webRoot);
		
		body.put("message", vmSnippet(webRoot, templateFileLocation, data).getString("snippet"));
		SendSESAsync(header, body, attachments);
	}
	
	
	public void SendSESAsync(JSONObject header,JSONObject body, String[] attachments) throws Exception {
		
		JSONObject data = new JSONObject();
		
		data.put("userid", getUserId());
		data.put("header", header);
		data.put("body", body);
		
		String uuid = UUID.randomUUID().toString();
		
		uuid = uuid.replace('-', '_');
		data.put("_pid", uuid);
		jedisMt.set(uuid, data.toString());
		jedisMt.publish(JedisMT.ASYNC_EMAIL_CHANNEL, uuid);

	}
	
	public void SendSES(JSONObject header,JSONObject body, String[] attachments) throws IOException, JSONException {
		
		log.info("EMail Mode : " + getMode());

		if ( getMode().equals("local")) return;
		
		log.info("EMail Header : " + header.toString());
		
	    PropertiesCredentials credentials = new PropertiesCredentials(
				this.getClass()
                        .getResourceAsStream("/AwsCredentials.properties"));
		
        // AmazonSimpleEmailService ses = new AmazonSimpleEmailServiceClient(credentials);	
        
        // Remove this for production
        // verifyEmailAddress(ses, from);
        
        Properties props = new Properties();
        props.setProperty("mail.transport.protocol", "aws");
        props.setProperty("mail.aws.user", "AKIAILPTVZYCS5DD5TAQ"); //  credentials.getAWSAccessKeyId());
        props.setProperty("mail.aws.password", "QvOZu4nuaEbkOkll5Xe5kYzzG5pzlI5+v7NPFBuf"); // credentials.getAWSSecretKey());

        Session session = Session.getInstance(props);

    	try {
    	
    	MimeMessage msg = new MimeMessage(session);
            
        if ( getMode().equals("production"))
        		msg.setRecipients(Message.RecipientType.TO, header.getString("to"));
        else if ( getMode().equals("staging") ) {
        	msg.setRecipients(Message.RecipientType.TO, ResourceBundle.getBundle("referralwire", new Locale("en", "US")).getString("ses_recipient"));
        }
        else   // unknown
        	return;
        
        msg.setFrom(new InternetAddress(header.getString("from"), header.getString("fromDescription")));  
        Address[] ar = {new InternetAddress(header.getString("replyTo"), header.getString("replyToDescription") )};
        msg.setReplyTo( ar );
        
        
        if ( header.has("cc")) {
	        String[] cc = (String [])header.get("cc");	        
	        if ( cc != null && cc.length > 0) {
	        	InternetAddress[] ccAddresses = new InternetAddress[cc.length];	        	
	        	for(int i=0; i < cc.length; i++){
	        		ccAddresses[i] = new InternetAddress();
	        		ccAddresses[i].setAddress(cc[i]);
	        	}
	        	msg.setRecipients(Message.RecipientType.CC, ccAddresses);
	        }
        }
        
        msg.setSubject(header.getString("subject"));

        // create and fill the first message part
        Multipart mp = new MimeMultipart();
        
        if ( body.has("heading")) {
        	String heading = body.getString("heading");       
        	MimeBodyPart mbp = new MimeBodyPart();
        	mbp.setContent(heading, "text/html");
        	mp.addBodyPart(mbp);
        }
        
        if ( body.has("message")) {
        	String message = body.getString("message");       
        	MimeBodyPart mbp = new MimeBodyPart();
        	mbp.setContent(message, "text/html");
        	mp.addBodyPart(mbp);
        }
        
        if ( body.has("closing")) {
            String closing = body.getString("closing");       
        	MimeBodyPart mbp = new MimeBodyPart();
        	mbp.setContent(closing, "text/html");
        	mp.addBodyPart(mbp);
        }
        
        if ( body.has("signature")) {
            String signature = body.getString("signature");       
        	MimeBodyPart mbp = new MimeBodyPart();
        	mbp.setContent(signature, "text/html");
        	mp.addBodyPart(mbp);
        }
     
        if ( attachments != null && attachments.length > 0) {
        	// create the second message part

        	for(int i=0;i<attachments.length;i++) {
        		// attach the file to the message
            	MimeBodyPart mbpAttach = new MimeBodyPart();
        		FileDataSource fds = new FileDataSource(attachments[i]);
        		mbpAttach.setDataHandler(new DataHandler(fds));
        		mbpAttach.setFileName(fds.getName());
            	mp.addBodyPart(mbpAttach);
        	}
        }
        // add the Multipart to the message
        msg.setContent(mp);

        // set the Date: header
        msg.setSentDate(new Date());

        Transport t = new AWSJavaMailTransport(session, null);
        t.connect();
        t.sendMessage(msg, null);
        // Close your transport when you're completely done sending
        // all your messages
        t.close();
        log.info("Sent Email to : " + header.getString("to"));
            
	    } catch (MessagingException mex) {
			log.info(mex.getStackTrace());
	        Exception ex = null;
	        if ((ex = mex.getNextException()) != null) {
	        	log.info(ex.getStackTrace());
	        }
	    } 
	    catch (UnsupportedEncodingException mex) {
	    	log.info(mex.getStackTrace());
		} 
	    catch (Exception mex) {
	    	log.info(mex.getStackTrace());
		} 
	}
	
	JSONObject header(String from, String fromDescription, String replyTo, String replyToDescription, String subject, String to, String[] cc ) throws JSONException {
		JSONObject header = new JSONObject();
		header.put("from", from);
		header.put("fromDescription", fromDescription);
		header.put("replyTo", replyTo);
		header.put("replyToDescription", replyToDescription );
		header.put("subject", subject);
		header.put("to", to);		
		if ( cc != null)
			header.put("cc", cc);		
		return header;
	}
	
	JSONObject header(String from, String fromDescription, String replyTo, String replyToDescription, String subject, String to ) throws JSONException {
		return header(from, fromDescription, replyTo, replyToDescription, subject, to, null );
	}

}
