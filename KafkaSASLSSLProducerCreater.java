package com.changing.esun;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.text.DateFormat;
import java.text.ParseException;
import java.util.Date;
import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.ThreadContext;

import com.changing.esun.utils.BatchProperties;
import com.google.gson.JsonObject;

public class KafkaSASLSSLProducerCreater {
	
	private static Logger catLog = LogManager.getLogger(KafkaSASLSSLProducerCreater.class);
	private static Logger catLogOut = LogManager.getLogger("OutBound");
	private Long startTime;
	private Long endTime;
	
	private final static Properties kafkaProps = new Properties();	
	private String userName;
	private String SRT;
	private String topic;
	private String messageCount = "";
	private String secureProtocol = "";
	private String sasl = "";
	
	private BatchProperties batch;
	private boolean flag = false;
	
    public static final String SASL_MECHANISM = "sasl.mechanism";
    public static final String SECURITY_PROTOCOL = "security.protocol";
    public static final String MESSAGE_COUNT = "message.count";
    
	
	public KafkaSASLSSLProducerCreater(String path, String seed, String topic, String acc, String SRT, String mode) {
			
		catLog.trace("Create SMs Object for Kafka.");
		
		startTime = System.currentTimeMillis();
		
		this.userName = acc;
		catLog.debug("[UserName] : {}", this.userName);
		
		this.SRT = SRT;
		catLog.debug("[Password] : {}", this.SRT);
		
		this.topic = topic;
		catLog.debug("[Topic] : {}", this.topic);
		
		catLog.debug("[Seed] : {}", seed);
		this.batch = new BatchProperties(seed);
		
		catLog.trace("Read Kafka Properties Start...");
		FileInputStream fis = null;
		try {
			catLog.debug("[FileInput Path] : {}", path);
			fis = new FileInputStream(path);
			kafkaProps.load(fis);
		} catch (FileNotFoundException e) {
			 StringWriter sw = new StringWriter();
			 PrintWriter pw = new PrintWriter(sw);
			 e.printStackTrace(pw);
			 String tmp = sw.toString();
	    	 JsonObject tmpJson = new JsonObject();
	         tmpJson.addProperty("test", tmp);
	         tmp = tmpJson.get("test").toString();
	         tmp = tmp.substring(1, tmp.length()-1);
	         catLog.error(tmp);
		} catch (IOException e) {
			 StringWriter sw = new StringWriter();
			 PrintWriter pw = new PrintWriter(sw);
			 e.printStackTrace(pw);
			 
			 String tmp = sw.toString();
	    	 JsonObject tmpJson = new JsonObject();
	         tmpJson.addProperty("test", tmp);
	         tmp = tmpJson.get("test").toString();
	         tmp = tmp.substring(1, tmp.length()-1);
	         catLog.error(tmp);
		} finally {
		  if(fis != null) {
		    try {
		     fis.close();
		    } catch (IOException e) {
		       StringWriter sw = new StringWriter();
		       PrintWriter pw = new PrintWriter(sw);
		       e.printStackTrace(pw);
		       String tmp = sw.toString();
		       JsonObject tmpJson = new JsonObject();
	           tmpJson.addProperty("test", tmp);
	           tmp = tmpJson.get("test").toString();
	           tmp = tmp.substring(1, tmp.length()-1);
	           catLog.error(tmp);
		    }
		  }
		}
		catLog.trace("Read Kafka Properties End.");
		
		catLog.trace("Get Special Properties from Kafka Start...");

		this.messageCount = kafkaProps.getProperty(MESSAGE_COUNT);
		catLog.debug("[MessageCount] : {}", this.messageCount);
		
		this.secureProtocol =  kafkaProps.getProperty(SECURITY_PROTOCOL);
		catLog.debug("[Security Protocol] : {}", this.secureProtocol);
		
		this.sasl = kafkaProps.getProperty(SASL_MECHANISM);
		catLog.debug("[Sasl Mechanism] : {}", this.sasl);
		
		String jaasTemplate = 
				"org.apache.kafka.common.security.scram.ScramLoginModule required username=\"%s\" password=\"%s\";";
		
		catLog.debug("[Mode] : {}", mode);
		String rtnSRT;
		if("ENC".equals(mode)) {
			rtnSRT = batch.getDecData(SRT);
		}else {
			rtnSRT = SRT;
		}
		String jaasCfg = String.format(jaasTemplate, acc, rtnSRT);  //設定認證的帳號、密碼
		kafkaProps.put("sasl.jaas.config", jaasCfg);
		
		/** Kafka 固定參數 **/
		kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		kafkaProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		kafkaProps.put("acks", "all");//確保拋送出去的資料不會掉則填all
		
		getPropertyAsString(kafkaProps);
	}
	
	public String startSendSm( String txnId,
							   String userName,
							   String SRT,
							   String clientId,
							   String replyTopic,
							   String campaignedId,
							   String payBranch,
							   String phoneNo,
							   String smBody,
							   String smType,
							   String forceSend,
							   String orderTime,
							   String exprieTime,
							   String mask,
							   String maskSmBody,
							   String sysId,
							   String customerId) throws ParseException {
		catLog.trace("Check Send SMs Message Param Start...");
		JsonObject rtnJson = new JsonObject();
		String rst = "";

		if("Y".equalsIgnoreCase(mask)) {
			if(null == maskSmBody || maskSmBody.length() == 0) {
				catLog.error("maskSmBody cannot be blank");
				rtnJson.addProperty("errorCode", ErrorCode.SERVER_RTN_GENERAL_ERROR);
				rtnJson.addProperty("errorMsg", "maskSmBody cannot be blank");
			}
		}

		if(null == txnId || txnId.length() == 0) {
			catLog.error("TxnId cannot be blank");
			rtnJson.addProperty("errorCode", ErrorCode.SERVER_RTN_GENERAL_ERROR);
			rtnJson.addProperty("errorMsg", "TxnId cannot be blank");
		}else if(null == userName || userName.length() == 0) {
			catLog.error("SMS UserName cannot be blank");
			rtnJson.addProperty("errorCode", ErrorCode.SERVER_RTN_GENERAL_ERROR);
			rtnJson.addProperty("errorMsg", "SMS UserName cannot be blank");
		}
		else if(null == SRT || SRT.length() == 0) {
			catLog.error("SMS Password cannot be blank");
			rtnJson.addProperty("errorCode", ErrorCode.SERVER_RTN_GENERAL_ERROR);
			rtnJson.addProperty("errorMsg", "SMS Password cannot be blank");
		}
		else if(null == clientId || clientId.length() == 0) {
			catLog.error("ClientId cannot be blank");
			rtnJson.addProperty("errorCode", ErrorCode.SERVER_RTN_GENERAL_ERROR);
			rtnJson.addProperty("errorMsg", "ClientId cannot be blank");
		} else if(null == replyTopic || replyTopic.length() == 0) {
			catLog.error("ReplyTopic cannot be blank");
			rtnJson.addProperty("errorCode", ErrorCode.SERVER_RTN_GENERAL_ERROR);
			rtnJson.addProperty("errorMsg", "ReplyTopic cannot be blank");
		} else if(null == phoneNo || phoneNo.length() == 0) {
			catLog.error("PhoneNo cannot be blank");
			rtnJson.addProperty("errorCode", ErrorCode.SERVER_RTN_GENERAL_ERROR);
			rtnJson.addProperty("errorMsg", "PhoneNo cannot be blank");
		} else if(phoneNo.length() != 10 || !(phoneNo.substring(0, 1).equals("0"))) {
			catLog.error("PhoneNo must start with 0 and 10 digits");
			rtnJson.addProperty("errorCode", ErrorCode.SERVER_RTN_ARGUMENT_ILLEGALE);
			rtnJson.addProperty("errorMsg", "PhoneNo must start with 0 and 10 digits");
		} else if(null == smBody || smBody.length() == 0) {
			catLog.error("SmBody cannot be blank");
			rtnJson.addProperty("errorCode", ErrorCode.SERVER_RTN_GENERAL_ERROR);
			rtnJson.addProperty("errorMsg", "SmBody cannot be blank");
		} else if(null == smType || smType.length() == 0){
			catLog.error("SmType cannot be blank");
			rtnJson.addProperty("errorCode", ErrorCode.SERVER_RTN_GENERAL_ERROR);
			rtnJson.addProperty("errorMsg", "SmType cannot be blank");
		} else if(!"Y".equalsIgnoreCase(mask) && !"N".equalsIgnoreCase(mask)) {
			catLog.error("Mask must be Y or N");
			rtnJson.addProperty("errorCode", ErrorCode.SERVER_RTN_ARGUMENT_ILLEGALE);
			rtnJson.addProperty("errorMsg", "Mask must be Y or N");
		} else if(null == sysId || sysId.length() == 0) {
			catLog.error("SysId cannot be blank");
			rtnJson.addProperty("errorCode", ErrorCode.SERVER_RTN_GENERAL_ERROR);
			rtnJson.addProperty("errorMsg", "SysId cannot be blank");
		} else if(null == customerId || customerId.length() == 0) {
			catLog.error("CustomerId cannot be blank");
			rtnJson.addProperty("errorCode", ErrorCode.SERVER_RTN_GENERAL_ERROR);
			rtnJson.addProperty("errorMsg", "CustomerId cannot be blank");
		} else {
			catLog.trace("Check SMs Message Param End.");
			KafkaProducer<String, String> producer = new KafkaProducer<String, String>(kafkaProps);//載入參數建立producer物件
			long s = System.currentTimeMillis();
			
			try {
				catLog.debug("[Send To Kafka MessageCount] : {}", this.messageCount);
				long nEvents = 1;
				while(nEvents <= Integer.parseInt(this.messageCount)){
					Date date = new Date();
					DateFormat mediumFormat = DateFormat.getDateTimeInstance(DateFormat.MEDIUM, DateFormat.MEDIUM);
					//組出符合簡訊系統規格的JSON參數, 塞進msg裡, 請留意JSON格式的處理(例如:跳脫字元)
					// String msg = "{\"txnid\":" + txnid + ",\"userid\":\""+ this.userName +"\"}";
					
					JsonObject sendObj = new JsonObject();
					sendObj.addProperty("TxnId", txnId);
					sendObj.addProperty("UserId", userName);
					sendObj.addProperty("PSW", SRT);
					sendObj.addProperty("CampaignId", campaignedId);
					sendObj.addProperty("PayBranch", payBranch);
					sendObj.addProperty("Phone", phoneNo);
					sendObj.addProperty("SmBody", smBody);
					sendObj.addProperty("SmType", smType);
					sendObj.addProperty("ForceSend", forceSend);
					sendObj.addProperty("OrderTime", orderTime);
					sendObj.addProperty("ExpireTime", exprieTime);
					sendObj.addProperty("ClientId", clientId);
					sendObj.addProperty("ReplyTopic", replyTopic);
					sendObj.addProperty("Mask", mask);
					sendObj.addProperty("MaskSmBody", maskSmBody);
					sendObj.addProperty("Channel", "K");
					sendObj.addProperty("SysId", sysId);
					sendObj.addProperty("CustomerId", customerId);

					String msg = sendObj.toString();
					catLog.debug("[Send To Kafka Msg] : {}", msg);
					String tmp = msg;
					JsonObject tmpJson = new JsonObject();
					tmpJson.addProperty("test", tmp);
					tmp = tmpJson.get("test").toString();
					tmp = tmp.substring(1, tmp.length()-1);
					ThreadContext.put("reqOut", tmp);
					catLogOut.info("");
					
					try {
						catLog.debug("[Send To Kafka Topic] : {}", this.topic);
						//建立key值
						catLog.trace("Send to Kafka Start...");
				        producer.send(new ProducerRecord<String, String>(this.topic, "" + System.currentTimeMillis(), msg), new Callback() {
				            //確認資料有沒有送成功, 有成功Kafka會回應metadata, 可藉此知道送到哪個topic, partition, offset位置
				        	public void onCompletion(RecordMetadata metadata, Exception e) {
				        		catLog.debug("Kafka Call Back ...");
				        		if (null != metadata) {
				        			String metaTopic = (null != metadata.topic()) ? metadata.topic() : "";
					        		int metaPartition =  metadata.partition();
					        		long metaOffset = metadata.offset();
					        		if(metaOffset<0){
					        			catLog.debug("Kafka Call Back Offset is {}", metaOffset);
										rtnJson.addProperty("errorCode", ErrorCode.API_RTN_KAFKA_NO_RESPONSE);
						        		rtnJson.addProperty("errorMsg",  "Kafka Call Back Offset is "+ metaOffset);
					        		}else {
						        		rtnJson.addProperty("errorCode", ErrorCode.SERVER_RTN_SUCCESS);
						        		rtnJson.addProperty("errorMsg",  "Success, Kafka Topic: " + topic);
						        		rtnJson.addProperty("topic", metaTopic);
						        		rtnJson.addProperty("partition", metaPartition);
						        		rtnJson.addProperty("offset", metaOffset);
						        		
						        		
						        		catLog.debug("Metadata: {}", metadata.toString());
						        		catLog.debug("The topic we just sent: {}", metadata.topic());
						        		catLog.debug("The partition we just sent: {}", metadata.partition());
						        		catLog.debug("The offset of the record we just sent: {}", metadata.offset());
					        		}
				        		} else {
				        			catLog.debug("Kafka Call Back Value is Null.");
									rtnJson.addProperty("errorCode", ErrorCode.API_RTN_KAFKA_NO_RESPONSE);
					        		rtnJson.addProperty("errorMsg",  "Kafka Call Back Value is Null.");
				        		}
	
				            }
				        });
				    	catLog.trace("Send to Kafka Start End.");
					    
						nEvents++;
						//Thread.sleep(Integer.parseInt(KafkaProperties.SLEEP_TIME));//控制送資料的時間用的, 視情況這一行可以拿掉
						// sync
						//producer.send(new ProducerRecord<String, String>("ssl", "" + System.currentTimeMillis(), msg)).get();
					} catch (KafkaException e) {
						//log.error("printStackTrace" + ", 筆數:" + nEvents , e);
						rtnJson.addProperty("errorCode", ErrorCode.SERVER_RTN_GENERAL_ERROR);
		        		rtnJson.addProperty("errorMsg",  "Send Kafka Occur Exception. Please check log.");
						catLog.error("error message: {}", msg);
					}
				}
				
			} catch (Exception e) {
				rtnJson.addProperty("errorCode", ErrorCode.SERVER_RTN_GENERAL_ERROR);
        		rtnJson.addProperty("errorMsg",  "New KafkaProducer Occur Exception. Please check log.");
				catLog.error("printStackTrace", e);
			} finally {
				producer.close();
			}
			endTime = System.currentTimeMillis();
			ThreadContext.put("execTimeOut", "SMS(Kafka) producer execute time: " + (endTime-startTime)+" ms");
			catLog.info("END {}", ((System.currentTimeMillis() - s) / 1000));
		}
		
		
		if(null != rtnJson) {
			rst = rtnJson.toString();
		}
		
		String tmp = rst;
		JsonObject tmpJson = new JsonObject();
		tmpJson.addProperty("test", tmp);
		tmp = tmpJson.get("test").toString();
		tmp = tmp.substring(1, tmp.length()-1);
		ThreadContext.put("respOut", tmp);
		catLogOut.info("");

		catLog.trace("End Kafka SendSm.");
		
		return rst;
	}
	
	public String startSearchSM(String txnid,
								String replyTopic,
								String esunMsgId,
								String ispMsgId,
								String clientId,
								String replyKafkaServer) throws ParseException, NumberFormatException, InterruptedException {
		catLog.trace("Check Search SMs Message Param Start...");	
		JsonObject rtnJson = new JsonObject();
		String rst = "";
		
		if(null == txnid || txnid.length() == 0) {
			catLog.error("TxnId cannot be blank");
			rtnJson.addProperty("errorCode", ErrorCode.SERVER_RTN_GENERAL_ERROR);
			rtnJson.addProperty("errorMsg", "TxnId cannot be blank");
		} else if(null == replyTopic || replyTopic.length() == 0) {
			catLog.error("ReplyTopic cannot be blank");
			rtnJson.addProperty("errorCode", ErrorCode.SERVER_RTN_GENERAL_ERROR);
			rtnJson.addProperty("errorMsg", "ReplyTopic cannot be blank");
		} else if(null == esunMsgId || esunMsgId.length() == 0) {
			catLog.error("EsunMsgId cannot be blank");
			rtnJson.addProperty("errorCode", ErrorCode.SERVER_RTN_GENERAL_ERROR);
			rtnJson.addProperty("errorMsg", "EsunMsgId cannot be blank");
		} else if(null == ispMsgId || ispMsgId.length() == 0) {
			catLog.error("IspMsgId cannot be blank");
			rtnJson.addProperty("errorCode", ErrorCode.SERVER_RTN_GENERAL_ERROR);
			rtnJson.addProperty("errorMsg", "IspMsgId cannot be blank");
		} else if(null == clientId || clientId.length() == 0) {
			catLog.error("ClientId cannot be blank");
			rtnJson.addProperty("errorCode", ErrorCode.SERVER_RTN_GENERAL_ERROR);
			rtnJson.addProperty("errorMsg", "ClientId cannot be blank");
		} else if(null == replyKafkaServer || replyKafkaServer.length() == 0) {
			catLog.error("ReplyKafkaServer cannot be blank");
			rtnJson.addProperty("errorCode", ErrorCode.SERVER_RTN_GENERAL_ERROR);
			rtnJson.addProperty("errorMsg", "ReplyKafkaServer cannot be blank");
		} else {
			KafkaProducer<String, String> producer = new KafkaProducer<String, String>(kafkaProps);//載入參數建立producer物件
			long s = System.currentTimeMillis();
			
			try {

				long nEvents = 1;
				catLog.debug("[Send To Kafka MessageCount] : " + this.messageCount);
				while(nEvents <= Integer.parseInt(this.messageCount)){
					Date date = new Date();
					DateFormat mediumFormat = DateFormat.getDateTimeInstance(DateFormat.MEDIUM, DateFormat.MEDIUM);
					//組出符合簡訊系統規格的JSON參數, 塞進msg裡, 請留意JSON格式的處理(例如:跳脫字元)
					//String msg = "{\"筆數\":"+nEvents+",\"時間\":\""+ mediumFormat.format(date) +"\"}";
					
					JsonObject sendObj = new JsonObject();
					sendObj.addProperty("TxnId", txnid);
					sendObj.addProperty("UserId", this.userName);
					sendObj.addProperty("PSW", this.SRT);
					sendObj.addProperty("EsunMsgId", esunMsgId);
					sendObj.addProperty("IspMsgId", ispMsgId);
					sendObj.addProperty("ReplyTopic", replyTopic);
					sendObj.addProperty("ClientId", clientId);
					sendObj.addProperty("ReplyKafkaServer", replyKafkaServer);

					String msg = sendObj.toString();
					catLog.debug("[Send To Kafka Msg] : " + msg);
					
					
					try {
						catLog.debug("[Send To Kafka Topic] : " + this.topic);
						//建立key值
						catLog.trace("Send to Kafka Start...");
				        producer.send(new ProducerRecord<String, String>(this.topic, "" + System.currentTimeMillis(), msg), new Callback() {
				            //確認資料有沒有送成功, 有成功Kafka會回應metadata, 可藉此知道送到哪個topic, partition, offset位置
				        	public void onCompletion(RecordMetadata metadata, Exception e) {
				        		JsonObject rtnJson = new JsonObject();
				        		rtnJson.addProperty("errorCode", ErrorCode.SERVER_RTN_SUCCESS);
				        		rtnJson.addProperty("topic", metadata.topic());
				        		rtnJson.addProperty("partition",  metadata.partition());
				        		rtnJson.addProperty("offset",  metadata.offset());
				        		
				        		catLog.debug("Metadata: " + metadata.toString());
				        		catLog.debug("The topic we just sent: " + metadata.topic());
				        		catLog.debug("The partition we just sent: " + metadata.partition());
				        		catLog.debug("The offset of the record we just sent: " + metadata.offset());
				            }
				        });
				    	catLog.trace("Send to Kafka Start End.");
					    
						nEvents++;
						//Thread.sleep(Integer.parseInt(KafkaProperties.SLEEP_TIME));//控制送資料的時間用的, 視情況這一行可以拿掉
						// sync
						//producer.send(new ProducerRecord<String, String>("ssl", "" + System.currentTimeMillis(), msg)).get();
					} catch (KafkaException e) {
						//log.error("printStackTrace" + ", 筆數:" + nEvents , e);
						rtnJson.addProperty("errorCode", ErrorCode.SERVER_RTN_GENERAL_ERROR);
		        		rtnJson.addProperty("errorMsg",  "Send Kafka Occur Exception. Please check log.");
						catLog.error("error message: " + msg);
					}
				}
				
			} catch (Exception e) {
				rtnJson.addProperty("errorCode", ErrorCode.SERVER_RTN_GENERAL_ERROR);
        		rtnJson.addProperty("errorMsg",  "New KafkaProducer Occur Exception. Please check log.");
				catLog.error("printStackTrace", e);
			} finally {
				producer.close();
			}

		}
		
		if(null != rtnJson) {
			rst = rtnJson.toString();
		}
		
		String tmp = rst;
		JsonObject tmpJson = new JsonObject();
		tmpJson.addProperty("test", tmp);
		tmp = tmpJson.get("test").toString();
		tmp = tmp.substring(1, tmp.length()-1);
		ThreadContext.put("respOut", tmp);
		endTime = System.currentTimeMillis();
		if(ThreadContext.get("execTimeOut")!=null) {
			ThreadContext.remove("execTimeOut");
		}
		ThreadContext.put("execTimeOut", "SMS producer execute time: " + (endTime-startTime)+" ms");
        catLogOut.info("");
		
		catLog.info("End Kafka SearchSm.");		
		
		return rst;

	}
	
	
	
	
	private static void getPropertyAsString(Properties prop) {    
		  StringWriter writer = new StringWriter();
		  prop.list(new PrintWriter(writer));
		  String rst = writer.toString().replace("\r", "");
		  rst = rst.replace("\n", " ");
		  catLog.info(rst);
	}
	
}
