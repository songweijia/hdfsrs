package edu.cornell.cs.sa;

import java.io.Serializable;
import java.util.*;

public class SAMessage<LC extends ILogicalClock> implements Serializable {
	@Override
	public String toString() {
		StringBuffer sb = new StringBuffer();
		sb.append("SAMessage{");
		sb.append("t:"+this.type+";");
		sb.append("snd:"+this.sender+";");
		sb.append("rec:"+this.receiver+";");
		sb.append("sid:"+this.sid+";");
		sb.append("rtc:"+this.rtc+";");
		sb.append("lc:"+this.lc+";");
		sb.append("}");
		return sb.toString();
	}

	private static final long serialVersionUID = 8387879580921006308L;
	public static final int SAM_REQ_CREAT_I1 = 0x00010001; // mask bit 0~15(0xffff0000) is for type 0x0001=request, 0x0002=response
	public static final int SAM_RES_CREAT_P1 = 0x00020001;
	public static final int SAM_REQ_CREAT_I2 = 0x00010002;
	public static final int SAM_RES_CREAT_P2 = 0x00020002;
	public static final int SAM_REQ_DELET_SS  = 0x00010003;
	public static final int SAM_RES_DELET_SS  = 0x00020003;
	public static final int SAM_RES_NAK = 0x0002ffff;
	public static final int SAM_SHUTDOWN = 0xffffffff; //shutdown
	
	/**
	 * @param mypid
	 * @return
	 */
	SAMessage<LC> createResponse(int mypid){
		SAMessage<LC> res = new SAMessage<LC>();
		res.sender = mypid;
		res.receiver = this.sender;
		return res;
	}
	
	/**
	 * the sender
	 */
	int sender;
	/**
	 * recipients
	 * if recipients < enSize, its a unicase
	 * if recipients == enSize, its a broadcast.
	 */
	int receiver;
	/**
	 * message type
	 */
	int type;
	/**
	 * snapshot id
	 */
	String sid;
	/**
	 * real-time clock, if required
	 */
	long rtc;
	/**
	 * logical clock of the snapshot
	 */
	LC lc = null;
	
	int getSender(){
		return this.sender;
	}
	
	int getReceiver(){
		return this.receiver;
	}
}
