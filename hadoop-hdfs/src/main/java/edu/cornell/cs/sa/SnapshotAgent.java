/**
 * 
 */
package edu.cornell.cs.sa;

import java.io.IOException;
import java.util.*;

/**
 * @author sonic
 * The SnapshotAgent is responsible for the coordinating the snapshots taken at
 * different processes. Specifically, it manages a virtual clock for each "host"
 * process so that the local snapshot is causally consistent with snapshots local
 * to other processes. In this way, a consistent distributed snapshot is achieved.
 */
public class SnapshotAgent<LC extends ILogicalClock> implements Runnable {
	
	LC lc;
	
	/**
	 * enSize is number the processes in the system. It can be any
	 * unique identifier system wide.
	 */
	int enSize;
	
	/**
	 * my rank is between 0 ~ (enSize - 1) 
	 */
	int myRank;
	
	/**
	 * environment context of the hosting process
	 */
	ISnapshotAgentCtxt<LC> ctxt;
	

	/**
	 * constructing a sa
	 * @param enSize ensemble size
	 * @param rank my rank in the ensemble
	 * @param ilc init logical clock
	 * @param ctxt context
	 */
	public SnapshotAgent(int enSize, int rank, LC ilc, ISnapshotAgentCtxt<LC> ctxt){
		this.enSize = enSize;
		this.myRank = rank;
		this.ctxt = ctxt;
		this.lc = ilc;
	};
	
	/**
	 * tick the virtual clock
	 * @return the value of the clock after tick
	 */
	@SuppressWarnings("unchecked")
	public LC tick(){
		return (LC)lc.tick();
	}
	
	/**
	 * tick the virtual clock on receiving
	 * @param mlc
	 * @return the virtual clock after tick
	 */
	@SuppressWarnings("unchecked")
	public LC tickOnRecv(LC mlc){
		return (LC)lc.tickOnRecv(mlc);
	}
	
	/**
	 * create the snapshot, this is reentrant
	 * @param rtc - rtc clock for the snapshot, 0 for latest status
	 * @param sid - ID of the snapshot
	 * @param pset - the set of interested processes. Use null for all processes.
	 * @throws Exception
	 */
	@SuppressWarnings("unchecked")
	public void createSnapshot(long rtc, int[] pset, String sid)throws Exception{
		SAMessage<LC> sam = new SAMessage<LC>();
		List<SAMessage<LC>> responseList = new Vector<SAMessage<LC>>();
		synchronized(responseMap){
			responseMap.put(sid, responseList);
		}
		try{
			// step one: broadcast a snapshot request to all processes
			sam.type = SAMessage.SAM_REQ_CREAT_I1;
			sam.rtc = rtc;
			sam.sender = myRank;
//			sam.receiver = enSize;
			sam.sid = sid;
			// step two: receive all snapshot id.
//			List<ILogicalClock>[] pl = new Vector[enSize];
			int nP=(pset==null)?enSize:pset.length;
      ILogicalClock[] vcs = new ILogicalClock[nP];
      int pos;
			synchronized(responseList){
			  if(pset==null){
				  for(int i=0;i<enSize;i++)
				    ctxt.saSend(i, sam);
			  }
			  else{
			    for(int i=0;i<pset.length;i++)
			      ctxt.saSend(pset[i], sam);
			  }
				do{
					responseList.wait();
				}while(responseList.size()<nP);
				pos = 0;
				Iterator<SAMessage<LC>> msgitr = responseList.iterator();
				while(msgitr.hasNext())
				{
					SAMessage<LC> msg = msgitr.next();
					if(msg.type == SAMessage.SAM_RES_NAK)
						throw new Exception("Wait for"+SAMessage.SAM_RES_CREAT_P1+", but NAK message received from process-"+msg.sender);
					vcs[pos++] = msg.lc;
				}
				responseList.clear();
			}
			// step three: calculate and broadcast the snapshot LC
			
			LC slc = (LC)((LC)vcs[0]).getCausallyConsistentClock(vcs);
			
			sam.type = SAMessage.SAM_REQ_CREAT_I2;
			sam.lc = slc;
			// step four: wait for all response.
			synchronized(responseList){
				for(int i=0;i<nP;i++)
					ctxt.saSend(pset==null?i:pset[i], sam);
				do{
					responseList.wait();
				}while(responseList.size()<nP);
				Iterator<SAMessage<LC>> itr = responseList.iterator();
				while(itr.hasNext()){
					SAMessage<LC> msg = itr.next();
					if(msg.type == SAMessage.SAM_RES_NAK)
						throw new Exception("Wait for"+SAMessage.SAM_RES_CREAT_P2+", but NAK message received from process-"+msg.sender);
				}
			}
		}finally{	
			synchronized(responseMap){
				responseMap.remove(sid);
			}
		}
	}
	
	/**
	 * delete a distributed snapshot
	 * @param sid
	 */
	public void deleteSnapshot(String sid)throws Exception{
		// broadcast delete message
		List<SAMessage<LC>> responseList = new Vector<SAMessage<LC>>();
		synchronized(responseMap){
			responseMap.put("d"+sid, responseList);
		}
		try{
			SAMessage<LC> sam = new SAMessage<LC>();
			sam.receiver = enSize;
			sam.sid = sid;
			sam.type = SAMessage.SAM_REQ_DELET_SS;
			for(int i=0;i<enSize;i++){
				ctxt.saSend(i, sam);
			}
			// wait for response
			synchronized(responseList){
				do{
					responseList.wait();
				}while(responseList.size()<enSize);
				Iterator<SAMessage<LC>> itr = responseList.iterator();
				while(itr.hasNext()){
					SAMessage<LC> msg = itr.next();
					if(msg.type == SAMessage.SAM_RES_NAK)
						throw new Exception("Wait for"+SAMessage.SAM_RES_DELET_SS+", but NAK message received from process-"+msg.sender);
				}
			}
		}finally{
			synchronized(responseMap){
				responseMap.remove("d"+sid);
			}
		}
	}
	
	/**
	 * responseMap is used for receive message from the sender
	 */
	Map<String, List<SAMessage<LC>>> responseMap = new HashMap<String,List<SAMessage<LC>>>();

	@Override
	public void run() {
		// SA is a thread that polling the message and response to requests
		// TODO: multi-thread
		do{
			@SuppressWarnings("unchecked")
			SAMessage<LC> msg = (SAMessage<LC>)ctxt.saRecvMsg();
			if(msg.type==SAMessage.SAM_SHUTDOWN)break;
			else if((msg.type&0xffff0000)==0x20000){//response
				List<SAMessage<LC>> rl = responseMap.get(msg.sid);
				synchronized(rl){
					rl.add(msg);
					//if(rl.size()==enSize)rl.notify();
					rl.notify();
				}
			}
			else switch(msg.type)
			{//request messages
			case SAMessage.SAM_REQ_CREAT_I1:
				try{
					LC lc = ctxt.since(msg.rtc);
					SAMessage<LC> res = msg.createResponse(myRank);
					res.type = SAMessage.SAM_RES_CREAT_P1;
					res.sid = msg.sid;
					res.lc = lc;
					ctxt.saSend(msg.sender, res);
				}catch(IOException ioe){
					//TODO log
					System.err.println(ioe);
					ioe.printStackTrace(System.err);
				}
				break;
			case SAMessage.SAM_REQ_CREAT_I2:
				try{
					ctxt.createLocalSnapshot(msg.sid, msg.lc);
					SAMessage<LC> res = msg.createResponse(myRank);
					res.type = SAMessage.SAM_RES_CREAT_P2;
					res.sid = msg.sid;
					ctxt.saSend(msg.sender, res);
				}catch(Exception ioe){
					//TODO log
					System.err.println(ioe);
					ioe.printStackTrace(System.err);
				}
				break;
			case SAMessage.SAM_REQ_DELET_SS:
				try{
					ctxt.deleteLocalSnapshot(msg.sid);
					SAMessage<LC> res = msg.createResponse(myRank);
					res.type = SAMessage.SAM_RES_DELET_SS;
					res.sid = msg.sid;
					ctxt.saSend(msg.sender, res);
				}catch(Exception ioe){
					//TODO log
					System.err.println(ioe);
					ioe.printStackTrace(System.err);
				}
				break;
			}
		}while(true);
	}
}
