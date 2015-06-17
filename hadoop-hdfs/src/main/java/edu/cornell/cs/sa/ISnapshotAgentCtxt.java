/**
 * 
 */
package edu.cornell.cs.sa;

import java.io.IOException;

import java.io.Serializable;

/**
 * @author sonic
 * The snapshot agent context is responsible for
 * - creating local snapshots.
 * - sending/receiving snapshot agent messages.
 *
 */
public interface ISnapshotAgentCtxt<LC extends ILogicalClock> {
	
	/**
	 * createLocalSnapshot: create a local snapshot
	 * @param sid: snapshot name.
	 * @throws Exception
	 */
	void createLocalSnapshot(String sid, LC lc) throws Exception;
/*	
	/**
	 * createLocalSnapshot: create a local snapshot
	 * @param rtc: the Real-time Clock for the snapshot.
	 * @param sn: snapshot name.
	 *
	void createLocalSnapshot(long rtc, String sid);
*/	
	/**
	 * deleteLocalSnapshot: delete a local snapshot.
	 * Note that the snapshot agent will use this to clear
	 * some unsuccessful snapshot. So the context may be
	 * asked to delete a local snapshot not created. Please
	 * just do nothing for that situation.
	 * @param sid
	 */
	void deleteLocalSnapshot(String sid);
	
	/**
	 * get the list of logic event since given rtc.
	 * @param rtc
	 * @return
	 */
	LC since(long rtc);
	
	/**
	 * set the message to recipients
	 * @param msg
	 */
	void saSend(int rpid,Serializable obj)throws IOException;
	/**
	 * read a message from my receiving queue.
	 * @return
	 */
	Serializable saRecvMsg();
}
