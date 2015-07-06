package edu.cornell.cs.lab;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Console;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import edu.cornell.cs.sa.ISnapshotAgentCtxt;
import edu.cornell.cs.sa.SAMessage;
import edu.cornell.cs.sa.SnapshotAgent;
import edu.cornell.cs.sa.VectorClock;

/**
 * @author sonic
 * This is a demo process that show how distributed snapshot works.
 * TokenProcess is a process that exchange token with peer processes
 * through UDP diagram. Users can send a token manually through its
 * Cli. The user can also create a causally distributed snapshot for
 * all the processes. 
 */
public class TokenProcess implements ISnapshotAgentCtxt<VectorClock>, Runnable, Serializable {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = -831072921153382818L;

	public class TPSnapshot{
		VectorClock vc;
		int nToken;
		@Override
		public String toString(){
			StringBuffer sb = new StringBuffer();
			sb.append("SNAPSHOT:");
			sb.append(vc.toString());
			sb.append(",nToken="+nToken);
			return sb.toString();
		}
	}
	
	static public class TPMessage implements Serializable{
		private static final long serialVersionUID = 180906165872487651L;
		final static int TPM_SEND_TOKEN = 0;
		int type = TPM_SEND_TOKEN;
		VectorClock vc;
	}
	
	public class Event{
		static final int ET_LOCAL = 0x0;
		static final int ET_SEND = 0x1;
		static final int ET_RECV = 0x2;

		long rtc;
		VectorClock vc;
		int type;
		int nToken;
	}
	
	int nToken = 1;
	int myrank;
	VectorClock cvc; // current vectorclock
	String ensemble[] = null; // ensemble
	Map<String,TPSnapshot> snapshots = null; // snapshot map
	List<Event> eventList = null; // event list
	SnapshotAgent<VectorClock> sa = null;
	Thread daemonThread = null,saThread = null;
	DatagramSocket ds = null;
	List<SAMessage<VectorClock>> saMsgQueue = null;
	
	
	private TokenProcess(String ensemble[],int myrank)throws IOException{
		this.ensemble = ensemble;
		this.myrank = myrank;
		this.cvc = new VectorClock(myrank);
		this.snapshots = new HashMap<String,TPSnapshot>();
		this.eventList = new Vector<Event>(1024);
		Event ie = new Event();
		ie.nToken = this.nToken;
		ie.rtc = readRTC();
		ie.type = Event.ET_LOCAL;
		ie.vc = new VectorClock(this.cvc);
		eventList.add(ie);//add initial event
		sa = new SnapshotAgent<VectorClock>(ensemble.length, myrank, cvc, this);
		ds = new DatagramSocket();
		saMsgQueue = new LinkedList<SAMessage<VectorClock>>();
	}
	
	void send(int rank, Serializable obj)throws IOException{
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		ObjectOutputStream oos = new ObjectOutputStream(baos);
		oos.writeObject(obj);
		oos.flush();
		byte [] buf = baos.toByteArray();
		send(rank,buf,buf.length);
	}
	
	private String getHost(int rank){
		String [] hostAndPort = ensemble[rank].split(":");
		return hostAndPort[0];
	}
	private int getPort(int rank){
		String [] hostAndPort = ensemble[rank].split(":");
		return Integer.parseInt(hostAndPort[1]);
	}
	
	void send(int rank, byte[] buf, int length)throws IOException{
		InetAddress address = InetAddress.getByName(getHost(rank));
		DatagramPacket pkt = new DatagramPacket(buf,length,address,getPort(rank));
		ds.send(pkt);
	}
	
	@Override
	public void run(){
		try{
			// step 1: start sa
			saThread = new Thread(sa);
			saThread.start();
			// step 2: start network server
			final DatagramSocket ss = new DatagramSocket(getPort(myrank));
			final TokenProcess THIS = this;
			daemonThread = new Thread(new Runnable(){
				@SuppressWarnings("unchecked")
				@Override
				public void run() {
					byte [] buf = new byte[65536];
					
					while(true){
						DatagramPacket pkt = new DatagramPacket(buf, buf.length);
						try {
							ss.receive(pkt);
							ByteArrayInputStream bais = new ByteArrayInputStream(pkt.getData(),pkt.getOffset(),pkt.getLength());
							ObjectInputStream ois = new ObjectInputStream(bais);
							Object o = ois.readObject();
							if(o instanceof SAMessage){
								synchronized(THIS.saMsgQueue){
									THIS.saMsgQueue.add((SAMessage<VectorClock>)o);
									THIS.saMsgQueue.notify();
								}
							}
							else if(o instanceof TPMessage){
								TPMessage msg = (TPMessage)o;
								synchronized(THIS){
									Event e = new Event();
									e.vc = new VectorClock((VectorClock)THIS.cvc.tickOnRecv(msg.vc));
									e.rtc = readRTC();
									e.type = Event.ET_RECV;
									THIS.nToken ++;
									e.nToken = THIS.nToken;
									THIS.eventList.add(e);
								}
							}
						} catch (Exception e) {
							System.err.println(e);
							e.printStackTrace();
						}
					}
				}
			});
			daemonThread.start();
			Console console = System.console();
			while(true){
				System.out.print("# ");
				String cmdline = console.readLine();
				this.handleCmdLine(cmdline);
			}
		}catch(IOException ioe){
			System.err.println(ioe);
			ioe.printStackTrace();
		}finally{
			
		}
	}
	
	final static String CMD_HELP="help";
	final static String CMD_TICK="tick";
	final static String CMD_STAT="stat";
	final static String CMD_SEND="sendto";
	final static String CMD_CREATE_SS="css";
	final static String CMD_DELETE_SS="dss";
	final static String CMD_DUMP_EL="de";
	final static String CMD_LIST_SS="lss";
	final static String HELP_INFO = CMD_HELP+"\n\tPrint this message.\n" + 
			CMD_STAT+"\n\tShow my token number, rtc and logical clock time.\n" +
			CMD_TICK+"\n\tTick my Clock.\n" +
			CMD_SEND+" <rank>\n\tSend a token to peer\n" +
			CMD_CREATE_SS+" <rtc> <sid> [p1] [p2] ...\n\tCreate a snapshot.\n" +
			CMD_DELETE_SS+" <sid>\n\tDelete snapshot.\n" + 
			CMD_LIST_SS+" <sid>\n\tList local snapshot.\n" + 
			CMD_DUMP_EL+"\n\t dump event list";
	
	private void handleCmdLine(String cmdLine){
		String [] cmdTks = cmdLine.split("\\s");
		if(cmdTks[0].equals(CMD_HELP))
			handleHelp();
		else if(cmdTks[0].equals(CMD_STAT))
			handleStat();
		else if(cmdTks[0].equals(CMD_TICK))
			handleTick();
		else if(cmdTks[0].equals(CMD_SEND))
			handleSend(Integer.parseInt(cmdTks[1]));
		else if(cmdTks[0].equals(CMD_CREATE_SS))
			try {
				//sa.createSnapshot(Long.parseLong(cmdTks[1]), cmdTks[2]);
			  int pset[] = null;
			  if(cmdTks.length > 3){
			    pset = new int[cmdTks.length - 3];
			    for(int i=3;i<cmdTks.length;i++)
			      pset[i-3]=Integer.parseInt(cmdTks[i]);
			  }
			  sa.createSnapshot(Long.parseLong(cmdTks[1]), pset, cmdTks[2]);
			} catch (Exception e) {
				System.err.println(e);
				e.printStackTrace();
			}
		else if(cmdTks[0].equals(CMD_DELETE_SS))
			try {
				sa.deleteSnapshot(cmdTks[1]);
			} catch (Exception e) {
				System.err.println(e);
				e.printStackTrace();
			}
		else if(cmdTks[0].equals(CMD_DUMP_EL))
			dumpEvents();
		else if(cmdTks[0].equals(CMD_LIST_SS))
			listSnapshots();
	}
	
	private void handleHelp(){
		System.out.println(HELP_INFO);
	}
	
	private void handleStat(){
		// 1 - CLOCK
		System.out.print("VECTOR CLOCK: "+this.cvc);
		System.out.println("RTC CLOCK: "+readRTC());
		// 2 - Token
		System.out.println("TOKENS: "+this.nToken);
		// 3 - Processes and Rank
		System.out.println("Processes:");
		for(int i=0;i<ensemble.length;i++)
			System.out.println("\t"+i+((i==this.myrank)?"*":"")+"-"+ensemble[i]);
	}
	
	void handleTick(){
		Event e = new Event();
		e.nToken = this.nToken;
		e.rtc = readRTC();
		e.type = Event.ET_LOCAL;
		e.vc = new VectorClock((VectorClock)this.cvc.tick());
		synchronized(eventList){
			eventList.add(e);
		}
		System.out.println("done.");
	}
	
	void handleSend(int rank){
		if(nToken > 0){
			Event e = new Event();
			synchronized(this){
				e.nToken = --this.nToken;
				e.rtc = readRTC();
				e.type = Event.ET_SEND;
				e.vc = new VectorClock((VectorClock)this.cvc.tick());
				synchronized(eventList){
					eventList.add(e);
				}
			}
			TPMessage msg = new TPMessage();
			msg.vc = e.vc;
			try {
				send(rank,msg);
			} catch (IOException e1) {
				System.err.println(e1);
				e1.printStackTrace();
			}
		}else
			System.out.println("no more token, cannot send");
	}
	
	private void dumpEvents(){
		synchronized(eventList){
			Iterator<Event> itr = eventList.iterator();
			while(itr.hasNext()){
				Event e = itr.next();
				System.out.println("\tType="+e.type+",VC="+e.vc+",nToken="+e.nToken+",rtc="+e.rtc);
			}
		}
	}
	
	private void listSnapshots(){
		synchronized(this.snapshots){
			Iterator<String> itr = snapshots.keySet().iterator();
			while(itr.hasNext()){
				String sid = itr.next();
				TPSnapshot s = snapshots.get(sid);
				System.out.println(sid+"\t"+s);
			}
		}
	}
	
	public static long readRTC(){
		return System.currentTimeMillis();
	}
	
	/**
	 * @param args <rank> <P0> <P1> <P2> ...
	 * @throws IOException 
	 */
	public static void main(String args[]) throws IOException{
		if(args.length < 2){
			System.out.println("Params: <rank> <p0> <p1> <p2> ...");
			return;
		}
		String [] servers = new String[args.length - 1];
		int myrank = Integer.parseInt(args[0]);
		for(int i=1;i<args.length;i++)
			servers[i-1]=args[i];
		TokenProcess tp = new TokenProcess(servers,myrank);
		tp.run();
	}

	@Override
	public void createLocalSnapshot(String sid, VectorClock lc)
			throws Exception {
		long myver = lc.vc.get(myrank);
		Event e = eventList.get((int)myver);
		TPSnapshot s = new TPSnapshot();
		s.nToken = e.nToken;
		s.vc = lc;
		synchronized(snapshots){
			snapshots.put(sid, s);
		}
	}

	@Override
	public void deleteLocalSnapshot(String sid) {
		synchronized(snapshots){
			snapshots.remove(sid);
		}
	}

	@Override
	public VectorClock since(long rtc) {
		while(System.currentTimeMillis() < rtc){
			try {
				Thread.sleep(Math.min(5000, rtc-System.currentTimeMillis()));
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		int idx = eventList.size() - 1 ;
		while(idx>=0 && eventList.get(idx).rtc>rtc)idx--;
		if(idx<0)idx=0;
		return eventList.get(idx).vc;
	}

	@Override
	public void saSend(int rpid, Serializable obj) throws IOException {
		send(rpid,obj);
	}

	@Override
	public Serializable saRecvMsg() {
		synchronized(this.saMsgQueue){
			if(this.saMsgQueue.isEmpty()){
				try {
					this.saMsgQueue.wait();
				} catch (InterruptedException e) {
					System.err.println(e);
					e.printStackTrace();
				}
			}
			SAMessage<VectorClock> msg = this.saMsgQueue.remove(0);
			return msg;
		}
	}
}
