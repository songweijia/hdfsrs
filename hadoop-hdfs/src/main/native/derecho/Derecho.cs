using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Derecho
{

    public class ipAddr : IEquatable<ipAddr>
    {
        public int theAddr;

        public ipAddr(int who)
        {
            theAddr = who;
        }

        public int getPid()
        {
            return theAddr;
        }

        public bool Equals(ipAddr someone)
        {
            if(someone == null)
            {
                return false;
            }
            return someone.theAddr == theAddr;
        }

        public override string ToString()
        {
            return "<" + theAddr + ">";
        }
    }

    public class View
    {
        public const int N = 10;
        internal static int vcntr;
        internal int ViewInstance = ++vcntr;

        public int vid = 0;				                        // Sequential: 0, 1, …
        public ipAddr[] members = new ipAddr[N];		        // Members in view k
        public int n = 0;				                        // Number of members
        public bool[] Failed = new bool[N];			            // True if members[i] is considered to have failed
        public int nFailed = 0;
        internal ipAddr who;			                        // Process that joined or departed since the prior view
        public int myRank = 0; 			                        // For member p, returns rankOf(p) 
        public RDMC[] rdmc = new RDMC[N];                       // One per group member.  Process p sends in rdmc[p]; 
        public SST gmsSST;            	                        // See discussion of gmsSST below

        public View()
        {
        }

        public void newView(View Vc)
        {
            Console.WriteLine("Process " + Vc.members[Vc.myRank] + "New view: " + Vc.ToString());
        }

        public ipAddr Joined()
        {
            if (who == null)
                return null;
            for (int r = 0; r < n; r++)
            {
                if (members[r].Equals(who))
                {
                    return who;
                }
            }
            return null;
        }

        public ipAddr Departed()
        {
            if (who == null)
                return null;
            for (int r = 0; r < n; r++)
            {
                if (members[r].Equals(who))
                {
                    return null;
                }
            }
            return who;
        }
        public int RankOfLeader()
        {
            for (int r = 0; r < Failed.Length; r++)
            {
                if (!Failed[r])
                {
                    return r;
                }
            }
            return -1;
        }
        public int RankOf(ipAddr who)
        {
            for (int r = 0; r < n && members[r] != null; r++)
            {
                if (members[r].Equals(who))
                {
                    return r;
                }
            }
            return -1;
        }


        public bool IKnowIAmLeader = false;                     // I am the leader (and know it)

        public bool IAmLeader()
        {
            return (RankOfLeader() == myRank);  // True if I know myself to be the leader
        }

        public void Destroy()
        {
            for (int n = 0; n < N; n++)
            {
                if (rdmc[n] != null)
                {
                    rdmc[n].Destroy();
                }
            }
        }

        public override string ToString()
        {
            string s = "View " + vid + ": MyRank=" + myRank + "... ";
            string ms = " ";
            for (int m = 0; m < n; m++)
            {
                ms += members[m].ToString() + "  ";
            }

            s += "Members={" + ms + "}, ";
            string fs = " ";
            for (int m = 0; m < n; m++)
            {
                fs += Failed[m] ? " T " : " F ";
            }

            s += "Failed={" + fs + " }, nFailed=" + nFailed;
            ipAddr dep = Departed();
            if (dep != null)
            {
                s += ", Departed: " + dep;
            }

            ipAddr join = Joined();
            if (join != null)
            {
                s += ", Joined: " + join;
            }

            s += "\n" + gmsSST.ToString();
            return s;
        }
    }

    public class Msg				                    // Pass these using smart pointers!
    {
        public int len;				                // Length in bytes, > 0
        public byte[] body;			                // Data for this message, sequentially allocated, pinned
    };


    public class SST
    {
        internal static int scntr;
        internal int SSTInstance = ++scntr;

        public int myPid;
        public GMSSST[] row = new GMSSST[View.N];

        public SST(int vid, int pid, int nRows, ipAddr[] pids)
        {
            myPid = pid;
            for (int n = 0; n < nRows; n++)
            {
                row[n] = new GMSSST(vid, pids[n]);
            }
        }

        public static void InitializeFromOldSST(View Vnext, SST old, int whichFailed)
        {
            int m = 0;
            for (int n = 0; n < Vnext.n && old.row[n] != null; n++)
            {
                if (n != whichFailed)
                {
                    old.row[n].UseToInitialize(Vnext.gmsSST.row[m++]);
                }
            }
        }

        public void Push(int myRank, int vid)
        {
        }

        public void Pull(View Vc)
        {
        }

        public void Freeze(int r)
        {
            // Freezes row r
        }

        public void Disable()
        {
            // Disables rule evaluation, but the SST remains live in the sense that gmsSST.Push(Vc.myRank) still works
            // SST will never be re-enabled
        }

        public void Enable()
        {
            // Enables rule evaluation in a new SST instance
        }

        public void Destroy()
        {
            // Tear down this SST
        }

        public override string ToString()
        {
            string s = "SST:\n";
            for (int r = 0; r < row.Length; r++)
            {
                if (row[r] != null)
                {
                    s += row[r].ToString();
                }
            }
            return s;
        }
    }

    public class RDMC
    {
        private bool wedged;
        private bool[] frozen = new bool[View.N];

        public RDMC(int sender, ipAddr[] members, int howMany)
        {
            // Set up an RDMC with the designated sender from this list of members.  members[sender] is the ipAddr of the sender
        }

        public bool Send(Msg m)
        {
            return !wedged;
        }

        public void Wedge()
        {
            wedged = true;
        }

        public void Destroy()
        {
            // Tear down connections
        }

        public void PrepareToReceive(int n)
        {
        }

        public void Receive(Msg m)
        {
        }

        internal static void Destroy(RDMC[] rDMC)
        {
        }
    }

    public class GMSSST
    {
        // Fields used in debugging
        internal static int rcntr;
        internal int gmsSSTRowInstance = ++rcntr;
        internal int gmsSSTRowTime;
        public ipAddr theIPA;                                       // Saves hassle and simplifies debugging to have this handy
        internal int vid;                                           // Viewid associated with this SST
        internal static int maxChanges;                             // Debugging: largest value nChanges ever had

        volatile public bool[] Suspected = new bool[View.N]; 		// Aggregated vector of length |Vc.members|, <= N
        volatile public ipAddr[] Changes = new ipAddr[View.N];      // A vector of nChanges “proposed changes”.   Total number never exceeds N/2
        //      If a request is a Join, Changes[i] in Vc.members
        //      If a request is a Departure, Changes[i] not-in Vc.members
        volatile public int nChanges;			                    // How many changes are pending
        volatile public int nCommitted;		                        // How many of those have reached the commit point
        volatile public int nAcked;		                            // Incremented by a member to acknowledge that it has seen a proposed change.
        volatile public int[] nReceived = new int[View.N];		    // For each sender k, nReceived[k] is the number received (a.k.a. “locally stable”) 
        volatile public bool Wedged;			                    // Set after calling RDMC.Wedged(), reports that this member is wedged. Must be after nReceived!
        volatile public int[] GlobalMin = new int[View.N];          // Vector of how many to accept from each sender, K1-able
        volatile public bool GlobalMinReady;	                    // Must come after GlobalMin

        public GMSSST(int v, ipAddr ipa)
        {
            vid = v;
            if((theIPA = ipa) == null)
            {
                throw new Exception("GMSST constructor");
            }
        }

        public static int ModIndexer(int n)
        {
            return n % View.N;
        }

        public void UseToInitialize(GMSSST newSST)
        {
            Array.Copy(Changes, newSST.Changes, Changes.Length);
            newSST.nChanges = nChanges;
            newSST.nCommitted = nCommitted;
            newSST.nAcked = nAcked;
            newSST.gmsSSTRowTime = gmsSSTRowTime;
        }

        public void CopyTo(GMSSST destSST)
        {
            if (vid > 0 && destSST.vid > 0 && destSST.vid != vid) throw new ArgumentOutOfRangeException("V." + destSST.vid + " attempting to overwrite SST for V." + vid);
            if (!destSST.theIPA.Equals(theIPA)) throw new ArgumentOutOfRangeException("Setting GMSSST row for " + destSST.theIPA + " from the SST row for " + theIPA);
            if (destSST.gmsSSTRowTime > gmsSSTRowTime) throw new ArgumentOutOfRangeException("Setting GMSSST row for " + destSST.theIPA + " from an OLDER SST row for " + theIPA);
            Array.Copy(Suspected, destSST.Suspected, Suspected.Length);
            Array.Copy(Changes, destSST.Changes, Changes.Length);
            destSST.nChanges = nChanges;
            destSST.nCommitted = nCommitted;
            destSST.nAcked = nAcked;
            Array.Copy(nReceived, destSST.nReceived, nReceived.Length);
            if (destSST.Wedged && !Wedged) throw new ArgumentOutOfRangeException("Setting instance " + destSST.gmsSSTRowInstance + " Wedged = " + Wedged + " from " + gmsSSTRowInstance);
            destSST.Wedged = Wedged;
            Array.Copy(GlobalMin, destSST.GlobalMin, GlobalMin.Length);
            destSST.GlobalMinReady = GlobalMinReady;
            destSST.gmsSSTRowTime = gmsSSTRowTime;
        }

        public override string ToString()
        {
            string s = theIPA + "@ vid=" + vid + "[row-time=" + gmsSSTRowTime + "]: ";
            string tf = " ";
            for (int n = 0; n < View.N; n++)
            {
                tf += (Suspected[n]? "T": "F") + " ";
            }

            s += "Suspected={" + tf + "}, nChanges=" + nChanges + ", nCommitted=" + nCommitted;
            string ch = " ";
            for (int n = nCommitted; n < nChanges; n++)
            {
                ch += Changes[GMSSST.ModIndexer(n)];
            }
            string rs = " ";
            for (int n = 0; n < nReceived.Length; n++)
            {
                rs += nReceived[n] + " ";
            }

            s += ", Changes={" + ch + " }, nAcked=" + nAcked + ", nReceived={" + rs + "}";
            string gs = " ";
            for (int n = 0; n < GlobalMin.Length; n++)
            {
                gs += GlobalMin[n] + " ";
            }

            s += ", Wedged = " + (Wedged? "T": "F") + ", GlobalMin = {" + gs + "}, GlobalMinReady=" + GlobalMinReady + "\n";
            return s;
        }
    }

    class Group
    {
        internal string gname;
        internal View theView;

        public Group(string gn)
        {
            gname = gn;
        }

        public void SetView(View Vc)
        {
            theView = Vc;
        }

        public void Restart(int pid)
        {
            Console.WriteLine("Process <" + pid + ">: RESTART Derecho");
            ipAddr p = new ipAddr(pid);
            View Vc = theView;
            Vc.IKnowIAmLeader = true;
            Vc.members[0] = p;
            Vc.n = 1;
            Vc.vid = 0;
            SetupSSTandRDMC(pid, Vc, Vc.members);
        }

        public void Leave(View Vc)
        {
            Console.WriteLine("Process " + Vc.members[Vc.myRank].getPid() + ": Leave Derecho");
            ((SST)Vc.gmsSST).row[Vc.myRank].Suspected[Vc.myRank] = true;
            Vc.gmsSST.Push(Vc.myRank, Vc.vid);
        }

        static volatile ipAddr[] Joiners = new ipAddr[10];
        static volatile int nJoiners = 0;
        static volatile int JoinsProcessed = 0;

        public void Join(int pid)
        {
            int cnt = 0;
            Console.WriteLine("Process <" + pid + ">: JOIN Derecho");
            Joiners[nJoiners] = new ipAddr(pid);
            nJoiners++;
            while (theView == null)
            {
                Thread.Sleep(2500);
                if(cnt++ == 30)
                {
                    throw new Exception("Join failed");
                }
            }
            View Vc = theView;
            Console.WriteLine("Process <" + pid + ">: JOIN Derecho successful, I was added in view " + Vc.vid);
            SetupSSTandRDMC(pid, Vc, Vc.members);
            Vc.gmsSST.Pull(Vc);
            Vc.newView(Vc);
        }

        private void SetupSSTandRDMC(int pid, View Vc, ipAddr[] pids)
        {
            if(Vc.gmsSST != null)
            {
                throw new Exception("Overwritting the SST");
            }

            Vc.gmsSST = new SST(Vc.vid, pid, Vc.n, pids);
            for (int sender = 0; sender < Vc.n; sender++)
            {
                Vc.rdmc[sender] = new RDMC(sender, Vc.members, Vc.n);
            }

            Vc.gmsSST.Enable();
        }

        public bool JoinsPending()
        {
            return nJoiners > JoinsProcessed;
        }

        public void ReceiveJoin(View Vc)
        {
            ipAddr q = Joiners[JoinsProcessed++];
            if (q == null) throw new Exception("q null in ReceiveJoin");
            SST gmsSST = Vc.gmsSST;
            if ((gmsSST.row[Vc.myRank].nChanges-gmsSST.row[Vc.myRank].nCommitted) == gmsSST.row[Vc.myRank].Changes.Length)
            {
                throw new Exception("Too many changes to allow a Join right now");
            }

            gmsSST.row[Vc.myRank].Changes[GMSSST.ModIndexer(gmsSST.row[Vc.myRank].nChanges)] = q;
            gmsSST.row[Vc.myRank].nChanges++;
            if (Vc.gmsSST.row[Vc.myRank].nChanges > GMSSST.maxChanges)
            {
                GMSSST.maxChanges = Vc.gmsSST.row[Vc.myRank].nChanges;
            }
            for (int n = 0; n < Vc.n; n++)
                Vc.rdmc[n].Wedge();				                    // RDMC finishes sending, then stops sending or receiving in Vc
            gmsSST.row[Vc.myRank].Wedged = true;				    // True if RDMC has halted new sends and receives in Vc
            gmsSST.Push(Vc.myRank, Vc.vid);
        }

        public void CommitJoin(View Vc, ipAddr q)
        {
            Console.WriteLine("CommitJoin: Vid=" + Vc.vid + "... joiner is " + q);
            new Thread(delegate()
            {
                // Runs in a separate thread in case state transfer is (in the future) at all slow
                View Vcp = new View();
                Vcp.vid = Vc.vid;
                Array.Copy(Vc.members, Vcp.members, Vc.n);
                Vcp.n = Vc.n;
                Vcp.myRank = Vc.RankOf(q);
                Vcp.who = Vcp.members[Vcp.myRank];
                Array.Copy(Vc.Failed, Vcp.Failed, Vc.Failed.Length);
                Vcp.nFailed = Vc.nFailed;
                Console.WriteLine("Sending View " + Vcp.vid + " to " + q);
                // EDWARD TO DO //
            }).Start();
        }

        public void ReportFailure(View Vc, ipAddr who)
        {
            int r = Vc.RankOf(who);
            ((SST)Vc.gmsSST).row[Vc.myRank].Suspected[Vc.myRank] = true;
            Vc.gmsSST.Push(Vc.myRank, Vc.vid);
        }
    }
    class Program
    {
        static void Main(string[] args)
        {
            Group g = new Group("Derecho-Test");
            for (int pid = 0; pid < 10; pid++)
            {
                Launch(g, pid);
            }
        }

        static void Launch(Group g, int pid)
        {
            Thread t = new Thread(delegate()
                {
                    if (pid == 0)
                    {
                        /* Restart the system */
                        g.SetView(new View());
                        g.Restart(pid);
                        beNode(g, pid);
                    }
                    else
                    {
                        Thread.Sleep(pid * 5000);
                        g.Join(pid);
                        beNode(g, pid);
                    }

                    Console.WriteLine("TERMINATION: Pid<" + pid + ">");
                }) { Name = "Process " + pid };
            t.Start();
        }

        static bool NotEqual(View Vc, bool[] old)
        {
            for (int r = 0; r < Vc.n; r++)
            {
                for (int who = 0; who < View.N; who++)
                {
                    if (Vc.gmsSST.row[r].Suspected[who] && !old[who])
                    {
                        return true;
                    }
                }
            }
            return false;
        }

        static void Copy(View Vc, bool[] old)
        {
            int myRank = Vc.myRank;
            for (int who = 0; who < Vc.n; who++)
            {
                old[who] = Vc.gmsSST.row[myRank].Suspected[who];
            }
        }

        static bool ChangesContains(View Vc, ipAddr q)
        {
            GMSSST myRow = Vc.gmsSST.row[Vc.myRank];
            for (int n = myRow.nCommitted; n < myRow.nChanges; n++)
            {
                ipAddr p = myRow.Changes[GMSSST.ModIndexer(n)];
                if (p != null && p.Equals(q))
                {
                    return true;
                }
            }
            return false;
        }

        static int MinAcked(View Vc, bool[] Failed)
        {
            int myRank = Vc.myRank;
            int min = Vc.gmsSST.row[myRank].nAcked;
            for (int n = 0; n < Vc.n; n++)
            {
                if (!Failed[n] && Vc.gmsSST.row[n].nAcked < min)
                {
                    min = Vc.gmsSST.row[n].nAcked;
                }
            }

            return min;
        }

        static bool IAmTheNewLeader(View Vc, SST sst)
        {
            if (Vc.IKnowIAmLeader)
            {
                return false;                                       // I am the OLD leader
            }

            for (int n = 0; n < Vc.myRank; n++)
            { 
                for (int row = 0; row < Vc.myRank; row++)
                {
                    if (!Vc.Failed[n] && !Vc.gmsSST.row[row].Suspected[n])
                    {
                        return false;                               // I'm not the new leader, or some failure suspicion hasn't fully propagated
                    }
                }
            }
            Vc.IKnowIAmLeader = true;
            return true;
        }

        static void Merge(View Vc, int myRank)
        {
            // Merge the change lists
            for (int n = 0; n < Vc.n; n++)
            {
                if (Vc.gmsSST.row[myRank].nChanges < Vc.gmsSST.row[n].nChanges)
                {
                    Array.Copy(Vc.gmsSST.row[n].Changes, Vc.gmsSST.row[myRank].Changes, Vc.gmsSST.row[myRank].Changes.Length);
                    Vc.gmsSST.row[myRank].nChanges = Vc.gmsSST.row[n].nChanges;
                    if(Vc.gmsSST.row[myRank].nChanges > GMSSST.maxChanges)
                    {
                        GMSSST.maxChanges = Vc.gmsSST.row[myRank].nChanges;
                    }
                }

                if (Vc.gmsSST.row[myRank].nCommitted < Vc.gmsSST.row[n].nCommitted)	// How many I know to have been committed
                {
                    Vc.gmsSST.row[myRank].nCommitted = Vc.gmsSST.row[n].nCommitted;
                }
            }
            bool found = false;
            for (int n = 0; n < Vc.n; n++)
            {
                if (Vc.Failed[n])
                {
                    // Make sure that the failed process is listed in the Changes vector as a proposed change
                    for (int c = Vc.gmsSST.row[myRank].nCommitted; c < Vc.gmsSST.row[myRank].nChanges && !found; c++)
                    {
                        if (Vc.gmsSST.row[myRank].Changes[GMSSST.ModIndexer(c)].Equals(Vc.members[n]))
                        {
                            // Already listed
                            found = true;
                        }
                    }
                }
                else
                {
                    // Not failed
                    found = true;
                }

                if (!found)
                {
                    Vc.gmsSST.row[myRank].Changes[GMSSST.ModIndexer(Vc.gmsSST.row[myRank].nChanges)] = Vc.members[n];
                    Vc.gmsSST.row[myRank].nChanges++;
                    if (Vc.gmsSST.row[myRank].nChanges > GMSSST.maxChanges)
                    {
                        GMSSST.maxChanges = Vc.gmsSST.row[myRank].nChanges;
                    }
                }
            }
            Vc.gmsSST.Push(Vc.myRank, Vc.vid);
        }

        public static void beNode(Group g, int pid)
        {
            ipAddr myAddr = new ipAddr(pid);
            bool[] oldSuspected = new bool[View.N];

            while (true)
            {
                View Vc = g.theView;
                SST gmsSST = Vc.gmsSST;
                int Leader = Vc.RankOfLeader(), myRank = Vc.myRank;

                if (NotEqual(Vc, oldSuspected))
                {
                    // Aggregate suspicions into gmsSST[myRank].Suspected;
                    for (int r = 0; r < Vc.n; r++)
                    {
                        for (int who = 0; who < Vc.n; who++)
                        {
                            gmsSST.row[myRank].Suspected[who] |= gmsSST.row[r].Suspected[who];
                        }
                    }

                    for (int q = 0; q < Vc.n; q++)
                    {
                        if (gmsSST.row[myRank].Suspected[q] && !Vc.Failed[q])
                        {
                            if (Vc.nFailed + 1 >= View.N / 2)
                            {
                                throw new Exception("Majority of a Derecho group simultaneously failed … shutting down");
                            }

                            gmsSST.Freeze(q); 				                    // Cease to accept new updates from q
                            for (int n = 0; n < Vc.n; n++)
                            {
                                Vc.rdmc[n].Wedge();				                // RDMC finishes sending, then stops sending or receiving in Vc
                            }

                            gmsSST.row[myRank].Wedged = true;                   // RDMC has halted new sends and receives in Vc
                            Vc.Failed[q] = true;
                            Vc.nFailed++; 

                            if (Vc.nFailed > Vc.n / 2 || (Vc.nFailed == Vc.n / 2 && Vc.n % 2 == 0))
                            {
                                throw new Exception("Potential partitioning event: this node is no longer in the majority and must shut down!");
                            }

                            gmsSST.Push(Vc.myRank, Vc.vid);
                            if (Vc.IAmLeader() && !ChangesContains(Vc, Vc.members[q]))		// Leader initiated
                            {
                                if ((gmsSST.row[myRank].nChanges - gmsSST.row[myRank].nCommitted) == gmsSST.row[myRank].Changes.Length)
                                {
                                    throw new Exception("Ran out of room in the pending changes list");
                                }

                                gmsSST.row[myRank].Changes[GMSSST.ModIndexer(gmsSST.row[myRank].nChanges)] = Vc.members[q];    		// Reports the failure (note that q NotIn members)
                                gmsSST.row[myRank].nChanges++;
                                Console.WriteLine("NEW SUSPICION: adding " + Vc.members[q] + " to the CHANGES/FAILED list");
                                if (gmsSST.row[myRank].nChanges > GMSSST.maxChanges)
                                {
                                    GMSSST.maxChanges = gmsSST.row[myRank].nChanges;
                                }
                                gmsSST.Push(Vc.myRank, Vc.vid);
                            }
                        }
                    }
                    Copy(Vc, oldSuspected);
                }

                if (Vc.IAmLeader() && g.JoinsPending())
                {
                    g.ReceiveJoin(Vc);
                }

                int M;
                if (myRank == Leader && (M = MinAcked(Vc, Vc.Failed)) > gmsSST.row[myRank].nCommitted)
                {
                    gmsSST.row[myRank].nCommitted = M;				                // Leader commits a new request
                    gmsSST.Push(Vc.myRank, Vc.vid);
                }

                if (gmsSST.row[Leader].nChanges > gmsSST.row[myRank].nAcked)
                {
                    WedgeView(Vc, gmsSST, myRank);		                // True if RDMC has halted new sends, receives in Vc
                    if (myRank != Leader)
                    {
                        Array.Copy(gmsSST.row[Leader].Changes, gmsSST.row[myRank].Changes, gmsSST.row[myRank].Changes.Length);		// Echo (copy) the vector including the new changes
                        gmsSST.row[myRank].nChanges = gmsSST.row[Leader].nChanges;	    // Echo the count
                        gmsSST.row[myRank].nCommitted = gmsSST.row[Leader].nCommitted;
                    }

                    gmsSST.row[myRank].nAcked = gmsSST.row[Leader].nChanges;        // Notice a new request, acknowledge it
                    gmsSST.Push(Vc.myRank, Vc.vid);
                }

                if (gmsSST.row[Leader].nCommitted > Vc.vid)
                {
                    gmsSST.Disable();                                               // Disables the SST rule evaluation for this SST
                    WedgeView(Vc, gmsSST, myRank);
                    ipAddr q = gmsSST.row[myRank].Changes[GMSSST.ModIndexer(Vc.vid)];
                    View Vnext = new View();
                    Vnext.vid = Vc.vid + 1;
                    Vnext.IKnowIAmLeader = Vc.IKnowIAmLeader;
                    ipAddr myIPAddr = Vc.members[myRank];
                    bool failed;
                    int whoFailed = Vc.RankOf(q);
                    if (whoFailed != -1)
                    {
                        failed = true;
                        Vnext.nFailed = Vc.nFailed - 1;
                        Vnext.n = Vc.n - 1;
                    }
                    else
                    {
                        failed = false;
                        Vnext.nFailed = Vc.nFailed;
                        Vnext.n = Vc.n;
                        Vnext.members[Vnext.n++] = q;
                    }

                    int m = 0;
                    for (int n = 0; n < Vc.n; n++)
                    {
                        if (n != whoFailed)
                        {
                            Vnext.members[m] = Vc.members[n];
                            Vnext.Failed[m] = Vc.Failed[n];
                            ++m;
                        }
                    }

                    Vnext.who = q;
                    if((Vnext.myRank = Vnext.RankOf(myIPAddr)) == -1)
                    {
                        Console.WriteLine("Some other process reported that I failed.  Process " + myIPAddr + " terminating");
                        return;
                    }

                    if (Vnext.gmsSST != null)
                    {
                        throw new Exception("Overwritting the SST");
                    }

                    Vc.gmsSST.Pull(Vc);
                    Vnext.gmsSST = new SST(Vnext.vid, Vc.gmsSST.myPid, Vnext.n, Vnext.members);
                    SST.InitializeFromOldSST(Vnext, gmsSST, whoFailed);
                    Vnext.gmsSST.Pull(Vnext);

                    // The intent of these next lines is that we move entirely to Vc+1 and cease to use Vc
                    RaggedEdgeCleanup(Vc);			            // Finalize deliveries in Vc

                    for (int sender = 0; sender < Vnext.n; sender++)
                    {
                        Vnext.rdmc[sender] = new RDMC(sender, Vnext.members, Vnext.n);
                    }

                    if (Vc.IAmLeader() && !failed)
                    {
                        g.CommitJoin(Vnext, q);
                    }

                    Vnext.gmsSST.Enable();
                    Vnext.gmsSST.Push(Vnext.myRank, Vc.vid);

                    View oldView = Vc;
                    Vc = Vnext;
                    gmsSST = Vc.gmsSST;
                    Vc.newView(Vnext);			                // Announce the new view to the application 

                    // Finally, some cleanup.  These could be in a background thread
                    RDMC.Destroy(oldView.rdmc);		                // The old RDMC instance is no longer active
                    oldView.gmsSST.Destroy();		        // The old SST instance can be discarded too
                    oldView.Destroy();				                // VC no longer active 

                    // First task with my new view...
                    if (IAmTheNewLeader(Vc, gmsSST))		// I’m the new leader and everyone who hasn’t failed agrees
                    {
                        Merge(Vc, Vc.myRank);                  // Create a combined list of Changes
                    }
                }
            }
        }

        private static void WedgeView(View Vc, SST gmsSST, int myRank)
        {
            for (int n = 0; n < Vc.n; n++)
            {
                Vc.rdmc[n].Wedge();						                    // RDMC finishes sending, stops new sends or receives in Vc
            }

            gmsSST.row[myRank].Wedged = true;
        }

        public static void AwaitMetaWedged(View Vc)
        {
            int cnt = 0;
            for (int n = 0; n < Vc.n; n++)
            {
                while (!Vc.Failed[n] && !Vc.gmsSST.row[n].Wedged)
                {
                    /* busy-wait */
                    if (cnt++ % 100 == 0)
                    {
                        Console.WriteLine("Process " + Vc.members[Vc.myRank] + "... loop in AwaitMetaWedged / " + Vc.gmsSST.row[n].gmsSSTRowInstance);
                    }

                    Thread.Sleep(10);
                    Vc.gmsSST.Pull(Vc);
                }
            }
        }

        public static int AwaitLeaderGlobalMinReady(View Vc)
        {
            int Leader = Vc.RankOfLeader();
            while (!Vc.gmsSST.row[Leader].GlobalMinReady)
            {
                Leader = Vc.RankOfLeader();
                Vc.gmsSST.Pull(Vc);
            }
            return Leader;
        }

        public static void DeliverInOrder(View Vc, int Leader)
        {
            // Ragged cleanup is finished, deliver in the implied order
            string deliveryOrder = "Delivery Order (View " + Vc.vid + ") { ";
            for (int n = 0; n < Vc.n; n++)
            {
                deliveryOrder += Vc.gmsSST.myPid + ":0.." + Vc.gmsSST.row[Leader].GlobalMin[n] + " ";
            }

            Console.WriteLine(deliveryOrder + "}");
        }

        public static void RaggedEdgeCleanup(View Vc)
        {
            AwaitMetaWedged(Vc);
            int myRank = Vc.myRank;
            // This logic depends on the transitivity of the K1 operator, as discussed last week
            int Leader = Vc.RankOfLeader();				// We don’t want this to change under our feet
            if (Vc.IAmLeader())
            {
                Console.WriteLine("Running RaggedEdgeCleanup: " + Vc);
                bool found = false;
                Vc.gmsSST.Pull(Vc);
                for (int n = 0; n < Vc.n && !found; n++)
                {
                    if (Vc.gmsSST.row[n].GlobalMinReady)
                    {
                        Array.Copy(Vc.gmsSST.row[myRank].GlobalMin, Vc.gmsSST.row[n].GlobalMin, Vc.n);
                        found = true;
                    }
                }

                if (!found)
                {
                    for (int n = 0; n < Vc.n; n++)
                    {
                        int min = Vc.gmsSST.row[myRank].nReceived[n];
                        for (int r = 0; r < Vc.n; r++)
                        {
                            if (!Vc.Failed[r] && min > Vc.gmsSST.row[r].nReceived[n])
                            {
                                min = Vc.gmsSST.row[r].nReceived[n];
                            }
                        }

                        Vc.gmsSST.row[myRank].GlobalMin[n] = min;
                    }
                }

                Vc.gmsSST.row[myRank].GlobalMinReady = true;
                Vc.gmsSST.Push(Vc.myRank, Vc.vid);
                Console.WriteLine("RaggedEdgeCleanup: FINAL = " + Vc);
            }
            else
            {
                // Learn the leader’s data and push it before acting upon it
                Leader = AwaitLeaderGlobalMinReady(Vc);
                Array.Copy(Vc.gmsSST.row[myRank].GlobalMin, Vc.gmsSST.row[Leader].GlobalMin, Vc.n);
                Vc.gmsSST.row[myRank].GlobalMinReady = true;
                Vc.gmsSST.Push(Vc.myRank, Vc.vid);
            }

            DeliverInOrder(Vc, Leader);
        }
    }
 }
