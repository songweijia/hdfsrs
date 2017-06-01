#include "Derecho.h"

#include <algorithm>
#include <exception>
#include <thread>
#include <chrono>
#include <future>

namespace Derecho
{

      ipAddr::ipAddr(int who)
      {
          theAddr = who;
      }

      int ipAddr::getPid()
      {
          return theAddr;
      }

      bool ipAddr::Equals(const ipAddr& someone) const
      {

          return someone.theAddr == theAddr;
      }

      std::string ipAddr::ToString() const
      {
          return std::string("<") + std::to_string(theAddr) + std::string(">");
      }

int View::vcntr = 0;

      View::View()
      {
      }

      void View::newView(const std::shared_ptr<View>& Vc)
      {
//C# TO C++ CONVERTER TODO TASK: There is no native C++ equivalent to 'ToString':
          std::cout << std::string("Process ") << Vc->members[Vc->myRank].ToString() << std::string("New view: ") << Vc->ToString() << std::endl;
      }

      std::shared_ptr<ipAddr> View::Joined() const
      {
          if (who == nullptr)
          {
              return nullptr;
          }
          for (int r = 0; r < n; r++)
          {
              if (members[r].Equals(*who))
              {
                  return who;
              }
          }
          return nullptr;
      }

      std::shared_ptr<ipAddr> View::Departed() const
      {
          if (who == nullptr)
          {
              return nullptr;
          }
          for (int r = 0; r < n; r++)
          {
              if (members[r].Equals(*who))
              {
                  return nullptr;
              }
          }
          return who;
      }

      int View::RankOfLeader() const
      {
          for (int r = 0; r < failed.size(); r++)
          {
              if (!failed[r])
              {
                  return r;
              }
          }
          return -1;
      }

      int View::RankOf(const ipAddr& who) const
      {
          for (int r = 0; r < n; r++)
          {
              if (members[r].Equals(who))
              {
                  return r;
              }
          }
          return -1;
      }

      bool View::IAmLeader() const
      {
          return (RankOfLeader() == myRank); // True if I know myself to be the leader
      }

      void View::Destroy()
      {
          for (int n = 0; n < N; n++)
          {
              rdmc[n].Destroy();
          }
      }

      std::string View::ToString() const
      {
          std::string s = std::string("View ") + std::to_string(vid) + std::string(": MyRank=") + std::to_string(myRank) + std::string("... ");
          std::string ms = " ";
          for (int m = 0; m < n; m++)
          {
              ms += members[m].ToString() + std::string("  ");
          }

          s += std::string("Members={") + ms + std::string("}, ");
          std::string fs = " ";
          for (int m = 0; m < n; m++)
          {
              fs += failed[m] ? " T " : " F ";
          }

          s += std::string("Failed={") + fs + std::string(" }, nFailed=") + std::to_string(nFailed);
          std::shared_ptr<ipAddr> dep = Departed();
          if (dep != nullptr)
          {
              s += std::string(", Departed: ") + dep->ToString();
          }

          std::shared_ptr<ipAddr> join = Joined();
          if (join != nullptr)
          {
              s += std::string(", Joined: ") + join->ToString();
          }

//C# TO C++ CONVERTER TODO TASK: There is no native C++ equivalent to 'ToString':
          s += std::string("\n") + gmsSST->ToString();
          return s;
      }

int SST::scntr = 0;

      SST::SST(int vid, int pid, int nRows, std::vector<ipAddr>& pids)
      {
          myPid = pid;
          for (int n = 0; n < nRows; n++)
          {
              row[n] = GMSSST(vid, pids[n]);
          }
      }

      void SST::InitializeFromOldSST(const std::shared_ptr<View>& Vnext, const std::shared_ptr<SST>& old, int whichFailed)
      {
          int m = 0;
          for (int n = 0; n < Vnext->n; n++)
          {
              if (n != whichFailed)
              {
                  old->row[n].UseToInitialize(Vnext->gmsSST->row[m++]);
              }
          }
      }

      void SST::Push(int myRank, int vid)
      {
      }

      void SST::Pull(const std::shared_ptr<View>& Vc)
      {
      }

      void SST::Freeze(int r)
      {
          // Freezes row r
      }

      void SST::Disable()
      {
          // Disables rule evaluation, but the SST remains live in the sense that gmsSST.Push(Vc.myRank) still works
          // SST will never be re-enabled
      }

      void SST::Enable()
      {
          // Enables rule evaluation in a new SST instance
      }

      void SST::Destroy()
      {
          // Tear down this SST
      }

      std::string SST::ToString()
      {
          std::string s = "SST:\n";
          for (int r = 0; r < row.size(); r++)
          {
              s += row[r].ToString();
          }
          return s;
      }

      RDMC::RDMC(int sender, std::vector<ipAddr>& members, int howMany)
      {
          // Set up an RDMC with the designated sender from this list of members.  members[sender] is the ipAddr of the sender
      }

      bool RDMC::Send(const std::shared_ptr<Msg>& m)
      {
          return !wedged;
      }

      void RDMC::Wedge()
      {
          wedged = true;
      }

      void RDMC::Destroy()
      {
          // Tear down connections
      }

      void RDMC::PrepareToReceive(int n)
      {
      }

      void RDMC::Receive(const std::shared_ptr<Msg>& m)
      {
      }

      void RDMC::Destroy(std::vector<RDMC>& rDMC)
      {
      }

int GMSSST::rcntr = 0;
int GMSSST::maxChanges = 0;

      GMSSST::GMSSST(int v, const ipAddr& ipa) : vid(v), theIPA(ipa)
      {
      }

      int GMSSST::ModIndexer(int n)
      {
          return n % View::N;
      }

      void GMSSST::UseToInitialize(GMSSST& newSST)
      {
          std::copy_n(changes.begin(), View::N, newSST.changes.begin());
          newSST.nChanges = nChanges;
          newSST.nCommitted = nCommitted;
          newSST.nAcked = nAcked;
          newSST.gmsSSTRowTime = gmsSSTRowTime;
      }

      void GMSSST::CopyTo(GMSSST& destSST) const
      {
          if (vid > 0 && destSST.vid > 0 && destSST.vid != vid)
          {
                throw std::exception(std::string("V.") + std::to_string(destSST.vid) + std::string(" attempting to overwrite SST for V.") + std::to_string(vid));
          }
          if (!destSST.theIPA.Equals(theIPA))
          {
                throw std::exception(std::string("Setting GMSSST row for ") + destSST.theIPA.ToString() + std::string(" from the SST row for ") + theIPA.ToString());
          }
          if (destSST.gmsSSTRowTime > gmsSSTRowTime)
          {
                throw std::exception(std::string("Setting GMSSST row for ") + destSST.theIPA.ToString() + std::string(" from an OLDER SST row for ") + theIPA.ToString());
          }
          std::copy(suspected.begin(), suspected.end(), destSST.suspected.begin());
          std::copy(changes.begin(), changes.end(), destSST.changes.begin());
          destSST.nChanges = nChanges;
          destSST.nCommitted = nCommitted;
          destSST.nAcked = nAcked;
          std::copy(nReceived.begin(), nReceived.end(), destSST.nReceived.begin());
          if (destSST.wedged && !wedged)
          {
                throw std::exception(std::string("Setting instance ") + destSST.gmsSSTRowInstance + std::string(" Wedged = ") + std::to_string(wedged) + std::string(" from ") + gmsSSTRowInstance);
          }
          destSST.wedged = wedged;
          std::copy(globalMin.begin(), globalMin.end(), destSST.globalMin.begin());
          destSST.globalMinReady = globalMinReady;
          destSST.gmsSSTRowTime = gmsSSTRowTime;
      }

      std::string GMSSST::ToString() const
      {
          std::string s = theIPA + std::string("@ vid=") + vid + std::string("[row-time=") + gmsSSTRowTime + std::string("]: ");
          std::string tf = " ";
          for (int n = 0; n < View::N; n++)
          {
              tf += (suspected[n]? "T": "F") + std::string(" ");
          }

          s += std::string("Suspected={") + tf + std::string("}, nChanges=") + nChanges + std::string(", nCommitted=") + nCommitted;
          std::string ch = " ";
          for (int n = nCommitted; n < nChanges; n++)
          {
              ch += changes[GMSSST::ModIndexer(n)];
          }
          std::string rs = " ";
          for (int n = 0; n < View::N; n++)
          {
              rs += nReceived[n] + std::string(" ");
          }

          s += std::string(", Changes={") + ch + std::string(" }, nAcked=") + nAcked + std::string(", nReceived={") + rs + std::string("}");
          std::string gs = " ";
          for (int n = 0; n < View::N; n++)
          {
              gs += globalMin[n] + std::string(" ");
          }

          s += std::string(", Wedged = ") + (wedged? "T": "F") + std::string(", GlobalMin = {") + gs + std::string("}, GlobalMinReady=") + globalMinReady + std::string("\n");
          return s;
      }

      Group::Group(const std::string& gn)
      {
          gname = gn;
      }

      void Group::SetView(const std::shared_ptr<View>& Vc)
      {
          theView = Vc;
      }

      void Group::Restart(int pid)
      {
          std::cout << std::string("Process <") << pid << std::string(">: RESTART Derecho") << std::endl;
          std::shared_ptr<ipAddr> p = std::make_shared<ipAddr>(pid);
          std::shared_ptr<View> Vc = theView;
          Vc->IKnowIAmLeader = true;
          Vc->members[0] = p;
          Vc->n = 1;
          Vc->vid = 0;
          SetupSSTandRDMC(pid, Vc, Vc->members);
      }

      void Group::Leave(const std::shared_ptr<View>& Vc)
      {
          std::cout << std::string("Process ") << Vc->members[Vc->myRank].getPid() << std::string(": Leave Derecho") << std::endl;
          (std::static_pointer_cast<SST>(Vc->gmsSST))->row[Vc->myRank].suspected[Vc->myRank] = true;
          Vc->gmsSST->Push(Vc->myRank, Vc->vid);
      }

volatile std::vector<ipAddr> Group::Joiners = std::vector<ipAddr>(10);
volatile int Group::nJoiners = 0;
volatile int Group::JoinsProcessed = 0;

      void Group::Join(int pid)
      {
          int cnt = 0;
          std::cout << std::string("Process <") << pid << std::string(">: JOIN Derecho") << std::endl;
          Joiners[nJoiners] = std::make_shared<ipAddr>(pid);
          nJoiners++;
          while (theView == nullptr)
          {
              std::this_thread::sleep_for(std::chrono::milliseconds(2500));
              if (cnt++ == 30)
              {
                  throw std::exception("Join failed");
              }
          }
          std::shared_ptr<View> Vc = theView;
          std::cout << std::string("Process <") << pid << std::string(">: JOIN Derecho successful, I was added in view ") << Vc->vid << std::endl;
          SetupSSTandRDMC(pid, Vc, Vc->members);
          Vc->gmsSST->Pull(Vc);
          Vc->newView(Vc);
      }

      void Group::SetupSSTandRDMC(int pid, const std::shared_ptr<View>& Vc, std::vector<ipAddr>& pids)
      {
          if (Vc->gmsSST != nullptr)
          {
              throw std::exception("Overwritting the SST");
          }

          Vc->gmsSST = std::make_shared<SST>(Vc->vid, pid, Vc->n, pids);
          for (int sender = 0; sender < Vc->n; sender++)
          {
              Vc->rdmc[sender] = std::make_shared<RDMC>(sender, Vc->members, Vc->n);
          }

          Vc->gmsSST->Enable();
      }

      bool Group::JoinsPending()
      {
          return nJoiners > JoinsProcessed;
      }

      void Group::ReceiveJoin(const std::shared_ptr<View>& Vc)
      {
          std::shared_ptr<ipAddr> q = Joiners[JoinsProcessed++];
          if (q == nullptr)
          {
                throw std::exception("q null in ReceiveJoin");
          }
          std::shared_ptr<SST> gmsSST = Vc->gmsSST;
          if ((gmsSST->row[Vc->myRank].nChanges - gmsSST->row[Vc->myRank].nCommitted) == gmsSST->row[Vc->myRank].changes.size())
          {
              throw std::exception("Too many changes to allow a Join right now");
          }

          gmsSST->row[Vc->myRank].changes[GMSSST::ModIndexer(gmsSST->row[Vc->myRank].nChanges)] = q;
          gmsSST->row[Vc->myRank].nChanges++;
          if (Vc->gmsSST->row[Vc->myRank].nChanges > GMSSST::maxChanges)
          {
              GMSSST::maxChanges = Vc->gmsSST->row[Vc->myRank].nChanges;
          }
          for (int n = 0; n < Vc->n; n++)
          {
              Vc->rdmc[n].Wedge(); // RDMC finishes sending, then stops sending or receiving in Vc
          }
          gmsSST->row[Vc->myRank].wedged = true; // True if RDMC has halted new sends and receives in Vc
          gmsSST->Push(Vc->myRank, Vc->vid);
      }

      void Group::CommitJoin(const View& Vc, ipAddr& q)
      {
          std::cout << std::string("CommitJoin: Vid=") << Vc.vid << std::string("... joiner is ") << q << std::endl;
          std::async([&] ()
              // Runs in a separate thread in case state transfer is (in the future) at all slow
              // EDWARD TO DO //
          {
                std::shared_ptr<View> Vcp = std::make_shared<View>();
                Vcp->vid = Vc.vid;
                std::copy(Vc.members.begin(), Vc.members.end(), Vcp->members.begin());
                Vcp->n = Vc.n;
                Vcp->myRank = Vc.RankOf(q);
                Vcp->who = Vcp->members[Vcp->myRank];
                std::copy(Vc.failed.begin(), Vc.failed.end(), Vcp->failed.begin());
                Vcp->nFailed = Vc.nFailed;
                std::cout << std::string("Sending View ") << Vcp->vid << std::string(" to ") << q << std::endl;
          });
      }

      void Group::ReportFailure(const View& Vc, const ipAddr& who)
      {
          int r = Vc.RankOf(who);
          Vc.gmsSST->row[Vc.myRank].suspected[r] = true;
          Vc.gmsSST->Push(Vc.myRank, Vc.vid);
      }

      void Program::Main(std::vector<std::string>& args)
      {
          std::shared_ptr<Group> g = std::make_shared<Group>("Derecho-Test");
          for (int pid = 0; pid < 10; pid++)
          {
              Launch(g, pid);
          }
      }

      void Program::Launch(const std::shared_ptr<Group>& g, int pid)
      {
          std::thread t([&] ()
                      /* Restart the system */
          {
                if (pid == 0)
                {
                      g->SetView(std::make_shared<View>());
                      g->Restart(pid);
                      beNode(g, pid);
                }
                else
                {
                    std::this_thread::sleep_for(std::chrono::milliseconds(pid * 5000));
                    g->Join(pid);
                    beNode(g, pid);
                }
                std::cout << std::string("TERMINATION: Pid<") << pid << std::string(">") << std::endl;
          });
          t.detach();
      }

      bool Program::NotEqual(const std::shared_ptr<View>& Vc, std::vector<bool>& old)
      {
          for (int r = 0; r < Vc->n; r++)
          {
              for (int who = 0; who < View::N; who++)
              {
                  if (Vc->gmsSST->row[r].suspected[who] && !old[who])
                  {
                      return true;
                  }
              }
          }
          return false;
      }

      void Program::Copy(const std::shared_ptr<View>& Vc, std::vector<bool>& old)
      {
          int myRank = Vc->myRank;
          for (int who = 0; who < Vc->n; who++)
          {
              old[who] = Vc->gmsSST->row[myRank].suspected[who];
          }
      }

      bool Program::ChangesContains(const View& Vc, const ipAddr& q)
      {
          std::shared_ptr<GMSSST> myRow = Vc.gmsSST->row[Vc.myRank];
          for (int n = myRow->nCommitted; n < myRow->nChanges; n++)
          {
              std::shared_ptr<ipAddr> p = myRow->changes[GMSSST::ModIndexer(n)];
              if (p != nullptr && p->Equals(q))
              {
                  return true;
              }
          }
          return false;
      }

      int Program::MinAcked(const std::shared_ptr<View>& Vc, std::vector<bool>& Failed)
      {
          int myRank = Vc->myRank;
          int min = Vc->gmsSST->row[myRank].nAcked;
          for (int n = 0; n < Vc->n; n++)
          {
              if (!Failed[n] && Vc->gmsSST->row[n].nAcked < min)
              {
                  min = Vc->gmsSST->row[n].nAcked;
              }
          }

          return min;
      }

      bool Program::IAmTheNewLeader(const std::shared_ptr<View>& Vc, const std::shared_ptr<SST>& sst)
      {
          if (Vc->IKnowIAmLeader)
          {
              return false; // I am the OLD leader
          }

          for (int n = 0; n < Vc->myRank; n++)
          {
              for (int row = 0; row < Vc->myRank; row++)
              {
                  if (!Vc->failed[n] && !Vc->gmsSST->row[row].suspected[n])
                  {
                      return false; // I'm not the new leader, or some failure suspicion hasn't fully propagated
                  }
              }
          }
          Vc->IKnowIAmLeader = true;
          return true;
      }

      void Program::Merge(const std::shared_ptr<View>& Vc, int myRank)
      {
          // Merge the change lists
          for (int n = 0; n < Vc->n; n++)
          {
              if (Vc->gmsSST->row[myRank].nChanges < Vc->gmsSST->row[n].nChanges)
              {
                  std::copy(Vc->gmsSST->row[n].changes.begin(), Vc->gmsSST->row[n].changes.end(), Vc->gmsSST->row[myRank].changes.begin());
                  Vc->gmsSST->row[myRank].nChanges = Vc->gmsSST->row[n].nChanges;
                  if (Vc->gmsSST->row[myRank].nChanges > GMSSST::maxChanges)
                  {
                      GMSSST::maxChanges = Vc->gmsSST->row[myRank].nChanges;
                  }
              }

              if (Vc->gmsSST->row[myRank].nCommitted < Vc->gmsSST->row[n].nCommitted) // How many I know to have been committed
              {
                  Vc->gmsSST->row[myRank].nCommitted = Vc->gmsSST->row[n].nCommitted;
              }
          }
          bool found = false;
          for (int n = 0; n < Vc->n; n++)
          {
              if (Vc->failed[n])
              {
                  // Make sure that the failed process is listed in the Changes vector as a proposed change
                  for (int c = Vc->gmsSST->row[myRank].nCommitted; c < Vc->gmsSST->row[myRank].nChanges && !found; c++)
                  {
                      if (Vc->gmsSST->row[myRank].changes[GMSSST::ModIndexer(c)].Equals(Vc->members[n]))
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
                  Vc->gmsSST->row[myRank].changes[GMSSST::ModIndexer(Vc->gmsSST->row[myRank].nChanges)] = Vc->members[n];
                  Vc->gmsSST->row[myRank].nChanges++;
                  if (Vc->gmsSST->row[myRank].nChanges > GMSSST::maxChanges)
                  {
                      GMSSST::maxChanges = Vc->gmsSST->row[myRank].nChanges;
                  }
              }
          }
          Vc->gmsSST->Push(Vc->myRank, Vc->vid);
      }

      void Program::beNode(const std::shared_ptr<Group>& g, int pid)
      {
          std::shared_ptr<ipAddr> myAddr = std::make_shared<ipAddr>(pid);
          std::vector<bool> oldSuspected = std::vector<bool>(View::N);

          while (true)
          {
              std::shared_ptr<View> Vc = g->theView;
              std::shared_ptr<SST> gmsSST = Vc->gmsSST;
              int Leader = Vc->RankOfLeader(), myRank = Vc->myRank;

              //This part should be a predicate that GMS registers with its SST
              if (NotEqual(Vc, oldSuspected)) //This means there's a change in theView.suspected
              {
                  // Aggregate suspicions into gmsSST[myRank].Suspected;
                  for (int r = 0; r < Vc->n; r++)
                  {
                      for (int who = 0; who < Vc->n; who++)
                      {
                          gmsSST->row[myRank].suspected[who] |= gmsSST->row[r].suspected[who];
                      }
                  }

                  for (int q = 0; q < Vc->n; q++)
                  {
                      if (gmsSST->row[myRank].suspected[q] && !Vc->failed[q])
                      {
                          if (Vc->nFailed + 1 >= View::N / 2)
                          {
                              throw std::exception("Majority of a Derecho group simultaneously failed … shutting down");
                          }

                          gmsSST->Freeze(q); // Cease to accept new updates from q
                          for (int n = 0; n < Vc->n; n++)
                          {
                              Vc->rdmc[n].Wedge(); // RDMC finishes sending, then stops sending or receiving in Vc
                          }

                          gmsSST->row[myRank].wedged = true; // RDMC has halted new sends and receives in Vc
                          Vc->failed[q] = true;
                          Vc->nFailed++;

                          if (Vc->nFailed > Vc->n / 2 || (Vc->nFailed == Vc->n / 2 && Vc->n % 2 == 0))
                          {
                              throw std::exception("Potential partitioning event: this node is no longer in the majority and must shut down!");
                          }

                          gmsSST->Push(Vc->myRank, Vc->vid);
                          if (Vc->IAmLeader() && !ChangesContains(*Vc, Vc->members[q])) // Leader initiated
                          {
                              if ((gmsSST->row[myRank].nChanges - gmsSST->row[myRank].nCommitted) == gmsSST->row[myRank].changes.size())
                              {
                                  throw std::exception("Ran out of room in the pending changes list");
                              }

                              gmsSST->row[myRank].changes[GMSSST::ModIndexer(gmsSST->row[myRank].nChanges)] = Vc->members[q]; // Reports the failure (note that q NotIn members)
                              gmsSST->row[myRank].nChanges++;
                              std::cout << std::string("NEW SUSPICION: adding ") << Vc->members[q] << std::string(" to the CHANGES/FAILED list") << std::endl;
                              if (gmsSST->row[myRank].nChanges > GMSSST::maxChanges)
                              {
                                  GMSSST::maxChanges = gmsSST->row[myRank].nChanges;
                              }
                              gmsSST->Push(Vc->myRank, Vc->vid);
                          }
                      }
                  }
                  Copy(Vc, oldSuspected);
              }

              if (Vc->IAmLeader() && g->JoinsPending()) //This is God's-eye-view magic. How would the leader actually know it has joins pending?
              {
                  g->ReceiveJoin(Vc);
              }

              //Each of these if statements could also be a predicate, I think

              int M;
              if (myRank == Leader && (M = MinAcked(Vc, Vc->failed)) > gmsSST->row[myRank].nCommitted)
              {
                  gmsSST->row[myRank].nCommitted = M; // Leader commits a new request
                  gmsSST->Push(Vc->myRank, Vc->vid);
              }

              if (gmsSST->row[Leader].nChanges > gmsSST->row[myRank].nAcked)
              {
                  WedgeView(Vc, gmsSST, myRank); // True if RDMC has halted new sends, receives in Vc
                  if (myRank != Leader)
                  {
                      std::copy(gmsSST->row[Leader].changes.begin(), gmsSST->row[Leader].changes.end(), gmsSST->row[myRank].changes.begin()); // Echo (copy) the vector including the new changes
                      gmsSST->row[myRank].nChanges = gmsSST->row[Leader].nChanges; // Echo the count
                      gmsSST->row[myRank].nCommitted = gmsSST->row[Leader].nCommitted;
                  }

                  gmsSST->row[myRank].nAcked = gmsSST->row[Leader].nChanges; // Notice a new request, acknowledge it
                  gmsSST->Push(Vc->myRank, Vc->vid);
              }

              if (gmsSST->row[Leader].nCommitted > Vc->vid)
              {
                  gmsSST->Disable(); // Disables the SST rule evaluation for this SST
                  WedgeView(Vc, gmsSST, myRank);
                  ipAddr q = gmsSST->row[myRank].changes[GMSSST::ModIndexer(Vc->vid)];
                  std::shared_ptr<View> Vnext = std::make_shared<View>();
                  Vnext->vid = Vc->vid + 1;
                  Vnext->IKnowIAmLeader = Vc->IKnowIAmLeader;
                  ipAddr myIPAddr = Vc->members[myRank];
                  bool failed;
                  int whoFailed = Vc->RankOf(q);
                  if (whoFailed != -1)
                  {
                      failed = true;
                      Vnext->nFailed = Vc->nFailed - 1;
                      Vnext->n = Vc->n - 1;
                  }
                  else
                  {
                      failed = false;
                      Vnext->nFailed = Vc->nFailed;
                      Vnext->n = Vc->n;
                      Vnext->members[Vnext->n++] = q;
                  }

                  int m = 0;
                  for (int n = 0; n < Vc->n; n++)
                  {
                      if (n != whoFailed)
                      {
                          Vnext->members[m] = Vc->members[n];
                          Vnext->failed[m] = Vc->failed[n];
                          ++m;
                      }
                  }

                  Vnext->who = q;
                  if ((Vnext->myRank = Vnext->RankOf(myIPAddr)) == -1)
                  {
                      std::cout << std::string("Some other process reported that I failed.  Process ") << myIPAddr << std::string(" terminating") << std::endl;
                      return;
                  }

                  if (Vnext->gmsSST != nullptr)
                  {
                      throw std::exception("Overwritting the SST");
                  }

                  Vc->gmsSST->Pull(Vc);
                  Vnext->gmsSST = std::make_shared<SST>(Vnext->vid, Vc->gmsSST->myPid, Vnext->n, Vnext->members);
                  SST::InitializeFromOldSST(Vnext, gmsSST, whoFailed);
                  Vnext->gmsSST->Pull(Vnext);

                  // The intent of these next lines is that we move entirely to Vc+1 and cease to use Vc
                  RaggedEdgeCleanup(Vc); // Finalize deliveries in Vc

                  for (int sender = 0; sender < Vnext->n; sender++)
                  {
                      Vnext->rdmc[sender] = std::make_shared<RDMC>(sender, Vnext->members, Vnext->n);
                  }

                  if (Vc->IAmLeader() && !failed)
                  {
                      g->CommitJoin(*Vnext, q);
                  }

                  Vnext->gmsSST->Enable();
                  Vnext->gmsSST->Push(Vnext->myRank, Vc->vid);

                  std::shared_ptr<View> oldView = Vc;
                  Vc = Vnext;
                  gmsSST = Vc->gmsSST;
                  Vc->newView(Vnext); // Announce the new view to the application

                  // Finally, some cleanup.  These could be in a background thread
                  RDMC::Destroy(oldView->rdmc); // The old RDMC instance is no longer active
                  oldView->gmsSST->Destroy(); // The old SST instance can be discarded too
                  oldView->Destroy(); // VC no longer active

                  // First task with my new view...
                  if (IAmTheNewLeader(Vc, gmsSST)) // I’m the new leader and everyone who hasn’t failed agrees
                  {
                      Merge(Vc, Vc->myRank); // Create a combined list of Changes
                  }
              }
          }
      }

      void Program::WedgeView(const std::shared_ptr<View>& Vc, const std::shared_ptr<SST>& gmsSST, int myRank)
      {
          for (int n = 0; n < Vc->n; n++)
          {
              Vc->rdmc[n].Wedge(); // RDMC finishes sending, stops new sends or receives in Vc
          }

          gmsSST->row[myRank].wedged = true;
      }

      void Program::AwaitMetaWedged(const std::shared_ptr<View>& Vc)
      {
          int cnt = 0;
          for (int n = 0; n < Vc->n; n++)
          {
              while (!Vc->failed[n] && !Vc->gmsSST->row[n].wedged)
              {
                  /* busy-wait */
                  if (cnt++ % 100 == 0)
                  {
                      std::cout << std::string("Process ") << Vc->members[Vc->myRank] << std::string("... loop in AwaitMetaWedged / ") << Vc->gmsSST->row[n].gmsSSTRowInstance << std::endl;
                  }

                  std::this_thread::sleep_for(std::chrono::milliseconds(10));
                  Vc->gmsSST->Pull(Vc);
              }
          }
      }

      int Program::AwaitLeaderGlobalMinReady(const std::shared_ptr<View>& Vc)
      {
          int Leader = Vc->RankOfLeader();
          while (!Vc->gmsSST->row[Leader].globalMinReady)
          {
              Leader = Vc->RankOfLeader();
              Vc->gmsSST->Pull(Vc);
          }
          return Leader;
      }

      void Program::DeliverInOrder(const std::shared_ptr<View>& Vc, int Leader)
      {
          // Ragged cleanup is finished, deliver in the implied order
          std::string deliveryOrder = std::string("Delivery Order (View ") + std::to_string(Vc->vid) + std::string(" { ");
          for (int n = 0; n < Vc->n; n++)
          {
              deliveryOrder += Vc->gmsSST->myPid + std::string(":0..") + std::to_string(Vc->gmsSST->row[Leader].globalMin[n]) + std::string(" ");
          }

          std::cout << deliveryOrder << std::string("}") << std::endl;
      }

      void Program::RaggedEdgeCleanup(const std::shared_ptr<View>& Vc)
      {
          AwaitMetaWedged(Vc);
          int myRank = Vc->myRank;
          // This logic depends on the transitivity of the K1 operator, as discussed last week
          int Leader = Vc->RankOfLeader(); // We don’t want this to change under our feet
          if (Vc->IAmLeader())
          {
              std::cout << std::string("Running RaggedEdgeCleanup: ") << Vc << std::endl;
              bool found = false;
              Vc->gmsSST->Pull(Vc);
              for (int n = 0; n < Vc->n && !found; n++)
              {
                  if (Vc->gmsSST->row[n].globalMinReady)
                  {
                      std::copy_n(Vc->gmsSST->row[myRank].globalMin.begin(), Vc->n, Vc->gmsSST->row[n].globalMin.begin());
                      found = true;
                  }
              }

              if (!found)
              {
                  for (int n = 0; n < Vc->n; n++)
                  {
                      int min = Vc->gmsSST->row[myRank].nReceived[n];
                      for (int r = 0; r < Vc->n; r++)
                      {
                          if (!Vc->failed[r] && min > Vc->gmsSST->row[r].nReceived[n])
                          {
                              min = Vc->gmsSST->row[r].nReceived[n];
                          }
                      }

                      Vc->gmsSST->row[myRank].globalMin[n] = min;
                  }
              }

              Vc->gmsSST->row[myRank].globalMinReady = true;
              Vc->gmsSST->Push(Vc->myRank, Vc->vid);
              std::cout << std::string("RaggedEdgeCleanup: FINAL = ") << Vc << std::endl;
          }
          else
          {
              // Learn the leader’s data and push it before acting upon it
              Leader = AwaitLeaderGlobalMinReady(Vc);
              //Why on earth does this copy FROM my row TO Leader's row? It's like that in the original C# too, but it must be an error.
              //This should copy FROM the leader's row TO my row, if we want to learn the leader's data and then push it.
              std::copy_n(Vc->gmsSST->row[myRank].globalMin.begin(), Vc->n, Vc->gmsSST->row[Leader].globalMin.begin());
              Vc->gmsSST->row[myRank].globalMinReady = true;
              Vc->gmsSST->Push(Vc->myRank, Vc->vid);
          }

          DeliverInOrder(Vc, Leader);
      }
}
