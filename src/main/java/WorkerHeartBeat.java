import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ConnectException;
import java.net.Socket;
import java.text.SimpleDateFormat;
import java.util.*;

public class WorkerHeartBeat extends Thread {
        private int workerid;
        private int masterport;
        private String phase;
        private boolean runstatus = true;
        private Socket s;
    public WorkerHeartBeat(int workerid, int masterport, String phase) {
            this.masterport = masterport;
            this.workerid = workerid;
            this.phase = phase;

        }
        //Publish a heartbeat every 2 seconds to support fault tolerance
        @Override
        public void run() {
            while (runstatus) {

                try{
                    Thread.sleep(2000);
                    this.s = new Socket("localhost", this.masterport);
                    ObjectOutputStream oos = new ObjectOutputStream(s.getOutputStream());
                    SimpleDateFormat currdate = new SimpleDateFormat("yyMMddHHmmssZ");
                    Date date = new Date();
                    System.out.println("HeartBeat sent from worker: " + workerid);
                    oos.writeObject( "Alive"+ " " + phase + " " + this.workerid+ " " + currdate.format(date));
                    oos.close();
                    s.close();
                    s = null;

                }catch (ConnectException ce){
                    //Do nothing as this arises when the thread wakes up but the work is complete
                }
                catch (Exception e) {
                    e.printStackTrace();
                }

        }
    }
    // called by main worker process to indicate the end of the work allocated to the worker and no need to send the heartbeat.
    public void end(){
            this.runstatus = false;

    }
}
