import org.json.simple.JSONObject;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;

public  class Master {
    private int N;
    private String inputfile;
    private MapReduce obj;
    private String objectFile = "taskobject.ser";
    private int baseport;
    ServerSocket server;
    public boolean[] status = null; // public to test fault tolerance
    private static String phase;
    private static String intermediate;
    private static String output;
    public long[] processid = null; // public to test fault tolerance
    private HashMap<Integer, String> lastHeard;
    public Master(JSONObject jobj, MapReduce obj){
        this.N = Math.toIntExact((Long) jobj.get("workers"));
        this.inputfile = (String) jobj.get("inputfile");
        this.obj = obj;
        this.baseport = Math.toIntExact((Long) jobj.get("baseport"));
        this.intermediate = (String) jobj.get("intermediatedir");
        this.output = (String) jobj.get("outputdir");


    }

    // Create a worker process depending on the type of Phase , i.e Mapper or Reducer process
    public void createWorkerProcess(int id, String phase){
        try {
            String input;
            String output;
            if(this.phase.equals("mapper")){
                input = this.inputfile;
                output = this.intermediate;

            }else{
                input = this.intermediate;
                output = this.output;
            }
            String javaHome = System.getProperty("java.home");
            String javaBin = javaHome +
                    File.separator + "bin" +
                    File.separator + "java";
            String classpath = System.getProperty("java.class.path");
            String className = "Worker";
//            System.out.println(javaBin + " -cp " + classpath +" " + className +" " + String.valueOf(id) +" " + inputfile +" " + Integer.toString(this.N) +" " + objectFile + " " + phase + " " + baseport);
            ProcessBuilder builder = new ProcessBuilder(
                    javaBin, "-cp", classpath, className, String.valueOf(id), input,output, Integer.toString(this.N), objectFile, phase, String.valueOf(baseport));
            Process process = builder.inheritIO().start();
            processid[id] = process.pid();
            SimpleDateFormat currdate = new SimpleDateFormat("yyMMddHHmmssZ");
            Date date = new Date();
            lastHeard.put(id, currdate.format(date));

        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Exception in creating worker");
        }

    }
    //Run a loop to initialize N process
    private void createProcess(String phase){

        for(int i=0; i<this.N; i++){
            this.createWorkerProcess(i, phase);
        }
    }
    //Initialize socket for hearbeat messages and work completion messages. Close after usage, checks for worker failure and respawn
    private void globalBarrier(){
        int count = 0;
        try{
            this.server = new ServerSocket(baseport);
            while (count < N){
                SimpleDateFormat currdate = new SimpleDateFormat("yyMMddHHmmssZ");
                Date firstParsedDate = new Date();
                for(int i: lastHeard.keySet()){
                    if(!status[i]) {
                        Date secondParsedDate = currdate.parse(lastHeard.get(i));
                        long diff = firstParsedDate.getTime() - secondParsedDate.getTime() ;
//                            System.out.println("Difference: " + diff + " " + i);
                        if (diff > 2000) {
                            System.out.println("Worker Process with id: " + i + " died.");
                            createWorkerProcess(i, phase);
                            System.out.println("Worker Process with id: " + i + " respawned.");
                        }
                    }
                }
                Socket socket = null;
                socket = server.accept();
                ObjectInputStream ois = new ObjectInputStream(socket.getInputStream());

                String message;
                try {
                    message = (String) ois.readObject();
                    String[] messageparts = message.split("\\s+");
                    String messageinfo = messageparts[0];
                    String workerPhase = messageparts[1];
                    int id =  Integer.parseInt(messageparts[2]);
                    if (messageinfo.equals("Done") && workerPhase.equals(this.phase) && !status[id]){
                        count += 1;
                        status[id] = true;
                        ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
                        oos.writeObject("Message Received " + id);
                        lastHeard.remove(id);
                        oos.close();
                    }else if (messageinfo.equals("Alive") && workerPhase.equals(this.phase) ){
                        System.out.println("Received Heartbeat from " + id);
                        lastHeard.put(id, messageparts[3]);

                    }else{
                        System.out.println("Some Error in global barrier");
                        System.out.println("Error Details: " + messageinfo + " " + workerPhase + " " + id);
                        System.out.println("Error Details: " + phase + " " + status[id]);
                    }


                } catch (ClassNotFoundException e) {
                    e.printStackTrace();
                }
                socket.close();
            }
            this.server.close();
        }catch (Exception e){
            e.printStackTrace();
        }
        System.out.println("Out of global barrier");


    }
    //Method for seriablization and saving the state of object
    private void serializeObject(){
        try {
            FileOutputStream fileOut =
                    new FileOutputStream(this.objectFile);
            ObjectOutputStream out = new ObjectOutputStream(fileOut);
            out.writeObject(this.obj);
            out.close();
            fileOut.close();
        } catch ( Exception ee){
            ee.printStackTrace();
        }
    }
     void run(){
         status = new boolean[this.N];
         processid = new long[this.N];
         serializeObject();
         this.phase = "mapper";
         lastHeard = new HashMap<>();
         createProcess("mapper");
         globalBarrier();
         status = new boolean[this.N];
         lastHeard = new HashMap<>();
         processid = new long[this.N];
         this.phase = "reducer";
         createProcess("reducer");
         globalBarrier();
     }

}
