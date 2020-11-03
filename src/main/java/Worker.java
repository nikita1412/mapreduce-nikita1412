import java.io.*;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;

public class Worker {
    private static int workerid;
    private static int N;
    private static MapReduce obj;
    private static String phase;
    private static String input;
    private static String output;
    private static int port;
    private WorkerHeartBeat heartBeat;
    // For each worker process, depending on the phase mapper or reducer, assign intermediate files and start heartbeat thread
    Worker(int id, String input, String output, int N, MapReduce obj, String phase, int port){
        this.workerid = id;
        this.N = N;
        this.obj = obj;
        this.phase = phase;
        this.port = port;
        if(phase.equals("mapper")){
            this.input = input;
        }else{
            this.input = input + "//" + workerid + ".txt";
        }
        this.output = output + "//" + workerid + ".txt";

        heartBeat = new WorkerHeartBeat(workerid, port, phase);
        heartBeat.start();

    }
    // returns the size of the file in bytes.
    private long getFileSize(){
        long bytes = 0;
        try{
            Path path = Paths.get(this.input);
            bytes = Files.size(path);
        }catch( Exception e){
            e.printStackTrace();

        }
        return bytes;
    }
    // Reads the chunk of the file: this reads only the chunk allocated to this worker which is decided by the worker id
    // we divide the file equally in bytes amongst the workers to keep the load consistent.
    private String readChunk() throws Exception{
        int filesize = (int) getFileSize();
        int chunksize =  (int) Math.ceil(filesize*1.0/N);
        int start = this.workerid*chunksize;
        RandomAccessFile file = new RandomAccessFile(this.input, "r");
        file.seek(start);
        while (true && workerid>0 && file.getFilePointer() <filesize){
            byte[] charbuff = new byte[1];
            file.readFully(charbuff);
            String ch = new String(charbuff);
            if (ch.equals(obj.getResourceSeperator())){
                break;
            }
        }
        int readsize = Math.min(chunksize, filesize - (int)file.getFilePointer());
        byte[] buff = new byte[readsize];
        file.readFully(buff);
        String chunk = new String(buff);
        while ( file.getFilePointer() <filesize){
            byte[] charbuff = new byte[1];
            file.readFully(charbuff);
            String ch = new String(charbuff);
            if (ch.equals(obj.getResourceSeperator()) ){
                break;
            }
            chunk += ch;

        }
        file.close();
//        System.out.println("Chunk: " + chunk + " " + workerid);
        return chunk;
    }
    // Start the mapper phase for a particular worker, worker reads from intermediate files to map,
    // and outputs the output back to intermediate files to be used in the reducer phase.
    private void workAsMapper(){

        ArrayList<String[]> keyValuePair;

        try {
            // start the mapper phase
            System.out.println("Started Mapping Phase for Worker: " + workerid);
            String chunk = readChunk();
            String[] lines = chunk.split("\\r?\\n");
            if (lines.length >0){
                PrintWriter outintermediateFile = new PrintWriter(new FileWriter(this.output));
                for(int j=0; j<lines.length;j++){
                    lines[j] = lines[j].trim();
                    if (lines[j].length()>0) {
                        keyValuePair = obj.mapper(lines[j]);
                        for (int i = 0; i < keyValuePair.size(); i++) {
                            outintermediateFile.println(keyValuePair.get(i)[0] + "," + keyValuePair.get(i)[1]);
                        }
                    }
                }
                outintermediateFile.close();
            }
            heartBeat.end();
            // only one process so after this we can start the reducer phase
            System.out.println("End Mapping Phase for Worker: " + workerid);

        }catch (Exception e){
            e.printStackTrace();
        }
    }
    // Start the reducer phase for a particular worker, worker reads from files to reduce,
    // and outputs the output to intermediate files.
    private void workAsReducer(){
        try{
            System.out.println("Started Combiner Phase for Worker: " + workerid);
            HashMap<String, ArrayList<String>> combinerOutput =  combiner();

            createFile(this.output);
            System.out.println("Started Reducer Phase for Worker: " + workerid);
            PrintWriter outputFilePointer = new PrintWriter(new FileWriter(this.output));
            for(String key: combinerOutput.keySet()){
                outputFilePointer.println(key + " " + obj.reducer(combinerOutput.get(key)));
            }
            outputFilePointer.close();
            heartBeat.end();
            System.out.println("End Reducing Phase for Worker: " + workerid);
        }catch (Exception e){
            e.printStackTrace();
        }
    }
    public void run(){
        if (phase.equals("mapper")){
            workAsMapper();
        }else{
            workAsReducer();
        }
        sendMessageToMaster("Done",1);
    }
    private void sendMessageToMaster(String msg, int trials){
        try{
            Socket s = new Socket("localhost", this.port);
            ObjectOutputStream oos = new ObjectOutputStream(s.getOutputStream());
            String str1;
            oos.writeObject(msg + " " + phase + " " + this.workerid);
            ObjectInputStream ois = new ObjectInputStream(s.getInputStream());
            str1 = (String) ois.readObject();
            if(!str1.equals("Message Received "+ this.workerid ) && trials>0){
                sendMessageToMaster(msg, trials-1);
            }
            s.close();
        }catch (Exception e){
            e.printStackTrace();
        }

    }
    private void createFile(String path){
        try {
            File f = new File(path);
            if (!f.exists()) {
                f.createNewFile();
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }
    //Summarize the map output records with the same key by using hashmap and skip any bad records
    private HashMap<String, ArrayList<String>> combiner(){
        HashMap<String, ArrayList<String>> keyValueMap = new HashMap<String, ArrayList<String>>();
        try {
            FileReader fr=new FileReader(this.input);   //reads the file
            BufferedReader br=new BufferedReader(fr);  //creates a buffering character input stream
            String line;
            while ((line=br.readLine()) != null){
                String[] lineSplit = line.split(",");
                if (lineSplit.length ==2){
                    ArrayList<String> emptyArrayList = new ArrayList<String>();
                    keyValueMap.putIfAbsent(lineSplit[0],emptyArrayList);
                    emptyArrayList = keyValueMap.get(lineSplit[0]);
                    emptyArrayList.add(lineSplit[1]);
                    keyValueMap.put(lineSplit[0], emptyArrayList);
                }else {
                    System.out.println("Skipping Bad Record");
                }
            }

        }catch (Exception e){
            e.printStackTrace();
        }
        return keyValueMap;
    }
    public static void main (String[] args){
        int id = Integer.parseInt(args[0]);
        String input = args[1]; // this will be used to calculate chunk size in multi-worker implementation
        String output = args[2];
        int N = Integer.parseInt(args[3]); // this will be used to calculate chunk size in multi-worker implementation
        String objectFile = args[4];
        String phase = args[5];
        int port = Integer.parseInt(args[6]);
        try {
            FileInputStream fileIn = new FileInputStream(objectFile);
            ObjectInputStream in = new ObjectInputStream(fileIn);
            MapReduce obj = (MapReduce) in.readObject();
            in.close();
            fileIn.close();
            Worker worker = new Worker(id, input, output, N, obj, phase, port);
            worker.run();
        } catch (IOException i) {
            i.printStackTrace();
            return;
        } catch (ClassNotFoundException c) {
            System.out.println("Class not found");
            c.printStackTrace();
            return;
        }


    }
}
