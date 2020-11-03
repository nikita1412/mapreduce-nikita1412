import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import java.io.FileReader;
import java.util.Random;

class TestFaultTolerance extends Thread{
     Master obj;
     int N;
     TestFaultTolerance(Master obj, int N){
         this.obj = obj;
         this.N = N;
     }
    @Override
     public void run(){
         try {

             Thread.sleep(200);
             while (true) {

                 if (obj.status != null && obj.processid != null) {
                     Random rand = new Random();

                     int workerid = rand.nextInt(N);
                     long pid = obj.processid[workerid];
                     if (pid > 0 && !obj.status[workerid]) {
                         String cmd = "taskkill /F /PID " + pid;
                         Runtime.getRuntime().exec(cmd);
                         System.out.println("Process with id: " + pid + " of worker " + workerid + " killed");
                         break;
                     }

                 }
             }
         }catch(Exception e){
             e.printStackTrace();

         }
     }
 }

public class RunTests {
    // Method to create the JSON object and read a JSON file for configuration information
    private static JSONObject readJson(String filename){
        JSONObject jo = new JSONObject(); ;
        try{
            Object obj = new JSONParser().parse(new FileReader(filename));
            jo = (JSONObject) obj;

        }catch (Exception e){
            e.printStackTrace();
        }
        return jo;
    }
    // Reads configuration for input output and number of process from json files and run the program
    // run the Testcases and check the fault tolerance also
    public static  void main(String[] args){
        try {
            System.out.println("----------------Running WordCount on Master--------------------");

            MapReduce wc = new WordCount();
            JSONObject jobj1 = readJson("wordcount.json");
            Master master1 = new Master(jobj1, wc);
            TestFaultTolerance toleranceTest = new TestFaultTolerance(master1, Math.toIntExact((Long) jobj1.get("workers")));
            toleranceTest.start();
            master1.run();

            System.out.println("----------------Completed WordCount on Master--------------------");
            Thread.sleep(4000);
            System.out.println("----------------Running Distributed Grep on Master--------------------");

            MapReduce dg = new DistributedGrep();
            JSONObject jobj2 = readJson("distributedgrep.json");
            Master master2 = new Master(jobj2, dg);
            TestFaultTolerance toleranceTest2 = new TestFaultTolerance(master2, Math.toIntExact((Long) jobj2.get("workers")));
            toleranceTest2.start();
            master2.run();

            System.out.println("----------------Completed Distributed Grep on Master--------------------");
            Thread.sleep(4000);
            System.out.println("----------------Running URL Frequency on Master--------------------");

            MapReduce urlfreq = new URLFrequency();
            JSONObject jobj3 = readJson("urlfrequency.json");
            Master master3 = new Master(jobj3, urlfreq);
            TestFaultTolerance toleranceTest3 = new TestFaultTolerance(master3, Math.toIntExact((Long) jobj3.get("workers")));
            toleranceTest3.start();
            master3.run();

            System.out.println("----------------Completed URL Frequency on Master--------------------");
        }catch (Exception e){
            e.printStackTrace();
        }


    }
}
