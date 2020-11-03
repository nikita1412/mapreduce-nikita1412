import java.io.Serializable;
import java.util.ArrayList;

public class WordCount implements MapReduce, Serializable {
    @Override
    public String getResourceSeperator() {
        return " ";
    }
    // Returns an array list containing items in the format [['word1','1'],['word2','1'] .... .]
    @Override
    public ArrayList<String[]> mapper(String line) {
        ArrayList<String[]> keyValuePair = new ArrayList<>();
        String[] words = line.replaceAll("\\p{P}", "").toLowerCase().split("\\s+");

        for(int i=0; i<words.length; i++){
            keyValuePair.add(new String[]{words[i], Integer.toString(1)});
        }
        return keyValuePair;

    }
    // Returns total number of words same type by using the size of array arraylist i.e size([['word1','1'],['word1','1'] .... .])
    public String reducer(ArrayList<String> values){
        return Integer.toString(values.size());
    }



}
