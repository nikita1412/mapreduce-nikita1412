import java.io.Serializable;
import java.util.ArrayList;

public class URLFrequency implements MapReduce, Serializable {
    @Override
    public String getResourceSeperator() {
        return "\n";
    }
    // Returns an array list containing items in the format [['URL1','1'],['URL2','1'] .... .]
    @Override
    public ArrayList<String[]> mapper(String line) {
        ArrayList<String[]> keyValuePair = new ArrayList<>();
        keyValuePair.add(new String[]{line, Integer.toString(1)});
        return keyValuePair;
    }
    // Returns total number of URLs of same type by using the size of array arraylist i.e size([['URL1','1'],['URL1','1'] .... .])
    @Override
    public String reducer(ArrayList<String> values) {
        return Integer.toString(values.size());
    }
}
