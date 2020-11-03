import java.io.Serializable;
import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DistributedGrep implements MapReduce, Serializable {
    @Override
    public String getResourceSeperator() {
        return " ";
    }
    // Returns an array list containing items in the format [['distributed','<N>'].... .]
    @Override
    public ArrayList<String[]> mapper(String line) {
        ArrayList<String[]> keyValuePair = new ArrayList<>();
        Matcher matcher = Pattern.compile("distributed", Pattern.CASE_INSENSITIVE)
                .matcher(line);
        int count = 0;
        while (matcher.find()) count += 1;
        keyValuePair.add(new String[]{"distributed", Integer.toString(count)});
        return keyValuePair;
    }
    // Returns total number of words same type by aggregating the count in the array list for each tuple ['distributed','<N>'],
    // arraylist i.e ([['word1','1'],['word1','1'] .... .])
    @Override
    public String reducer(ArrayList<String> values) {
        int sum = 0;
        for(int i=0; i<values.size(); i++){
            sum += Integer.parseInt(values.get(i));
        }
        return Integer.toString(sum);
    }
}
