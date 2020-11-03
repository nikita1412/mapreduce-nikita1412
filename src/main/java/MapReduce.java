import java.util.ArrayList;

public interface MapReduce {
    public String getResourceSeperator();
    public ArrayList<String[]> mapper(String line);
    public String reducer(ArrayList<String> values);
}
