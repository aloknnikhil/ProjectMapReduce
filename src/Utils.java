import java.util.StringTokenizer;

/**
 * Created by alok on 4/11/15 in ProjectMapReduce
 */
public class Utils {

    public static String getNodeID(String nodeAddress)    {
        return new StringTokenizer(nodeAddress, ":").nextToken().hashCode() + "";
    }
}
