/**
 * Created by alok on 4/11/15 in ProjectMapReduce
 */
public class Task {

    enum Status {
        INITIALIZED,
        RUNNING,
        COMPLETE
    }

    private String taskID;
    private String executorID;
    private String taskDataPath;
}
