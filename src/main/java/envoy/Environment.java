package envoy;

public class Environment {

    public static final String CLUSTER_1 = "localhost:9092";
    public static final String CLUSTER_1_TOPIC = "apples";

    public static final String CLUSTER_2 = "localhost:9092";
    public static final String CLUSTER_2_TOPIC = "bananas";

    public static final String CLUSTER_3 = "localhost:9092";
    public static final String CLUSTER_3_TOPIC = "cherries";

    public static final String ENVOY = "localhost:19092";

    private Environment() {
    }

}
