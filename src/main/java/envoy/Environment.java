package envoy;

import static envoy.RandomHolder.RANDOM;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableList;

/**
 * Take a look into <b>README.md</b> and <b>kafka-all.yaml</b> to see what the ports mean.
 */
public class Environment {

    // The topics have to exist before the tests are executed.
    public static final UpstreamCluster CLUSTER1 = new UpstreamCluster("localhost:9492", 1, "apples", "apricots");
    public static final UpstreamCluster CLUSTER2 = new UpstreamCluster("localhost:9493", 1, "bananas", "berries");
    public static final UpstreamCluster CLUSTER3 = new UpstreamCluster("localhost:9494", 5, "cherries", "chocolates");

    public static final List<UpstreamCluster> CLUSTERS = ImmutableList.of(CLUSTER1, CLUSTER2, CLUSTER3);

    public static List<String> ALL_TOPICS = CLUSTERS.stream()
            .flatMap(cluster -> cluster.getTopics().stream())
            .collect(Collectors.toCollection(ArrayList::new));

    public static final String ENVOY_BROKER = "localhost:19092";
    public static final String ENVOY_MESH = "localhost:29092";

    public static String randomTopic() {
        return ALL_TOPICS.get(RANDOM.nextInt(ALL_TOPICS.size()));
    }

    private Environment() {
    }

}
