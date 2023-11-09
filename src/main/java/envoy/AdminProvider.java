package envoy;

import java.util.Properties;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClient;

public class AdminProvider {

    public static Admin makeBrokerAdmin() {
        final Properties properties = new Properties();
        properties.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, Environment.ENVOYS_BOOTSTRAP_ADDRESS);
        return AdminClient.create(properties);
    }

    public static Admin makeClusterAdmin(final UpstreamCluster cluster) {
        final Properties properties = new Properties();
        properties.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, cluster.getBrokers());
        return AdminClient.create(properties);
    }

}
