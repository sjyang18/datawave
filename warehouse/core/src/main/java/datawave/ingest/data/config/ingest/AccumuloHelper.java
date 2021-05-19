package datawave.ingest.data.config.ingest;

import java.nio.file.Paths;
import java.util.Properties;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.conf.Configuration;

import datawave.ingest.data.config.ConfigurationHelper;

/**
 * Helper class to validate configuration of Accumulo required parameters
 * 
 * 
 * 
 */
public class AccumuloHelper {
    
    public static final String USERNAME = "accumulo.username";
    public static final String PASSWORD = "accumulo.password";
    public static final String INSTANCE_NAME = "accumulo.instance.name";
    public static final String ZOOKEEPERS = "accumulo.zookeepers";
    
    private String username = null;
    private PasswordToken password;
    private String instanceName = null;
    private String zooKeepers = null;
    private String propsPath = null;
    private Properties cachedProps = null;
    
    public void setup(Configuration config) throws IllegalArgumentException {
        username = ConfigurationHelper.isNull(config, USERNAME, String.class);
        byte[] pw = Base64.decodeBase64(ConfigurationHelper.isNull(config, PASSWORD, String.class).getBytes());
        password = new PasswordToken(pw);
        instanceName = ConfigurationHelper.isNull(config, INSTANCE_NAME, String.class);
        zooKeepers = ConfigurationHelper.isNull(config, ZOOKEEPERS, String.class);
        propsPath = getClientPropsPath();
        cachedProps = newClientProperties();
    }
    
    public String getInstanceName() {
        return instanceName;
    }
    
    public String getZooKeepers() {
        return zooKeepers;
    }
    
    public String getUsername() {
        return username;
    }
    
    public byte[] getPassword() {
        return password.getPassword();
    }
    
    /**
     * @return an {@link AccumuloClient} to Accumulo given this object's settings.
     */
    public AccumuloClient newClient() {
        if (getClientPropsPath() != null) {
            final AccumuloClient client = Accumulo.newClient().from(getClientPropsPath()).as(username, password).build();
            return client;
        } else {
            return Accumulo.newClient().to(instanceName, zooKeepers).as(username, password).build();
        }
    }
    
    private String getClientPropsPath() {
        if (propsPath == null) {
            propsPath = System.getenv("ACCUMULO_CLIENT_PROPS");
            if (propsPath != null) {
                if (!Paths.get(propsPath).toFile().exists()) {
                    throw new IllegalArgumentException(propsPath + " does not exist!");
                }
            }
        }
        return propsPath;
    }
    
    public Properties newClientProperties() {
        if (getClientPropsPath() != null) {
            if (cachedProps == null) {
                cachedProps = Accumulo.newClientProperties().from(getClientPropsPath()).as(username, password).build();
            }
            return cachedProps;
            
        } else {
            return Accumulo.newClientProperties().to(instanceName, zooKeepers).as(username, password).build();
        }
        
    }
    
    public static void setUsername(Configuration conf, String username) {
        conf.set(USERNAME, username);
    }
    
    public static void setPassword(Configuration conf, byte[] password) {
        conf.set(PASSWORD, new String(Base64.encodeBase64(password)));
    }
    
    public static void setInstanceName(Configuration conf, String instanceName) {
        conf.set(INSTANCE_NAME, instanceName);
    }
    
    public static void setZooKeepers(Configuration conf, String zooKeepers) {
        conf.set(ZOOKEEPERS, zooKeepers);
    }
    
    public static AccumuloClient newClient(String instanceStr, String zookeepers, String username, PasswordToken passwd) {
        final String propsPath = System.getenv("ACCUMULO_CLIENT_PROPS");
        if (propsPath != null && Paths.get(propsPath).toFile().exists()) {
            return Accumulo.newClient().from(propsPath).as(username, passwd).build();
        } else {
            return Accumulo.newClient().to(instanceStr, zookeepers).as(username, passwd).build();
        }
    }
    
    public static AccumuloHelper newHelper(String instanceStr, String zookeepers, String username, String passwd) {
        return AccumuloHelper.newHelper(instanceStr, zookeepers, username, passwd.getBytes());
    }
    
    public static AccumuloHelper newHelper(String instanceStr, String zookeepers, String username, byte[] passwd) {
        Configuration conf = new Configuration();
        AccumuloHelper.setInstanceName(conf, instanceStr);
        AccumuloHelper.setZooKeepers(conf, zookeepers);
        if (username != null) {
            AccumuloHelper.setUsername(conf, username);
        }
        if (passwd != null) {
            AccumuloHelper.setPassword(conf, passwd);
        }
        AccumuloHelper cHelper = new AccumuloHelper();
        cHelper.setup(conf);
        return cHelper;
    }
}
