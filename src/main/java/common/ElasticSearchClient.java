package common;

import common.model.Pair;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Properties;
import java.util.stream.StreamSupport;

class ElasticSearchClient {

    private Pair<String,Integer>[] hostsAndPorts;

    private ElasticSearchClient(){

    }

    private String[] loadFromProperties() throws IOException {
        Properties properties = new Properties();
        InputStream is = getClass().getResourceAsStream("elasticSearchServer.properties");
        properties.load(is);
        String serverIpPorts = properties.getProperty(ElasticSearchConstants.SERVER_IP_PORTS);
        String[] splitArray = serverIpPorts.split(SymbolConstants.SYMBOL_DH);
        int size = splitArray.length;
        hostsAndPorts = new Pair[size];
        Arrays.stream(splitArray).forEach();
    }
}
