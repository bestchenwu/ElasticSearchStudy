package common;

import common.model.Pair;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

class ElasticSearchClient {

    private Pair<String,Integer>[] hostsAndPorts;

    private ElasticSearchClient() throws IOException{
        loadFromProperties();
    }

    private void loadFromProperties() throws IOException {
        Properties properties = new Properties();
        InputStream is = getClass().getResourceAsStream("elasticSearchServer.properties");
        properties.load(is);
        String serverIpPorts = properties.getProperty(ElasticSearchConstants.SERVER_IP_PORTS);
        String[] splitArray = serverIpPorts.split(SymbolConstants.SYMBOL_DH);
        int size = splitArray.length;
        List<Pair> hostsAndPortList = new ArrayList<Pair>();
        int index = 0;
        Arrays.stream(splitArray).forEach(item->{
            String[] ipPortArray = item.split(SymbolConstants.SYMBOL_MH);
            Pair<String,Integer> ipPort = new Pair<>(ipPortArray[0],Integer.parseInt(ipPortArray[1]));
            hostsAndPortList.add(ipPort);
        });
        hostsAndPortList.toArray(hostsAndPorts);
    }
}
