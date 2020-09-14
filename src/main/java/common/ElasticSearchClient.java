package common;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.client.indices.GetIndexResponse;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * es query的client
 *
 * @author chenwu on 2020.9.14
 */
public class ElasticSearchClient {

    private List<HttpHost> hostsAndPorts = new ArrayList<>();
    private RestHighLevelClient restHighLevelClient ;
    private static ElasticSearchClient instance;

    private ElasticSearchClient(){
        try{
            loadFromProperties();
        }catch(IOException e){
            throw new RuntimeException(e);
        }
       restHighLevelClient = new RestHighLevelClient(RestClient.builder(hostsAndPorts.toArray(new HttpHost[0])));
    }

    static {
        instance = new ElasticSearchClient();
    }

    public static ElasticSearchClient getInstance(){
        return instance;
    }

    private void loadFromProperties() throws IOException {
        Properties properties = new Properties();
        InputStream is = getClass().getClassLoader().getResourceAsStream("elasticSearchServer.properties");
        properties.load(is);
        String serverIpPorts = properties.getProperty(ElasticSearchConstants.SERVER_IP_PORTS);
        String[] splitArray = serverIpPorts.split(SymbolConstants.SYMBOL_DH);
        Arrays.stream(splitArray).forEach(item->{
            String[] ipPortArray = item.split(SymbolConstants.SYMBOL_MH);
            HttpHost ipPort = new HttpHost(ipPortArray[0],Integer.parseInt(ipPortArray[1]));
            hostsAndPorts.add(ipPort);
        });
    }

    public boolean existsIndexById(String indexName) throws IOException{
        GetIndexRequest getIndexRequest = new GetIndexRequest(indexName);
        boolean getIndexResponse = restHighLevelClient.indices().exists(getIndexRequest, RequestOptions.DEFAULT);
        return getIndexResponse;
    }
}
