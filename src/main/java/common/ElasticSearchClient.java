package common;

import org.apache.commons.lang.StringUtils;
import org.apache.http.HttpHost;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;

/**
 * es query的client
 *
 * @author chenwu on 2020.9.14
 */
public class ElasticSearchClient implements Closeable {

    private List<HttpHost> hostsAndPorts = new ArrayList<>();
    private RestHighLevelClient restHighLevelClient;
    private static ElasticSearchClient instance;

    private ElasticSearchClient() {
        try {
            loadFromProperties();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        restHighLevelClient = new RestHighLevelClient(RestClient.builder(hostsAndPorts.toArray(new HttpHost[0])));
    }

    static {
        instance = new ElasticSearchClient();
    }

    public static ElasticSearchClient getInstance() {
        return instance;
    }

    private void loadFromProperties() throws IOException {
        Properties properties = new Properties();
        InputStream is = getClass().getClassLoader().getResourceAsStream("elasticSearchServer.properties");
        properties.load(is);
        String serverIpPorts = properties.getProperty(ElasticSearchConstants.SERVER_IP_PORTS);
        String[] splitArray = serverIpPorts.split(SymbolConstants.SYMBOL_DH);
        Arrays.stream(splitArray).forEach(item -> {
            String[] ipPortArray = item.split(SymbolConstants.SYMBOL_MH);
            HttpHost ipPort = new HttpHost(ipPortArray[0], Integer.parseInt(ipPortArray[1]));
            hostsAndPorts.add(ipPort);
        });
    }

    @Override
    public void close() throws IOException {
        instance.restHighLevelClient.close();
    }

    /**
     * 根据索引名称判断索引是否存在
     *
     * @param indexName
     * @return boolean
     * @throws IOException
     * @author chenwu on 2020.9.15
     */
    public boolean existsIndexById(String indexName) throws IOException {
        GetIndexRequest getIndexRequest = new GetIndexRequest(indexName);
        boolean getIndexResponse = restHighLevelClient.indices().exists(getIndexRequest, RequestOptions.DEFAULT);
        return getIndexResponse;
    }

    /**
     * 根据索引名称、文档id获取文档内容
     *
     * @param indexName
     * @param docId
     * @param includes
     * @param excludes
     * @return {@link Map}
     * @throws IOException
     * @author chenwu on 2020.9.15
     */
    public Map<String, Object> getSingleDoc(String indexName, String docId, String[] includes, String[] excludes) throws IOException {
        GetRequest getRequest = new GetRequest(indexName, docId);
        FetchSourceContext fetchSourceContext = new FetchSourceContext(true, includes, excludes);
        getRequest.fetchSourceContext(fetchSourceContext);
        GetResponse getResponse = restHighLevelClient.get(getRequest, RequestOptions.DEFAULT);
        if (getResponse.isExists()) {
            Map<String, Object> sourceAsMap = getResponse.getSourceAsMap();
            return sourceAsMap;
        } else {
            return Collections.EMPTY_MAP;
        }
    }

    /**
     * 根据指定索引名称和文档ID删除文档
     *
     * @param indexName 索引名称
     * @param docId 文档ID
     * @return boolean true表示删除成功,false表示删除失败
     * @throws IOException
     * @author chenwu on 2020.9.15
     */
    public boolean deleteIndex(String indexName,String docId) throws IOException{
        if(StringUtils.isBlank(docId)){
           DeleteIndexRequest request = new DeleteIndexRequest(indexName);
            AcknowledgedResponse deleteResponse = restHighLevelClient.indices().delete(request, RequestOptions.DEFAULT);
            return deleteResponse.isAcknowledged();
        }else{
            DeleteRequest deleteRequest = new DeleteRequest(indexName,docId);
            DeleteResponse deleteResponse = restHighLevelClient.delete(deleteRequest, RequestOptions.DEFAULT);
            return deleteResponse.getResult().equals(DocWriteResponse.Result.DELETED);
        }
    }

    /**
     * 创建索引
     *
     * @param indexName
     * @throws IOException
     * @return boolean 返回是否创建成功
     * @author chenwu on 2020.9.15
     */
    public boolean createIndex(String indexName) throws IOException{
        CreateIndexRequest request = new CreateIndexRequest(indexName);
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder();
        xContentBuilder.startObject().
                startObject("properties")
                .startObject("name").field("type", "text").endObject()
                .startObject("age").field("type", "integer").endObject()
                .startObject("address").field("type", "text").field("store", false).endObject()
                .endObject()
                .startObject("_source")
                .field("enabled", true)
                .endObject()
                .endObject();
        request.mapping(xContentBuilder);
        CreateIndexResponse createIndexResponse = restHighLevelClient.indices().create(request, RequestOptions.DEFAULT);
        return createIndexResponse.index().equals(indexName);
    }

    /**
     * 向指定索引里存放单条数据
     *
     * @param indexName 索引名称
     * @param docId 文档ID
     * @throws IOException
     * @return boolean 查看索引插入的结果
     * @author chenwu on 2020.9.15
     */
    public boolean putSingleRowIntoIndex(String indexName,String docId,Map<String,Object> data) throws IOException{
        IndexRequest request = new IndexRequest(indexName);
        request.id(docId);
        request.timeout(TimeValue.timeValueSeconds(10l));
        request.source(data);
        IndexResponse indexResponse = restHighLevelClient.index(request, RequestOptions.DEFAULT);
        return indexResponse.status() == RestStatus.CREATED;
    }

    /**
     * 更新单条数据
     *
     * @param indexName
     * @param docId
     * @param data
     * @return boolean
     * @throws IOException
     * @author chenwu on 2020.9.15
     */
    public boolean updateSingleRowIntoIndex(String indexName,String docId,Map<String,Object> data) throws IOException{
        UpdateRequest updateRequest = new UpdateRequest(indexName,docId);
        updateRequest.timeout(TimeValue.timeValueSeconds(10l));
        updateRequest.doc(data);
        //更新后不获取source
        updateRequest.fetchSource(false);
        UpdateResponse updateResponse = restHighLevelClient.update(updateRequest, RequestOptions.DEFAULT);
        return updateResponse.status() == RestStatus.OK;
    }

    /**
     * 批处理的方式处理请求(同步)
     *
     * @param list
     * @return boolean
     * @throws IOException
     * @author chenwu on 2020.9.16
     */
    public boolean processRequestSync(List<DocWriteRequest> list) throws IOException{
        BulkRequest request = new BulkRequest();
        request.add(list.toArray(new DocWriteRequest[0]));
        BulkResponse bulkResponse = restHighLevelClient.bulk(request, RequestOptions.DEFAULT);
        return !bulkResponse.hasFailures();
    }

    /**
     * 异步处理
     *
     * @param list
     * @throws IOException
     * @author chenwu on 2020.9.16
     */
    public void processRequestASync(List<DocWriteRequest> list,long timeout) throws IOException{
        BulkRequest request = new BulkRequest();
        request.add(list.toArray(new DocWriteRequest[0]));
        request.timeout(TimeValue.timeValueSeconds(timeout));
        restHighLevelClient.bulkAsync(request, RequestOptions.DEFAULT, new ActionListener<BulkResponse>() {
            @Override
            public void onResponse(BulkResponse bulkItemResponses) {
                 if(bulkItemResponses.hasFailures()){
                    throw new RuntimeException(bulkItemResponses.buildFailureMessage());
                 };
            }
            @Override
            public void onFailure(Exception e) {
                throw new RuntimeException(e);
            }
        });
    }
}
