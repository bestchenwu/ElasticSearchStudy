package unit1;

import common.ElasticSearchClient;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 同步处理请求
 *
 * @author chenwu on 2020.9.16
 */
public class BulkProcessRequestSyncTest {

    public static void main(String[] args) throws IOException {
        //创建索引的同时 创建了一份文档
        IndexRequest createRequest = new IndexRequest();
        createRequest.index("user3");
        createRequest.id("1");
        createRequest.opType(DocWriteRequest.OpType.CREATE);
        Map<String,Object> data1 = new HashMap<>();
        data1.put("name","mary");
        data1.put("sex","woman");
        data1.put("height","170");
        createRequest.source(data1);
        UpdateRequest updateRequest = new UpdateRequest("user","1");
        Map<String,Object> data = new HashMap<>();
        data.put("age",31);
        data.put("address","gaoxin street");
        data.put("name","sweet");
        data.put("sex","man");
        updateRequest.doc(data);
        DeleteRequest deleteRequest = new DeleteRequest("users1","1");
        List<DocWriteRequest> list = new ArrayList<>();
        list.add(updateRequest);
        list.add(deleteRequest);
        list.add(createRequest);
        ElasticSearchClient elasticSearchClient = ElasticSearchClient.getInstance();
        boolean processRequestSync = elasticSearchClient.processRequestSync(list);
        System.out.println(processRequestSync);
        elasticSearchClient.close();

    }
}
