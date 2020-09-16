package unit1;

import common.ElasticSearchClient;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BulkProcessRequestAsyncTest {

    public static void main(String[] args) throws IOException {
        ElasticSearchClient instance = ElasticSearchClient.getInstance();
        //创建索引的同时 创建了一份文档
        IndexRequest createRequest = new IndexRequest();
        createRequest.index("user4");
        createRequest.id("1");
        createRequest.opType(DocWriteRequest.OpType.CREATE);
        Map<String,Object> data1 = new HashMap<>();
        data1.put("name","mary");
        data1.put("sex","woman");
        data1.put("height","170");
        createRequest.source(data1);
        UpdateRequest updateRequest = new UpdateRequest("user","1");
        Map<String,Object> data = new HashMap<>();
        data.put("age",21);
        data.put("address","gaoxin2 street");
        data.put("name","jack");
        updateRequest.doc(data);
        DeleteRequest deleteRequest = new DeleteRequest("user2","1");
        List<DocWriteRequest> list = new ArrayList<>();
        list.add(updateRequest);
        list.add(deleteRequest);
        list.add(createRequest);
        instance.processRequestASync(list,10l);
        instance.close();
    }
}
