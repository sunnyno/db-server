package com.dzytsiuk.dbserver.processor;

import com.dzytsiuk.dbserver.entity.Query;
import com.dzytsiuk.dbserver.entity.QueryType;
import com.dzytsiuk.dbserver.exception.QueryParseException;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

public class QueryParser {

    private static final String TYPE = "type";
    private static final String DATABASE = "database";
    private static final String TABLE = "table";
    private static final String DATA = "data";
    private static final String METADATA = "metadata";
    private static final byte ESCAPE_CHAR = '\\';
    private static final byte[] SAFE_WORD = "exit".getBytes();
    private static final int DEFAULT_BUFFER_SIZE = 512;

    private BufferedInputStream bufferedInputStream;

    public QueryParser(BufferedInputStream bufferedInputStream) {
        this.bufferedInputStream = bufferedInputStream;
    }

    public Query parseQuery() throws InterruptedException {
        JSONObject jsonObject = getJsonObject();
        Query query = new Query();

        query.setType(QueryType.getQueryTypeByName(jsonObject.getString(TYPE)));
        query.setDataBase(jsonObject.getString(DATABASE));

        if (!jsonObject.isNull(TABLE)) {
            query.setTable(jsonObject.getString(TABLE));
        }
        if (!jsonObject.isNull(DATA)) {
            setMapToQuery(query, jsonObject.getJSONObject(DATA));
        }
        if (!jsonObject.isNull(METADATA)) {
            setMetadataToQuery(query, jsonObject.getJSONArray(METADATA));
        }
        return query;
    }

    private void setMetadataToQuery(Query query, JSONArray jsonArray) {
        List<String> metadata = jsonArray.toList().stream()
                .map(object -> Objects.toString(object, null))
                .collect(Collectors.toList());
        query.setMetadata(metadata);

    }

    private void setMapToQuery(Query query, JSONObject data) {
        if (query.getMetadata() == null) {
            query.setMetadata(new ArrayList<>());
        }
        if (query.getData() == null) {
            query.setData(new HashMap<>());
        }
        Iterator<String> keys = data.keys();

        while (keys.hasNext()) {
            String column = keys.next();
            query.getData().put(column, data.getString(column));
            query.getMetadata().add(column);
        }
    }

    private JSONObject getJsonObject() throws InterruptedException {
        try {
            StringBuilder responseStrBuilder = new StringBuilder();
            byte[] buffer = new byte[DEFAULT_BUFFER_SIZE];
            int count;
            while ((count = bufferedInputStream.read(buffer)) != -1) {
                if (buffer[count - 2] == ESCAPE_CHAR) {
                    responseStrBuilder.append(new String(buffer, 0, count - 2));
                    break;
                } else {
                    if (count > SAFE_WORD.length) {
                        checkSafeWord(buffer, count);
                    }
                    responseStrBuilder.append(new String(buffer, 0, count));
                }
            }
            return new JSONObject(responseStrBuilder.toString());
        } catch (IOException e) {
            throw new QueryParseException("Error parsing query", e);
        }
    }

    private void checkSafeWord(byte[] buffer, int count) throws InterruptedException {
        byte[] safeWord = new byte[SAFE_WORD.length];
        System.arraycopy(buffer, count - SAFE_WORD.length - 1, safeWord, 0, SAFE_WORD.length);
        if (Arrays.equals(safeWord, SAFE_WORD)) {
            throw new InterruptedException();
        }
    }

}
