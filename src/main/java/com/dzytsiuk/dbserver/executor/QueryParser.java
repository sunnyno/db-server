package com.dzytsiuk.dbserver.executor;

import com.dzytsiuk.dbserver.entity.Query;
import com.dzytsiuk.dbserver.entity.QueryType;
import com.dzytsiuk.dbserver.exception.QueryParseException;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

public class QueryParser {

    private static final String TYPE = "type";
    private static final String DATABASE = "database";
    private static final String TABLE = "table";
    private static final String DATA = "data";
    private static final String METADATA = "metadata";
    private static final String ESCAPE_CHAR = "\\";

    private InputStream bufferedInputStream;

    public QueryParser(InputStream bufferedInputStream) {
        this.bufferedInputStream = bufferedInputStream;

    }

    public Query parseQuery() {
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
        if(query.getMetadata() == null){
            query.setMetadata(new ArrayList<>());
        }
        for (Object o : jsonArray) {
            query.getMetadata().add((String) o);
        }

    }

    private void setMapToQuery(Query query, JSONObject data) {
        if(query.getMetadata() == null){
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

    private JSONObject getJsonObject() {

        String line;

        try  {
            InputStreamReader in = new InputStreamReader(bufferedInputStream);
            BufferedReader bufferedReader = new BufferedReader(in);

            StringBuilder responseStrBuilder = new StringBuilder();
            while ((line = bufferedReader.readLine()) != null && !line.equals(ESCAPE_CHAR)) {

                responseStrBuilder.append(line);
            }

            return new JSONObject(responseStrBuilder.toString());

        } catch (IOException e) {
            throw new QueryParseException("Error parsing query", e);
        }
    }
}
