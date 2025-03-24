package edu.cmu.cc.minisite;

import com.google.gson.JsonObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.google.gson.JsonParser;
import com.mongodb.client.MongoCursor;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.Objects;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.bson.Document;


/**
 * Task 3:
 * Implement your logic to return all the comments authored by this user.
 *
 * You should sort the comments by ups in descending order (from the largest to the smallest one).
 * If there is a tie in the ups, sort the comments in descending order by their timestamp.
 */
public class HomepageServlet extends HttpServlet {

    /**
     * The endpoint of the database.
     *
     * To avoid hardcoding credentials, use environment variables to include
     * the credentials.
     *
     * e.g., before running "mvn clean package exec:java" to start the server
     * run the following commands to set the environment variables.
     * export MONGO_HOST=...
     */
    private static final String MONGO_HOST = System.getenv("MONGO_HOST");
    /**
     * MongoDB server URL.
     */
    private static final String URL = "mongodb://" + MONGO_HOST + ":27017";
    /**
     * Database name.
     */
    private static final String DB_NAME = "reddit_db";
    /**
     * Collection name.
     */
    private static final String COLLECTION_NAME = "posts";
    /**
     * MongoDB connection.
     */
    private static MongoCollection<Document> collection;

    /**
     * Initialize the connection.
     */
    public HomepageServlet() {
        Objects.requireNonNull(MONGO_HOST);
        MongoClientURI connectionString = new MongoClientURI(URL);
        MongoClient mongoClient = new MongoClient(connectionString);
        MongoDatabase database = mongoClient.getDatabase(DB_NAME);
        collection = database.getCollection(COLLECTION_NAME);
    }

    /**
     * Implement this method.
     *
     * @param request  the request object that is passed to the servlet
     * @param response the response object that the servlet
     *                 uses to return the headers to the client
     * @throws IOException      if an input or output error occurs
     * @throws ServletException if the request for the HEAD
     *                          could not be handled
     */
    @Override
    protected void doGet(final HttpServletRequest request,
                         final HttpServletResponse response) throws ServletException, IOException {

        JsonObject result = new JsonObject();
        String id = request.getParameter("id");
        // TODO: To be implemented
        // Create a JSON array to hold the comments.
        com.google.gson.JsonArray comments = new com.google.gson.JsonArray();

        try {
            // Build the query to filter by "id".
            Document query = new Document("uid", id);
            
            // Define the sort order: "ups" descending, then "timestamp" descending.
            Document sort = new Document("ups", -1).append("timestamp", -1);
            
            // Exclude the _id field from the results.
            Document projection = new Document("_id", 0);
            
            // Execute the query with sorting and projection.
            com.mongodb.client.FindIterable<Document> iterable = collection.find(query)
                    .projection(projection)
                    .sort(sort);
            
            // Iterate results and convert each Document to a JsonObject.
            for (Document doc : iterable) {
                String jsonString = doc.toJson();
                com.google.gson.JsonObject jsonComment = com.google.gson.JsonParser.parseString(jsonString).getAsJsonObject();
                comments.add(jsonComment);
            }
        } catch (Exception e) {
            // Log exceptions.
            e.printStackTrace();
        }

        // Wrap comments array into a result JSON object.
        result.add("comments", comments);

        response.setContentType("text/html; charset=UTF-8");
        response.setCharacterEncoding("UTF-8");
        PrintWriter writer = response.getWriter();
        writer.write(result.toString());
        writer.close();
    }
}

