package edu.cmu.cc.minisite;

import com.google.gson.JsonObject;
import com.google.gson.JsonArray;
import com.google.gson.JsonParser;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;
import org.bson.Document;
import org.bson.conversions.Bson;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Sorts;

import java.io.IOException;
import java.io.PrintWriter;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Arrays;


/**
 * In this task you will populate a user's timeline.
 * This task helps you understand the concept of fan-out. 
 * Practice writing complex fan-out queries that span multiple databases.
 *
 * Task 4 (1):
 * Get the name and profile of the user as you did in Task 1
 * Put them as fields in the result JSON object
 *
 * Task 4 (2);
 * Get the follower name and profiles as you did in Task 2
 * Put them in the result JSON object as one array
 *
 * Task 4 (3):
 * From the user's followees, get the 30 most popular comments
 * and put them in the result JSON object as one JSON array.
 * (Remember to find their parent and grandparent)
 *
 * The posts should be sorted:
 * First by ups in descending order.
 * Break tie by the timestamp in descending order.
 */
public class TimelineServlet extends HttpServlet {

    /**
     * Your initialization code goes here.
     */
    // ---------------------------------------------------
    // 1) MYSQL Credentials (from Task 1)
    // ---------------------------------------------------

    private static final String MYSQL_HOST = System.getenv("MYSQL_HOST");
    private static final String MYSQL_NAME = System.getenv("MYSQL_NAME");
    private static final String MYSQL_PWD  = System.getenv("MYSQL_PWD");
    private static final String DB_NAME    = "reddit_db";
    private static final String JDBC_URL   = "jdbc:mysql://" + MYSQL_HOST + ":3306/"
                                            + DB_NAME + "?useSSL=false&serverTimezone=UTC";
    private static Connection mysqlConn;

    // ---------------------------------------------------
    // 2) Neo4j Credentials (from Task 2)
    // ---------------------------------------------------
    private static final String NEO4J_HOST = System.getenv("NEO4J_HOST");
    private static final String NEO4J_NAME = System.getenv("NEO4J_NAME");
    private static final String NEO4J_PWD  = System.getenv("NEO4J_PWD");
    private Driver neo4jDriver;

    // ---------------------------------------------------
    // 3) MongoDB Credentials (from Task 3)
    // ---------------------------------------------------
    private static final String MONGO_HOST = System.getenv("MONGO_HOST");
    private static final String MONGO_URL  = "mongodb://" + MONGO_HOST + ":27017";
    private static final String MONGO_DB   = "reddit_db";
    private static final String MONGO_COLL = "posts";
    private MongoCollection<Document> postsCollection;

    public TimelineServlet() {
        try {
            // 1) MySQL
            try{
                Objects.requireNonNull(MYSQL_HOST);
                Objects.requireNonNull(MYSQL_NAME);
                Objects.requireNonNull(MYSQL_PWD);
                mysqlConn = DriverManager.getConnection(JDBC_URL, MYSQL_NAME, MYSQL_PWD);
            } catch (NullPointerException e) {
            System.err.println("WARNING: MySQL environment variables not set properly");
            }

            
            // 2) Neo4j
            try{
                Objects.requireNonNull(NEO4J_HOST);
                Objects.requireNonNull(NEO4J_NAME);
                Objects.requireNonNull(NEO4J_PWD);
                neo4jDriver = GraphDatabase.driver(
                    "bolt://" + NEO4J_HOST + ":7687",
                    AuthTokens.basic(NEO4J_NAME, NEO4J_PWD)
                );
            } catch (NullPointerException e) {
                System.err.println("WARNING: Neo4j environment variables not set properly");
            }

            // 3) MongoDB
            try{
                Objects.requireNonNull(MONGO_HOST);
                MongoClientURI connectionString = new MongoClientURI(MONGO_URL);
                MongoClient mongoClient = new MongoClient(connectionString);
                MongoDatabase database = mongoClient.getDatabase(MONGO_DB);
                postsCollection = database.getCollection(MONGO_COLL);
            } catch (NullPointerException e) {
                System.err.println("WARNING: MongoDB environment variables not set properly");
            }
            
        } catch (SQLException e) {
            e.printStackTrace();
            throw new RuntimeException("MySQL connection failed!", e);
        }
    }

    /**
     * Don't modify this method.
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

        // DON'T modify this method.
        String id = request.getParameter("id");
        String result = getTimeline(id);
        response.setContentType("text/html; charset=UTF-8");
        response.setCharacterEncoding("UTF-8");
        PrintWriter writer = response.getWriter();
        writer.print(result);
        writer.close();
    }

    /**
     * Method to get given user's timeline.
     *
     * @param id user id
     * @return timeline of this user
     */
    private String getTimeline(String id) {
        JsonObject result = new JsonObject();
        // TODO: implement this method
        // 1) MySQL: get user profile
        JsonObject profileInfo = getUserProfileFromMySQL(id);
        result.addProperty("name", profileInfo.get("name").getAsString());
        result.addProperty("profile", profileInfo.get("profile").getAsString());

        // 2) Neo4j: get followers
        JsonArray followers = getFollowersFromNeo4j(id);
        result.add("followers", followers);

        // Neo4j: get followees (the people userId follows)
        List<String> followees = getFolloweesFromNeo4j(id);

        // 3) MongoDB: get top 30 comments from followees
        JsonArray comments = getTopCommentsFromMongo(followees);
        result.add("comments", comments);
        return result.toString();
    }

    // ------------------------------------------------------------------
    // (1) MySQL Helper
    // ------------------------------------------------------------------
    public JsonObject getUserProfileFromMySQL(String userId) {
        
        JsonObject result = new JsonObject();
        String query = "SELECT username, profile_photo_url FROM users WHERE username = ? LIMIT 1";
        try (java.sql.PreparedStatement stmt = mysqlConn.prepareStatement(query)) {
            stmt.setString(1, userId);
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    result.addProperty("name", rs.getString("username"));
                    result.addProperty("profile", rs.getString("profile_photo_url"));
                } else {
                    // default if user not found
                    result.addProperty("name", "Unauthorized");
                    result.addProperty("profile", "#");
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
            result.addProperty("name", "Unauthorized");
            result.addProperty("profile", "#");
        }
        return result;
    }

    // ------------------------------------------------------------------
    // (2) Neo4j Helpers: getFollowers and getFollowees
    // ------------------------------------------------------------------

    public JsonArray getFollowersFromNeo4j(String userId) {
        JsonArray followers = new JsonArray();
        String cypher = "MATCH (follower:User)-[:FOLLOWS]->(u:User {username: $username}) "
                      + "RETURN follower.username AS name, follower.url AS profile "
                      + "ORDER BY name ASC";
        try (Session session = neo4jDriver.session()) {
            StatementResult rs = session.run(cypher, 
                org.neo4j.driver.v1.Values.parameters("username", userId));
            while (rs.hasNext()) {
                Record record = rs.next();
                JsonObject obj = new JsonObject();
                obj.addProperty("name", record.get("name").asString());
                obj.addProperty("profile", record.get("profile").asString());
                followers.add(obj);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return followers;
    }

    // For the timeline get the followees 
    public List<String> getFolloweesFromNeo4j(String userId) {
        List<String> followees = new ArrayList<>();
        String cypher = "MATCH (u:User {username: $username})-[:FOLLOWS]->(f:User) " +
                        "RETURN f.username AS followeeUid ORDER BY followeeUid ASC";
        try (Session session = neo4jDriver.session()) {
            StatementResult rs = session.run(cypher, 
                org.neo4j.driver.v1.Values.parameters("username", userId));
            while (rs.hasNext()) {
                followees.add(rs.next().get("followeeUid").asString());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return followees;
    }

    // ------------------------------------------------------------------
    // (3) MongoDB Helper: get top 30 comments from followees
    // ------------------------------------------------------------------
    public JsonArray getTopCommentsFromMongo(List<String> followees) {
        JsonArray comments = new JsonArray();
        Bson matchFollowee = Aggregates.match(Filters.in("uid", followees));
        Bson sortFollowee = Aggregates.sort(Sorts.orderBy(Sorts.descending("ups"), Sorts.descending("timestamp")));
        Bson limitFollowee = Aggregates.limit(30);
        AggregateIterable<Document> result = postsCollection.aggregate(Arrays.asList(matchFollowee, sortFollowee, limitFollowee));


        for (Document doc : result) {
            // JsonObject comment = JsonParser.parseString(doc.toJson()).getAsJsonObject();
            JsonObject comment = documentToJsonObjectConvert(doc);
            // For each comment, find parent and grandparent if they exist
            attachParentAndGrandParent(comment);
            comments.add(comment);
        }
        return comments;
    }

    // ------------------------------------------------------------------
    // (4) Helper for Parent / Grandparent
    // ------------------------------------------------------------------
    private void attachParentAndGrandParent(JsonObject comment) {
        if (!comment.has("parent_id") || comment.get("parent_id").getAsString().isEmpty()) {
            // no parent
            return; 
        }
        String parentId = comment.get("parent_id").getAsString();

        JsonObject parent = findCommentByCid(parentId);
        if (parent != null) {
            comment.add("parent", parent);
            // check for grandparent
            if (parent.has("parent_id") && !parent.get("parent_id").getAsString().isEmpty()) {
                String gpid = parent.get("parent_id").getAsString();
                if(!gpid.startsWith("t3_")){
                    JsonObject gp = findCommentByCid(gpid);
                    if (gp != null) {
                        comment.add("grand_parent", gp);
                    }
                }
            }
        }
    }

    private JsonObject documentToJsonObjectConvert(Document doc) {
        JsonObject json = new JsonObject();
        for (String key : doc.keySet()) {
            if (key.equals("_id")) continue;
            Object value = doc.get(key);
            if (value instanceof Integer) {
                json.addProperty(key, (Integer) value);
            } else if (value instanceof Long) {
                json.addProperty(key, (Long) value);
            } else if (value instanceof Double) {
                json.addProperty(key, (Double) value);
            } else if (value instanceof Boolean) {
                json.addProperty(key, (Boolean) value);
            } else if (value != null) {
                json.addProperty(key, value.toString());
            }
        }
        return json;
    }

    private JsonObject findCommentByCid(String cid) {
        if (cid == null || cid.isEmpty()) {
            return null;
        }
        Document query = new Document("cid", cid);
        Document projection = new Document("_id", 0);
        Document doc = postsCollection.find(query).projection(projection).first();
        if (doc == null) {
            return null;
        }
        return documentToJsonObjectConvert(doc);
    }


}
