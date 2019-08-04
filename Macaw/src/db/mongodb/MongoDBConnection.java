package db.mongodb;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.bson.Document;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;

// access static member!!
import static com.mongodb.client.model.Filters.eq;

import db.DBConnection;
import entity.Item;
import entity.Item.ItemBuilder;
import ticketmaster.TicketMasterClient;

public class MongoDBConnection implements DBConnection {
	
	private static MongoDBConnection instance;

	public static DBConnection getInstance() {
		if (instance == null) {
			instance = new MongoDBConnection();
		}
		return instance;
	}
		
	private MongoClient mongoClient;
	private MongoDatabase db;

	public MongoDBConnection() {
		// Connects to local mongodb server.
		mongoClient = MongoClients.create();
		db = mongoClient.getDatabase(MongoDBUtil.DB_NAME);
	}
	
	@Override
	public void close() {
		if (mongoClient != null) {
			mongoClient.close();
		}
	}

	@Override
	public void setFavoriteItems(String userId, List<String> itemIds) {
		if (db == null) {
			return;
		}
		/***********************
		db.users.updateOne
		(
		   {
		      “user_id”: “1111” 
		   },
		   { 
		     $push: 
		     {
		       “favorite”: 
		       {
		         $each: [“abcd”, “efgh”]
		       }
		     }
		   }
		)
		**********************/
		db.getCollection("users").updateOne(eq("user_id", userId), // only find "id" can use eq, 过滤
				new Document("$push", new Document("favorite", new Document("$each", itemIds))));
	}

	@Override
	public void unsetFavoriteItems(String userId, List<String> itemIds) {
		if (db == null) {
			return;
		}
		/***********************
		db.users.updateOne
		(
		   {
		      “user_id”: “1111” 
		   },
		   { 
		     $pullAll:  
		     {
		        “favorite”: [“abcd”, “efgh”],
		     }
		   }
		)
		**********************/
		db.getCollection("users").updateOne(eq("user_id", userId), 
				new Document("$pullAll", new Document("favorite", itemIds)));
	}

	@Override
	public Set<String> getFavoriteItemIds(String userId) {
		if (db == null) {
			return new HashSet<>();
		}
		Set<String> favoriteItems = new HashSet<>();
		FindIterable<Document> iterable = db.getCollection("users").find(eq("user_id", userId));
		
		if (iterable.first() != null && iterable.first().containsKey("favorite")) {
			@SuppressWarnings("unchecked")
			List<String> list = (List<String>) iterable.first().get("favorite");
			favoriteItems.addAll(list);
		}
		return favoriteItems;
	}

	@Override
	public Set<Item> getFavoriteItems(String userId) {
		if (db == null) {
			return new HashSet<>();
		}
		Set<Item> favoriteItems = new HashSet<>();		
		Set<String> itemIds = getFavoriteItemIds(userId);
		for (String itemId : itemIds) {
			FindIterable<Document> iterable = db.getCollection("items").find(eq("item_id", itemId));
			if (iterable.first() != null) {
				Document doc = iterable.first();
				
				ItemBuilder builder = new ItemBuilder();
				builder.setItemId(doc.getString("item_id"));
				builder.setName(doc.getString("name"));
				builder.setAddress(doc.getString("address"));
				builder.setUrl(doc.getString("url"));
				builder.setImageUrl(doc.getString("image_url"));
				builder.setRating(doc.getDouble("rating"));
				builder.setDistance(doc.getDouble("distance"));
				builder.setCategories(getCategories(itemId));
				
				favoriteItems.add(builder.build());
			}		
		}
		return favoriteItems;
	}

	@Override
	public Set<String> getCategories(String itemId) {
		if (db == null) {
			return new HashSet<>();
		}
		Set<String> categories = new HashSet<>();
		FindIterable<Document> iterable = db.getCollection("items").find(eq("item_id", itemId));
		
		if (iterable.first() != null && iterable.first().containsKey("categories")) {
			@SuppressWarnings("unchecked")
			List<String> list = (List<String>) iterable.first().get("categories");
			categories.addAll(list);
		}
		return categories;
	}

	@Override
	public List<Item> searchItems(double lat, double lon, String term) {
		TicketMasterClient ticketMasterClient = new TicketMasterClient();
		List<Item> items = ticketMasterClient.search(lat, lon, term);

		for (Item item : items) {
			saveItem(item);
		}

		return items;
	}

	@Override
	public void saveItem(Item item) {
		if (db == null) {
			return;
		}
		// if we don't import static package, we should use  Filters.eq("item_id", item.getItemId())
		FindIterable<Document> iterable = db.getCollection("items").find(eq("item_id", item.getItemId()));
		// itemID is unique, so we find the first one is enough
		if (iterable.first() == null) {
			db.getCollection("items")
					.insertOne(new Document().append("item_id", item.getItemId()).append("distance", item.getDistance())
							.append("name", item.getName()).append("address", item.getAddress())
							.append("url", item.getUrl()).append("image_url", item.getImageUrl())
							.append("rating", item.getRating()).append("categories", item.getCategories()));
		}
	}

	@Override
	public String getFullname(String userId) {
		FindIterable<Document> iterable = db.getCollection("users").find(new Document("user_id", userId));
		Document document = iterable.first();
		String firstName = document.getString("first_name");
		String lastName = document.getString("last_name");
		return firstName + " " + lastName;
	}

	@Override
	public boolean verifyLogin(String userId, String password) {
		FindIterable<Document> iterable = db.getCollection("users").find(new Document("user_id", userId));
		Document document = iterable.first();
		return document.getString("password").equals(password);
	}

}

