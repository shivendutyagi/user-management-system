// Initialize MongoDB database and collections
db = db.getSiblingDB('userdb');

// Create users collection
db.createCollection('users');

// Create indexes for better performance
db.users.createIndex({ "email": 1 }, { unique: true });
db.users.createIndex({ "city": 1 });
db.users.createIndex({ "status": 1 });
db.users.createIndex({ "created_at": -1 });
db.users.createIndex({ 
  "name": "text", 
  "email": "text", 
  "city": "text" 
}, { 
  name: "text_search_index",
  weights: {
    name: 10,
    email: 5,
    city: 3
  }
});

// Create compound index for common queries
db.users.createIndex({ "city": 1, "status": 1, "created_at": -1 });

// Insert sample data for testing
db.users.insertMany([
  {
    name: "Alice Johnson",
    email: "alice@example.com",
    city: "New York",
    phone: "1234567890",
    married: true,
    status: "active",
    metadata: { department: "Engineering" },
    created_at: new Date(),
    updated_at: new Date()
  },
  {
    name: "Bob Smith",
    email: "bob@example.com",
    city: "San Francisco",
    phone: "0987654321",
    married: false,
    status: "active",
    metadata: { department: "Sales" },
    created_at: new Date(),
    updated_at: new Date()
  },
  {
    name: "Charlie Brown",
    email: "charlie@example.com",
    city: "Los Angeles",
    phone: "5555555555",
    married: true,
    status: "active",
    metadata: { department: "Marketing" },
    created_at: new Date(),
    updated_at: new Date()
  }
]);

print("Database initialization completed!");
print("Created 'users' collection with indexes");
print("Inserted 3 sample users");
