# 🍃 MongoDB CRUD Operations Manager (IT'S NOT DONE YET)

A powerful, interactive command-line interface (CLI) tool for managing MongoDB databases using Python. This tool supports **all core CRUD operations**, advanced features like **aggregation pipelines**, **bulk writes**, **index management**, and more — all while following [MongoDB’s official CRUD documentation](https://www.mongodb.com/docs/manual/crud/).

> No more writing repetitive queries — build filters, updates, and pipelines interactively!

> @shn-enaa 「このコード、たぶんバグってるわ。もう時間かけすぎたから、文句は禁止ね！」

---

## Features

- **Full CRUD Support**  
  - Insert one or many documents  
  - Find with complex filters, projections, sorting, and pagination  
  - Update/replace with support for all MongoDB update operators (`$set`, `$inc`, `$push`, etc.)  
  - Safe delete with confirmation prompts  

- **Advanced Operations**  
  - Aggregation pipeline builder (with templates for `$match`, `$group`, `$lookup`, etc.)  
  - Bulk write operations (insert, update, delete in batches)  
  - Distinct values & document counting  
  - Collection statistics and index management  

- **User-Friendly CLI**  
  - Interactive query and update builders  
  - Automatic type parsing (numbers, booleans, dates, JSON, ObjectIds)  
  - Safe defaults with confirmation prompts for destructive actions  

- **Flexible Connection**  
  - Connect to local or remote MongoDB instances  
  - Switch databases and collections on-the-fly  

---

## Requirements

- Python 3.7+
- MongoDB server (local or cloud)
- Required packages:
  ```txt
  pymongo
  ```

---

## Installation

1. **Clone or download** this script:
   ```bash
   wget https://github.com/weiss-rn/MongoDB  # or just save the file
   ```

2. **Install dependencies**:
   ```bash
   pip install pymongo
   ```

3. **Ensure MongoDB is running** (e.g., `mongod` service active on `localhost:27017`).

---

## Usage

Run the script directly:

```bash
python mongodb_crud_manager.py
```

You’ll be guided through:
1. **Connection setup** (default: `localhost:27017`)
2. **Database & collection selection**
3. **Interactive menu** for all operations

### Example Workflow
```
🍃 MongoDB CRUD Operations Manager
   Professional MongoDB Database Management Tool

🔌 MONGODB CONNECTION
1. Use default (mongodb://localhost:27017/)
2. Enter custom connection string
Select option (1-2): 1
Database name (default: mydatabase): testdb
Collection name (default: mycollection): users

✓ Connected to MongoDB: testdb.users

Connected to: testdb.users

--- INSERT OPERATIONS ---
1.  Insert One Document
...

Select operation (0-20): 1

INSERT ONE DOCUMENT
Enter document fields (press Enter without input to finish):
Field name: name
Value for 'name': Alice
Field name: age
Value for 'age': 30
Field name: 
Document inserted with _id: 6612abcd1234567890ab0123
```

---

## Safety Notes

- **Delete operations require confirmation**  
- **Empty delete/update queries show warnings**  
- **Bulk operations support ordered/unordered execution**  
- **Connection timeout set to 5 seconds** for quick failure feedback

---

## Supported Data Types

The tool automatically parses input strings into appropriate BSON types:
- `true` / `false` → `bool`
- `42` / `3.14` → `int` / `float`
- `null` → `None`
- `ObjectId("...")` → `ObjectId`
- `2024-04-05T10:00:00` → `datetime`
- `[1,2,3]` or `{"key": "value"}` → `list` / `dict`

---

## License

MIT License | Feel free to use, modify, and distribute.

---

## Tips

- Use `_id=ObjectId("...")` to query by document ID  
- Build complex filters with operators: `age>25`, `name~^Ali`, `status:in:["active","pending"]`  
- In updates, use `score+=10` for `$inc`, `tags[]=new` for `$push`  
- Press **Enter** on empty input to finish building queries/updates  
