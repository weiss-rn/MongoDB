# Automated version mongodb_ESS
# Collaboration Between @weiss-gcm and @shn-enaa
# このコードは、GitHubリポジトリにアップロードされる前に何度も修正されています。
# https://www.mongodb.com/ja-jp/docs/manual/crud/
# Work In Progress - Code isn't entirely stable.
# 開発中 - コードは完全に安定していません。

from pymongo import MongoClient
from bson.objectid import ObjectId
import re

client = MongoClient("mongodb://localhost:27017/")
db = client["mydatabase"]
collection = db["mycollection"]

def is_valid_object_id(oid):
    """Check if a string is a valid ObjectId"""
    return bool(re.match(r'^[0-9a-fA-F]{24}$', oid))

def convert_value(value):
    """Convert string value to appropriate type"""
    if value.lower() == 'null':
        return None
    elif value.lower() == 'true':
        return True
    elif value.lower() == 'false':
        return False
    elif value.isdigit():
        return int(value)
    elif value.replace('.', '', 1).isdigit():
        return float(value)
    else:
        return value

def menu_db():
    print("\nMongoDB CRUD Operations")
    print("1. Insert Data")
    print("2. Read Data")
    print("3. Update Data")
    print("4. Delete Data")
    print("5. Exit")
    selection = input("Select Option (1-5): ")
    return selection

def insert_data():
    print("\nInsert Data")
    print("1. Insert One")
    print("2. Insert Many")
    print("3. Back to Main Menu")
    selection_in = input("Select Option (1-3): ")
    
    try:
        if selection_in == "1":
            # Insert One Document
            document = {}
            while True:
                key = input("Enter field name (or 'done' to finish): ")
                if key.lower() == 'done':
                    break
                if not key.strip():
                    print("Field name cannot be empty!")
                    continue
                value = input(f"Enter value for {key}: ")
                document[key] = convert_value(value)
            
            if document:
                result = collection.insert_one(document)
                print(f"Inserted document with ID: {result.inserted_id}")
            else:
                print("No data provided, insertion cancelled.")
        
        elif selection_in == "2":
            # Insert Many Documents
            documents = []
            while True:
                document = {}
                print(f"\nDocument #{len(documents) + 1}")
                while True:
                    key = input("Enter field name (or 'done' to finish this document): ")
                    if key.lower() == 'done':
                        break
                    if not key.strip():
                        print("Field name cannot be empty!")
                        continue
                    value = input(f"Enter value for {key}: ")
                    document[key] = convert_value(value)
                
                if document:
                    documents.append(document)
                else:
                    print("Empty document skipped.")
                
                more = input("Add another document? (y/n): ").lower()
                if more != 'y':
                    break
            
            if documents:
                result = collection.insert_many(documents)
                print(f"Inserted {len(result.inserted_ids)} documents with IDs: {result.inserted_ids}")
            else:
                print("No documents provided, insertion cancelled.")
        elif selection_in == "3":
            return
        else:
            print("Invalid selection!")
    except Exception as e:
        print(f"Error during insertion: {e}")

def read_data():
    print("\nRead Data")
    print("1. Read One")
    print("2. Read Many")
    print("3. Read All")
    print("4. Back to Main Menu")
    selection_rd = input("Select Option (1-4): ")
    
    try:
        if selection_rd in ["1", "2", "3"]:
            # Build query
            query = {}
            print("\nBuild your query (leave empty to match all documents)")
            while True:
                key = input("Enter field to filter by (or 'done' to finish): ")
                if key.lower() == 'done':
                    break
                if not key.strip():
                    print("Field name cannot be empty!")
                    continue
                value = input(f"Enter value for {key}: ")
                
                # Handle special case for _id field
                if key == "_id":
                    if is_valid_object_id(value):
                        query[key] = ObjectId(value)
                    else:
                        print("Invalid ObjectId format for _id field!")
                        continue
                else:
                    query[key] = convert_value(value)
            
            if selection_rd == "1":
                # Find One
                document = collection.find_one(query)
                if document:
                    print("\nFound document:")
                    for key, value in document.items():
                        if isinstance(value, ObjectId):
                            print(f"{key}: {value} (ObjectId)")
                        else:
                            print(f"{key}: {value}")
                else:
                    print("No document found matching the query.")
            
            elif selection_rd == "2":
                # Find Many with limit
                try:
                    limit_input = input("Enter maximum number of documents to display (default 5): ")
                    limit = int(limit_input) if limit_input.isdigit() else 5
                except ValueError:
                    limit = 5
                    print(f"Invalid input, defaulting to {limit} documents.")
                
                documents = list(collection.find(query).limit(limit))
                count = len(documents)
                for i, doc in enumerate(documents):
                    print(f"\nDocument {i+1}:")
                    for key, value in doc.items():
                        if isinstance(value, ObjectId):
                            print(f"{key}: {value} (ObjectId)")
                        else:
                            print(f"{key}: {value}")
                
                if count == 0:
                    print("No documents found matching the query.")
                else:
                    total = collection.count_documents(query)
                    if total > count:
                        print(f"\nShowing {count} of {total} matching documents.")
            
            elif selection_rd == "3":
                # Find All (with caution)
                confirm = input("This may return many documents. Continue? (y/n): ").lower()
                if confirm == 'y':
                    documents = list(collection.find(query))
                    count = len(documents)
                    for i, doc in enumerate(documents):
                        print(f"\nDocument {i+1}:")
                        for key, value in doc.items():
                            if isinstance(value, ObjectId):
                                print(f"{key}: {value} (ObjectId)")
                            else:
                                print(f"{key}: {value}")
                    
                    if count == 0:
                        print("No documents found matching the query.")
                    else:
                        print(f"\nTotal documents found: {count}")
        elif selection_rd == "4":
            return
        else:
            print("Invalid selection!")
    except Exception as e:
        print(f"Error during read operation: {e}")

def update_data():
    print("\nUpdate Data")
    print("1. Update One")
    print("2. Update Many")
    print("3. Back to Main Menu")
    selection_up = input("Select Option (1-3): ")
    
    try:
        if selection_up in ["1", "2"]:
            # Build query
            query = {}
            print("\nBuild your query to select documents to update")
            while True:
                key = input("Enter field to filter by (or 'done' to finish): ")
                if key.lower() == 'done':
                    break
                if not key.strip():
                    print("Field name cannot be empty!")
                    continue
                value = input(f"Enter value for {key}: ")
                
                # Handle special case for _id field
                if key == "_id":
                    if is_valid_object_id(value):
                        query[key] = ObjectId(value)
                    else:
                        print("Invalid ObjectId format for _id field!")
                        continue
                else:
                    query[key] = convert_value(value)
            
            if not query:
                print("Empty query will match all documents!")
                confirm = input("Are you sure you want to update all documents? (y/n): ").lower()
                if confirm != 'y':
                    return
            
            # Build update
            update = {}
            print("\nBuild your update operation")
            print("For setting values, use format: field=value")
            print("For incrementing, use format: field+=value")
            print("For other operators, see MongoDB documentation")
            while True:
                operation = input("Enter update operation (or 'done' to finish): ")
                if operation.lower() == 'done':
                    break
                if not operation.strip():
                    print("Operation cannot be empty!")
                    continue
                
                if "+=" in operation:
                    parts = operation.split("+=")
                    if len(parts) != 2:
                        print("Invalid format for increment operation!")
                        continue
                    field, value = parts
                    field = field.strip()
                    if not field:
                        print("Field name cannot be empty!")
                        continue
                    try:
                        value = value.strip()
                        value = int(value) if value.isdigit() else float(value)
                    except ValueError:
                        print(f"Invalid number: {value}")
                        continue
                    if "$inc" not in update:
                        update["$inc"] = {}
                    update["$inc"][field] = value
                elif "=" in operation:
                    parts = operation.split("=")
                    if len(parts) != 2:
                        print("Invalid format for set operation!")
                        continue
                    field, value = parts
                    field = field.strip()
                    if not field:
                        print("Field name cannot be empty!")
                        continue
                    value = value.strip()
                    if "$set" not in update:
                        update["$set"] = {}
                    update["$set"][field] = convert_value(value)
                else:
                    print("Invalid operation format. Use field=value or field+=value")
            
            if not update:
                print("No update operations provided.")
                return
            
            if selection_up == "1":
                # Update One
                result = collection.update_one(query, update)
                print(f"Matched {result.matched_count} document(s), modified {result.modified_count} document(s)")
            else:
                # Update Many
                result = collection.update_many(query, update)
                print(f"Matched {result.matched_count} document(s), modified {result.modified_count} document(s)")
        elif selection_up == "3":
            return
        else:
            print("Invalid selection!")
    except Exception as e:
        print(f"Error during update operation: {e}")

def delete_data():
    print("\nDelete Data")
    print("1. Delete One")
    print("2. Delete Many")
    print("3. Back to Main Menu")
    selection_del = input("Select Option (1-3): ")
    
    try:
        if selection_del in ["1", "2"]:
            # Build query
            query = {}
            print("\nBuild your query to select documents to delete")
            while True:
                key = input("Enter field to filter by (or 'done' to finish): ")
                if key.lower() == 'done':
                    break
                if not key.strip():
                    print("Field name cannot be empty!")
                    continue
                value = input(f"Enter value for {key}: ")
                
                # Handle special case for _id field
                if key == "_id":
                    if is_valid_object_id(value):
                        query[key] = ObjectId(value)
                    else:
                        print("Invalid ObjectId format for _id field!")
                        continue
                else:
                    query[key] = convert_value(value)
            
            if not query:
                print("Empty query will match all documents!")
                confirm = input("Are you sure you want to delete all documents? (y/n): ").lower()
                if confirm != 'y':
                    return
            
            if selection_del == "1":
                # Delete One
                result = collection.delete_one(query)
                print(f"Deleted {result.deleted_count} document(s)")
            else:
                # Delete Many
                result = collection.delete_many(query)
                print(f"Deleted {result.deleted_count} document(s)")
        elif selection_del == "3":
            return
        else:
            print("Invalid selection!")
    except Exception as e:
        print(f"Error during deletion: {e}")

def main_acv():
    print("MongoDB CRUD Operations Helper")
    try:
        while True:
            selection = menu_db()
            if selection == "1":
                insert_data()
            elif selection == "2":
                read_data()
            elif selection == "3":
                update_data()
            elif selection == "4":
                delete_data()
            elif selection == "5":
                print("Exiting...")
                client.close()
                break
            else:
                print("Invalid selection. Please try again.")
    except KeyboardInterrupt:
        print("\nProgram interrupted by user. Exiting...")
        client.close()
    except Exception as e:
        print(f"Unexpected error: {e}")
        client.close()

if __name__ == "__main__":
    main_acv()
