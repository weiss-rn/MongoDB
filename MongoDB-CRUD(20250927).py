# Automated version mongodb_ESS
# Collaboration Between @weiss-rn and @shn-enaa
# このコードは、GitHubリポジトリにアップロードされる前に何度も修正されています。
# https://www.mongodb.com/ja-jp/docs/manual/crud/
# Work In Progress - Code isn't entirely stable.
# 開発中 - コードは完全に安定していません。

from pymongo import MongoClient, errors, ASCENDING, DESCENDING
from pymongo.collection import Collection
from pymongo.database import Database
from bson.objectid import ObjectId
from bson.errors import InvalidId
from datetime import datetime
import json
import re
from typing import Dict, List, Any, Optional, Union
from enum import Enum

class SortOrder(Enum):
    """Sort order enumeration"""
    ASC = 1
    DESC = -1

class MongoDBCRUD:
    """MongoDB CRUD Operations Manager following official MongoDB patterns"""
    
    def __init__(self, connection_string: str = "mongodb://localhost:27017/", 
                 database_name: str = "mydatabase", 
                 collection_name: str = "mycollection"):
        """
        Initialize MongoDB connection
        
        Args:
            connection_string: MongoDB connection URI
            database_name: Name of the database
            collection_name: Name of the collection
        """
        try:
            self.client = MongoClient(connection_string, serverSelectionTimeoutMS=5000)
            # Test connection
            self.client.server_info()
            self.db: Database = self.client[database_name]
            self.collection: Collection = self.db[collection_name]
            print(f"✓ Connected to MongoDB: {database_name}.{collection_name}")
        except errors.ServerSelectionTimeoutError:
            raise ConnectionError("Failed to connect to MongoDB. Ensure MongoDB is running.")
        except Exception as e:
            raise ConnectionError(f"MongoDB connection error: {e}")
    
    def __del__(self):
        """Cleanup connection on object destruction"""
        if hasattr(self, 'client'):
            self.client.close()
    
    @staticmethod
    def validate_object_id(oid: str) -> bool:
        """Validate if string is a valid ObjectId"""
        try:
            ObjectId(oid)
            return True
        except (InvalidId, TypeError):
            return False
    
    @staticmethod
    def parse_value(value: str) -> Any:
        """
        Parse string value to appropriate Python/BSON type
        
        Args:
            value: String value to parse
            
        Returns:
            Parsed value in appropriate type
        """
        value = value.strip()
        
        # Handle null/None
        if value.lower() in ['null', 'none']:
            return None
        
        # Handle booleans
        if value.lower() == 'true':
            return True
        if value.lower() == 'false':
            return False
        
        # Handle ObjectId
        if value.startswith('ObjectId(') and value.endswith(')'):
            oid = value[9:-1].strip('"\'')
            if MongoDBCRUD.validate_object_id(oid):
                return ObjectId(oid)
        
        # Handle dates in ISO format
        if re.match(r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}', value):
            try:
                return datetime.fromisoformat(value.replace('Z', '+00:00'))
            except ValueError:
                pass
        
        # Handle numbers
        try:
            if '.' in value or 'e' in value.lower():
                return float(value)
            return int(value)
        except ValueError:
            pass
        
        # Handle JSON arrays and objects
        if (value.startswith('[') and value.endswith(']')) or \
           (value.startswith('{') and value.endswith('}')):
            try:
                return json.loads(value)
            except json.JSONDecodeError:
                pass
        
        # Return as string
        return value
    
    def build_query_filter(self) -> Dict[str, Any]:
        """
        Build MongoDB query filter with support for operators
        
        Returns:
            Query filter dictionary
        """
        query = {}
        print("\n　Build Query Filter (press Enter without input to finish)")
        print("Supported formats:")
        print("  - field=value (exact match)")
        print("  - field>value (greater than)")
        print("  - field>=value (greater than or equal)")
        print("  - field<value (less than)")
        print("  - field<=value (less than or equal)")
        print("  - field!=value (not equal)")
        print("  - field~regex (regex match)")
        print("  - field:in:[val1,val2] (in array)")
        print("  - field:exists:true/false (field exists)")
        print("  - _id=ObjectId (for document ID)")
        
        while True:
            condition = input("\nEnter condition (or press Enter to finish): ").strip()
            if not condition:
                break
            
            # Parse different operators
            if ':in:' in condition:
                field, values = condition.split(':in:', 1)
                try:
                    values_list = json.loads(values)
                    query[field] = {"$in": values_list}
                except json.JSONDecodeError:
                    print("⚠ Invalid array format. Use JSON array: [val1, val2]")
            elif ':exists:' in condition:
                field, exists = condition.split(':exists:', 1)
                query[field] = {"$exists": exists.lower() == 'true'}
            elif '~' in condition:
                field, pattern = condition.split('~', 1)
                query[field] = {"$regex": pattern}
            elif '>=' in condition:
                field, value = condition.split('>=', 1)
                query[field] = {"$gte": self.parse_value(value)}
            elif '<=' in condition:
                field, value = condition.split('<=', 1)
                query[field] = {"$lte": self.parse_value(value)}
            elif '>' in condition:
                field, value = condition.split('>', 1)
                query[field] = {"$gt": self.parse_value(value)}
            elif '<' in condition:
                field, value = condition.split('<', 1)
                query[field] = {"$lt": self.parse_value(value)}
            elif '!=' in condition:
                field, value = condition.split('!=', 1)
                query[field] = {"$ne": self.parse_value(value)}
            elif '=' in condition:
                field, value = condition.split('=', 1)
                field = field.strip()
                
                # Special handling for _id
                if field == '_id':
                    if self.validate_object_id(value.strip()):
                        query[field] = ObjectId(value.strip())
                    else:
                        print("⚠ Invalid ObjectId format")
                        continue
                else:
                    query[field] = self.parse_value(value)
            else:
                print("⚠ Invalid condition format")
        
        return query
    
    def build_update_document(self) -> Dict[str, Any]:
        """
        Build MongoDB update document with operators
        
        Returns:
            Update document dictionary
        """
        update = {}
        print("\n📝 Build Update Operations")
        print("Supported operations:")
        print("  - $set: field=value (set field value)")
        print("  - $inc: field+=value (increment by value)")
        print("  - $mul: field*=value (multiply by value)")
        print("  - $unset: field! (remove field)")
        print("  - $push: field[]=value (push to array)")
        print("  - $pull: field-=value (pull from array)")
        print("  - $addToSet: field[]!=value (add to set if unique)")
        print("  - $rename: field:newname (rename field)")
        
        while True:
            operation = input("\nEnter operation (or press Enter to finish): ").strip()
            if not operation:
                break
            
            try:
                if ':' in operation and not '[]' in operation:  # $rename
                    old_field, new_field = operation.split(':', 1)
                    update.setdefault("$rename", {})[old_field.strip()] = new_field.strip()
                elif '[]!=' in operation:  # $addToSet
                    field, value = operation.split('[]!=', 1)
                    update.setdefault("$addToSet", {})[field.strip()] = self.parse_value(value)
                elif '[]=' in operation:  # $push
                    field, value = operation.split('[]=', 1)
                    update.setdefault("$push", {})[field.strip()] = self.parse_value(value)
                elif '-=' in operation:  # $pull
                    field, value = operation.split('-=', 1)
                    update.setdefault("$pull", {})[field.strip()] = self.parse_value(value)
                elif '+=' in operation:  # $inc
                    field, value = operation.split('+=', 1)
                    update.setdefault("$inc", {})[field.strip()] = self.parse_value(value)
                elif '*=' in operation:  # $mul
                    field, value = operation.split('*=', 1)
                    update.setdefault("$mul", {})[field.strip()] = self.parse_value(value)
                elif operation.endswith('!'):  # $unset
                    field = operation[:-1].strip()
                    update.setdefault("$unset", {})[field] = ""
                elif '=' in operation:  # $set
                    field, value = operation.split('=', 1)
                    update.setdefault("$set", {})[field.strip()] = self.parse_value(value)
                else:
                    print("⚠ Invalid operation format")
            except ValueError as e:
                print(f"⚠ Error parsing operation: {e}")
        
        return update
    
    def insert_one(self):
        """Insert a single document"""
        print("\n➕ INSERT ONE DOCUMENT")
        document = {}
        
        print("Enter document fields (press Enter without input to finish):")
        while True:
            field = input("Field name: ").strip()
            if not field:
                break
            value = input(f"Value for '{field}': ").strip()
            document[field] = self.parse_value(value)
        
        if not document:
            print("⚠ No fields provided. Operation cancelled.")
            return
        
        try:
            result = self.collection.insert_one(document)
            print(f"✓ Document inserted with _id: {result.inserted_id}")
        except errors.DuplicateKeyError as e:
            print(f"✗ Duplicate key error: {e}")
        except Exception as e:
            print(f"✗ Error inserting document: {e}")
    
    def insert_many(self):
        """Insert multiple documents"""
        print("\n➕ INSERT MULTIPLE DOCUMENTS")
        documents = []
        
        while True:
            document = {}
            print(f"\n Document #{len(documents) + 1}")
            print("Enter fields (press Enter without input to finish document):")
            
            while True:
                field = input("Field name: ").strip()
                if not field:
                    break
                value = input(f"Value for '{field}': ").strip()
                document[field] = self.parse_value(value)
            
            if document:
                documents.append(document)
                print(f"✓ Document #{len(documents)} added")
            
            if not input("\nAdd another document? (y/n): ").lower().startswith('y'):
                break
        
        if not documents:
            print("⚠ No documents provided. Operation cancelled.")
            return
        
        try:
            ordered = input("Use ordered insert? (y/n): ").lower().startswith('y')
            result = self.collection.insert_many(documents, ordered=ordered)
            print(f"✓ Inserted {len(result.inserted_ids)} documents")
            print(f"   IDs: {result.inserted_ids}")
        except errors.BulkWriteError as e:
            print(f"✗ Bulk write error: {e.details}")
        except Exception as e:
            print(f"✗ Error inserting documents: {e}")
    
    def find_one(self):
        """Find a single document"""
        print("\n FIND ONE DOCUMENT")
        query = self.build_query_filter()
        
        # Projection
        projection = {}
        if input("\nUse projection? (y/n): ").lower().startswith('y'):
            print("Enter fields to include (1) or exclude (0):")
            while True:
                field = input("Field (or press Enter to finish): ").strip()
                if not field:
                    break
                include = input(f"Include '{field}'? (y/n): ").lower().startswith('y')
                projection[field] = 1 if include else 0
        
        try:
            document = self.collection.find_one(query, projection or None)
            if document:
                print("\n✓ Found document:")
                print(json.dumps(document, default=str, indent=2))
            else:
                print("✗ No document found matching the query")
        except Exception as e:
            print(f"✗ Error finding document: {e}")
    
    def find_many(self):
        """Find multiple documents with options"""
        print("\n🔍 FIND MULTIPLE DOCUMENTS")
        query = self.build_query_filter()
        
        # Projection
        projection = {}
        if input("\nUse projection? (y/n): ").lower().startswith('y'):
            print("Enter fields to include (1) or exclude (0):")
            while True:
                field = input("Field (or press Enter to finish): ").strip()
                if not field:
                    break
                include = input(f"Include '{field}'? (y/n): ").lower().startswith('y')
                projection[field] = 1 if include else 0
        
        # Sort
        sort = []
        if input("\nApply sorting? (y/n): ").lower().startswith('y'):
            while True:
                field = input("Sort by field (or press Enter to finish): ").strip()
                if not field:
                    break
                order = input(f"Order for '{field}' (asc/desc): ").lower()
                sort.append((field, ASCENDING if order == 'asc' else DESCENDING))
        
        # Limit and Skip
        limit = 0
        skip = 0
        try:
            limit_input = input("\nLimit results (0 for no limit): ").strip()
            limit = int(limit_input) if limit_input else 0
            
            skip_input = input("Skip documents (0 for no skip): ").strip()
            skip = int(skip_input) if skip_input else 0
        except ValueError:
            print("⚠ Invalid number, using defaults")
        
        try:
            cursor = self.collection.find(query, projection or None)
            
            if sort:
                cursor = cursor.sort(sort)
            if skip > 0:
                cursor = cursor.skip(skip)
            if limit > 0:
                cursor = cursor.limit(limit)
            
            documents = list(cursor)
            
            if documents:
                print(f"\n✓ Found {len(documents)} document(s):")
                for i, doc in enumerate(documents, 1):
                    print(f"\n--- Document {i} ---")
                    print(json.dumps(doc, default=str, indent=2))
                
                # Show total count if limited
                if limit > 0:
                    total = self.collection.count_documents(query)
                    if total > len(documents):
                        print(f"\n(Showing {len(documents)} of {total} total matches)")
            else:
                print("✗ No documents found matching the query")
        except Exception as e:
            print(f"✗ Error finding documents: {e}")
    
    def update_one(self):
        """Update a single document"""
        print("\n✏️ UPDATE ONE DOCUMENT")
        query = self.build_query_filter()
        update = self.build_update_document()
        
        if not update:
            print("⚠ No update operations provided. Operation cancelled.")
            return
        
        upsert = input("\nUpsert if not found? (y/n): ").lower().startswith('y')
        
        try:
            result = self.collection.update_one(query, update, upsert=upsert)
            print(f"✓ Matched: {result.matched_count}, Modified: {result.modified_count}")
            if result.upserted_id:
                print(f"   Upserted ID: {result.upserted_id}")
        except errors.WriteError as e:
            print(f"✗ Write error: {e}")
        except Exception as e:
            print(f"✗ Error updating document: {e}")
    
    def update_many(self):
        """Update multiple documents"""
        print("\n✏️ UPDATE MULTIPLE DOCUMENTS")
        query = self.build_query_filter()
        
        if not query:
            confirm = input("⚠ Empty query will update ALL documents. Continue? (y/n): ")
            if not confirm.lower().startswith('y'):
                print("Operation cancelled.")
                return
        
        update = self.build_update_document()
        
        if not update:
            print("⚠ No update operations provided. Operation cancelled.")
            return
        
        upsert = input("\nUpsert if not found? (y/n): ").lower().startswith('y')
        
        try:
            result = self.collection.update_many(query, update, upsert=upsert)
            print(f"✓ Matched: {result.matched_count}, Modified: {result.modified_count}")
            if result.upserted_id:
                print(f"   Upserted ID: {result.upserted_id}")
        except errors.WriteError as e:
            print(f"✗ Write error: {e}")
        except Exception as e:
            print(f"✗ Error updating documents: {e}")
    
    def replace_one(self):
        """Replace a document entirely"""
        print("\n REPLACE ONE DOCUMENT")
        query = self.build_query_filter()
        
        print("\nEnter replacement document:")
        replacement = {}
        while True:
            field = input("Field name (or press Enter to finish): ").strip()
            if not field:
                break
            value = input(f"Value for '{field}': ").strip()
            replacement[field] = self.parse_value(value)
        
        if not replacement:
            print("⚠ No replacement document provided. Operation cancelled.")
            return
        
        upsert = input("\nUpsert if not found? (y/n): ").lower().startswith('y')
        
        try:
            result = self.collection.replace_one(query, replacement, upsert=upsert)
            print(f"✓ Matched: {result.matched_count}, Modified: {result.modified_count}")
            if result.upserted_id:
                print(f"   Upserted ID: {result.upserted_id}")
        except Exception as e:
            print(f"✗ Error replacing document: {e}")
    
    def delete_one(self):
        """Delete a single document"""
        print("\n　DELETE ONE DOCUMENT")
        query = self.build_query_filter()
        
        if not query:
            print(" Empty query would delete a random document.")
            if not input("Continue? (y/n): ").lower().startswith('y'):
                print("Operation cancelled.")
                return
        
        # Show document to be deleted
        doc = self.collection.find_one(query)
        if doc:
            print("\nDocument to be deleted:")
            print(json.dumps(doc, default=str, indent=2))
            if not input("\nConfirm deletion? (y/n): ").lower().startswith('y'):
                print("Operation cancelled.")
                return
        else:
            print("✗ No document found matching the query")
            return
        
        try:
            result = self.collection.delete_one(query)
            print(f"✓ Deleted {result.deleted_count} document")
        except Exception as e:
            print(f"✗ Error deleting document: {e}")
    
    def delete_many(self):
        """Delete multiple documents"""
        print("\n DELETE MULTIPLE DOCUMENTS")
        query = self.build_query_filter()
        
        if not query:
            print("⚠ Empty query will delete ALL documents!")
            if not input("Are you SURE you want to delete ALL documents? (yes/n): ") == 'yes':
                print("Operation cancelled.")
                return
        
        # Show count of documents to be deleted
        count = self.collection.count_documents(query)
        if count > 0:
            print(f"\n⚠ This will delete {count} document(s)")
            if not input("Confirm deletion? (y/n): ").lower().startswith('y'):
                print("Operation cancelled.")
                return
        else:
            print("✗ No documents found matching the query")
            return
        
        try:
            result = self.collection.delete_many(query)
            print(f"✓ Deleted {result.deleted_count} document(s)")
        except Exception as e:
            print(f"✗ Error deleting documents: {e}")
    
    def aggregate(self):
        """Perform aggregation pipeline"""
        print("\n🔧 AGGREGATION PIPELINE")
        pipeline = []
        
        print("Build aggregation pipeline (common stages):")
        print("1. $match - Filter documents")
        print("2. $group - Group by field")
        print("3. $sort - Sort results")
        print("4. $limit - Limit results")
        print("5. $project - Shape output")
        print("6. $lookup - Join collections")
        print("7. Custom stage (JSON)")
        
        while True:
            stage_type = input("\nSelect stage (1-7, or press Enter to execute): ").strip()
            
            if not stage_type:
                break
            
            try:
                if stage_type == '1':  # $match
                    print("Build match filter:")
                    match_filter = self.build_query_filter()
                    if match_filter:
                        pipeline.append({"$match": match_filter})
                        print("✓ Added $match stage")
                
                elif stage_type == '2':  # $group
                    group_by = input("Group by field (use $_id for all): ").strip()
                    if not group_by.startswith('$'):
                        group_by = '$' + group_by if group_by != '_id' else None
                    
                    group_stage = {"_id": group_by}
                    
                    print("Add accumulator fields (e.g., count, sum, avg):")
                    while True:
                        acc_name = input("Field name (or press Enter to finish): ").strip()
                        if not acc_name:
                            break
                        acc_op = input(f"Operation for '{acc_name}' (sum/avg/min/max/count): ").lower()
                        
                        if acc_op == 'count':
                            group_stage[acc_name] = {"$sum": 1}
                        elif acc_op in ['sum', 'avg', 'min', 'max']:
                            field = input(f"Field to {acc_op}: ").strip()
                            if not field.startswith('$'):
                                field = '$' + field
                            group_stage[acc_name] = {f"${acc_op}": field}
                    
                    pipeline.append({"$group": group_stage})
                    print("✓ Added $group stage")
                
                elif stage_type == '3':  # $sort
                    sort_stage = {}
                    while True:
                        field = input("Sort by field (or press Enter to finish): ").strip()
                        if not field:
                            break
                        order = input(f"Order for '{field}' (asc/desc): ").lower()
                        sort_stage[field] = 1 if order == 'asc' else -1
                    
                    if sort_stage:
                        pipeline.append({"$sort": sort_stage})
                        print("✓ Added $sort stage")
                
                elif stage_type == '4':  # $limit
                    limit = int(input("Limit to how many documents: "))
                    pipeline.append({"$limit": limit})
                    print("✓ Added $limit stage")
                
                elif stage_type == '5':  # $project
                    project_stage = {}
                    print("Configure projection (1=include, 0=exclude):")
                    while True:
                        field = input("Field (or press Enter to finish): ").strip()
                        if not field:
                            break
                        include = input(f"Include '{field}'? (y/n): ").lower().startswith('y')
                        project_stage[field] = 1 if include else 0
                    
                    if project_stage:
                        pipeline.append({"$project": project_stage})
                        print("✓ Added $project stage")
                
                elif stage_type == '6':  # $lookup
                    from_coll = input("Join with collection: ").strip()
                    local_field = input("Local field: ").strip()
                    foreign_field = input("Foreign field: ").strip()
                    as_field = input("Output array field: ").strip()
                    
                    pipeline.append({
                        "$lookup": {
                            "from": from_coll,
                            "localField": local_field,
                            "foreignField": foreign_field,
                            "as": as_field
                        }
                    })
                    print("✓ Added $lookup stage")
                
                elif stage_type == '7':  # Custom
                    custom = input("Enter stage as JSON: ").strip()
                    stage = json.loads(custom)
                    pipeline.append(stage)
                    print("✓ Added custom stage")
                
            except (ValueError, json.JSONDecodeError) as e:
                print(f"⚠ Invalid input: {e}")
        
        if not pipeline:
            print("⚠ No pipeline stages added. Operation cancelled.")
            return
        
        print("\n📊 Executing pipeline:")
        print(json.dumps(pipeline, indent=2))
        
        try:
            results = list(self.collection.aggregate(pipeline))
            if results:
                print(f"\n✓ Aggregation returned {len(results)} result(s):")
                for i, doc in enumerate(results, 1):
                    print(f"\n--- Result {i} ---")
                    print(json.dumps(doc, default=str, indent=2))
            else:
                print("✗ No results from aggregation")
        except Exception as e:
            print(f"✗ Error executing aggregation: {e}")
    
    def count_documents(self):
        """Count documents matching a query"""
        print("\n📊 COUNT DOCUMENTS")
        query = self.build_query_filter()
        
        try:
            count = self.collection.count_documents(query)
            print(f"✓ Count: {count} document(s)")
        except Exception as e:
            print(f"✗ Error counting documents: {e}")
    
    def distinct_values(self):
        """Get distinct values for a field"""
        print("\n🔍 DISTINCT VALUES")
        field = input("Field name: ").strip()
        if not field:
            print("⚠ Field name required")
            return
        
        query = {}
        if input("Apply filter? (y/n): ").lower().startswith('y'):
            query = self.build_query_filter()
        
        try:
            values = self.collection.distinct(field, query)
            print(f"✓ Found {len(values)} distinct value(s) for '{field}':")
            for value in values:
                print(f"  • {value}")
        except Exception as e:
            print(f"✗ Error getting distinct values: {e}")
    
    def create_index(self):
        """Create an index on collection"""
        print("\n🔨 CREATE INDEX")
        
        index_keys = []
        while True:
            field = input("Field to index (or press Enter to finish): ").strip()
            if not field:
                break
            order = input(f"Order for '{field}' (asc/desc/text/2dsphere): ").lower()
            
            if order == 'asc':
                index_keys.append((field, ASCENDING))
            elif order == 'desc':
                index_keys.append((field, DESCENDING))
            elif order == 'text':
                index_keys.append((field, 'text'))
            elif order == '2dsphere':
                index_keys.append((field, '2dsphere'))
            else:
                print("⚠ Invalid order. Using ascending.")
                index_keys.append((field, ASCENDING))
        
        if not index_keys:
            print("⚠ No fields specified for index")
            return
        
        options = {}
        if input("Set index as unique? (y/n): ").lower().startswith('y'):
            options['unique'] = True
        
        name = input("Index name (or press Enter for auto): ").strip()
        if name:
            options['name'] = name
        
        try:
            index_name = self.collection.create_index(index_keys, **options)
            print(f"✓ Created index: {index_name}")
        except errors.DuplicateKeyError as e:
            print(f"✗ Duplicate key error: {e}")
        except Exception as e:
            print(f"✗ Error creating index: {e}")
    
    def list_indexes(self):
        """List all indexes on collection"""
        print("\n📋 COLLECTION INDEXES")
        try:
            indexes = list(self.collection.list_indexes())
            print(f"Found {len(indexes)} index(es):")
            for index in indexes:
                print(f"\n• Name: {index['name']}")
                print(f"  Keys: {index['key']}")
                if 'unique' in index:
                    print(f"  Unique: {index['unique']}")
        except Exception as e:
            print(f"✗ Error listing indexes: {e}")
    
    def collection_stats(self):
        """Get collection statistics"""
        print("\n📈 COLLECTION STATISTICS")
        try:
            stats = self.db.command("collStats", self.collection.name)
            print(f"Collection: {stats['ns']}")
            print(f"Document count: {stats.get('count', 0)}")
            print(f"Average document size: {stats.get('avgObjSize', 0)} bytes")
            print(f"Total collection size: {stats.get('size', 0)} bytes")
            print(f"Storage size: {stats.get('storageSize', 0)} bytes")
            print(f"Number of indexes: {stats.get('nindexes', 0)}")
            print(f"Total index size: {stats.get('totalIndexSize', 0)} bytes")
        except Exception as e:
            print(f"✗ Error getting collection stats: {e}")
    
    def bulk_operations(self):
        """Perform bulk write operations"""
        print("\n⚡ BULK WRITE OPERATIONS")
        from pymongo import InsertOne, UpdateOne, UpdateMany, ReplaceOne, DeleteOne, DeleteMany
        
        operations = []
        operation_types = {
            '1': 'InsertOne',
            '2': 'UpdateOne',
            '3': 'UpdateMany',
            '4': 'ReplaceOne',
            '5': 'DeleteOne',
            '6': 'DeleteMany'
        }
        
        print("Build bulk operations:")
        while True:
            print("\n1. InsertOne")
            print("2. UpdateOne")
            print("3. UpdateMany")
            print("4. ReplaceOne")
            print("5. DeleteOne")
            print("6. DeleteMany")
            op_type = input("Select operation (or press Enter to execute): ").strip()
            
            if not op_type:
                break
            
            if op_type not in operation_types:
                print("⚠ Invalid operation type")
                continue
            
            try:
                if op_type == '1':  # InsertOne
                    print("Enter document to insert:")
                    document = {}
                    while True:
                        field = input("Field name (or press Enter to finish): ").strip()
                        if not field:
                            break
                        value = input(f"Value for '{field}': ").strip()
                        document[field] = self.parse_value(value)
                    if document:
                        operations.append(InsertOne(document))
                        print(f"✓ Added InsertOne operation")
                
                elif op_type in ['2', '3']:  # UpdateOne/UpdateMany
                    print("Build filter:")
                    filter_doc = self.build_query_filter()
                    print("Build update:")
                    update_doc = self.build_update_document()
                    if update_doc:
                        if op_type == '2':
                            operations.append(UpdateOne(filter_doc, update_doc))
                        else:
                            operations.append(UpdateMany(filter_doc, update_doc))
                        print(f"✓ Added {operation_types[op_type]} operation")
                
                elif op_type == '4':  # ReplaceOne
                    print("Build filter:")
                    filter_doc = self.build_query_filter()
                    print("Enter replacement document:")
                    replacement = {}
                    while True:
                        field = input("Field name (or press Enter to finish): ").strip()
                        if not field:
                            break
                        value = input(f"Value for '{field}': ").strip()
                        replacement[field] = self.parse_value(value)
                    if replacement:
                        operations.append(ReplaceOne(filter_doc, replacement))
                        print(f"✓ Added ReplaceOne operation")
                
                elif op_type in ['5', '6']:  # DeleteOne/DeleteMany
                    print("Build filter:")
                    filter_doc = self.build_query_filter()
                    if op_type == '5':
                        operations.append(DeleteOne(filter_doc))
                    else:
                        operations.append(DeleteMany(filter_doc))
                    print(f"✓ Added {operation_types[op_type]} operation")
                
            except Exception as e:
                print(f"⚠ Error adding operation: {e}")
        
        if not operations:
            print("⚠ No operations added. Bulk write cancelled.")
            return
        
        print(f"\n📦 Executing {len(operations)} bulk operation(s)...")
        ordered = input("Execute in order? (y/n): ").lower().startswith('y')
        
        try:
            result = self.collection.bulk_write(operations, ordered=ordered)
            print(f"✓ Bulk write completed:")
            print(f"  Inserted: {result.inserted_count}")
            print(f"  Matched: {result.matched_count}")
            print(f"  Modified: {result.modified_count}")
            print(f"  Deleted: {result.deleted_count}")
            print(f"  Upserted: {result.upserted_count}")
            if result.upserted_ids:
                print(f"  Upserted IDs: {result.upserted_ids}")
        except errors.BulkWriteError as e:
            print(f"✗ Bulk write error: {e.details}")
        except Exception as e:
            print(f"✗ Error executing bulk write: {e}")


class MongoDBCLI:
    """Command-line interface for MongoDB CRUD operations"""
    
    def __init__(self):
        self.crud = None
        self.connected = False
    
    def connect(self):
        """Establish MongoDB connection"""
        print("\n🔌 MONGODB CONNECTION")
        
        # Connection options
        print("1. Use default (mongodb://localhost:27017/)")
        print("2. Enter custom connection string")
        choice = input("Select option (1-2): ").strip()
        
        if choice == '2':
            connection_string = input("Enter MongoDB URI: ").strip()
        else:
            connection_string = "mongodb://localhost:27017/"
        
        database_name = input("Database name (default: mydatabase): ").strip() or "mydatabase"
        collection_name = input("Collection name (default: mycollection): ").strip() or "mycollection"
        
        try:
            self.crud = MongoDBCRUD(connection_string, database_name, collection_name)
            self.connected = True
        except ConnectionError as e:
            print(f"✗ {e}")
            self.connected = False
    
    def switch_collection(self):
        """Switch to a different collection"""
        if not self.connected:
            print("⚠ Not connected to MongoDB")
            return
        
        database_name = input("Database name (or press Enter to keep current): ").strip()
        collection_name = input("Collection name: ").strip()
        
        if not collection_name:
            print("⚠ Collection name required")
            return
        
        try:
            if database_name:
                self.crud.db = self.crud.client[database_name]
            self.crud.collection = self.crud.db[collection_name]
            print(f"✓ Switched to {self.crud.db.name}.{collection_name}")
        except Exception as e:
            print(f"✗ Error switching collection: {e}")
    
    def list_databases(self):
        """List all databases"""
        if not self.connected:
            print("⚠ Not connected to MongoDB")
            return
        
        try:
            print("\n DATABASES:")
            for db_name in self.crud.client.list_database_names():
                print(f"  • {db_name}")
        except Exception as e:
            print(f"✗ Error listing databases: {e}")
    
    def list_collections(self):
        """List all collections in current database"""
        if not self.connected:
            print("⚠ Not connected to MongoDB")
            return
        
        try:
            print(f"\n COLLECTIONS IN '{self.crud.db.name}':")
            for coll_name in self.crud.db.list_collection_names():
                print(f"  • {coll_name}")
        except Exception as e:
            print(f"✗ Error listing collections: {e}")
    
    def display_menu(self):
        """Display main menu"""
        print("\n" + "="*60)
        print("🍃 MONGODB CRUD OPERATIONS MANAGER")
        print("="*60)
        
        if self.connected:
            print(f"📍 Connected to: {self.crud.db.name}.{self.crud.collection.name}")
        else:
            print("⚠ Not connected to MongoDB")
        
        print("\n--- INSERT OPERATIONS ---")
        print("1.  Insert One Document")
        print("2.  Insert Many Documents")
        
        print("\n--- READ OPERATIONS ---")
        print("3.  Find One Document")
        print("4.  Find Many Documents")
        print("5.  Count Documents")
        print("6.  Distinct Values")
        
        print("\n--- UPDATE OPERATIONS ---")
        print("7.  Update One Document")
        print("8.  Update Many Documents")
        print("9.  Replace One Document")
        
        print("\n--- DELETE OPERATIONS ---")
        print("10. Delete One Document")
        print("11. Delete Many Documents")
        
        print("\n--- ADVANCED OPERATIONS ---")
        print("12. Aggregation Pipeline")
        print("13. Bulk Write Operations")
        
        print("\n--- INDEX OPERATIONS ---")
        print("14. Create Index")
        print("15. List Indexes")
        
        print("\n--- DATABASE/COLLECTION ---")
        print("16. Collection Statistics")
        print("17. List Databases")
        print("18. List Collections")
        print("19. Switch Collection")
        
        print("\n--- CONNECTION ---")
        print("20. Connect/Reconnect")
        print("0.  Exit")
        print("="*60)
    
    def run(self):
        """Main application loop"""
        print("🍃 MongoDB CRUD Operations Manager")
        print("   Professional MongoDB Database Management Tool")
        print("   Following MongoDB Official Documentation")
        
        # Auto-connect on start
        self.connect()
        
        operations = {
            '1': lambda: self.crud.insert_one(),
            '2': lambda: self.crud.insert_many(),
            '3': lambda: self.crud.find_one(),
            '4': lambda: self.crud.find_many(),
            '5': lambda: self.crud.count_documents(),
            '6': lambda: self.crud.distinct_values(),
            '7': lambda: self.crud.update_one(),
            '8': lambda: self.crud.update_many(),
            '9': lambda: self.crud.replace_one(),
            '10': lambda: self.crud.delete_one(),
            '11': lambda: self.crud.delete_many(),
            '12': lambda: self.crud.aggregate(),
            '13': lambda: self.crud.bulk_operations(),
            '14': lambda: self.crud.create_index(),
            '15': lambda: self.crud.list_indexes(),
            '16': lambda: self.crud.collection_stats(),
            '17': lambda: self.list_databases(),
            '18': lambda: self.list_collections(),
            '19': lambda: self.switch_collection(),
            '20': lambda: self.connect()
        }
        
        while True:
            try:
                self.display_menu()
                choice = input("\nSelect operation (0-20): ").strip()
                
                if choice == '0':
                    print("\n　Closing MongoDB connection...")
                    if self.crud:
                        self.crud.client.close()
                    print("✓ Thank you for using MongoDB CRUD Manager!")
                    break
                
                if choice in operations:
                    if choice == '20' or self.connected:
                        operations[choice]()
                    else:
                        print("⚠ Please connect to MongoDB first (option 20)")
                else:
                    print("⚠ Invalid option. Please select a valid operation.")
                
                if choice not in ['0', '20']:
                    input("\nPress Enter to continue...")
                    
            except KeyboardInterrupt:
                print("\n\n⚠ Operation interrupted by user")
                if input("Exit application? (y/n): ").lower().startswith('y'):
                    if self.crud:
                        self.crud.client.close()
                    print("Goodbye!")
                    break
            except Exception as e:
                print(f"✗ Unexpected error: {e}")
                input("\nPress Enter to continue...")


def main():
    """Main entry point"""
    cli = MongoDBCLI()
    cli.run()


if __name__ == "__main__":
    main()
