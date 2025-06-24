#!/usr/bin/env python3
"""
Cleanup script for removing all test data created by pre_extract_filter
"""

import os
import sys
import json
from pathlib import Path
from datetime import datetime

# Add project root to path for direct script execution
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

# Import connection pool - it will handle its own configuration
from src.connection_pool import get_postgres_connection, get_redis_client, close_all_connections, test_connections

def verify_connections():
    """Verify that we can connect to all services"""
    print("🔍 Verifying connections...")
    
    # Test all connections
    status = test_connections()
    for service, is_connected in status.items():
        print(f"   {service}: {'✅ Connected' if is_connected else '❌ Not Connected'}")
    
    if not all(status.values()):
        print("❌ Some services are not connected. Please check your environment.")
        return False
    
    print("✅ All connections verified successfully")
    return True

def cleanup_redis_data():
    """Clean up Redis bloom filters and test data"""
    print("🧹 Cleaning up Redis data...")
    
    try:
        r = get_redis_client()
        if not r:
            print("   ❌ Redis client not available")
            return False
        
        # Get all keys first
        all_keys = r.keys('*')
        print(f"   Found {len(all_keys)} total keys in Redis")
        
        # Find bloom filter keys
        bloom_filter_keys = [key for key in all_keys if key.startswith('bloom_filter:')]
        print(f"   Found {len(bloom_filter_keys)} bloom filter keys:")
        for key in bloom_filter_keys:
            print(f"     - {key}")
        
        # Find other test-related keys
        test_keys = [key for key in all_keys if 'test' in key.lower() and not key.startswith('bloom_filter:')]
        print(f"   Found {len(test_keys)} other test-related keys:")
        for key in test_keys:
            print(f"     - {key}")
        
        # Delete bloom filter keys
        if bloom_filter_keys:
            deleted_count = r.delete(*bloom_filter_keys)
            print(f"   ✅ Deleted {deleted_count} bloom filter keys")
        else:
            print("   ℹ️  No bloom filter keys found")
        
        # Delete other test keys
        if test_keys:
            deleted_count = r.delete(*test_keys)
            print(f"   ✅ Deleted {deleted_count} other test-related keys")
        
        # Verify deletion
        remaining_keys = r.keys('*')
        print(f"   Remaining keys after cleanup: {len(remaining_keys)}")
        if remaining_keys:
            print("   Remaining keys:")
            for key in remaining_keys:
                print(f"     - {key}")
        
        print("   ✅ Redis cleanup completed")
        return True
        
    except Exception as e:
        print(f"   ❌ Redis cleanup failed: {e}")
        return False

def cleanup_database_data():
    """Clean up database test data"""
    print("🧹 Cleaning up database data...")
    
    try:
        with get_postgres_connection() as conn:
            with conn.cursor() as cursor:
                
                # Check if url_tracking table exists
                cursor.execute("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_name = 'url_tracking'
                    ) as table_exists
                """)
                result = cursor.fetchone()
                table_exists = result['table_exists']
                
                if not table_exists:
                    print("   ℹ️  url_tracking table does not exist")
                else:
                    # Clean up url_tracking table
                    print("   Cleaning up url_tracking table...")
                    cursor.execute("SELECT COUNT(*) as url_count FROM url_tracking")
                    result = cursor.fetchone()
                    url_count = result['url_count']
                    print(f"   Found {url_count} URL tracking records")
                    
                    if url_count > 0:
                        cursor.execute("DELETE FROM url_tracking")
                        deleted_urls = cursor.rowcount
                        print(f"   ✅ Deleted {deleted_urls} URL tracking records")
                    else:
                        print("   ℹ️  No URL tracking records found")
                
                # Check and clean up other tables
                tables_to_clean = [
                    ('sentiment_scores', "keyword IN ('HDFC', 'Tata Motors') AND created_at >= CURRENT_TIMESTAMP - INTERVAL '7 days'"),
                    ('articles', "keyword IN ('HDFC', 'Tata Motors') AND created_at >= CURRENT_TIMESTAMP - INTERVAL '7 days'"),
                    ('aggregate_scores', "keyword IN ('HDFC', 'Tata Motors') AND created_at >= CURRENT_TIMESTAMP - INTERVAL '7 days'"),
                    ('pipeline_runs', "run_id LIKE '%pipeline_run_%' AND created_at >= CURRENT_TIMESTAMP - INTERVAL '7 days'")
                ]
                
                for table_name, condition in tables_to_clean:
                    try:
                        cursor.execute(f"SELECT COUNT(*) as record_count FROM {table_name}")
                        result = cursor.fetchone()
                        count = result['record_count']
                        print(f"   Found {count} records in {table_name}")
                        
                        if count > 0:
                            cursor.execute(f"DELETE FROM {table_name} WHERE {condition}")
                            deleted_count = cursor.rowcount
                            print(f"   ✅ Deleted {deleted_count} records from {table_name}")
                    except Exception as e:
                        print(f"   ⚠️  Could not clean {table_name}: {e}")

                conn.commit()
                print("   ✅ Database cleanup completed")
                return True
                
    except Exception as e:
        print(f"   ❌ Database cleanup failed: {e}")
        return False

def cleanup_test_files():
    """Clean up test data files"""
    print("🧹 Cleaning up test files...")
    
    try:
        # Remove test data files
        test_files = [
            "data/temp/extracted_data.json",
            "data/temp/processed_data.json", 
            "data/temp/sentiment_data.json"
        ]
        
        deleted_files = 0
        for file_path in test_files:
            if os.path.exists(file_path):
                os.remove(file_path)
                print(f"   ✅ Deleted {file_path}")
                deleted_files += 1
            else:
                print(f"   ℹ️  File not found: {file_path}")
        
        if deleted_files == 0:
            print("   ℹ️  No test files found")
        else:
            print(f"   ✅ Deleted {deleted_files} test files")
        
        # Clean up temp directory if empty
        temp_dir = Path("data/temp")
        if temp_dir.exists() and not any(temp_dir.iterdir()):
            temp_dir.rmdir()
            print("   ✅ Removed empty temp directory")
        
        print("   ✅ File cleanup completed")
        return True
        
    except Exception as e:
        print(f"   ❌ File cleanup failed: {e}")
        return False

def verify_cleanup():
    """Verify that cleanup was successful"""
    print("\n🔍 Verifying cleanup...")
    
    # Check Redis
    try:
        r = get_redis_client()
        if not r:
            print("   Redis: Unable to check")
        else:
            remaining_keys = r.keys('*')
            bloom_filter_keys = [key for key in remaining_keys if key.startswith('bloom_filter:')]
            print(f"   Redis: {len(remaining_keys)} total keys, {len(bloom_filter_keys)} bloom filter keys")
            
            if bloom_filter_keys:
                print("   ⚠️  Bloom filter keys still exist:")
                for key in bloom_filter_keys:
                    print(f"     - {key}")
    except Exception as e:
        print(f"   Redis: Unable to check - {e}")
    
    # Check database
    try:
        with get_postgres_connection() as conn:
            with conn.cursor() as cursor:
                
                # Check url_tracking
                try:
                    cursor.execute("SELECT COUNT(*) as url_count FROM url_tracking")
                    result = cursor.fetchone()
                    url_count = result['url_count']
                    print(f"   Database: {url_count} URL tracking records")
                    
                    if url_count > 0:
                        cursor.execute("SELECT stock_name, COUNT(*) as count FROM url_tracking GROUP BY stock_name")
                        stock_counts = cursor.fetchall()
                        print("   ⚠️  URL tracking records by stock:")
                        for row in stock_counts:
                            print(f"     - {row['stock_name']}: {row['count']} records")
                except Exception as e:
                    print(f"   Database: Could not check url_tracking - {e}")
                
                # Check other tables
                tables_to_check = ['articles', 'sentiment_scores', 'aggregate_scores', 'pipeline_runs']
                for table in tables_to_check:
                    try:
                        cursor.execute(f"SELECT COUNT(*) as record_count FROM {table}")
                        result = cursor.fetchone()
                        count = result['record_count']
                        print(f"   Database: {count} records in {table}")
                    except Exception as e:
                        print(f"   Database: Could not check {table} - {e}")
                        
    except Exception as e:
        print(f"   Database: Unable to check - {e}")
    
    # Check files
    test_files = [
        "data/temp/extracted_data.json",
        "data/temp/processed_data.json", 
        "data/temp/sentiment_data.json"
    ]
    
    existing_files = sum(1 for f in test_files if os.path.exists(f))
    print(f"   Files: {existing_files} test files remaining")

def show_cleanup_summary():
    """Show summary of what was cleaned up"""
    print("\n📊 Cleanup Summary:")
    print("=" * 50)
    
    # Check Redis
    try:
        r = get_redis_client()
        if not r:
            print("   Redis: Unable to check")
        else:
            remaining_keys = len(r.keys('*'))
            bloom_filter_keys = len([k for k in r.keys('*') if k.startswith('bloom_filter:')])
            print(f"   Redis: {remaining_keys} total keys, {bloom_filter_keys} bloom filter keys")
    except Exception as e:
        print(f"   Redis: Unable to check - {e}")
    
    # Check database
    try:
        with get_postgres_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT COUNT(*) as url_count FROM url_tracking")
                result = cursor.fetchone()
                url_count = result['url_count']
                print(f"   URL tracking records: {url_count}")
                
                cursor.execute("SELECT COUNT(*) as article_count FROM articles")
                result = cursor.fetchone()
                article_count = result['article_count']
                print(f"   Articles: {article_count}")
                
                cursor.execute("SELECT COUNT(*) as sentiment_count FROM sentiment_scores")
                result = cursor.fetchone()
                sentiment_count = result['sentiment_count']
                print(f"   Sentiment scores: {sentiment_count}")
                
                cursor.execute("SELECT COUNT(*) as aggregate_count FROM aggregate_scores")
                result = cursor.fetchone()
                aggregate_count = result['aggregate_count']
                print(f"   Aggregate scores: {aggregate_count}")
                
                cursor.execute("SELECT COUNT(*) as pipeline_count FROM pipeline_runs")
                result = cursor.fetchone()
                pipeline_count = result['pipeline_count']
                print(f"   Pipeline runs: {pipeline_count}")
                
    except Exception as e:
        print(f"   Database: Unable to check - {e}")
    
    # Check files
    test_files = [
        "data/temp/extracted_data.json",
        "data/temp/processed_data.json", 
        "data/temp/sentiment_data.json"
    ]
    
    existing_files = sum(1 for f in test_files if os.path.exists(f))
    print(f"   Test files remaining: {existing_files}")

def main(close_connections=True):
    """Main cleanup function"""
    print("🧹 PRE-EXTRACT FILTER TEST DATA CLEANUP")
    print("=" * 60)
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    
    # Verify connections first
    if not verify_connections():
        print("❌ Connection verification failed. Exiting.")
        return
    
    # Perform cleanup
    redis_success = cleanup_redis_data()
    print()
    
    db_success = cleanup_database_data()
    print()
    
    file_success = cleanup_test_files()
    print()
    
    # Verify cleanup
    verify_cleanup()
    
    # Show summary
    show_cleanup_summary()
    
    print("\n" + "=" * 60)
    if all([redis_success, db_success, file_success]):
        print("🎉 CLEANUP COMPLETED SUCCESSFULLY")
    else:
        print("⚠️  CLEANUP COMPLETED WITH SOME ISSUES")
    print("=" * 60)
    
    # Close all connections only if requested
    if close_connections:
        close_all_connections()

if __name__ == "__main__":
    main() 