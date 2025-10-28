#!/usr/bin/env python3
"""
Comprehensive test for Starburst tag publishing with the exact table from the UI
"""

import requests
import json
import time

def test_starburst_with_real_table():
    """Test with the exact table from the UI: end_to_end_lineage"""
    print("🧪 Testing Starburst Tag Publishing with end_to_end_lineage table...")
    
    # Use the exact values from the screenshot
    test_payload = {
        "catalog": "torro_test_dataset_0914",
        "schema": "consent_analytics",
        "tableId": "end_to_end_lineage",  # The table from the screenshot
        "columnTags": [
            {
                "columnName": "consent_category",
                "tags": ["test_tag_comprehensive"],
                "piiFound": False,
                "piiType": ""
            }
        ],
        "catalogTag": None,
        "schemaTag": None,
        "tableTag": None
    }
    
    print(f"\n📋 Test Configuration:")
    print(f"   Catalog: {test_payload['catalog']}")
    print(f"   Schema: {test_payload['schema']}")
    print(f"   Table: {test_payload['tableId']}")
    print(f"   Column: consent_category")
    
    try:
        start_time = time.time()
        print(f"\n📤 Sending request to backend...")
        
        response = requests.post(
            'http://localhost:8000/api/starburst/publish-tags',
            headers={'Content-Type': 'application/json'},
            json=test_payload,
            timeout=120  # 2 minute timeout
        )
        
        elapsed = time.time() - start_time
        print(f"⏱️  Request completed in {elapsed:.2f} seconds")
        print(f"📥 Response status: {response.status_code}")
        
        if response.status_code == 200:
            result = response.json()
            if result.get('success'):
                print("\n✅ SUCCESS! Tag publishing works for end_to_end_lineage table!")
                print(f"   Message: {result.get('message')}")
                billing_msg = result.get('billingMessage', '')
                if billing_msg:
                    print(f"\n   Details:")
                    for line in billing_msg.split('\n')[:5]:
                        print(f"   {line}")
            else:
                print("\n⚠️ Partial success - some tags may have failed")
                print(f"   Message: {result.get('message')}")
                print(f"   Billing message: {result.get('billingMessage')}")
        elif response.status_code == 404:
            print(f"\n❌ Table not found!")
            error_data = response.json()
            print(f"   Error: {error_data.get('detail')}")
            print(f"\n   This means the catalog/schema/table lookup failed.")
            print(f"   Check the backend logs for more details.")
        else:
            print(f"\n❌ Failed: HTTP {response.status_code}")
            try:
                error_data = response.json()
                print(f"   Error: {error_data.get('detail', response.text)}")
            except:
                print(f"   Error: {response.text}")
            
    except requests.exceptions.Timeout:
        print(f"\n❌ Request timed out after 120 seconds!")
        print(f"   This suggests the lookup is taking too long.")
        print(f"   Possible causes:")
        print(f"   1. Too many catalogs/schemas/tables to search")
        print(f"   2. Starburst API is slow")
        print(f"   3. Network issues")
    except Exception as e:
        print(f"\n❌ Test failed: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_starburst_with_real_table()
