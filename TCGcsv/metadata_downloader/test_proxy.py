#!/usr/bin/env python3
"""
Test proxy configuration with TCG API
"""
import requests
import json
from enhanced_api_downloader import EnhancedTCGMetadataDownloader

def test_direct_connection():
    """Test direct connection to TCG API"""
    print("Testing direct connection...")
    try:
        response = requests.get("https://tcgcsv.com/tcgplayer/categories", timeout=10)
        print(f"Direct connection: {response.status_code}")
        return response.status_code == 200
    except Exception as e:
        print(f"Direct connection failed: {e}")
        return False

def test_proxy_connection():
    """Test connection through V2rayA proxy"""
    print("Testing proxy connection...")
    try:
        proxies = {
            'http': 'http://127.0.0.1:20172',
            'https': 'http://127.0.0.1:20172'
        }
        response = requests.get("https://tcgcsv.com/tcgplayer/categories", 
                              proxies=proxies, timeout=10)
        print(f"Proxy connection: {response.status_code}")
        return response.status_code == 200
    except Exception as e:
        print(f"Proxy connection failed: {e}")
        return False

def test_enhanced_downloader():
    """Test enhanced downloader with proxy"""
    print("Testing enhanced downloader with proxy...")
    try:
        downloader = EnhancedTCGMetadataDownloader(
            min_request_interval=0.5,
            batch_size=10
        )
        
        # Test downloading categories
        categories = downloader.download_categories()
        print(f"Enhanced downloader: Retrieved {len(categories)} categories")
        return len(categories) > 0
    except Exception as e:
        print(f"Enhanced downloader failed: {e}")
        return False

def main():
    print("=" * 50)
    print("TCG API PROXY CONNECTION TEST")
    print("=" * 50)
    
    # Test direct connection
    direct_ok = test_direct_connection()
    
    # Test proxy connection
    proxy_ok = test_proxy_connection()
    
    # Test enhanced downloader
    enhanced_ok = test_enhanced_downloader()
    
    print("\n" + "=" * 50)
    print("RESULTS:")
    print(f"Direct connection:     {'✅ OK' if direct_ok else '❌ FAILED'}")
    print(f"Proxy connection:      {'✅ OK' if proxy_ok else '❌ FAILED'}")
    print(f"Enhanced downloader:   {'✅ OK' if enhanced_ok else '❌ FAILED'}")
    print("=" * 50)
    
    if proxy_ok and enhanced_ok:
        print("🎉 Proxy configuration successful! Ready to bypass 403 errors.")
    elif direct_ok:
        print("⚠️  Direct connection works, proxy may not be needed.")
    else:
        print("❌ All connections failed. Check V2rayA configuration.")

if __name__ == "__main__":
    main()