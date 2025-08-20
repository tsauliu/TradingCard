#!/usr/bin/env python3
"""
Restart the metadata downloader with direct connection (no proxy switching)
"""
import subprocess
import requests
import json
import time
import os

def set_proxy_to_direct():
    """Set Mihomo proxy to DIRECT connection"""
    try:
        response = requests.put(
            "http://127.0.0.1:9090/proxies/manual-select",
            json={"name": "DIRECT"},
            timeout=10
        )
        if response.status_code == 204:
            print("✅ Successfully set proxy to DIRECT")
            return True
        else:
            print(f"⚠️ Proxy switch returned status {response.status_code}")
            return False
    except Exception as e:
        print(f"❌ Failed to set proxy: {e}")
        return False

def verify_proxy_setting():
    """Verify current proxy setting"""
    try:
        response = requests.get("http://127.0.0.1:9090/proxies/manual-select", timeout=10)
        if response.status_code == 200:
            data = response.json()
            current = data.get("now")
            print(f"Current proxy: {current}")
            return current == "DIRECT"
    except Exception as e:
        print(f"❌ Failed to verify proxy: {e}")
        return False

def start_downloader_in_screen():
    """Start the downloader in a new screen session"""
    try:
        # Kill any existing screen session
        try:
            subprocess.run(["screen", "-S", "tcg_direct", "-X", "quit"], 
                         capture_output=True, timeout=5)
            time.sleep(1)
        except:
            pass  # Ignore if no existing session
        
        # Start new screen session with the downloader
        cmd = [
            "screen", "-dmS", "tcg_direct", 
            "bash", "-c",
            "python3 run_full_categories_exclude_pokemon.py 2>&1 | tee -a tcg_direct_download.log"
        ]
        
        result = subprocess.run(cmd, capture_output=True, timeout=10)
        
        if result.returncode == 0:
            print("✅ Started downloader in screen session 'tcg_direct'")
            return True
        else:
            print(f"❌ Failed to start screen session: {result.stderr.decode()}")
            return False
            
    except Exception as e:
        print(f"❌ Error starting downloader: {e}")
        return False

def main():
    print("=== TCG Metadata Downloader Restart (Direct Connection) ===")
    
    # Step 1: Set proxy to DIRECT
    print("\n1. Setting proxy to DIRECT connection...")
    if not set_proxy_to_direct():
        print("Failed to set proxy, continuing anyway...")
    
    # Step 2: Verify setting
    print("\n2. Verifying proxy setting...")
    verify_proxy_setting()
    
    # Step 3: Start downloader
    print("\n3. Starting downloader in screen session...")
    if start_downloader_in_screen():
        print("\n✅ Downloader restarted successfully!")
        print("\nMonitoring commands:")
        print("  screen -r tcg_direct          # Attach to session")
        print("  tail -f tcg_direct_download.log  # View logs")
        print("  screen -list                  # List sessions")
    else:
        print("\n❌ Failed to restart downloader")

if __name__ == "__main__":
    main()