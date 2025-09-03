# Cookie Extraction Experience with MCP Browser Automation

## 🍪 Overview

This document details the complete experience and methodology for extracting authentication cookies from eBay using MCP (Model Context Protocol) browser automation tools. These cookies are essential for authenticated API access to eBay's research endpoints.

## 🎯 The Challenge

eBay's `/sh/research/api/search` endpoint requires valid authentication cookies that:
- Are tied to a logged-in session
- Include multiple security tokens (ebay-app-ctx, dp1, nonsession, etc.)
- Expire periodically and need refreshing
- Must match the user's browser fingerprint

## 🔧 MCP Browser Tools Available

### Core Browser Control
```python
# Navigation
mcp__playwright__browser_navigate(url="https://www.ebay.com")
mcp__playwright__browser_navigate_back()

# Page Interaction
mcp__playwright__browser_snapshot()  # Capture page state
mcp__playwright__browser_click(element="Sign in", ref="button#signin")
mcp__playwright__browser_type(element="Email", ref="input#userid", text="user@email.com")

# Form Handling
mcp__playwright__browser_fill_form(fields=[
    {"name": "username", "type": "textbox", "ref": "input#userid", "value": "user@email.com"},
    {"name": "password", "type": "textbox", "ref": "input#pass", "value": "password"}
])

# JavaScript Execution
mcp__playwright__browser_evaluate(function="() => document.cookie")
```

## 🚀 Cookie Extraction Methods

### Method 1: Direct JavaScript Extraction (Simplest)
```python
# After logging in to eBay
def extract_cookies_direct():
    # Navigate to eBay research page
    mcp__playwright__browser_navigate(url="https://www.ebay.com/sh/research")
    
    # Wait for page load
    mcp__playwright__browser_wait_for(time=3)
    
    # Extract all cookies via JavaScript
    cookies = mcp__playwright__browser_evaluate(
        function="() => document.cookie"
    )
    
    return cookies
```

**Pros:**
- Simple and direct
- Works for httpOnly=false cookies
- Fast execution

**Cons:**
- Misses httpOnly cookies
- Doesn't capture all security tokens

### Method 2: Network Request Interception (Most Reliable)
```python
def extract_cookies_from_network():
    # Navigate to research page
    mcp__playwright__browser_navigate(url="https://www.ebay.com/sh/research")
    
    # Perform a search to trigger API call
    mcp__playwright__browser_type(
        element="Search box",
        ref="input[placeholder='Search']",
        text="pokemon cards",
        submit=True
    )
    
    # Wait for API request
    mcp__playwright__browser_wait_for(text="results", time=5)
    
    # Get network requests
    requests = mcp__playwright__browser_network_requests()
    
    # Find the research API request
    for request in requests:
        if "/sh/research/api/search" in request["url"]:
            return request["headers"]["cookie"]
    
    return None
```

**Pros:**
- Captures ALL cookies including httpOnly
- Gets exact headers used for API
- Most reliable method

**Cons:**
- Requires triggering actual search
- More complex implementation

### Method 3: Browser Storage + Cookie Combination
```python
def extract_complete_auth():
    # Get regular cookies
    cookies = mcp__playwright__browser_evaluate(
        function="() => document.cookie"
    )
    
    # Get localStorage tokens
    local_storage = mcp__playwright__browser_evaluate(
        function="() => JSON.stringify(localStorage)"
    )
    
    # Get sessionStorage tokens
    session_storage = mcp__playwright__browser_evaluate(
        function="() => JSON.stringify(sessionStorage)"
    )
    
    # Combine all authentication data
    return {
        "cookies": cookies,
        "localStorage": local_storage,
        "sessionStorage": session_storage
    }
```

## 📝 Complete Automation Script

### Full Cookie Extraction Workflow
```python
def automated_cookie_extraction(username, password):
    """
    Complete automated workflow for eBay cookie extraction
    """
    
    # Step 1: Navigate to eBay
    print("Navigating to eBay...")
    mcp__playwright__browser_navigate(url="https://www.ebay.com")
    mcp__playwright__browser_wait_for(time=2)
    
    # Step 2: Click Sign In
    print("Clicking Sign In...")
    snapshot = mcp__playwright__browser_snapshot()
    # Find sign in button in snapshot
    mcp__playwright__browser_click(
        element="Sign in button",
        ref="a[href*='signin']"
    )
    
    # Step 3: Fill login form
    print("Filling login credentials...")
    mcp__playwright__browser_fill_form(fields=[
        {
            "name": "Email or username",
            "type": "textbox",
            "ref": "input#userid",
            "value": username
        }
    ])
    
    # Click continue
    mcp__playwright__browser_click(
        element="Continue button",
        ref="button#signin-continue-btn"
    )
    mcp__playwright__browser_wait_for(time=2)
    
    # Enter password
    mcp__playwright__browser_type(
        element="Password field",
        ref="input#pass",
        text=password,
        submit=True
    )
    
    # Step 4: Wait for login completion
    print("Waiting for login...")
    mcp__playwright__browser_wait_for(text="My eBay", time=10)
    
    # Step 5: Navigate to research page
    print("Navigating to research page...")
    mcp__playwright__browser_navigate(url="https://www.ebay.com/sh/research")
    mcp__playwright__browser_wait_for(time=3)
    
    # Step 6: Trigger a search to capture cookies
    print("Performing test search...")
    mcp__playwright__browser_type(
        element="Search input",
        ref="input[type='search']",
        text="test search",
        submit=True
    )
    
    # Step 7: Extract cookies from network
    print("Extracting cookies...")
    requests = mcp__playwright__browser_network_requests()
    
    for request in requests:
        if "/sh/research/api/search" in request["url"]:
            cookies = request["headers"].get("cookie", "")
            
            # Save cookies to file
            with open("ebay_cookies.txt", "w") as f:
                f.write(cookies)
            
            print(f"✅ Cookies extracted successfully!")
            print(f"Cookie length: {len(cookies)} characters")
            return cookies
    
    # Fallback: Get cookies via JavaScript
    print("Fallback: Getting cookies via JavaScript...")
    cookies = mcp__playwright__browser_evaluate(
        function="() => document.cookie"
    )
    
    with open("ebay_cookies.txt", "w") as f:
        f.write(cookies)
    
    return cookies
```

## 🔍 Cookie Validation

### Verify Cookie Validity
```python
def validate_cookies(cookie_string):
    """
    Validate that extracted cookies work for API access
    """
    import requests
    
    headers = {
        "cookie": cookie_string,
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "accept": "application/json",
    }
    
    # Test API endpoint
    test_url = "https://www.ebay.com/sh/research/api/search"
    params = {
        "marketplace": "EBAY-US",
        "keywords": "test",
        "dayRange": "365",
        "offset": "0",
        "limit": "1"
    }
    
    response = requests.get(test_url, headers=headers, params=params)
    
    if response.status_code == 200:
        print("✅ Cookies are valid!")
        data = response.json()
        if "modules" in data:
            print(f"Found {len(data['modules'])} data modules")
        return True
    else:
        print(f"❌ Cookie validation failed: {response.status_code}")
        return False
```

## 🛡️ Security Considerations

### Important Cookie Components
```
Essential eBay Cookies:
- ebay=%5E               # Session identifier
- dp1=                   # User preferences
- nonsession=            # Security token
- s=                     # Session tracking
- cssg=                  # CSRF token
- cid=                   # Client ID

Research-Specific:
- ebay-app-ctx=          # Application context
- pref=                  # Research preferences
```

### Cookie Refresh Strategy
```python
def should_refresh_cookies(cookie_file="ebay_cookies.txt"):
    """
    Determine if cookies need refreshing
    """
    import os
    import time
    
    # Check file age
    if os.path.exists(cookie_file):
        file_age = time.time() - os.path.getmtime(cookie_file)
        
        # Refresh if older than 6 hours
        if file_age > 6 * 3600:
            print("⚠️ Cookies are older than 6 hours")
            return True
        
        # Validate cookies work
        with open(cookie_file, "r") as f:
            cookies = f.read()
        
        if not validate_cookies(cookies):
            print("⚠️ Cookies are invalid")
            return True
            
        return False
    
    print("⚠️ Cookie file doesn't exist")
    return True
```

## 🐛 Common Issues & Solutions

### Issue 1: Login Captcha
```python
# Solution: Handle captcha detection
snapshot = mcp__playwright__browser_snapshot()
if "captcha" in str(snapshot).lower():
    print("⚠️ Captcha detected! Manual intervention required.")
    print("Please complete the captcha manually...")
    mcp__playwright__browser_wait_for(time=30)  # Wait for manual completion
```

### Issue 2: Two-Factor Authentication
```python
# Solution: Wait for 2FA completion
mcp__playwright__browser_wait_for(text="Enter code", time=5)
if "verification" in mcp__playwright__browser_snapshot().lower():
    print("📱 2FA required. Please enter code manually...")
    mcp__playwright__browser_wait_for(text="My eBay", time=60)
```

### Issue 3: Cookie Format Issues
```python
def clean_cookie_string(raw_cookies):
    """
    Clean and format cookie string
    """
    # Remove newlines and extra spaces
    cookies = raw_cookies.strip().replace('\n', '').replace('\r', '')
    
    # Ensure proper semicolon separation
    cookies = '; '.join([c.strip() for c in cookies.split(';')])
    
    # Remove duplicate cookies
    cookie_dict = {}
    for cookie in cookies.split('; '):
        if '=' in cookie:
            key, value = cookie.split('=', 1)
            cookie_dict[key] = value
    
    return '; '.join([f"{k}={v}" for k, v in cookie_dict.items()])
```

## 📊 Advanced Techniques

### Persistent Browser Session
```python
def setup_persistent_session():
    """
    Maintain browser session across multiple extractions
    """
    
    # Check if browser is already open
    tabs = mcp__playwright__browser_tabs(action="list")
    
    if not tabs:
        # Open new browser
        mcp__playwright__browser_navigate(url="https://www.ebay.com")
    
    # Keep session alive
    return {
        "session_id": tabs[0]["id"] if tabs else "new",
        "start_time": time.time()
    }
```

### Multi-Account Cookie Management
```python
def manage_multiple_accounts(accounts):
    """
    Extract and manage cookies for multiple eBay accounts
    """
    cookies_db = {}
    
    for account in accounts:
        print(f"Processing account: {account['username']}")
        
        # Clear existing session
        mcp__playwright__browser_evaluate(
            function="() => { document.cookie.split(';').forEach(c => document.cookie = c.replace(/^ +/, '').replace(/=.*/, '=;expires=' + new Date().toUTCString() + ';path=/')) }"
        )
        
        # Extract cookies for this account
        cookies = automated_cookie_extraction(
            account["username"],
            account["password"]
        )
        
        # Store with timestamp
        cookies_db[account["username"]] = {
            "cookies": cookies,
            "timestamp": time.time(),
            "valid": validate_cookies(cookies)
        }
        
        # Save to separate files
        with open(f"cookies_{account['username']}.txt", "w") as f:
            f.write(cookies)
    
    return cookies_db
```

## 🔄 Automated Refresh Pipeline

### Scheduled Cookie Refresh
```python
def automated_refresh_pipeline():
    """
    Automatically refresh cookies when needed
    """
    import schedule
    import time
    
    def refresh_job():
        print(f"[{datetime.now()}] Checking cookie validity...")
        
        if should_refresh_cookies():
            print("Refreshing cookies...")
            
            # Read credentials from secure storage
            username = os.environ.get("EBAY_USERNAME")
            password = os.environ.get("EBAY_PASSWORD")
            
            if username and password:
                cookies = automated_cookie_extraction(username, password)
                print(f"✅ Cookies refreshed: {len(cookies)} chars")
            else:
                print("❌ Credentials not found in environment")
    
    # Schedule refresh every 4 hours
    schedule.every(4).hours.do(refresh_job)
    
    # Initial check
    refresh_job()
    
    # Keep running
    while True:
        schedule.run_pending()
        time.sleep(60)
```

## 📋 Quick Reference Commands

### Extract Cookies Now
```bash
# Using MCP browser automation
python -c "
from mcp_browser import extract_cookies
cookies = extract_cookies()
print(f'Extracted {len(cookies)} characters')
"
```

### Validate Current Cookies
```bash
# Check if cookies are still valid
curl -H "Cookie: $(cat ebay_cookies.txt)" \
  "https://www.ebay.com/sh/research/api/search?keywords=test&limit=1" \
  | jq '.modules[0].name'
```

### Monitor Cookie Age
```bash
# Show cookie file age
stat -c "%y" ebay_cookies.txt 2>/dev/null || echo "No cookie file found"
```

## 🎯 Best Practices

1. **Always validate after extraction** - Ensure cookies work before using
2. **Store securely** - Never commit cookies to version control
3. **Refresh proactively** - Don't wait for failures
4. **Use network interception** - Most reliable extraction method
5. **Handle edge cases** - Captchas, 2FA, session timeouts
6. **Log everything** - Track extraction attempts and failures
7. **Implement retry logic** - Automatic retries with backoff

## 🔮 Future Improvements

1. **Headless Detection Bypass**
   - Implement stealth mode plugins
   - Randomize browser fingerprints

2. **Cookie Pool Management**
   - Rotate between multiple accounts
   - Load balancing for rate limits

3. **API Integration**
   - Direct cookie injection into requests
   - Session persistence across runs

4. **Monitoring Dashboard**
   - Real-time cookie validity status
   - Automatic alerts on failures

## 📝 Conclusion

Cookie extraction using MCP browser automation provides a robust solution for maintaining authenticated access to eBay's API endpoints. The combination of browser automation, network interception, and intelligent validation ensures reliable cookie management for your scraping operations.

Key takeaways:
- Network request interception is the most reliable method
- Always validate cookies after extraction
- Implement automatic refresh before expiration
- Handle authentication edge cases gracefully
- Keep cookies secure and never expose in logs

With this approach, you can maintain stable, long-running scraping operations with minimal manual intervention.