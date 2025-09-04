# eBay Product Research Login & Cookie Extraction Process

## Overview
This document outlines the complete process for logging into eBay Product Research and extracting authentication cookies for API access using MCP Playwright.

## Prerequisites
- MCP Playwright browser automation
- eBay account credentials in `ebay_account.json`
- Active internet connection (no proxy required)

## Account Configuration
File: `ebay_account.json`
```json
{
    "endpoint_login": "https://www.ebay.com/sh/research?marketplace=EBAY-US&tabName=SOLD",
    "account": "tsau.liu@gmail.com", 
    "passwd": "eBayBDA123"
}
```

## Step-by-Step Login Process

### 1. Navigate to Main eBay Page First
```
URL: https://www.ebay.com
```
**Important**: Do NOT navigate directly to the research page as this triggers hCaptcha challenges.

### 2. Click Sign In
- Locate the "Sign in" link in the header
- Click to access login form

### 3. Enter Credentials
- Enter email: `tsau.liu@gmail.com`
- Enter password: `eBayBDA123`
- Click "Sign in" button

### 4. Navigate to Product Research
```
URL: https://www.ebay.com/sh/research?marketplace=EBAY-US&tabName=SOLD
```
- After successful login, navigate to the research page
- This ensures cookies are set in the correct research context

### 5. Perform Test Search
- Search for any keyword (e.g., "pokemon card") to activate the research session
- Wait for results to load completely
- This step is crucial for proper API authentication

### 6. Extract Cookies
Use JavaScript evaluation in browser:
```javascript
document.cookie
```

### 7. Save Cookies to File
Save the complete cookie string to `ebay_cookies.txt`:

Example format:
```
__uzma=f955be59-58c8-44b3-8a96-03a5d89e5a04; __uzmb=1756873984; dp1=bu1p/dHNfODQ5MDk16c7a2cdb^kms/in6c7a2cdb^pbf/%23200000000000000000080800000046a98f95b^u1f/Liu6c7a2cdb^tzo/1a468b7d3b8^bl/CN6c7a2cdb^; ebay=%5Ejs%3D1%5EsfLMD%3D0%5Esin%3Din%5Esbf%3D%2300000004%5E; ds1=ats/1756874186793; ds2=amsg/0dd988641990a2562cab19beffd4573f^; totp=1756874188315.u6XCco0pgB1o/pmyeiCBZutl7TBlwyfgIedF1HV8O02nRil3ZwLKB6FKcbkBsmzTcbmFgROLGL5L9ajAm8IK5Q==.VRcjRIH_McYop1B6r1K5IULfViKV64ur0I2wn0PG0zo
```

## Key Success Factors

### 1. Avoid hCaptcha
- Never navigate directly to research page
- Always login through main eBay page first
- Complete the full authentication flow

### 2. Research Context Cookies  
- Cookies must be extracted from the research page context
- General eBay cookies may not work for research API
- Perform a test search to ensure proper session establishment

### 3. No Proxy Required
- Direct connection works better than proxy
- Proxy connections may cause authentication issues
- Use `--no-proxy` flag if needed in Python scripts

## Troubleshooting

### hCaptcha Challenges
- **Problem**: Direct navigation to research page triggers captcha
- **Solution**: Navigate to main eBay page first, then sign in normally

### Authentication Errors in Python Script
- **Problem**: "AUTHENTICATION REQUIRED - Cookies have expired!"
- **Cause**: Cookies not extracted from research page context
- **Solution**: Ensure test search is performed before cookie extraction

### Proxy Connection Issues
- **Problem**: ProxyError connection refused
- **Solution**: Disable proxy usage for eBay connections

## Cookie Validation
To test if cookies are working:
```bash
python ebay_search.py "pokemon card" --verbose --excel --days 180 --no-proxy
```

## MCP Playwright Commands Used

1. **Navigate**: `mcp__playwright__browser_navigate`
2. **Snapshot**: `mcp__playwright__browser_snapshot` 
3. **Click**: `mcp__playwright__browser_click`
4. **Type**: `mcp__playwright__browser_type`
5. **Evaluate**: `mcp__playwright__browser_evaluate`

## Session Management
- Cookies typically expire after 24-48 hours
- Re-run this process when authentication errors occur
- Always backup working cookies before refreshing

## Notes
- UI may display in Chinese but functionality remains the same
- Research page URL includes `marketplace=EBAY-US&tabName=SOLD`
- Cookie string contains multiple authentication tokens including `totp`, `dp1`, `ebay`, `ds1`, `ds2`