# Enhanced TCG Downloader - Rate Limiting Guide

## Overview
The TCG downloader has been enhanced with intelligent rate limiting controls to handle HTTP 403 Forbidden errors from TCGcsv.com. These enhancements allow the downloader to automatically detect, handle, and recover from rate limiting while maintaining resumable downloads.

## Key Features

### 🛡️ Rate Limiting Protection
- **Request Delays**: Configurable delays between API requests (default: 2-4 seconds)
- **Reduced Concurrency**: Lower concurrent workers (default: 3 instead of 10)
- **Automatic Detection**: Detects 403 Forbidden errors and adjusts behavior
- **Exponential Backoff**: Progressive delays for repeated rate limiting

### 🔄 Intelligent Recovery
- **Automatic Cooldown**: 5-30 minute cooldown periods when rate limited
- **Dynamic Adjustment**: Request delays automatically increase/decrease
- **Smart Retries**: Different retry strategies for rate limits vs network errors
- **State Reset**: Automatically resets rate limiting state on successful requests

### 📊 Enhanced Resume Support
- **Failed Group Tracking**: Tracks and retries specific failed groups
- **Failed Category Recovery**: Removes categories from failed list when they succeed
- **Checkpoint Granularity**: More detailed progress tracking
- **Retry on Startup**: Automatically retries failed groups when restarting

## Usage

### Basic Usage (Recommended)
```python
from resumable_downloader import ResumableDownloader

# Use enhanced defaults (conservative settings)
downloader = ResumableDownloader()
success = downloader.run_full_download()
```

### Custom Configuration
```python
# More aggressive settings (higher risk of rate limiting)
downloader = ResumableDownloader(
    max_workers=5,           # More concurrent requests
    request_delay=1.0,       # Shorter delays
    rate_limit_delay=180     # 3-minute cooldown
)

# Very conservative settings (safest)
downloader = ResumableDownloader(
    max_workers=1,           # Sequential requests only  
    request_delay=5.0,       # 5-second delays
    rate_limit_delay=900     # 15-minute cooldown
)
```

### Command Line Usage
```bash
# Use the enhanced downloader directly
python resumable_downloader.py

# Or run in screen for background execution
./run_in_screen.sh start
```

## Current Status

Based on your checkpoint data:
- **Categories Completed**: 66 out of 89 (74%)
- **Products Downloaded**: 357,614
- **Failed Categories**: 1 (My Little Pony - ID: 21)  
- **Failed Groups**: 110 groups from Marvel Comics and My Little Pony

## Recovery Strategy

### Phase 1: Immediate Recovery (Automatic)
When you restart the enhanced downloader, it will:
1. Load the existing checkpoint
2. Automatically retry the 110 failed groups with rate limiting protection
3. Continue with remaining categories (23 left)

### Phase 2: Manual Recovery (If Needed)
If rate limiting persists, you can:
```python
# Run only failed group recovery
downloader = ResumableDownloader(max_workers=1, request_delay=10.0)
downloader.retry_failed_groups()
```

### Phase 3: Extended Strategy
For persistent rate limiting:
1. Use even more conservative settings
2. Run during off-peak hours
3. Consider daily quotas/limits

## Monitoring

### Log Messages to Watch For
- `Rate limiting detected` - 403 errors caught
- `Setting X-minute cooldown period` - Automatic cooldown activated  
- `Request delay increased to X seconds` - Dynamic adjustment working
- `Successful request - resetting rate limit counters` - Recovery in progress

### Progress Tracking
```bash
# Check current progress
tail -f tcg_download.log

# Monitor in screen session
./run_in_screen.sh logs
```

## Best Practices

### ✅ Do
- Use the enhanced defaults for most situations
- Monitor logs for rate limiting messages
- Allow the system to automatically handle 403 errors
- Run during off-peak hours if possible

### ❌ Don't  
- Increase `max_workers` above 5
- Set `request_delay` below 1.0 seconds
- Interrupt the process during cooldown periods
- Manually retry immediately after 403 errors

## Troubleshooting

### Still Getting Rate Limited?
1. **Increase delays**: Set `request_delay=10.0` or higher
2. **Reduce workers**: Set `max_workers=1`
3. **Wait longer**: Increase `rate_limit_delay` to 1800 (30 minutes)

### Slow Progress?
1. **Check logs**: Look for successful requests vs rate limiting
2. **Gradual increase**: Start conservative, then gradually increase speed
3. **Time of day**: Try different hours to find less restricted periods

### Stuck on Failed Groups?
1. **Manual retry**: Use the `retry_failed_groups()` method
2. **Category restart**: Delete specific categories from checkpoint to retry
3. **Skip problematic**: Manually mark problematic groups as completed

## Configuration Reference

| Parameter | Default | Conservative | Aggressive | Description |
|-----------|---------|-------------|------------|-------------|
| `max_workers` | 3 | 1 | 5 | Concurrent API requests |
| `request_delay` | 2.0s | 5.0s | 1.0s | Delay between requests |
| `rate_limit_delay` | 300s | 900s | 180s | Cooldown when rate limited |

## Next Steps

The enhanced downloader is ready to resume from your current checkpoint with smart rate limiting. Simply run:

```bash
python resumable_downloader.py
```

The system will automatically:
1. Retry the 110 failed groups with protective delays
2. Continue downloading the remaining 23 categories  
3. Handle any new rate limiting that occurs
4. Maintain your existing 357K product checkpoint

Expected completion time with conservative settings: 2-4 hours (depending on rate limiting)