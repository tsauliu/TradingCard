#!/usr/bin/env python3
"""
eBay Search Resume Manager
Handles session persistence, recovery, and intelligent retry logic
"""

import json
import os
import time
import signal
import shutil
import hashlib
from pathlib import Path

# Platform-specific imports
try:
    import fcntl
    HAS_FCNTL = True
except ImportError:
    # Windows doesn't have fcntl
    HAS_FCNTL = False
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Tuple
import logging
from dataclasses import dataclass, asdict
from enum import Enum

logger = logging.getLogger(__name__)


class SearchStatus(Enum):
    """Search status enumeration"""
    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    SUCCESS = "success"
    FAILED = "failed"
    SKIPPED = "skipped"
    RETRY = "retry"


@dataclass
class SearchRecord:
    """Individual search record"""
    index: int
    keyword: str
    status: str
    timestamp: str
    file: Optional[str] = None
    error: Optional[str] = None
    retries: int = 0
    response_size: Optional[int] = None
    duration: Optional[float] = None
    
    def to_dict(self):
        return asdict(self)


class RateLimiter:
    """Intelligent rate limiting with adaptive delays"""
    
    def __init__(self, min_delay: float = 10.0):
        self.min_delay = max(10.0, min_delay)  # Enforce 10s minimum
        self.last_request_time = 0
        self.consecutive_errors = 0
        self.success_streak = 0
        self.adaptive_delay = self.min_delay
        self.request_history = []  # Track recent request times
        self.max_history = 10
        
    def wait_if_needed(self):
        """Apply rate limiting with adaptive delay"""
        current_time = time.time()
        time_since_last = current_time - self.last_request_time
        
        # Calculate adaptive delay based on recent performance
        delay = self._calculate_adaptive_delay()
        
        if time_since_last < delay:
            wait_time = delay - time_since_last
            logger.info(f"Rate limiting: waiting {wait_time:.1f}s (adaptive delay: {delay:.1f}s)")
            
            # Show countdown for long waits
            if wait_time > 5:
                self._show_countdown(wait_time)
            else:
                time.sleep(wait_time)
        
        self.last_request_time = time.time()
        self.request_history.append(self.last_request_time)
        
        # Keep history size limited
        if len(self.request_history) > self.max_history:
            self.request_history.pop(0)
    
    def _calculate_adaptive_delay(self) -> float:
        """Calculate delay based on recent success/failure patterns"""
        base_delay = self.min_delay
        
        # Increase delay after errors
        if self.consecutive_errors > 0:
            # Exponential backoff: 10s, 15s, 22.5s, 33.75s...
            error_multiplier = 1.5 ** min(self.consecutive_errors, 5)
            base_delay = self.min_delay * error_multiplier
            
        # Slightly decrease delay after consistent successes (but never below minimum)
        elif self.success_streak > 10:
            # Can reduce to 90% of current delay, but never below minimum
            base_delay = max(self.min_delay, self.adaptive_delay * 0.9)
        
        # Check request rate over last minute
        if len(self.request_history) >= 5:
            recent_rate = self._get_recent_request_rate()
            if recent_rate > 6:  # More than 6 requests per minute
                logger.warning(f"High request rate detected: {recent_rate:.1f} req/min")
                base_delay = base_delay * 1.5
        
        self.adaptive_delay = min(base_delay, 60.0)  # Cap at 60 seconds
        return self.adaptive_delay
    
    def _get_recent_request_rate(self) -> float:
        """Calculate requests per minute for recent history"""
        if len(self.request_history) < 2:
            return 0
        
        time_span = self.request_history[-1] - self.request_history[0]
        if time_span > 0:
            return (len(self.request_history) - 1) / (time_span / 60)
        return 0
    
    def _show_countdown(self, wait_time: float):
        """Show countdown for long waits"""
        import sys
        wait_int = int(wait_time)
        for i in range(wait_int, 0, -1):
            print(f"\rWaiting: {i}s ", end='', flush=True)
            time.sleep(1)
        print("\r" + " " * 20 + "\r", end='', flush=True)
        
        # Sleep for remaining fractional seconds
        remaining = wait_time - wait_int
        if remaining > 0:
            time.sleep(remaining)
    
    def record_success(self):
        """Record successful request"""
        self.consecutive_errors = 0
        self.success_streak += 1
        
    def record_error(self):
        """Record failed request"""
        self.consecutive_errors += 1
        self.success_streak = 0


class SessionLock:
    """Prevent concurrent access to same session"""
    
    def __init__(self, lock_file: Path):
        self.lock_file = lock_file
        self.lock_fd = None
        
    def acquire(self, timeout: int = 5) -> bool:
        """Try to acquire session lock"""
        self.lock_file.parent.mkdir(parents=True, exist_ok=True)
        
        if not HAS_FCNTL:
            # Simple file-based locking for Windows
            try:
                if self.lock_file.exists():
                    # Check if lock is stale (older than 5 minutes)
                    lock_age = time.time() - self.lock_file.stat().st_mtime
                    if lock_age < 300:  # 5 minutes
                        return False
                    self.lock_file.unlink()  # Remove stale lock
                
                # Create lock file
                with open(self.lock_file, 'w') as f:
                    f.write(json.dumps({
                        'pid': os.getpid(),
                        'timestamp': datetime.now().isoformat()
                    }))
                return True
            except Exception as e:
                logger.error(f"Lock acquisition error: {e}")
                return False
        
        try:
            self.lock_fd = open(self.lock_file, 'w')
            
            # Try to acquire lock with timeout
            start_time = time.time()
            while time.time() - start_time < timeout:
                try:
                    fcntl.flock(self.lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    
                    # Write PID and timestamp
                    import platform
                    self.lock_fd.write(json.dumps({
                        'pid': os.getpid(),
                        'timestamp': datetime.now().isoformat(),
                        'host': platform.node()
                    }))
                    self.lock_fd.flush()
                    return True
                    
                except IOError:
                    time.sleep(0.1)
            
            logger.error(f"Could not acquire lock on {self.lock_file}")
            return False
            
        except Exception as e:
            logger.error(f"Lock acquisition error: {e}")
            return False
    
    def release(self):
        """Release session lock"""
        if not HAS_FCNTL:
            # Simple file deletion for Windows
            try:
                self.lock_file.unlink(missing_ok=True)
            except:
                pass
            return
            
        if self.lock_fd:
            try:
                fcntl.flock(self.lock_fd, fcntl.LOCK_UN)
                self.lock_fd.close()
                self.lock_file.unlink(missing_ok=True)
            except:
                pass


class ResumeManager:
    """Enhanced session persistence and resume capability"""
    
    def __init__(self, 
                 session_id: Optional[str] = None,
                 temp_dir: str = ".ebay_temp",
                 auto_save: bool = True,
                 enable_lock: bool = True):
        
        self.temp_dir = Path(temp_dir)
        self.session_id = self._resolve_session_id(session_id)
        self.session_path = self.temp_dir / f"session_{self.session_id}"
        
        # Directory structure
        self.state_file = self.session_path / "state.json"
        self.results_dir = self.session_path / "results"
        self.logs_dir = self.session_path / "logs"
        self.checkpoints_dir = self.session_path / "checkpoints"
        self.exports_dir = self.session_path / "exports"
        
        # Add permanent raw JSON storage (never deleted)
        self.permanent_raw_dir = Path("permanent_raw_json") / self.session_id
        self.permanent_raw_dir.mkdir(parents=True, exist_ok=True)
        
        # Session lock
        self.lock = None
        if enable_lock:
            self.lock = SessionLock(self.session_path / ".lock")
            if not self.lock.acquire():
                raise RuntimeError(f"Session {self.session_id} is locked by another process")
        
        # Setup directories
        self._setup_directories()
        
        # Load or initialize state
        self.state = self._load_state()
        
        # Rate limiter
        self.rate_limiter = RateLimiter(
            min_delay=self.state.get('rate_limit', {}).get('min_delay', 10.0)
        )
        
        # Setup signal handlers
        if auto_save:
            signal.signal(signal.SIGINT, self._emergency_save)
            signal.signal(signal.SIGTERM, self._emergency_save)
            signal.signal(signal.SIGHUP, self._emergency_save)
        
        # Log session info
        logger.info(f"Session initialized: {self.session_id}")
        logger.info(f"Session path: {self.session_path}")
    
    def _resolve_session_id(self, session_id: Optional[str]) -> str:
        """Resolve session ID (handle 'last' keyword)"""
        if session_id == "last":
            # Find most recent session
            sessions = sorted([
                d for d in self.temp_dir.glob("session_*")
                if d.is_dir()
            ], key=lambda x: x.stat().st_mtime, reverse=True)
            
            if sessions:
                return sessions[0].name.replace("session_", "")
            else:
                logger.warning("No previous sessions found, creating new")
                return self._generate_session_id()
        
        elif session_id:
            return session_id
        else:
            return self._generate_session_id()
    
    def _generate_session_id(self) -> str:
        """Generate unique session ID with hostname"""
        import platform
        hostname = platform.node()[:8].replace(' ', '_')
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        return f"{timestamp}_{hostname}"
    
    def _setup_directories(self):
        """Create directory structure"""
        for dir_path in [self.results_dir, self.logs_dir, 
                        self.checkpoints_dir, self.exports_dir]:
            dir_path.mkdir(parents=True, exist_ok=True)
    
    def _load_state(self) -> Dict:
        """Load existing state or create new"""
        if self.state_file.exists():
            try:
                with open(self.state_file, 'r') as f:
                    state = json.load(f)
                    logger.info(f"Loaded existing session: {state['session_id']}")
                    logger.info(f"  Completed: {state['completed']}")
                    logger.info(f"  Failed: {state['failed']}")
                    logger.info(f"  Total: {state['total_keywords']}")
                    return state
            except json.JSONDecodeError as e:
                logger.error(f"Corrupted state file: {e}")
                # Try to load from checkpoint
                return self._recover_from_checkpoint()
        
        return {
            'session_id': self.session_id,
            'start_time': datetime.now().isoformat(),
            'last_update': datetime.now().isoformat(),
            'total_keywords': 0,
            'completed': 0,
            'failed': 0,
            'skipped': 0,
            'in_progress': 0,
            'total_retries': 0,
            'search_params': {},
            'searches': [],
            'rate_limit': {
                'min_delay': 10.0,
                'adaptive_delay': 10.0,
                'last_request': None,
                'total_requests': 0,
                'total_wait_time': 0
            },
            'statistics': {
                'total_data_size': 0,
                'total_duration': 0,
                'avg_response_time': 0,
                'error_types': {}
            }
        }
    
    def _recover_from_checkpoint(self) -> Dict:
        """Attempt to recover from most recent checkpoint"""
        checkpoints = sorted(
            self.checkpoints_dir.glob("checkpoint_*.json"),
            key=lambda x: x.stat().st_mtime,
            reverse=True
        )
        
        for checkpoint in checkpoints:
            try:
                with open(checkpoint, 'r') as f:
                    state = json.load(f)
                    logger.warning(f"Recovered from checkpoint: {checkpoint.name}")
                    return state
            except:
                continue
        
        logger.error("No valid checkpoint found, starting fresh")
        return self._load_state()  # This will return default state
    
    def save_state(self):
        """Save current state with backup"""
        self.state['last_update'] = datetime.now().isoformat()
        
        # Create backup of existing state
        if self.state_file.exists():
            backup_file = self.state_file.with_suffix('.json.bak')
            shutil.copy2(self.state_file, backup_file)
        
        # Save state
        try:
            with open(self.state_file, 'w') as f:
                json.dump(self.state, f, indent=2)
        except Exception as e:
            logger.error(f"Failed to save state: {e}")
            # Restore from backup
            backup_file = self.state_file.with_suffix('.json.bak')
            if backup_file.exists():
                shutil.copy2(backup_file, self.state_file)
    
    def save_search_result(self, 
                          keyword: str, 
                          result: Dict,
                          status: str = "success",
                          error: Optional[str] = None,
                          duration: Optional[float] = None):
        """Save individual search result with metadata"""
        
        # Generate filename
        index = len([s for s in self.state['searches'] if s['status'] != 'pending'])
        safe_keyword = "".join(c if c.isalnum() or c in '- ' else '_' 
                              for c in keyword).strip()[:50]
        filename = f"{index:04d}_{safe_keyword}.json"
        filepath = self.results_dir / filename
        
        # Calculate response size
        response_size = len(json.dumps(result))
        
        # Save result to file
        with open(filepath, 'w') as f:
            json.dump({
                'keyword': keyword,
                'status': status,
                'timestamp': datetime.now().isoformat(),
                'duration': duration,
                'response_size': response_size,
                'result': result,
                'error': error
            }, f, indent=2)
        
        # Also save a permanent copy of raw response (never deleted)
        if status == "success" and result:
            permanent_file = self.permanent_raw_dir / f"{filename}_permanent.json"
            with open(permanent_file, 'w') as f:
                json.dump({
                    'keyword': keyword,
                    'timestamp': datetime.now().isoformat(),
                    'session_id': self.session_id,
                    'raw_response_file': result.get('_raw_response_file'),
                    'full_result': result
                }, f, indent=2)
            logger.debug(f"Permanent copy saved: {permanent_file.name}")
        
        # Check if this keyword already exists (for retries)
        existing_idx = None
        for idx, search in enumerate(self.state['searches']):
            if search['keyword'] == keyword:
                existing_idx = idx
                break
        
        # Create or update search record
        search_record = SearchRecord(
            index=index,
            keyword=keyword,
            status=status,
            timestamp=datetime.now().isoformat(),
            file=filename,
            error=error,
            retries=self.state['searches'][existing_idx]['retries'] + 1 if existing_idx else 0,
            response_size=response_size,
            duration=duration
        )
        
        if existing_idx is not None:
            # Update existing record
            self.state['searches'][existing_idx] = search_record.to_dict()
        else:
            # Add new record
            self.state['searches'].append(search_record.to_dict())
        
        # Update counters
        if status == 'success':
            self.state['completed'] += 1
            if existing_idx and self.state['searches'][existing_idx]['status'] == 'failed':
                self.state['failed'] -= 1
        elif status == 'failed':
            if existing_idx is None or self.state['searches'][existing_idx]['status'] != 'failed':
                self.state['failed'] += 1
        elif status == 'skipped':
            self.state['skipped'] += 1
        
        # Update statistics
        self.state['statistics']['total_data_size'] += response_size
        if duration:
            self.state['statistics']['total_duration'] += duration
            
        # Track error types
        if error and status == 'failed':
            error_type = error.split(':')[0] if ':' in error else error[:50]
            self.state['statistics']['error_types'][error_type] = \
                self.state['statistics']['error_types'].get(error_type, 0) + 1
        
        # Save state
        self.save_state()
        
        # Log result
        logger.debug(f"Saved {status}: {keyword} -> {filename}")
    
    def get_pending_keywords(self, keywords_list: List[str]) -> List[str]:
        """Get keywords that haven't been processed yet"""
        completed = self.get_completed_keywords()
        failed = self.get_failed_keywords()
        in_progress = self.get_in_progress_keywords()
        
        # All processed keywords
        processed = set(completed + failed + in_progress)
        
        # Return unprocessed keywords
        return [k for k in keywords_list if k not in processed]
    
    def get_completed_keywords(self) -> List[str]:
        """Get successfully completed keywords"""
        return [s['keyword'] for s in self.state['searches']
                if s['status'] == 'success']
    
    def get_failed_keywords(self, max_retries: int = 3) -> List[str]:
        """Get failed keywords that haven't exceeded retry limit"""
        return [s['keyword'] for s in self.state['searches']
                if s['status'] == 'failed' and s.get('retries', 0) < max_retries]
    
    def get_in_progress_keywords(self) -> List[str]:
        """Get keywords that were in progress (for recovery)"""
        return [s['keyword'] for s in self.state['searches']
                if s['status'] == 'in_progress']
    
    def mark_in_progress(self, keyword: str):
        """Mark a keyword as in progress"""
        for search in self.state['searches']:
            if search['keyword'] == keyword:
                search['status'] = 'in_progress'
                break
        else:
            self.state['searches'].append({
                'keyword': keyword,
                'status': 'in_progress',
                'timestamp': datetime.now().isoformat()
            })
        
        self.state['in_progress'] += 1
        self.save_state()
    
    def load_previous_results(self) -> List[Dict]:
        """Load all successful results from disk"""
        results = []
        
        for search in self.state['searches']:
            if search['status'] == 'success' and search.get('file'):
                filepath = self.results_dir / search['file']
                if filepath.exists():
                    try:
                        with open(filepath, 'r') as f:
                            data = json.load(f)
                            results.append({
                                'keywords': search['keyword'],
                                'timestamp': search['timestamp'],
                                'raw_data': data['result'],
                                'metrics': {}  # Will be recalculated
                            })
                    except Exception as e:
                        logger.error(f"Failed to load {filepath}: {e}")
        
        return results
    
    def save_checkpoint(self):
        """Save checkpoint with compression of old checkpoints"""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        checkpoint_file = self.checkpoints_dir / f"checkpoint_{timestamp}.json"
        
        # Save checkpoint
        with open(checkpoint_file, 'w') as f:
            json.dump(self.state, f, indent=2)
        
        logger.debug(f"Checkpoint saved: {checkpoint_file.name}")
        
        # Compress old checkpoints (keep last 5)
        checkpoints = sorted(
            self.checkpoints_dir.glob("checkpoint_*.json"),
            key=lambda x: x.stat().st_mtime,
            reverse=True
        )
        
        if len(checkpoints) > 5:
            for old_checkpoint in checkpoints[5:]:
                old_checkpoint.unlink()
    
    def export_current_results(self, format: str = 'json') -> Path:
        """Export current results to file"""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        if format == 'json':
            export_file = self.exports_dir / f"export_{timestamp}.json"
            results = self.load_previous_results()
            
            with open(export_file, 'w') as f:
                json.dump({
                    'session_id': self.session_id,
                    'export_time': datetime.now().isoformat(),
                    'completed': self.state['completed'],
                    'failed': self.state['failed'],
                    'results': results
                }, f, indent=2)
        
        elif format == 'csv':
            import pandas as pd
            export_file = self.exports_dir / f"export_{timestamp}.csv"
            
            # Create DataFrame from results
            data = []
            for search in self.state['searches']:
                if search['status'] == 'success':
                    data.append({
                        'keyword': search['keyword'],
                        'timestamp': search['timestamp'],
                        'response_size': search.get('response_size', 0),
                        'duration': search.get('duration', 0)
                    })
            
            df = pd.DataFrame(data)
            df.to_csv(export_file, index=False)
        
        logger.info(f"Exported results to: {export_file}")
        return export_file
    
    def get_session_summary(self) -> Dict:
        """Get comprehensive session summary"""
        total_duration = 0
        if self.state['start_time']:
            start = datetime.fromisoformat(self.state['start_time'])
            total_duration = (datetime.now() - start).total_seconds()
        
        return {
            'session_id': self.session_id,
            'status': self._get_session_status(),
            'progress': {
                'total': self.state['total_keywords'],
                'completed': self.state['completed'],
                'failed': self.state['failed'],
                'skipped': self.state['skipped'],
                'remaining': self.state['total_keywords'] - 
                           (self.state['completed'] + self.state['failed'] + self.state['skipped'])
            },
            'performance': {
                'total_duration_seconds': total_duration,
                'total_duration_human': self._format_duration(total_duration),
                'avg_search_time': self.state['statistics']['total_duration'] / 
                                  max(1, self.state['completed']),
                'total_data_mb': self.state['statistics']['total_data_size'] / (1024 * 1024),
                'requests_per_minute': (self.state['completed'] + self.state['failed']) / 
                                      max(1, total_duration / 60)
            },
            'errors': self.state['statistics']['error_types'],
            'rate_limiting': {
                'total_wait_time': self.state['rate_limit']['total_wait_time'],
                'current_delay': self.state['rate_limit']['adaptive_delay']
            }
        }
    
    def _get_session_status(self) -> str:
        """Determine overall session status"""
        if self.state['completed'] == self.state['total_keywords']:
            return "completed"
        elif self.state['failed'] > 0 and self.state['completed'] == 0:
            return "failed"
        elif self.state['completed'] > 0:
            return "partial"
        else:
            return "pending"
    
    def _format_duration(self, seconds: float) -> str:
        """Format duration in human readable format"""
        if seconds < 60:
            return f"{seconds:.1f}s"
        elif seconds < 3600:
            return f"{seconds/60:.1f}m"
        else:
            hours = seconds // 3600
            minutes = (seconds % 3600) // 60
            return f"{hours}h {minutes}m"
    
    def _emergency_save(self, signum, frame):
        """Emergency save on interrupt"""
        logger.warning(f"\n\nCaught signal {signum}, saving state...")
        
        # Mark any in-progress as failed
        for search in self.state['searches']:
            if search['status'] == 'in_progress':
                search['status'] = 'failed'
                search['error'] = 'Interrupted by user'
        
        # Save everything
        self.save_state()
        self.save_checkpoint()
        
        # Export current results
        export_file = self.export_current_results('json')
        
        # Print summary
        summary = self.get_session_summary()
        logger.info("\n" + "="*60)
        logger.info("SESSION INTERRUPTED")
        logger.info("="*60)
        logger.info(f"Session ID: {self.session_id}")
        logger.info(f"Progress: {summary['progress']['completed']}/{summary['progress']['total']}")
        logger.info(f"Failed: {summary['progress']['failed']}")
        logger.info(f"Duration: {summary['performance']['total_duration_human']}")
        logger.info(f"Exported to: {export_file}")
        logger.info("-"*60)
        logger.info(f"To resume: python ebay_batch_search.py --resume {self.session_id}")
        logger.info("="*60)
        
        # Release lock
        if self.lock:
            self.lock.release()
        
        # Exit
        import sys
        sys.exit(0)
    
    def cleanup(self, archive: bool = True):
        """Clean up or archive session (permanent_raw_json is NEVER deleted)"""
        if archive:
            # Move to completed directory
            completed_dir = self.temp_dir / "completed"
            completed_dir.mkdir(exist_ok=True)
            
            archive_path = completed_dir / f"session_{self.session_id}"
            if archive_path.exists():
                shutil.rmtree(archive_path)
            
            shutil.move(str(self.session_path), str(archive_path))
            logger.info(f"Session archived to: {archive_path}")
            logger.info(f"Permanent raw JSON preserved in: permanent_raw_json/{self.session_id}")
        else:
            # Delete session (but NEVER delete permanent_raw_json)
            shutil.rmtree(self.session_path)
            logger.info(f"Session deleted: {self.session_path}")
            logger.info(f"Permanent raw JSON preserved in: permanent_raw_json/{self.session_id}")
        
        # Release lock
        if self.lock:
            self.lock.release()
    
    def __del__(self):
        """Cleanup on deletion"""
        if hasattr(self, 'lock') and self.lock:
            self.lock.release()


def list_sessions(temp_dir: str = ".ebay_temp") -> List[Dict]:
    """List all available sessions"""
    temp_path = Path(temp_dir)
    sessions = []
    
    for session_dir in sorted(temp_path.glob("session_*")):
        state_file = session_dir / "state.json"
        if state_file.exists():
            try:
                with open(state_file, 'r') as f:
                    state = json.load(f)
                    
                    sessions.append({
                        'session_id': state['session_id'],
                        'start_time': state['start_time'],
                        'last_update': state['last_update'],
                        'completed': state['completed'],
                        'failed': state['failed'],
                        'total': state['total_keywords'],
                        'status': 'active' if session_dir / ".lock" in session_dir.iterdir() else 'idle'
                    })
            except:
                pass
    
    return sessions


def cleanup_old_sessions(temp_dir: str = ".ebay_temp", days: int = 7):
    """Clean up sessions older than specified days"""
    temp_path = Path(temp_dir)
    cutoff_time = time.time() - (days * 24 * 3600)
    cleaned = 0
    
    for session_dir in temp_path.glob("session_*"):
        if session_dir.stat().st_mtime < cutoff_time:
            # Check if locked
            if not (session_dir / ".lock").exists():
                shutil.rmtree(session_dir)
                cleaned += 1
                logger.info(f"Cleaned up old session: {session_dir.name}")
    
    return cleaned


if __name__ == "__main__":
    # Test resume manager
    import sys
    
    if len(sys.argv) > 1 and sys.argv[1] == "list":
        sessions = list_sessions()
        print("\nAvailable Sessions:")
        print("-" * 80)
        for session in sessions:
            print(f"ID: {session['session_id']}")
            print(f"  Started: {session['start_time']}")
            print(f"  Progress: {session['completed']}/{session['total']}")
            print(f"  Failed: {session['failed']}")
            print(f"  Status: {session['status']}")
            print()
    
    elif len(sys.argv) > 1 and sys.argv[1] == "cleanup":
        days = int(sys.argv[2]) if len(sys.argv) > 2 else 7
        cleaned = cleanup_old_sessions(days=days)
        print(f"Cleaned up {cleaned} old sessions")
    
    else:
        print("Usage:")
        print("  python ebay_resume_manager.py list     - List all sessions")
        print("  python ebay_resume_manager.py cleanup [days] - Clean old sessions")