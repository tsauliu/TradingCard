#!/usr/bin/env python3
"""
Mihomo Subscription Manager
Fetches and manages proxy subscriptions for automatic switching
"""

import requests
import yaml
import json
import os
import time
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
import logging


class SubscriptionManager:
    def __init__(self, 
                 subscription_url: str,
                 config_dir: str = "/etc/mihomo",
                 config_file: str = "config.yaml",
                 backup_dir: str = "backups"):
        """
        Initialize subscription manager
        
        Args:
            subscription_url: URL to fetch Clash subscription
            config_dir: Directory to store mihomo config
            config_file: Config filename
            backup_dir: Directory for config backups
        """
        self.subscription_url = subscription_url
        self.config_dir = config_dir
        self.config_file = config_file
        self.backup_dir = backup_dir
        self.config_path = os.path.join(config_dir, config_file)
        self.backup_path = os.path.join(config_dir, backup_dir)
        
        # Setup logging
        self.logger = logging.getLogger('subscription_manager')
        self.logger.setLevel(logging.INFO)
        
        # Console handler
        if not self.logger.handlers:
            console_handler = logging.StreamHandler()
            console_handler.setLevel(logging.INFO)
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            console_handler.setFormatter(formatter)
            self.logger.addHandler(console_handler)
        
        # Create backup directory
        os.makedirs(self.backup_path, exist_ok=True)
    
    def fetch_subscription(self) -> Dict[str, Any]:
        """
        Fetch subscription configuration from URL
        
        Returns:
            Dict containing parsed YAML configuration
        """
        self.logger.info(f"Fetching subscription from: {self.subscription_url}")
        
        try:
            response = requests.get(self.subscription_url, timeout=30)
            response.raise_for_status()
            
            # Parse YAML content
            config = yaml.safe_load(response.text)
            self.logger.info(f"Successfully fetched subscription with {len(config.get('proxies', []))} proxies")
            
            return config
            
        except requests.RequestException as e:
            self.logger.error(f"Failed to fetch subscription: {e}")
            raise
        except yaml.YAMLError as e:
            self.logger.error(f"Failed to parse YAML: {e}")
            raise
    
    def enhance_config(self, base_config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Enhance the base configuration with proxy groups for automatic switching
        
        Args:
            base_config: Base configuration from subscription
            
        Returns:
            Enhanced configuration with proxy groups
        """
        self.logger.info("Enhancing configuration with proxy groups...")
        
        # Extract proxy names
        proxy_names = [proxy['name'] for proxy in base_config.get('proxies', [])]
        
        if not proxy_names:
            self.logger.warning("No proxies found in subscription")
            return base_config
        
        # Create enhanced proxy groups with mihomo-supported types
        proxy_groups = [
            {
                'name': 'auto-switch',
                'type': 'url-test',  # Changed from 'urltest' to 'url-test'
                'proxies': proxy_names.copy(),
                'url': 'https://tcgcsv.com',
                'interval': 300,
                'tolerance': 100
            },
            {
                'name': 'fallback-group',
                'type': 'fallback',
                'proxies': ['auto-switch', 'DIRECT'],
                'url': 'https://tcgcsv.com',
                'interval': 300
            },
            {
                'name': 'load-balance',
                'type': 'load-balance',
                'proxies': proxy_names.copy(),
                'url': 'https://tcgcsv.com',
                'interval': 300,
                'strategy': 'consistent-hashing'
            },
            {
                'name': 'manual-select',
                'type': 'select',
                'proxies': ['auto-switch', 'fallback-group', 'load-balance', 'DIRECT'] + proxy_names
            }
        ]
        
        # Update configuration
        enhanced_config = base_config.copy()
        enhanced_config['proxy-groups'] = proxy_groups
        
        # Add rules for proxy usage
        rules = [
            'DOMAIN-SUFFIX,tcgcsv.com,manual-select',
            'DOMAIN-SUFFIX,api.tcgcsv.com,manual-select',
            'DOMAIN-KEYWORD,tcg,manual-select',
            'MATCH,DIRECT'
        ]
        
        enhanced_config['rules'] = rules
        
        # Configure external controller for API access
        enhanced_config.update({
            'external-controller': '127.0.0.1:9090',
            'secret': enhanced_config.get('secret', 'your-secret-key'),
            'allow-lan': False,
            'bind-address': '*',
            'mode': 'rule',
            'log-level': 'info'
        })
        
        self.logger.info(f"Enhanced config with {len(proxy_groups)} proxy groups")
        return enhanced_config
    
    def backup_current_config(self) -> bool:
        """
        Backup current configuration file
        
        Returns:
            True if backup successful, False otherwise
        """
        if not os.path.exists(self.config_path):
            self.logger.info("No existing config to backup")
            return True
        
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            backup_filename = f"config_backup_{timestamp}.yaml"
            backup_full_path = os.path.join(self.backup_path, backup_filename)
            
            with open(self.config_path, 'r') as src, open(backup_full_path, 'w') as dst:
                dst.write(src.read())
            
            self.logger.info(f"Config backed up to: {backup_full_path}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to backup config: {e}")
            return False
    
    def save_config(self, config: Dict[str, Any]) -> bool:
        """
        Save configuration to file
        
        Args:
            config: Configuration dictionary to save
            
        Returns:
            True if save successful, False otherwise
        """
        try:
            # Ensure config directory exists
            os.makedirs(self.config_dir, exist_ok=True)
            
            # Write configuration
            with open(self.config_path, 'w') as f:
                yaml.dump(config, f, default_flow_style=False, allow_unicode=True, indent=2)
            
            self.logger.info(f"Configuration saved to: {self.config_path}")
            
            # Set appropriate permissions
            try:
                import stat
                os.chmod(self.config_path, stat.S_IRUSR | stat.S_IWUSR | stat.S_IRGRP)
            except:
                pass  # Ignore permission errors
            
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to save config: {e}")
            return False
    
    def validate_proxies(self, config: Dict[str, Any]) -> List[str]:
        """
        Validate proxy configurations
        
        Args:
            config: Configuration to validate
            
        Returns:
            List of validation errors (empty if all valid)
        """
        errors = []
        proxies = config.get('proxies', [])
        
        if not proxies:
            errors.append("No proxies found in configuration")
            return errors
        
        required_fields = {'name', 'type', 'server', 'port'}
        
        for i, proxy in enumerate(proxies):
            missing_fields = required_fields - set(proxy.keys())
            if missing_fields:
                errors.append(f"Proxy {i}: Missing fields: {missing_fields}")
            
            # Type-specific validation
            proxy_type = proxy.get('type', '').lower()
            if proxy_type == 'ssr':
                if not all(k in proxy for k in ['cipher', 'password', 'protocol']):
                    errors.append(f"Proxy {i}: SSR proxy missing required fields")
            elif proxy_type == 'ss':
                if not all(k in proxy for k in ['cipher', 'password']):
                    errors.append(f"Proxy {i}: Shadowsocks proxy missing required fields")
        
        if errors:
            self.logger.warning(f"Found {len(errors)} validation errors")
        else:
            self.logger.info(f"All {len(proxies)} proxies validated successfully")
        
        return errors
    
    def update_subscription(self) -> bool:
        """
        Complete subscription update process
        
        Returns:
            True if update successful, False otherwise
        """
        try:
            self.logger.info("=== Starting subscription update ===")
            
            # Backup current config
            if not self.backup_current_config():
                self.logger.warning("Config backup failed, continuing anyway...")
            
            # Fetch new subscription
            base_config = self.fetch_subscription()
            
            # Validate proxies
            validation_errors = self.validate_proxies(base_config)
            if validation_errors:
                self.logger.error("Proxy validation failed:")
                for error in validation_errors:
                    self.logger.error(f"  - {error}")
                return False
            
            # Enhance configuration
            enhanced_config = self.enhance_config(base_config)
            
            # Save new configuration
            if not self.save_config(enhanced_config):
                return False
            
            # Log summary
            proxy_count = len(enhanced_config.get('proxies', []))
            group_count = len(enhanced_config.get('proxy-groups', []))
            
            self.logger.info("=== Subscription update completed ===")
            self.logger.info(f"✅ Proxies: {proxy_count}")
            self.logger.info(f"✅ Proxy groups: {group_count}")
            self.logger.info(f"✅ Config saved: {self.config_path}")
            
            return True
            
        except Exception as e:
            self.logger.error(f"Subscription update failed: {e}")
            return False
    
    def get_proxy_stats(self) -> Dict[str, Any]:
        """
        Get statistics about current proxy configuration
        
        Returns:
            Dictionary with proxy statistics
        """
        if not os.path.exists(self.config_path):
            return {'error': 'Config file not found'}
        
        try:
            with open(self.config_path, 'r') as f:
                config = yaml.safe_load(f)
            
            proxies = config.get('proxies', [])
            proxy_groups = config.get('proxy-groups', [])
            
            # Count by type
            proxy_types = {}
            for proxy in proxies:
                proxy_type = proxy.get('type', 'unknown')
                proxy_types[proxy_type] = proxy_types.get(proxy_type, 0) + 1
            
            stats = {
                'total_proxies': len(proxies),
                'proxy_groups': len(proxy_groups),
                'proxy_types': proxy_types,
                'config_path': self.config_path,
                'last_modified': datetime.fromtimestamp(os.path.getmtime(self.config_path)).isoformat()
            }
            
            return stats
            
        except Exception as e:
            return {'error': str(e)}


def main():
    """Main function for command line usage"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Mihomo Subscription Manager')
    parser.add_argument('--url', 
                      default='https://sub.ssr.sh/link/EFiFySavhktuEdHk?clash=2',
                      help='Subscription URL')
    parser.add_argument('--config-dir', 
                      default='/etc/mihomo',
                      help='Configuration directory')
    parser.add_argument('--stats', action='store_true',
                      help='Show proxy statistics')
    
    args = parser.parse_args()
    
    # Create subscription manager
    manager = SubscriptionManager(
        subscription_url=args.url,
        config_dir=args.config_dir
    )
    
    if args.stats:
        # Show statistics
        stats = manager.get_proxy_stats()
        print("\n=== Proxy Statistics ===")
        for key, value in stats.items():
            print(f"{key}: {value}")
    else:
        # Update subscription
        success = manager.update_subscription()
        exit(0 if success else 1)


if __name__ == "__main__":
    main()