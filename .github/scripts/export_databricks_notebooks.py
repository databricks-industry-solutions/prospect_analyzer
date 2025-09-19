#!/usr/bin/env python3
"""
Export notebooks from Databricks workspace for version control.
This script helps maintain notebook synchronization between workspace and Git.
"""

import os
import subprocess
import sys
from pathlib import Path

def run_command(cmd, cwd=None):
    """Run a shell command and return the result."""
    try:
        result = subprocess.run(
            cmd, 
            shell=True, 
            capture_output=True, 
            text=True, 
            cwd=cwd
        )
        if result.returncode != 0:
            print(f"❌ Command failed: {cmd}")
            print(f"Error: {result.stderr}")
            return False
        return True
    except Exception as e:
        print(f"❌ Exception running command {cmd}: {e}")
        return False

def export_notebooks():
    """Export notebooks from Databricks workspace."""
    
    # Check if Databricks CLI is configured
    if not run_command("databricks workspace list --output json"):
        print("❌ Databricks CLI not configured or not working")
        print("💡 Configure with: databricks configure --token")
        return False
    
    # Create notebooks directory if it doesn't exist
    notebooks_dir = Path("notebooks")
    notebooks_dir.mkdir(exist_ok=True)
    
    # Export notebooks from workspace - try common paths
    workspace_paths = [
        "/Workspace/Repos/prospect_analyzer/notebooks",
        "/Repos/prospect_analyzer/notebooks", 
        "/Users/shared/prospect_analyzer/notebooks",
        "/Workspace/prospect_analyzer/notebooks"
    ]
    
    for workspace_path in workspace_paths:
        print(f"🔍 Checking workspace path: {workspace_path}")
        
        cmd = f"databricks workspace export-dir '{workspace_path}' ./notebooks --fmt SOURCE"
        if run_command(cmd):
            print(f"✅ Successfully exported from: {workspace_path}")
            return True
        else:
            print(f"⚠️ No notebooks found at: {workspace_path}")
    
    print("❌ No notebooks found in any expected workspace location")
    print("💡 Try manually exporting notebooks or check workspace paths")
    return False

def validate_exported_notebooks():
    """Validate that exported notebooks are properly formatted."""
    notebooks_dir = Path("notebooks")
    
    if not notebooks_dir.exists():
        print("❌ Notebooks directory doesn't exist")
        return False
    
    notebook_files = list(notebooks_dir.glob("*.py")) + list(notebooks_dir.glob("*.sql"))
    
    if not notebook_files:
        print("❌ No notebook files found after export")
        return False
    
    valid_count = 0
    for notebook_file in notebook_files:
        try:
            with open(notebook_file, 'r') as f:
                content = f.read()
            
            # Check for Databricks notebook header
            if content.startswith('# Databricks notebook source'):
                print(f"✅ Valid notebook: {notebook_file.name}")
                valid_count += 1
            else:
                print(f"⚠️ Missing header: {notebook_file.name}")
        except Exception as e:
            print(f"❌ Error reading {notebook_file.name}: {e}")
    
    print(f"📊 Validation: {valid_count}/{len(notebook_files)} notebooks valid")
    return valid_count > 0

def main():
    """Main export function."""
    print("🚀 Starting Databricks notebook export...")
    
    if export_notebooks():
        print("✅ Notebook export completed")
        
        if validate_exported_notebooks():
            print("✅ Notebook validation passed")
            return 0
        else:
            print("⚠️ Notebook validation had issues")
            return 1
    else:
        print("❌ Notebook export failed")
        print("\n💡 Manual steps:")
        print("1. Check your Databricks CLI configuration")
        print("2. Verify notebook paths in your workspace")
        print("3. Ensure you have read access to the notebooks")
        return 1

if __name__ == "__main__":
    sys.exit(main())