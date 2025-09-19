#!/usr/bin/env python3
"""
Convert Databricks notebooks to various formats for CI/CD pipeline.
This script ensures notebooks are properly formatted for Databricks deployment.
"""

import os
import json
import sys
import re
import glob
from pathlib import Path

def parse_databricks_notebook(filepath):
    """Parse a Databricks .py notebook format into cells"""
    with open(filepath, 'r') as f:
        content = f.read()
    
    # Split by COMMAND ----------
    sections = re.split(r'# COMMAND ----------', content)
    cells = []
    
    for section in sections:
        if not section.strip():
            continue
            
        # Check if this is a markdown cell
        if '# MAGIC %md' in section:
            # Extract markdown content
            lines = section.split('\n')
            md_lines = []
            for line in lines:
                if line.startswith('# MAGIC %md'):
                    md_lines.append(line[11:].strip())
                elif line.startswith('# MAGIC '):
                    md_lines.append(line[8:])
                elif line.startswith('# MAGIC'):
                    md_lines.append(line[7:])
            
            md_content = '\n'.join(md_lines)
            cells.append({'type': 'markdown', 'content': md_content})
        else:
            # This is a code cell
            lines = section.split('\n')
            code_lines = []
            for line in lines:
                if not line.startswith('# DBTITLE'):
                    code_lines.append(line)
            
            code_content = '\n'.join(code_lines).strip()
            if code_content:
                cells.append({'type': 'code', 'content': code_content})
    
    return cells

def validate_notebook_structure(filepath):
    """Validate that notebook follows Databricks conventions"""
    issues = []
    
    with open(filepath, 'r') as f:
        content = f.read()
    
    # Check for proper Databricks notebook header
    if not content.startswith('# Databricks notebook source'):
        issues.append("Missing Databricks notebook header")
    
    # Check for COMMAND separators
    if '# COMMAND ----------' not in content:
        issues.append("No command separators found")
    
    # Check for proper MAGIC comments in markdown cells
    md_sections = re.findall(r'# MAGIC %md.*?(?=# COMMAND ----------|$)', content, re.DOTALL)
    for section in md_sections:
        lines = section.split('\n')
        for line in lines[1:]:  # Skip first line with %md
            if line.strip() and not line.startswith('# MAGIC'):
                issues.append(f"Markdown cell has improper formatting: {line[:50]}...")
                break
    
    return issues

def convert_notebook(notebook_path):
    """Convert and validate a Databricks notebook."""
    try:
        # Validate structure
        issues = validate_notebook_structure(notebook_path)
        if issues:
            print(f"⚠️  Issues found in {notebook_path}:")
            for issue in issues:
                print(f"   - {issue}")
            return False
        
        # Parse cells for additional validation
        cells = parse_databricks_notebook(notebook_path)
        if not cells:
            print(f"⚠️  No cells found in {notebook_path}")
            return False
        
        print(f"✅ Validated: {notebook_path} ({len(cells)} cells)")
        return True
        
    except Exception as e:
        print(f"❌ Error processing {notebook_path}: {e}")
        return False

def main():
    """Convert and validate all notebooks in the repository."""
    notebook_dir = Path("notebooks")
    
    if not notebook_dir.exists():
        print("No notebooks directory found")
        return 0
    
    success_count = 0
    total_count = 0
    
    # Process .py notebooks (Databricks format)
    for notebook_path in notebook_dir.glob("*.py"):
        total_count += 1
        if convert_notebook(notebook_path):
            success_count += 1
    
    # Process .sql notebooks
    for notebook_path in notebook_dir.glob("*.sql"):
        total_count += 1
        try:
            with open(notebook_path, 'r') as f:
                content = f.read()
            
            # Basic SQL notebook validation
            if '-- Databricks notebook source' in content:
                print(f"✅ Validated: {notebook_path} (SQL notebook)")
                success_count += 1
            else:
                print(f"⚠️  Missing SQL notebook header: {notebook_path}")
        except Exception as e:
            print(f"❌ Error processing {notebook_path}: {e}")
    
    print(f"\n📊 Validation Summary: {success_count}/{total_count} notebooks validated successfully")
    
    return 0 if success_count == total_count else 1

if __name__ == "__main__":
    sys.exit(main())