#!/bin/bash

# Prospect Analyzer Cleanup Script
# This script removes your deployed prospect analyzer resources

set -e  # Exit on any error

echo "🧹 Prospect Analyzer Cleanup"
echo "============================"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Check if databricks CLI is installed
if ! command -v databricks &> /dev/null; then
    print_error "Databricks CLI is not installed or not in PATH"
    print_error "Please install it first: https://docs.databricks.com/dev-tools/cli/index.html"
    exit 1
fi

# Show what will be destroyed
print_status "Getting current deployment summary..."
if databricks bundle summary 2>/dev/null; then
    echo ""
    print_warning "⚠️  This will permanently delete all deployed resources!"
    print_warning "   • Prospect analysis pipeline will be deleted"
    print_warning "   • All data tables will be removed (prospect_raw_data, etc.)"
    print_warning "   • Dashboard will be deleted"
    print_warning "   • All notebooks will be removed from workspace"
    echo ""
    
    read -p "Are you sure you want to destroy the prospect analyzer? (y/N): " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        print_status "Destroying prospect analyzer..."
        if databricks bundle destroy; then
            print_success "Prospect analyzer destroyed successfully!"
        else
            print_error "Destruction failed"
            exit 1
        fi
    else
        print_status "Cleanup cancelled"
        exit 0
    fi
else
    print_warning "No deployed bundle found"
    print_status "Nothing to clean up"
fi

echo ""
print_success "Cleanup completed! 🎉" 
print_status "You can redeploy anytime with: databricks bundle deploy"