#!/usr/bin/env python3
"""
Build script for Salesforce PubSub Spark Data Source wheel package.

This script builds a wheel (.whl) file that can be installed on Databricks clusters
during startup to avoid module serialization issues.

Usage:
    python build_wheel.py
    
Output:
    dist/spark_datasource-1.0.0-py3-none-any.whl
"""

import os
import sys
import subprocess
import shutil
from pathlib import Path

def main():
    """Build the wheel package."""
    
    print("🔧 Building Spark Data Source Wheel")
    print("=" * 60)
    
    # Get the project root directory
    project_root = Path(__file__).parent
    os.chdir(project_root)
    
    print(f"📁 Working directory: {project_root}")
    
    # Clean previous builds
    print("\n🧹 Cleaning previous builds...")
    dirs_to_clean = ["build", "dist", "*.egg-info"]
    for dir_pattern in dirs_to_clean:
        for path in project_root.glob(dir_pattern):
            if path.is_dir():
                shutil.rmtree(path)
                print(f"   ✅ Removed {path}")
    
    # Ensure required tools are installed
    print("\n📦 Checking build tools...")
    required_tools = ["wheel", "setuptools"]
    for tool in required_tools:
        try:
            subprocess.check_call([
                "python3", "-m", "pip", "install", "--upgrade", tool
            ], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            print(f"   ✅ {tool}")
        except subprocess.CalledProcessError:
            print(f"   ❌ Failed to install {tool}")
            return False
    
    # Build the wheel
    print("\n🏗️ Building wheel package...")
    
    # Temporarily rename pyproject.toml to avoid conflicts with setup.py
    pyproject_file = project_root / "pyproject.toml"
    pyproject_backup = project_root / "pyproject.toml.bak"
    renamed_pyproject = False
    
    try:
        if pyproject_file.exists():
            print("   📝 Temporarily renaming pyproject.toml to avoid conflicts...")
            pyproject_file.rename(pyproject_backup)
            renamed_pyproject = True
        
        result = subprocess.run([
            "python3", "setup.py", "bdist_wheel"
        ], capture_output=True, text=True, check=True)
        
        print("   ✅ Wheel built successfully!")
        if result.stdout:
            # Only show important output lines
            for line in result.stdout.split('\n'):
                if 'creating' in line.lower() and 'wheel' in line.lower():
                    print(f"   {line.strip()}")
            
    except subprocess.CalledProcessError as e:
        print(f"   ❌ Build failed: {e}")
        if e.stderr:
            print(f"   Error: {e.stderr}")
        return False
    finally:
        # Restore pyproject.toml if it was renamed
        if renamed_pyproject and pyproject_backup.exists():
            print("   📝 Restoring pyproject.toml...")
            pyproject_backup.rename(pyproject_file)
    
    # Check output
    print("\n📋 Build output:")
    dist_dir = project_root / "dist"
    if dist_dir.exists():
        wheel_files = list(dist_dir.glob("*.whl"))
        if wheel_files:
            for wheel_file in wheel_files:
                size_mb = wheel_file.stat().st_size / (1024 * 1024)
                print(f"   ✅ {wheel_file.name} ({size_mb:.2f} MB)")
        else:
            print("   ❌ No wheel files found!")
            return False
    else:
        print("   ❌ dist/ directory not found!")
        return False
    
    # Provide installation instructions
    print("\n🚀 Installation Instructions for Databricks:")
    print("=" * 50)
    print("""
1. Upload the wheel file to DBFS:
   - Go to Databricks workspace
   - Navigate to Data > Upload
   - Upload the .whl file to /dbfs/tmp/wheels/

2. Install during cluster startup:
   - Go to Cluster configuration
   - Advanced Options > Init Scripts
   - Add this script:
   
   #!/bin/bash
   pip install /dbfs/tmp/wheels/spark_datasource-1.0.0-py3-none-any.whl

3. Or install via notebook:
   %pip install /dbfs/tmp/wheels/spark_datasource-1.0.0-py3-none-any.whl
   dbutils.library.restartPython()

4. Use in your notebook:
   from spark_datasource import register_data_source
   register_data_source(spark)
   
   df = spark.readStream.format("salesforce_pubsub") \\
       .option("username", "your-username") \\
       .option("password", "your-password") \\
       .option("topic", "/data/AccountChangeEvent") \\
       .load()
""")
    
    print("\n✅ Wheel package build complete!")
    return True

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1) 