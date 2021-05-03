import os

from dotenv import load_dotenv

load_dotenv()

# Create a local .library directory to store temporary files
base_path = ".cache"

if not os.path.isdir(base_path):
    os.makedirs(base_path, exist_ok=True)
    # create .gitignore so that files in this directory aren't tracked
    with open(f"{base_path}/.gitignore", "w") as f:
        f.write("*")
