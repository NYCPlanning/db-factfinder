import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

API_KEY = os.environ["API_KEY"]
ROOT_PATH = Path(__file__).parent.parent
BASE_PATH = ROOT_PATH / ".output"

if not os.path.isdir(BASE_PATH):
    os.makedirs(BASE_PATH, exist_ok=True)
    # with open(BASE_PATH / ".gitignore", "w") as f:
    #     f.write("*")
    # os.makedirs(BASE_PATH/"datasets", exist_ok=True)
    # os.makedirs(BASE_PATH/"configurations", exist_ok=True)
